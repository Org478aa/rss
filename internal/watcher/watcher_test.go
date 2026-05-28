package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"rss/internal/loader"
	"rss/internal/model"
	"rss/internal/registry"
)

// fakePublisher captures the deltas the watcher hands it. Lets the
// tests assert on the wire-side outcome without standing up NATS.
type fakePublisher struct {
	mu     sync.Mutex
	deltas []model.RuleDelta
}

func (f *fakePublisher) PublishDelta(_ context.Context, d model.RuleDelta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deltas = append(f.deltas, d)
	return nil
}

func (f *fakePublisher) seen() []model.RuleDelta {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.RuleDelta, len(f.deltas))
	copy(out, f.deltas)
	return out
}

// waitForDelta polls until at least `n` deltas have been captured or
// timeout. Returns the slice for assertion or nil on timeout.
func (f *fakePublisher) waitForDelta(t *testing.T, n int, timeout time.Duration) []model.RuleDelta {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := f.seen(); len(got) >= n {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("only %d deltas captured; want %d", len(f.seen()), n)
	return nil
}

// shortenDebounce drops the watcher's debounce window to a few ms so
// tests finish promptly. Restored on cleanup.
func shortenDebounce(t *testing.T) {
	t.Helper()
	prev := DebounceWindow
	DebounceWindow = 20 * time.Millisecond
	t.Cleanup(func() { DebounceWindow = prev })
}

// newWatcher starts a watcher over a temp dir with a fake publisher.
// Returns the dir, the fake publisher, the registry (for state asserts),
// and a cleanup func that cancels the watcher and waits for shutdown.
func newWatcher(t *testing.T) (dir string, fake *fakePublisher, reg *registry.Registry) {
	t.Helper()
	shortenDebounce(t)
	dir = t.TempDir()
	reg = registry.New("test")
	fake = &fakePublisher{}
	w := New(dir, reg, fake)

	ctx, cancel := context.WithCancel(context.Background())
	stop, err := w.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stop()
	})
	return dir, fake, reg
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestWatcher_NewFile_PublishesUpsert covers the simplest happy path:
// drop a new YAML into the watched dir, expect one upsert delta with
// the parsed id.
func TestWatcher_NewFile_PublishesUpsert(t *testing.T) {
	dir, fake, reg := newWatcher(t)

	writeFile(t, filepath.Join(dir, "alpha.yaml"), "id: alpha\nwhen: \"true\"\n")
	got := fake.waitForDelta(t, 1, 2*time.Second)

	if got[0].Operation != model.OperationUpsert {
		t.Errorf("op = %q; want upsert", got[0].Operation)
	}
	if got[0].RuleID != "alpha" {
		t.Errorf("rule_id = %q; want alpha", got[0].RuleID)
	}
	if got[0].RuleVersion <= 0 || got[0].SnapshotVersion <= 0 {
		t.Errorf("versions = (snap=%d, rule=%d); want both > 0", got[0].SnapshotVersion, got[0].RuleVersion)
	}
	if entry, ok := reg.Get("alpha"); !ok || entry.YAML == "" {
		t.Errorf("registry missing alpha after watcher fire: %+v", entry)
	}
}

// TestWatcher_DeleteFile_PublishesDelete covers the deletion path:
// remove a file the watcher saw, expect one delete delta carrying the
// pre-deletion rule_version.
func TestWatcher_DeleteFile_PublishesDelete(t *testing.T) {
	dir, fake, reg := newWatcher(t)

	// Seed: write + observe upsert, then prime the watcher's path → id
	// map by calling Seed (the production path calls it from main after
	// the initial directory scan).
	path := filepath.Join(dir, "alpha.yaml")
	writeFile(t, path, "id: alpha\nwhen: \"true\"\n")
	fake.waitForDelta(t, 1, 2*time.Second)

	// Now delete.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	got := fake.waitForDelta(t, 2, 2*time.Second)
	last := got[len(got)-1]
	if last.Operation != model.OperationDelete {
		t.Errorf("op = %q; want delete", last.Operation)
	}
	if last.RuleID != "alpha" {
		t.Errorf("rule_id = %q; want alpha", last.RuleID)
	}
	if _, ok := reg.Get("alpha"); ok {
		t.Error("registry still has alpha after delete delta")
	}
}

// TestWatcher_NoOpUpsert_Suppressed asserts the silent-on-identical-
// content contract — a touch that doesn't change bytes must NOT fire
// a delta. Critical for fsnotify chmod/touch noise.
func TestWatcher_NoOpUpsert_Suppressed(t *testing.T) {
	dir, fake, _ := newWatcher(t)

	path := filepath.Join(dir, "alpha.yaml")
	body := "id: alpha\nwhen: \"true\"\n"
	writeFile(t, path, body)
	fake.waitForDelta(t, 1, 2*time.Second)

	// Re-save same content.
	writeFile(t, path, body)
	time.Sleep(150 * time.Millisecond) // debounce + breathing room

	if got := fake.seen(); len(got) != 1 {
		t.Errorf("captured %d deltas after no-op save; want 1 (first save only)", len(got))
	}
}

// TestWatcher_Seed_AllowsLaterDeleteToResolveID covers the bookkeeping
// the watcher does at startup: a file already on disk when the
// watcher starts must be resolvable on a later delete, even though
// the watcher never saw a Create event for it. Seed() populates the
// path → id map from the initial loader pass.
func TestWatcher_Seed_AllowsLaterDeleteToResolveID(t *testing.T) {
	shortenDebounce(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "alpha.yaml")
	writeFile(t, path, "id: alpha\nwhen: \"true\"\n")

	// Initial load (mimics what main does before Start).
	files, err := loader.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	reg := registry.New("test")
	seed := make(map[string]string, len(files))
	for _, f := range files {
		seed[f.ID] = f.YAML
	}
	if _, err := reg.ReplaceAll(seed); err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}

	fake := &fakePublisher{}
	w := New(dir, reg, fake)
	w.Seed(files)
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := w.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stop()
	})

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	got := fake.waitForDelta(t, 1, 2*time.Second)
	if got[0].Operation != model.OperationDelete || got[0].RuleID != "alpha" {
		t.Errorf("delta = %+v; want delete alpha", got[0])
	}
}
