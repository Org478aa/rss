package watcher

import (
	"context"
	"fmt"
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
//
// failsLeft, when > 0, makes the next N PublishDelta calls return an error
// before the publish succeeds — used to exercise the watcher's retry loop.
// attempts counts every call (failed or not).
type fakePublisher struct {
	mu        sync.Mutex
	deltas    []model.RuleDelta
	failsLeft int
	attempts  int
}

func (f *fakePublisher) PublishDelta(_ context.Context, d model.RuleDelta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failsLeft > 0 {
		f.failsLeft--
		return fmt.Errorf("simulated broker unreachable")
	}
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

// shortenRetry drops the publish-retry backoff so a failing publisher's
// retries complete within a test's patience. Restored on cleanup.
func shortenRetry(t *testing.T) {
	t.Helper()
	pi, pm := publishRetryInitial, publishRetryMax
	publishRetryInitial = 5 * time.Millisecond
	publishRetryMax = 20 * time.Millisecond
	t.Cleanup(func() { publishRetryInitial, publishRetryMax = pi, pm })
}

// startWatcher starts a watcher over dir with the given publisher and
// registry, wiring cleanup. Shared by newWatcher and the tests that need a
// custom publisher.
func startWatcher(t *testing.T, dir string, reg *registry.Registry, pub Publisher) *Watcher {
	t.Helper()
	w := New(dir, reg, pub)
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := w.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stop()
	})
	return w
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
	startWatcher(t, dir, reg, fake)
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

// TestWatcher_PublishRetries_EventuallyLands covers fix 1's durability
// half: a publisher that fails a few times must not lose the delta — the
// watcher retries until the broker acks, and the registry version it ends
// up reporting is one that actually reached the wire.
func TestWatcher_PublishRetries_EventuallyLands(t *testing.T) {
	shortenDebounce(t)
	shortenRetry(t)
	dir := t.TempDir()
	reg := registry.New("test")
	fake := &fakePublisher{failsLeft: 3}
	startWatcher(t, dir, reg, fake)

	writeFile(t, filepath.Join(dir, "alpha.yaml"), "id: alpha\nwhen: \"true\"\n")
	got := fake.waitForDelta(t, 1, 2*time.Second)

	if got[0].RuleID != "alpha" || got[0].Operation != model.OperationUpsert {
		t.Errorf("delta = %+v; want upsert alpha", got[0])
	}
	fake.mu.Lock()
	attempts := fake.attempts
	fake.mu.Unlock()
	if attempts < 4 {
		t.Errorf("attempts = %d; want >= 4 (3 failures + 1 success)", attempts)
	}
	// The version RSS now advertises is the one that landed on the wire.
	if got[0].SnapshotVersion != reg.SnapshotVersion() {
		t.Errorf("published snapshot_version %d != registry %d; registry advanced past an unpublished delta",
			got[0].SnapshotVersion, reg.SnapshotVersion())
	}
}

// TestWatcher_ConcurrentSaves_PublishInVersionOrder is the regression test
// for fix 1's ordering half: two files saved in the same window must reach
// the wire in ascending snapshot_version order, because LTC drops any delta
// not strictly newer than the last it applied. Without the fireMu
// serialization the two fire() goroutines can publish out of order.
func TestWatcher_ConcurrentSaves_PublishInVersionOrder(t *testing.T) {
	dir, fake, _ := newWatcher(t)

	writeFile(t, filepath.Join(dir, "alpha.yaml"), "id: alpha\nwhen: \"true\"\n")
	writeFile(t, filepath.Join(dir, "bravo.yaml"), "id: bravo\nwhen: \"true\"\n")

	got := fake.waitForDelta(t, 2, 2*time.Second)
	for i := 1; i < len(got); i++ {
		if got[i].SnapshotVersion <= got[i-1].SnapshotVersion {
			t.Errorf("delta %d snapshot_version %d not strictly greater than prior %d; published out of order",
				i, got[i].SnapshotVersion, got[i-1].SnapshotVersion)
		}
	}
}

// TestWatcher_DuplicateID_Rejected covers fix 2: a second file claiming an
// id another file already owns is ignored, and removing that duplicate
// leaves the original rule intact. Only deleting the owning file evicts it.
func TestWatcher_DuplicateID_Rejected(t *testing.T) {
	dir, fake, reg := newWatcher(t)

	// First file establishes ownership of id "dup".
	first := filepath.Join(dir, "first.yaml")
	writeFile(t, first, "id: dup\nwhen: \"first\"\n")
	fake.waitForDelta(t, 1, 2*time.Second)

	// Second file claims the same id with different content — must be
	// rejected, leaving the registry holding the first file's body.
	second := filepath.Join(dir, "second.yaml")
	writeFile(t, second, "id: dup\nwhen: \"second\"\n")
	time.Sleep(150 * time.Millisecond) // debounce + breathing room

	if got := fake.seen(); len(got) != 1 {
		t.Fatalf("captured %d deltas; want 1 (duplicate must be ignored)", len(got))
	}
	if e, ok := reg.Get("dup"); !ok || e.YAML != "id: dup\nwhen: \"first\"\n" {
		t.Errorf("registry body = %q; duplicate file must not overwrite the owner", e.YAML)
	}

	// Removing the duplicate file must NOT delete the rule.
	if err := os.Remove(second); err != nil {
		t.Fatalf("remove second: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := fake.seen(); len(got) != 1 {
		t.Errorf("captured %d deltas after removing duplicate; want 1 (no delete expected)", len(got))
	}
	if _, ok := reg.Get("dup"); !ok {
		t.Error("rule dup evicted by removing the duplicate file; the owner still defines it")
	}

	// Removing the owner DOES delete the rule.
	if err := os.Remove(first); err != nil {
		t.Fatalf("remove first: %v", err)
	}
	got := fake.waitForDelta(t, 2, 2*time.Second)
	last := got[len(got)-1]
	if last.Operation != model.OperationDelete || last.RuleID != "dup" {
		t.Errorf("delta = %+v; want delete dup", last)
	}
	if _, ok := reg.Get("dup"); ok {
		t.Error("rule dup still present after removing the owning file")
	}
}
