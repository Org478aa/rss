// Package watcher keeps RSS's registry in sync with on-disk rule YAML
// files. It watches one directory non-recursively and, on each filesystem
// event, decides whether to Upsert or Delete in the registry — then asks
// the publisher to emit the resulting RuleDelta on RULE_UPDATES.
//
// Event model:
//
//   - Editors save in many shapes: a direct write, a write-then-rename
//     ("atomic save"), or a delete-and-recreate. Rather than handle each
//     shape, the watcher debounces by path (~100 ms) and then re-checks
//     reality: if the file exists, it's an upsert; if it doesn't, it's a
//     delete. This is the same idiom most fsnotify consumers settle on.
//   - The watcher tracks path → rule_id pairs so a Remove event can be
//     resolved to the id it referred to (the file is gone, so we can no
//     longer read the id out of it). The map is populated at Seed time
//     from the initial load and maintained on every upsert.
//   - It also tracks the inverse id → owning-path so the startup
//     duplicate-id invariant (LoadDir rejects two files sharing an id)
//     holds at runtime too: a second file introducing an already-owned id
//     is skipped, and a delete only evicts a rule when the removed path is
//     the one that owns it.
//   - Non-YAML files (anything not matching *.yaml / *.yml) are ignored.
//
// Ordering + durability: a single fireMu serializes the whole
// mutate-registry → publish-delta section. The registry assigns a
// monotonic snapshot_version per mutation and LTC drops any delta that
// isn't strictly newer than the last it applied, so version order must
// equal wire order — two files saved within one debounce window would
// otherwise run concurrent fire() goroutines that publish out of order.
// Within that serialized section publish retries until the broker acks or
// the context is cancelled; RSS deliberately stops absorbing new edits
// during a broker outage rather than letting registry state run ahead of
// what it has shipped to LTC.
package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"rss/internal/loader"
	"rss/internal/model"
)

// Registry is the subset of *registry.Registry the watcher needs. Keeping
// it as an interface in this package lets tests substitute a fake without
// pulling in the real implementation (and lets the watcher stay
// dependency-light).
type Registry interface {
	Upsert(id, yaml string) (model.RuleDelta, bool)
	Delete(id string) (model.RuleDelta, bool)
	Get(id string) (model.RuleEntry, bool)
}

// Publisher is the subset of *publisher.Publisher the watcher needs.
type Publisher interface {
	PublishDelta(ctx context.Context, d model.RuleDelta) error
}

// DebounceWindow is how long the watcher waits after the last event for a
// path before acting. Tuned for editor atomic-save (vim's swap-then-
// rename + chmod fires up to three events within ~10 ms) without
// noticeably delaying interactive edits.
//
// Declared as var so tests can dial it down to single-digit ms. Set once
// before Start; no concurrent mutation.
var DebounceWindow = 100 * time.Millisecond

// publishRetryInitial / publishRetryMax bound the capped-exponential
// backoff the watcher uses when a delta publish fails (broker briefly
// unreachable). Declared as var so tests can shrink them. The retry runs
// while fireMu is held, so a sustained outage pauses new edits — see the
// package doc.
var (
	publishRetryInitial = 50 * time.Millisecond
	publishRetryMax     = 2 * time.Second
)

// Watcher couples an fsnotify.Watcher with a per-path debouncer and the
// registry/publisher pair that turns each fired debouncer into a wire
// event.
type Watcher struct {
	dir       string
	registry  Registry
	publisher Publisher

	// fireMu serializes the mutate→publish section of fire() so the
	// registry's version order equals the order deltas reach the wire.
	// Held across publish retries; never nested inside mu.
	fireMu sync.Mutex

	// pathToID maps watched file paths to the rule_id last observed there.
	// Required to translate a Remove event (where we can't read the file
	// anymore) into a Delete(id) call. idToPath is the inverse, naming the
	// path that currently owns each id, used to enforce the duplicate-id
	// invariant at runtime. Both mutated under mu.
	mu       sync.Mutex
	pathToID map[string]string
	idToPath map[string]string

	// pending tracks the per-path debouncer state. Mutated under mu.
	pending map[string]*pendingFile
}

type pendingFile struct {
	timer *time.Timer
}

// New constructs a watcher targeting `dir`. Doesn't start the inotify
// goroutine — Start does. Seed must be called before Start so the
// watcher knows which path holds which rule id at the moment we begin
// watching (otherwise a Remove event for a pre-existing file can't be
// resolved to a rule id).
func New(dir string, reg Registry, pub Publisher) *Watcher {
	return &Watcher{
		dir:       dir,
		registry:  reg,
		publisher: pub,
		pathToID:  make(map[string]string, 16),
		idToPath:  make(map[string]string, 16),
		pending:   make(map[string]*pendingFile, 16),
	}
}

// Seed populates the path → id map from the initial directory load. Call
// once after registry.ReplaceAll, before Start. Passing in the files
// rather than re-walking the disk keeps the watcher decoupled from the
// loader's behavior (e.g., file ordering).
func (w *Watcher) Seed(files []loader.File) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, f := range files {
		w.pathToID[f.Path] = f.ID
		// LoadDir already rejected duplicate ids, so this inverse map is
		// unambiguous at seed time: each id maps to exactly one path.
		w.idToPath[f.ID] = f.Path
	}
}

// Start begins watching dir. Returns a stop func that drains the inotify
// goroutine and any in-flight debouncers. Safe to call once.
//
// Filesystem events from the kernel are inherently noisy — atomic save
// produces 2-3 events per file, chmod toggles produce events without
// content changes. The debouncer + "re-check reality after the window"
// design absorbs this without callers needing to care.
func (w *Watcher) Start(ctx context.Context) (stop func(), err error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("fsnotify new: %w", err)
	}
	if err := fsw.Add(w.dir); err != nil {
		_ = fsw.Close()
		return nil, fmt.Errorf("fsnotify add %s: %w", w.dir, err)
	}
	slog.Info("watcher started", "dir", w.dir, "debounce", DebounceWindow)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-fsw.Events:
				if !ok {
					return
				}
				if !isRuleFile(ev.Name) {
					continue
				}
				w.schedule(ctx, ev.Name)
			case err, ok := <-fsw.Errors:
				if !ok {
					return
				}
				slog.Warn("watcher error", "err", err)
			}
		}
	}()

	return func() {
		_ = fsw.Close()
		<-done
		// Drain any timers still pending. We do this AFTER fsnotify is
		// closed so no new events can resurrect a timer mid-stop.
		w.mu.Lock()
		for path, p := range w.pending {
			if p.timer.Stop() {
				delete(w.pending, path)
			}
		}
		w.mu.Unlock()
	}, nil
}

// schedule starts or resets the debounce timer for the given path. When
// the timer fires we re-check reality and act.
func (w *Watcher) schedule(ctx context.Context, path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, ok := w.pending[path]; ok {
		existing.timer.Reset(DebounceWindow)
		return
	}
	pf := &pendingFile{}
	pf.timer = time.AfterFunc(DebounceWindow, func() {
		w.fire(ctx, path)
	})
	w.pending[path] = pf
}

// fire runs after the debounce window. It removes the path's entry from
// `pending`, then decides between Upsert and Delete based on whether the
// file currently exists.
//
// The whole body holds fireMu: registry mutation assigns the monotonic
// snapshot_version and the matching publish must reach the wire in that
// same order (LTC drops anything not strictly newer than it holds). One
// lock makes version order == wire order and keeps the registry from
// advancing past a delta we couldn't ship.
func (w *Watcher) fire(ctx context.Context, path string) {
	w.fireMu.Lock()
	defer w.fireMu.Unlock()

	w.mu.Lock()
	delete(w.pending, path)
	prevID := w.pathToID[path]
	w.mu.Unlock()

	// LoadFile both reads + extracts the id. A read error here usually
	// means "file removed", which we treat as a Delete using the id we
	// remembered.
	f, err := loader.LoadFile(path)
	if err != nil {
		if prevID == "" {
			// Nothing we can do — never saw this path before, and the
			// file is gone. Most often this is a transient temp file an
			// editor created and removed in the same beat.
			slog.Debug("watcher: ignoring transient", "path", path, "err", err)
			return
		}

		// Only evict the rule if THIS path is the one that owns the id, so a
		// duplicate-id file being removed can't delete the rule the surviving
		// file still defines. Defensive: because the upsert side rejects an
		// id another path owns (and never remembers the rejected file), no
		// two paths share an id today, so prevID being set implies this path
		// owns it. Kept so the invariant is enforced at the delete site too,
		// not assumed.
		w.mu.Lock()
		owner := w.idToPath[prevID]
		w.mu.Unlock()
		if owner != path {
			w.mu.Lock()
			delete(w.pathToID, path)
			w.mu.Unlock()
			slog.Info("watcher: duplicate-id file removed; rule retained",
				"rule_id", prevID, "path", path, "owner", owner)
			return
		}

		delta, changed := w.registry.Delete(prevID)
		if !changed {
			w.forget(path, prevID)
			return
		}
		if !w.publish(ctx, delta) {
			slog.Error("delete delta abandoned at shutdown; LTC keeps the stale rule until its next snapshot",
				"rule_id", prevID, "path", path)
			return
		}
		w.forget(path, prevID)
		slog.Info("rule deleted", "rule_id", prevID, "path", path)
		return
	}

	// File exists → upsert. Enforce the duplicate-id invariant first: a
	// second file introducing an id another path already owns is rejected,
	// the same way LoadDir rejects it at startup.
	w.mu.Lock()
	owner, owned := w.idToPath[f.ID]
	w.mu.Unlock()
	if owned && owner != path {
		slog.Warn("duplicate rule id; ignoring file (another file owns this id)",
			"rule_id", f.ID, "path", path, "owner", owner)
		return
	}

	// File exists. If the rule id moved (operator renamed a file's id
	// in-place), delete the previous id before upserting the new one.
	if prevID != "" && prevID != f.ID {
		if delta, changed := w.registry.Delete(prevID); changed {
			if !w.publish(ctx, delta) {
				slog.Error("rename-delete delta abandoned at shutdown", "old_id", prevID, "path", path)
				return
			}
			w.mu.Lock()
			delete(w.idToPath, prevID)
			w.mu.Unlock()
			slog.Info("rule deleted (renamed)", "old_id", prevID, "new_id", f.ID, "path", path)
		}
	}

	delta, changed := w.registry.Upsert(f.ID, f.YAML)
	if !changed {
		// No-op upsert (file touched but content identical). Suppress the
		// wire event, but still record ownership so a later delete resolves
		// correctly.
		w.remember(path, f.ID)
		return
	}
	if !w.publish(ctx, delta) {
		slog.Error("upsert delta abandoned at shutdown; registry advanced past the last published delta",
			"rule_id", f.ID, "path", path)
		return
	}
	w.remember(path, f.ID)
	slog.Info("rule upserted", "rule_id", f.ID, "rule_version", delta.RuleVersion, "path", path)
}

// publish ships one delta on RULE_UPDATES, retrying with capped
// exponential backoff until the broker acks or the context is cancelled.
// Returns true on success, false only when the context is done (shutdown).
// Called with fireMu held, so a stuck publish also blocks new edits — RSS
// stops advancing rule state it cannot ship.
func (w *Watcher) publish(ctx context.Context, d model.RuleDelta) bool {
	backoff := publishRetryInitial
	for attempt := 1; ; attempt++ {
		if err := w.publisher.PublishDelta(ctx, d); err == nil {
			return true
		} else if ctx.Err() != nil {
			return false
		} else {
			slog.Warn("publish delta failed; retrying",
				"attempt", attempt, "op", d.Operation, "rule_id", d.RuleID, "err", err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < publishRetryMax {
			if backoff *= 2; backoff > publishRetryMax {
				backoff = publishRetryMax
			}
		}
	}
}

// remember records that `path` owns `id` in both direction maps. Called
// after a successful (or no-op) upsert. Mutated under mu.
func (w *Watcher) remember(path, id string) {
	w.mu.Lock()
	w.pathToID[path] = id
	w.idToPath[id] = path
	w.mu.Unlock()
}

// forget drops a path and the id it owned from both maps. Called after a
// delete lands. Mutated under mu.
func (w *Watcher) forget(path, id string) {
	w.mu.Lock()
	delete(w.pathToID, path)
	delete(w.idToPath, id)
	w.mu.Unlock()
}

// isRuleFile reports whether the path looks like a rule file. We accept
// both .yaml and .yml to match the loader's globs.
func isRuleFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".yaml" || ext == ".yml"
}
