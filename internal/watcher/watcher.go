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
//   - Non-YAML files (anything not matching *.yaml / *.yml) are ignored.
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

// Watcher couples an fsnotify.Watcher with a per-path debouncer and the
// registry/publisher pair that turns each fired debouncer into a wire
// event.
type Watcher struct {
	dir       string
	registry  Registry
	publisher Publisher

	// pathToID maps watched file paths to the rule_id last observed there.
	// Required to translate a Remove event (where we can't read the file
	// anymore) into a Delete(id) call. Mutated under mu.
	mu       sync.Mutex
	pathToID map[string]string

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
func (w *Watcher) fire(ctx context.Context, path string) {
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
		delta, changed := w.registry.Delete(prevID)
		if !changed {
			return
		}
		w.mu.Lock()
		delete(w.pathToID, path)
		w.mu.Unlock()
		if err := w.publisher.PublishDelta(ctx, delta); err != nil {
			slog.Warn("publish delete failed", "rule_id", prevID, "err", err)
			return
		}
		slog.Info("rule deleted", "rule_id", prevID, "path", path)
		return
	}

	// File exists. If the rule id moved (operator renamed a file's id
	// in-place), we need to delete the previous id before upserting.
	if prevID != "" && prevID != f.ID {
		if delta, changed := w.registry.Delete(prevID); changed {
			if err := w.publisher.PublishDelta(ctx, delta); err != nil {
				slog.Warn("publish rename-delete failed", "old_id", prevID, "err", err)
			} else {
				slog.Info("rule deleted (renamed)", "old_id", prevID, "new_id", f.ID, "path", path)
			}
		}
	}

	delta, changed := w.registry.Upsert(f.ID, f.YAML)
	if !changed {
		// No-op upsert (file touched but content identical). Suppress
		// the wire event so editors that touch + save don't spam.
		return
	}
	w.mu.Lock()
	w.pathToID[path] = f.ID
	w.mu.Unlock()
	if err := w.publisher.PublishDelta(ctx, delta); err != nil {
		slog.Warn("publish upsert failed", "rule_id", f.ID, "err", err)
		return
	}
	slog.Info("rule upserted", "rule_id", f.ID, "rule_version", delta.RuleVersion, "path", path)
}

// isRuleFile reports whether the path looks like a rule file. We accept
// both .yaml and .yml to match the loader's globs.
func isRuleFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".yaml" || ext == ".yml"
}
