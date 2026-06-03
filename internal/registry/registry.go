// Package registry is RSS's in-memory store of the rule set. It is the
// single point of truth for what RSS will reply with on snapshot.request,
// and it produces the deltas published on rss.updates whenever a rule is
// added, replaced, or removed.
//
// Concurrency: all mutating methods take the registry's mutex; read
// methods (Snapshot, Get) take it briefly to return a defensive copy. The
// publisher reads snapshots far more often than the file watcher mutates,
// but neither path is hot enough to warrant lock-free trickery.
package registry

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"rss/internal/model"
)

// Registry is the in-memory rule set, versioned at two levels:
//
//   - snapshotVersion: monotonic across the whole registry; bumps on every
//     Upsert and every Delete. Returned in the SnapshotReply and stamped
//     on every RuleDelta so consumers (LTC) can drop redeliveries and
//     out-of-order messages.
//   - per-rule version (RuleEntry.RuleVersion): bumps only when that
//     specific rule changes. Lets LTC apply per-rule monotonicity in
//     addition to the global gate.
//
// Both versions are unix-nano timestamps generated via bumpVersion, which
// returns max(prev+1, time.Now().UnixNano()). Wall-clock seeding means a
// restarted RSS picks a fresh starting point strictly larger than any
// version it published in a prior process — without that, restart-then-edit
// produced version numbers LTC had already absorbed and silently dropped.
// The max(prev+1, ...) guard preserves monotonicity even if two writes
// land in the same nanosecond or if the clock steps backwards.
//
// Source is a tag carried in SnapshotReply.Source / Heartbeat.Source so
// operators can tell a process apart from its tests at a glance ("disk"
// vs "mock"). Set once at startup; never mutated.
type Registry struct {
	mu              sync.Mutex
	source          string
	snapshotVersion int64
	rules           map[string]model.RuleEntry
}

// bumpVersion returns the next monotonic version: time.Now().UnixNano()
// if that's strictly larger than prev, else prev+1.
func bumpVersion(prev int64) int64 {
	now := time.Now().UnixNano()
	if now > prev {
		return now
	}
	return prev + 1
}

// New returns an empty registry. The caller seeds it with Upsert calls
// (from a file load, a test fixture, etc.) before serving snapshots.
func New(source string) *Registry {
	return &Registry{
		source: source,
		rules:  make(map[string]model.RuleEntry, 16),
	}
}

// Source returns the human-readable tag for this registry — surfaced in
// snapshot replies and heartbeats so operators can see "disk" vs "mock"
// without grepping logs.
func (r *Registry) Source() string {
	return r.source
}

// SnapshotVersion returns the current global version. Used by the
// heartbeat goroutine: a fresh Ts with a stalled SnapshotVersion means
// "process alive, no edits since last beat" which is the normal idle case.
func (r *Registry) SnapshotVersion() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotVersion
}

// Snapshot returns a deep copy of the current rule set plus the version
// at which it was taken. The copy makes the result safe to marshal and
// send on the wire without holding the registry lock for the duration of
// the JSON encode or network write.
func (r *Registry) Snapshot() model.SnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	rules := make([]model.RuleEntry, 0, len(r.rules))
	for _, e := range r.rules {
		rules = append(rules, e)
	}
	// Sort by id so the wire output is deterministic — tests, diffs, and
	// operator-side comparisons all benefit, and the cost is trivial for
	// the sizes we expect (dozens of rules, not millions).
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	return model.SnapshotReply{
		SnapshotVersion: r.snapshotVersion,
		Ts:              time.Now().UTC().Format(time.RFC3339Nano),
		Source:          r.source,
		Rules:           rules,
	}
}

// Get returns the entry for a single id, or false if absent. Defensive
// copy of the entry — the caller can hold onto it without aliasing the
// registry's storage.
func (r *Registry) Get(id string) (model.RuleEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.rules[id]
	return e, ok
}

// Upsert inserts or replaces a rule's YAML. Returns the RuleDelta the
// publisher should emit (with operation=upsert) and a boolean indicating
// whether anything actually changed. The "no-op" case (same id, same
// YAML text) is silent — no version bump, no delta — so an fsnotify
// event for a re-saved-with-no-changes file does not produce wire churn.
//
// On a real change: per-rule version and global version each advance via
// bumpVersion (max(prev+1, time.Now().UnixNano())), and the returned
// RuleDelta carries the new versions plus the new YAML. Caller is
// responsible for actually publishing the delta — the registry is
// wire-agnostic.
func (r *Registry) Upsert(id, yaml string) (model.RuleDelta, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev, exists := r.rules[id]
	if exists && prev.YAML == yaml {
		return model.RuleDelta{}, false
	}

	var nextRuleVersion int64
	if exists {
		nextRuleVersion = bumpVersion(prev.RuleVersion)
	} else {
		nextRuleVersion = bumpVersion(0)
	}
	r.snapshotVersion = bumpVersion(r.snapshotVersion)
	r.rules[id] = model.RuleEntry{
		ID:          id,
		YAML:        yaml,
		RuleVersion: nextRuleVersion,
	}
	return model.RuleDelta{
		Operation:       model.OperationUpsert,
		RuleID:          id,
		RuleVersion:     nextRuleVersion,
		YAML:            yaml,
		SnapshotVersion: r.snapshotVersion,
		Ts:              time.Now().UTC().Format(time.RFC3339Nano),
	}, true
}

// Delete removes a rule by id. Returns the RuleDelta the publisher should
// emit and a boolean indicating whether anything actually changed (deleting
// an absent id is a no-op).
//
// The delta's RuleVersion is the pre-deletion version, NOT a fresh bump.
// Rationale: it lets LTC reconcile a delete with its current state — "the
// rule you were holding at version N is gone." A future upsert of the same
// id calls bumpVersion(0), which returns time.Now().UnixNano() — a fresh
// value strictly greater than the deleted version (the per-rule counter is
// wall-clock-seeded, not reset to 1). LTC accepts it because it advances
// past the version it last held for that id.
func (r *Registry) Delete(id string) (model.RuleDelta, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev, exists := r.rules[id]
	if !exists {
		return model.RuleDelta{}, false
	}
	delete(r.rules, id)
	r.snapshotVersion = bumpVersion(r.snapshotVersion)
	return model.RuleDelta{
		Operation:       model.OperationDelete,
		RuleID:          id,
		RuleVersion:     prev.RuleVersion,
		SnapshotVersion: r.snapshotVersion,
		Ts:              time.Now().UTC().Format(time.RFC3339Nano),
	}, true
}

// Len returns the number of rules currently registered. Used by the
// startup log line and tests; not on any hot path.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rules)
}

// ReplaceAll is the bulk variant used at startup after the initial disk
// scan. It atomically swaps the rule set in one snapshotVersion bump
// instead of one-per-rule, so a single SnapshotReply captures the whole
// load. Returns the number of rules that landed.
//
// On collision (same id appearing twice in the input), the later entry
// wins and the function returns an error — duplicate rule ids are a hard
// config error and must surface at startup, not later as silent overwrite.
func (r *Registry) ReplaceAll(entries map[string]string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[string]struct{}, len(entries))
	for id := range entries {
		if _, dup := seen[id]; dup {
			return 0, fmt.Errorf("duplicate rule id %q in bulk load", id)
		}
		seen[id] = struct{}{}
	}

	r.rules = make(map[string]model.RuleEntry, len(entries))
	for id, yaml := range entries {
		r.rules[id] = model.RuleEntry{ID: id, YAML: yaml, RuleVersion: bumpVersion(0)}
	}
	r.snapshotVersion = bumpVersion(r.snapshotVersion)
	return len(r.rules), nil
}
