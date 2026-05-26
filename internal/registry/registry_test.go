package registry

import (
	"testing"

	"rss/internal/model"
)

// TestUpsert_NewRule covers the happy path: an id never seen before
// returns a delta with op=upsert, RuleVersion=1, and a global
// snapshot_version that bumped by exactly 1.
func TestUpsert_NewRule(t *testing.T) {
	r := New("test")
	d, ok := r.Upsert("rule_a", "id: rule_a\nwhen: 'true'\n")
	if !ok {
		t.Fatal("first upsert must return changed=true")
	}
	if d.Operation != model.OperationUpsert {
		t.Errorf("op = %q; want upsert", d.Operation)
	}
	if d.RuleVersion != 1 {
		t.Errorf("rule_version = %d; want 1", d.RuleVersion)
	}
	if d.SnapshotVersion != 1 {
		t.Errorf("snapshot_version = %d; want 1", d.SnapshotVersion)
	}
}

// TestUpsert_Repeat_NoOp asserts that re-saving a file with byte-identical
// YAML is silent — no version bump, no delta. This is the contract the
// watcher relies on to absorb fsnotify's chmod-and-touch noise.
func TestUpsert_Repeat_NoOp(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "yaml_v1")
	_, ok := r.Upsert("rule_a", "yaml_v1")
	if ok {
		t.Error("upsert with identical YAML must return changed=false")
	}
	if got := r.SnapshotVersion(); got != 1 {
		t.Errorf("snapshot_version after no-op = %d; want 1", got)
	}
}

// TestUpsert_Replace bumps both versions: a real edit must move rule_version
// to 2 and snapshot_version to 2.
func TestUpsert_Replace(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "yaml_v1")
	d, ok := r.Upsert("rule_a", "yaml_v2")
	if !ok {
		t.Fatal("upsert with new YAML must return changed=true")
	}
	if d.RuleVersion != 2 {
		t.Errorf("rule_version = %d; want 2", d.RuleVersion)
	}
	if d.SnapshotVersion != 2 {
		t.Errorf("snapshot_version = %d; want 2", d.SnapshotVersion)
	}
}

// TestDelete_Existing returns the pre-deletion rule_version and bumps
// snapshot_version. LTC keys on (rule_id, follower) edge state and a
// delete must not change rule_version semantics for the gone rule —
// only mark the snapshot moved.
func TestDelete_Existing(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "yaml_v1")
	r.Upsert("rule_a", "yaml_v2") // rule_version=2, snapshot_version=2

	d, ok := r.Delete("rule_a")
	if !ok {
		t.Fatal("delete of existing id must return changed=true")
	}
	if d.Operation != model.OperationDelete {
		t.Errorf("op = %q; want delete", d.Operation)
	}
	if d.RuleVersion != 2 {
		t.Errorf("delete delta carries pre-deletion rule_version; got %d, want 2", d.RuleVersion)
	}
	if d.SnapshotVersion != 3 {
		t.Errorf("snapshot_version after delete = %d; want 3", d.SnapshotVersion)
	}
	if _, present := r.Get("rule_a"); present {
		t.Error("Get after Delete must return present=false")
	}
}

// TestDelete_Absent is a no-op: the registry's state is unchanged, the
// snapshot_version does NOT bump, the delta is the zero value.
func TestDelete_Absent(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "yaml")

	_, ok := r.Delete("rule_b")
	if ok {
		t.Error("delete of absent id must return changed=false")
	}
	if got := r.SnapshotVersion(); got != 1 {
		t.Errorf("snapshot_version after no-op delete = %d; want 1", got)
	}
}

// TestSnapshot_Deterministic locks in the wire-output ordering: rules in
// the SnapshotReply are sorted by id. Tests, diffs, and operator-side
// comparisons all benefit from this.
func TestSnapshot_Deterministic(t *testing.T) {
	r := New("test")
	r.Upsert("rule_c", "c")
	r.Upsert("rule_a", "a")
	r.Upsert("rule_b", "b")

	snap := r.Snapshot()
	if len(snap.Rules) != 3 {
		t.Fatalf("len(Rules) = %d; want 3", len(snap.Rules))
	}
	for i, want := range []string{"rule_a", "rule_b", "rule_c"} {
		if snap.Rules[i].ID != want {
			t.Errorf("Rules[%d].ID = %q; want %q (snapshot must be sorted by id)", i, snap.Rules[i].ID, want)
		}
	}
}

// TestReplaceAll bulk-loads in one snapshot_version bump and rejects
// duplicate ids. Used by the startup path so the initial load is a
// single coherent snapshot, not N per-file events.
func TestReplaceAll(t *testing.T) {
	r := New("test")
	n, err := r.ReplaceAll(map[string]string{
		"rule_a": "yaml_a",
		"rule_b": "yaml_b",
	})
	if err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if n != 2 {
		t.Errorf("loaded count = %d; want 2", n)
	}
	if got := r.SnapshotVersion(); got != 1 {
		t.Errorf("snapshot_version after bulk load = %d; want 1 (one bump for the whole set)", got)
	}
}

// TestUpsert_AfterDelete_RestartsCounter documents the per-rule version
// reset when an id is re-introduced. LTC must tolerate this — see
// model.RuleEntry comments.
func TestUpsert_AfterDelete_RestartsCounter(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "v1")
	r.Upsert("rule_a", "v2") // rule_version=2
	r.Delete("rule_a")
	d, ok := r.Upsert("rule_a", "v3")
	if !ok {
		t.Fatal("re-add after delete must return changed=true")
	}
	if d.RuleVersion != 1 {
		t.Errorf("re-add rule_version = %d; want 1 (counter restarts on re-add)", d.RuleVersion)
	}
}
