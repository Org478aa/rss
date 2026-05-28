package registry

import (
	"testing"

	"rss/internal/model"
)

// TestUpsert_NewRule covers the happy path: an id never seen before
// returns a delta with op=upsert, a strictly-positive RuleVersion, and a
// snapshot_version that advanced from the zero starting point.
func TestUpsert_NewRule(t *testing.T) {
	r := New("test")
	d, ok := r.Upsert("rule_a", "id: rule_a\nwhen: 'true'\n")
	if !ok {
		t.Fatal("first upsert must return changed=true")
	}
	if d.Operation != model.OperationUpsert {
		t.Errorf("op = %q; want upsert", d.Operation)
	}
	if d.RuleVersion <= 0 {
		t.Errorf("rule_version = %d; want > 0", d.RuleVersion)
	}
	if d.SnapshotVersion <= 0 {
		t.Errorf("snapshot_version = %d; want > 0", d.SnapshotVersion)
	}
}

// TestUpsert_Repeat_NoOp asserts that re-saving a file with byte-identical
// YAML is silent — no version bump, no delta. This is the contract the
// watcher relies on to absorb fsnotify's chmod-and-touch noise.
func TestUpsert_Repeat_NoOp(t *testing.T) {
	r := New("test")
	d1, _ := r.Upsert("rule_a", "yaml_v1")
	before := r.SnapshotVersion()
	_, ok := r.Upsert("rule_a", "yaml_v1")
	if ok {
		t.Error("upsert with identical YAML must return changed=false")
	}
	if got := r.SnapshotVersion(); got != before {
		t.Errorf("snapshot_version after no-op = %d; want unchanged %d", got, before)
	}
	if d1.SnapshotVersion != before {
		t.Errorf("first upsert delta snapshot_version = %d; registry holds %d", d1.SnapshotVersion, before)
	}
}

// TestUpsert_Replace bumps both versions: a real edit must move both
// rule_version and snapshot_version strictly forward.
func TestUpsert_Replace(t *testing.T) {
	r := New("test")
	d1, _ := r.Upsert("rule_a", "yaml_v1")
	d2, ok := r.Upsert("rule_a", "yaml_v2")
	if !ok {
		t.Fatal("upsert with new YAML must return changed=true")
	}
	if d2.RuleVersion <= d1.RuleVersion {
		t.Errorf("rule_version did not advance: v1=%d v2=%d", d1.RuleVersion, d2.RuleVersion)
	}
	if d2.SnapshotVersion <= d1.SnapshotVersion {
		t.Errorf("snapshot_version did not advance: v1=%d v2=%d", d1.SnapshotVersion, d2.SnapshotVersion)
	}
}

// TestDelete_Existing carries the pre-deletion rule_version and bumps
// snapshot_version. LTC keys on (rule_id, follower) edge state and a
// delete must not change rule_version semantics for the gone rule —
// only mark the snapshot moved.
func TestDelete_Existing(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "yaml_v1")
	d2, _ := r.Upsert("rule_a", "yaml_v2")

	d, ok := r.Delete("rule_a")
	if !ok {
		t.Fatal("delete of existing id must return changed=true")
	}
	if d.Operation != model.OperationDelete {
		t.Errorf("op = %q; want delete", d.Operation)
	}
	if d.RuleVersion != d2.RuleVersion {
		t.Errorf("delete delta rule_version = %d; want pre-deletion %d", d.RuleVersion, d2.RuleVersion)
	}
	if d.SnapshotVersion <= d2.SnapshotVersion {
		t.Errorf("snapshot_version did not advance on delete: pre=%d post=%d", d2.SnapshotVersion, d.SnapshotVersion)
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
	before := r.SnapshotVersion()

	_, ok := r.Delete("rule_b")
	if ok {
		t.Error("delete of absent id must return changed=false")
	}
	if got := r.SnapshotVersion(); got != before {
		t.Errorf("snapshot_version after no-op delete = %d; want unchanged %d", got, before)
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
	before := r.SnapshotVersion()
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
	if got := r.SnapshotVersion(); got <= before {
		t.Errorf("snapshot_version after bulk load = %d; want > %d", got, before)
	}
}

// TestUpsert_AfterDelete uses bumpVersion to keep the re-added rule's
// version strictly above any prior version the consumer might still
// hold — a Delete + Upsert sequence within one process must not produce
// a RuleVersion that re-collides with the deleted rule's last value.
func TestUpsert_AfterDelete(t *testing.T) {
	r := New("test")
	r.Upsert("rule_a", "v1")
	d2, _ := r.Upsert("rule_a", "v2")
	r.Delete("rule_a")
	d, ok := r.Upsert("rule_a", "v3")
	if !ok {
		t.Fatal("re-add after delete must return changed=true")
	}
	if d.RuleVersion <= d2.RuleVersion {
		t.Errorf("re-add rule_version = %d; want > pre-delete %d", d.RuleVersion, d2.RuleVersion)
	}
}

// TestSnapshotVersion_AdvancesAcrossRestart simulates the "RSS restarts
// while LTC is still up" scenario the unix-nano version scheme is
// designed for: a fresh Registry's first bump must produce a
// snapshot_version strictly larger than any version a prior process
// could have published, so LTC doesn't drop subsequent deltas as stale.
func TestSnapshotVersion_AdvancesAcrossRestart(t *testing.T) {
	// Simulate the prior process's last-published version: pick a value
	// derived from "now" so it reflects what a real prior RSS would have
	// produced in steady-state. bumpVersion off the result must still
	// move forward — that's the property LTC's gate depends on.
	prior := bumpVersion(0)

	// Fresh process.
	r := New("test")
	d, _ := r.Upsert("rule_a", "yaml")
	if d.SnapshotVersion <= prior {
		t.Errorf("fresh registry snapshot_version = %d; want > prior process %d", d.SnapshotVersion, prior)
	}
	if d.RuleVersion <= prior {
		t.Errorf("fresh registry rule_version = %d; want > prior process %d", d.RuleVersion, prior)
	}
}
