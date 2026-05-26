// Package model defines the wire types RSS publishes on NATS. Every type is
// JSON-serialized; field names are stable contract between RSS and its
// consumers (today: LTC).
//
// Mirrors sdm/internal/model in shape and naming: a SnapshotReply for the
// cold-boot RPC, a Heartbeat for liveness broadcast, and a RuleDelta for
// per-rule mutations on the JetStream RULE_UPDATES stream.
package model

// Operation is the kind of mutation a RuleDelta encodes. The enum is closed
// so consumers can switch exhaustively. We do NOT have a "rename" — a
// rename is one delete + one upsert with the same content under the new id.
type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

// RuleEntry is one rule's full state as carried on the wire. The YAML
// field is the raw, unparsed YAML text — RSS is intentionally a transport
// layer for the rule body, and LTC's loader is the source of truth for
// schema (id pattern, when expression, follower-reference gate, schedule,
// emit.on enum). Trying to parse + re-serialize in RSS would invite
// drift between RSS's understanding of the schema and LTC's.
//
// RuleVersion is per-rule, monotonic on the RSS side: each upsert bumps it
// by 1 (or more, if the operator races multiple edits). Consumers use it
// to drop redeliveries and out-of-order deltas for the same rule id.
type RuleEntry struct {
	ID          string `json:"rule_id"`
	YAML        string `json:"yaml"`
	RuleVersion int64  `json:"rule_version"`
}

// SnapshotReply is sent in response to rss.snapshot.request. SnapshotVersion
// is monotonic across the entire RSS process — every change (any rule's
// upsert or delete) bumps it. Consumers track the highest SnapshotVersion
// they've observed and use it as the threshold for accepting RuleDelta
// messages (anything <= currentVersion is a redelivery).
type SnapshotReply struct {
	SnapshotVersion int64       `json:"snapshot_version"`
	Ts              string      `json:"ts"` // RFC3339Nano, UTC
	Source          string      `json:"source"`
	Rules           []RuleEntry `json:"rules"`
}

// Heartbeat is broadcast every HeartbeatInterval on the rss.heartbeat
// subject (NATS Core, no persistence — presence-based). LTC's watchdog uses
// (a) freshness of Ts and (b) progress of SnapshotVersion to detect "RSS
// stale" (no recent heartbeat) vs "RSS frozen" (heartbeats fresh but
// version hasn't advanced — process alive but stuck).
type Heartbeat struct {
	SnapshotVersion int64  `json:"snapshot_version"`
	Ts              string `json:"ts"`
	Source          string `json:"source"`
}

// RuleDelta is published on rss.updates (JetStream RULE_UPDATES) whenever a
// rule changes mid-process. One message describes one logical mutation.
// The SnapshotVersion field carries the *post-mutation* version — i.e., what
// SnapshotReply would carry if a consumer requested a snapshot right after
// this delta was published. Consumers apply the delta only when
// SnapshotVersion > current; otherwise it's silently dropped.
//
// For OperationDelete, YAML is empty and RuleVersion is the version at the
// moment of deletion (so a future upsert can re-introduce the same id with
// a strictly-greater RuleVersion).
type RuleDelta struct {
	Operation       Operation `json:"op"`
	RuleID          string    `json:"rule_id"`
	RuleVersion     int64     `json:"rule_version"`
	YAML            string    `json:"yaml,omitempty"`
	SnapshotVersion int64     `json:"snapshot_version"`
	Ts              string    `json:"ts"`
}
