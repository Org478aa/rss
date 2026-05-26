// Package publisher owns RSS's NATS surface:
//
//   - rss.snapshot.request (NATS Core, request/reply) — cold-boot pull,
//     answered with the full rule set held in the registry;
//   - rss.heartbeat (NATS Core, broadcast every HeartbeatInterval) — liveness
//     signal for LTC's watchdog;
//   - RULE_UPDATES (JetStream, owned here) — per-rule deltas (upsert /
//     delete) so LTC can hot-reload without polling.
//
// Wire-mix rationale: the heartbeat + snapshot RPC mirror SDM exactly
// (presence-only signaling, synchronous pull at bootstrap). The deltas go
// over JetStream so a brief LTC disconnect doesn't drop a rule edit —
// LTC's snapshot.request remains the authoritative recovery path on cold
// start, but small gaps are absorbed by the stream's 24 h history.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"atp/nats"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"rss/internal/model"
	"rss/internal/registry"
)

const (
	// Re-exported from atp/nats so internal callers and tests can keep
	// using the publisher's own names without an extra import; the shared
	// package remains the source of truth for the wire strings.
	SubjectSnapshotRequest = nats.SubjectRSSSnapshotRequest
	SubjectHeartbeat       = nats.SubjectRSSHeartbeat
	SubjectUpdates         = nats.SubjectRuleUpdates
)

// HeartbeatInterval matches the 5 s cadence the other services use. LTC's
// watchdog applies the same soft / hard thresholds it does to SDM and TSS,
// so the cadence here is the bottom of that budget.
//
// Declared as `var` (not `const`) so the RSS binary's `--heartbeat-interval`
// flag can override at startup — the e2e harness uses 500 ms to keep the
// bootstrap-mute window short.
var HeartbeatInterval = 5 * time.Second

// Publisher wires the four NATS surfaces RSS exposes. Constructed by main
// and then driven by Run; PublishDelta is invoked by the file watcher
// (and by tests).
type Publisher struct {
	nc       *natslib.Conn
	js       jetstream.JetStream
	registry *registry.Registry

	// publishMu serializes PublishDelta calls — keeps the Nats-Msg-Id
	// allocation linear and prevents two concurrent saves from racing on
	// the same rule id (the registry's mutex covers the data race; this
	// one covers the wire-order race).
	publishMu sync.Mutex
}

func New(nc *natslib.Conn, reg *registry.Registry) (*Publisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &Publisher{nc: nc, js: js, registry: reg}, nil
}

// Run wires the NATS surface:
//
//   - creates / asserts the RULE_UPDATES JetStream stream (RSS is owner);
//   - subscribes to rss.snapshot.request (NATS Core, request/reply);
//   - starts the heartbeat goroutine on rss.heartbeat (NATS Core).
//
// Returns a stop func that drains all three. Safe to call once.
func (p *Publisher) Run(ctx context.Context) (stop func(), err error) {
	if _, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       nats.StreamRuleUpdates,
		Subjects:   []string{nats.SubjectRuleUpdates},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     nats.RuleUpdatesStreamMaxAge,
		Duplicates: nats.RuleUpdatesStreamDuplicates,
	}); err != nil {
		return nil, fmt.Errorf("ensure %s stream: %w", nats.StreamRuleUpdates, err)
	}
	slog.Info("updates stream ready", "name", nats.StreamRuleUpdates, "max_age", nats.RuleUpdatesStreamMaxAge)

	sub, err := p.nc.Subscribe(SubjectSnapshotRequest, p.handleSnapshotRequest)
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", SubjectSnapshotRequest, err)
	}
	slog.Info("snapshot request handler subscribed", "subject", SubjectSnapshotRequest)

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		// Publish once immediately — same race-closing trick SDM uses. A
		// subscriber that attached just before Run() shouldn't have to
		// wait a full HeartbeatInterval to see RSS is alive.
		p.publishHeartbeat()
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.publishHeartbeat()
			}
		}
	}()
	slog.Info("heartbeat goroutine started", "subject", SubjectHeartbeat, "interval", HeartbeatInterval)

	return func() {
		_ = sub.Unsubscribe()
		<-hbDone
	}, nil
}

// PublishDelta sends one RuleDelta on RULE_UPDATES. The watcher gets a
// delta back from registry.Upsert / Delete and hands it here. We do a
// synchronous Publish (waits for broker ack) so a publish failure can
// surface in the caller's log immediately — and so the broker's 5 min
// dedup window has a stable Nats-Msg-Id to deduplicate on.
//
// The dedup id is built from rule id + per-rule version. That way an
// fsnotify event that fires twice for one atomic save (write + close)
// produces the same RuleDelta (same RuleVersion, same id) and the broker
// drops the duplicate.
func (p *Publisher) PublishDelta(ctx context.Context, d model.RuleDelta) error {
	p.publishMu.Lock()
	defer p.publishMu.Unlock()

	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal delta: %w", err)
	}
	dedup := d.RuleID + ":" + strconv.FormatInt(d.RuleVersion, 10) + ":" + string(d.Operation)
	_, err = p.js.PublishMsg(ctx, &natslib.Msg{
		Subject: SubjectUpdates,
		Header:  natslib.Header{natslib.MsgIdHdr: []string{dedup}},
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("publish delta: %w", err)
	}
	slog.Info("delta published",
		"op", d.Operation,
		"rule_id", d.RuleID,
		"rule_version", d.RuleVersion,
		"snapshot_version", d.SnapshotVersion,
	)
	return nil
}

func (p *Publisher) handleSnapshotRequest(msg *natslib.Msg) {
	snap := p.registry.Snapshot()
	respond(msg, snap)
	// Publish a fresh heartbeat as a side effect of every snapshot.request —
	// same race-closer SDM uses to eliminate the muted_rss_stale boot
	// window for fast LTC bootstraps.
	p.publishHeartbeat()
}

func respond(msg *natslib.Msg, s model.SnapshotReply) {
	data, err := json.Marshal(s)
	if err != nil {
		slog.Warn("snapshot marshal failed", "err", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		slog.Warn("snapshot respond failed", "err", err)
	}
}

func (p *Publisher) publishHeartbeat() {
	data, err := json.Marshal(model.Heartbeat{
		SnapshotVersion: p.registry.SnapshotVersion(),
		Ts:              time.Now().UTC().Format(time.RFC3339Nano),
		Source:          p.registry.Source(),
	})
	if err != nil {
		slog.Warn("heartbeat marshal failed", "err", err)
		return
	}
	if err := p.nc.Publish(SubjectHeartbeat, data); err != nil {
		slog.Warn("heartbeat publish failed", "err", err)
	}
}
