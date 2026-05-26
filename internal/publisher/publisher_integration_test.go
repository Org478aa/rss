package publisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"atp/nats"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"rss/internal/model"
	"rss/internal/natstest"
	"rss/internal/publisher"
	"rss/internal/registry"
)

// quickHeartbeats shortens the heartbeat cadence so tests run fast —
// same trick the e2e harness uses. Restored on cleanup so test order
// doesn't matter.
func quickHeartbeats(t *testing.T) {
	t.Helper()
	prev := publisher.HeartbeatInterval
	publisher.HeartbeatInterval = 100 * time.Millisecond
	t.Cleanup(func() { publisher.HeartbeatInterval = prev })
}

// runPublisher builds the full publisher + broker + registry trio and
// starts Run(). t.Cleanup cancels the context BEFORE invoking stop —
// the publisher's stop func blocks on ctx.Done, so the reverse order
// would deadlock (LIFO defer + waiting-on-cancel = no progress).
func runPublisher(t *testing.T) (*publisher.Publisher, *registry.Registry, *natslib.Conn, jetstream.JetStream) {
	t.Helper()
	quickHeartbeats(t)
	nc, js := natstest.Server(t)
	reg := registry.New("test")
	pub, err := publisher.New(nc, reg)
	if err != nil {
		t.Fatalf("publisher.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := pub.Run(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stop()
	})
	return pub, reg, nc, js
}

// TestRun_CreatesStream asserts that calling Run on a fresh broker
// brings the RULE_UPDATES stream into existence. Without this the
// LTC bootstrap fails ("stream not found") with no useful clue.
func TestRun_CreatesStream(t *testing.T) {
	_, _, _, js := runPublisher(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.Stream(ctx, nats.StreamRuleUpdates); err != nil {
		t.Errorf("expected stream %s to exist after Run; got %v", nats.StreamRuleUpdates, err)
	}
}

// TestSnapshotRequest_ReturnsRegistryContents covers the bootstrap
// path: a client sends an empty body to rss.snapshot.request, RSS
// replies with the full rule set as JSON.
func TestSnapshotRequest_ReturnsRegistryContents(t *testing.T) {
	_, reg, nc, _ := runPublisher(t)
	if _, err := reg.ReplaceAll(map[string]string{
		"rule_a": "id: rule_a\nwhen: \"true\"\n",
		"rule_b": "id: rule_b\nwhen: \"true\"\n",
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, nats.SubjectRSSSnapshotRequest, []byte{})
	if err != nil {
		t.Fatalf("RequestWithContext: %v", err)
	}
	var reply model.SnapshotReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if reply.Source != "test" {
		t.Errorf("source = %q; want test", reply.Source)
	}
	if len(reply.Rules) != 2 {
		t.Fatalf("got %d rules; want 2", len(reply.Rules))
	}
	for _, want := range []string{"rule_a", "rule_b"} {
		found := false
		for _, r := range reply.Rules {
			if r.ID == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing rule %q in reply", want)
		}
	}
}

// TestSnapshotRequest_PublishesHeartbeat asserts the race-closing
// side effect: every snapshot.request reply is followed by an immediate
// heartbeat on rss.heartbeat. LTC's bootstrap relies on this to escape
// muted_rss_stale fast.
func TestSnapshotRequest_PublishesHeartbeat(t *testing.T) {
	_, reg, nc, _ := runPublisher(t)
	_, _ = reg.Upsert("rule_a", "id: rule_a\n")

	beats := make(chan model.Heartbeat, 4)
	sub, err := nc.Subscribe(nats.SubjectRSSHeartbeat, func(m *natslib.Msg) {
		var hb model.Heartbeat
		if err := json.Unmarshal(m.Data, &hb); err == nil {
			select {
			case beats <- hb:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("subscribe heartbeat: %v", err)
	}
	defer sub.Unsubscribe()

	// Drain any heartbeats that fired before we attached. Then fire
	// the snapshot request and require a heartbeat within 500 ms
	// (well below the 100 ms ticker interval, but the side-effect
	// publish should be immediate).
	for {
		select {
		case <-beats:
		default:
			goto fire
		}
	}
fire:
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := nc.RequestWithContext(ctx, nats.SubjectRSSSnapshotRequest, []byte{}); err != nil {
		t.Fatalf("snapshot request: %v", err)
	}
	select {
	case <-beats:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no heartbeat within 500 ms of snapshot.request — race-closer regressed")
	}
}

// TestPublishDelta_RoundTripsViaJetStream asserts that PublishDelta
// lands a RuleDelta on rss.updates, observable by a fresh ephemeral
// consumer. This is the contract LTC relies on.
func TestPublishDelta_RoundTripsViaJetStream(t *testing.T) {
	pub, reg, _, js := runPublisher(t)

	d, ok := reg.Upsert("rule_a", "id: rule_a\n")
	if !ok {
		t.Fatal("Upsert: want changed=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.PublishDelta(ctx, d); err != nil {
		t.Fatalf("PublishDelta: %v", err)
	}

	cons, err := js.CreateConsumer(ctx, nats.StreamRuleUpdates, jetstream.ConsumerConfig{
		FilterSubject: nats.SubjectRuleUpdates,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	got := make(chan model.RuleDelta, 1)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var rd model.RuleDelta
		if err := json.Unmarshal(msg.Data(), &rd); err != nil {
			t.Errorf("unmarshal: %v", err)
			_ = msg.Term()
			return
		}
		select {
		case got <- rd:
		default:
		}
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer cc.Stop()

	select {
	case rd := <-got:
		if rd.RuleID != "rule_a" || rd.Operation != model.OperationUpsert {
			t.Errorf("delta = %+v; want rule_a upsert", rd)
		}
		if rd.SnapshotVersion != 1 || rd.RuleVersion != 1 {
			t.Errorf("versions = (snapshot %d, rule %d); want (1, 1)", rd.SnapshotVersion, rd.RuleVersion)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delta on rss.updates")
	}
}

// TestPublishDelta_DedupOnRedelivery exercises the Nats-Msg-Id-based
// 5-min broker dedup: publishing the same delta twice in rapid
// succession must land exactly one stream message. Critical because
// fsnotify reliably fires duplicate events for atomic saves.
func TestPublishDelta_DedupOnRedelivery(t *testing.T) {
	pub, reg, _, js := runPublisher(t)

	d, _ := reg.Upsert("rule_a", "id: rule_a\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := pub.PublishDelta(ctx, d); err != nil {
			t.Fatalf("PublishDelta #%d: %v", i, err)
		}
	}

	stream, err := js.Stream(ctx, nats.StreamRuleUpdates)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var info *jetstream.StreamInfo
	for time.Now().Before(deadline) {
		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("stream info: %v", err)
		}
		if info.State.Msgs >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if info.State.Msgs != 1 {
		t.Errorf("stream messages = %d; want 1 (broker dedup on Nats-Msg-Id)", info.State.Msgs)
	}
}
