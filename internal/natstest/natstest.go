// Package natstest spins up an in-process NATS server with JetStream enabled
// for integration tests. The server listens on a random port, uses a per-test
// temp directory for JetStream storage, and is torn down via t.Cleanup so
// each test gets an isolated broker (no stream/consumer carryover).
//
// Production code MUST NOT import this package — it pulls
// github.com/nats-io/nats-server/v2 (a ~20 MB dep tree) which has no place in
// the rss binary. The single allowed importer pattern is "<pkg>_test.go in a
// package that talks to NATS".
package natstest

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server starts an in-process NATS server with JetStream, opens a client
// connection, and registers cleanup with t. Returns the connection and a
// JetStream context bound to it. Each call yields a fresh, isolated broker —
// stream/consumer state from one test cannot leak into another.
func Server(t *testing.T) (*natslib.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &natsserver.Options{
		Host:       "127.0.0.1",
		Port:       -1, // random free port
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
		MaxPayload: 1 << 20,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("natstest: new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		t.Fatal("natstest: server not ready in 5s")
	}

	nc, err := natslib.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("natstest: connect: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("natstest: jetstream context: %v", err)
	}

	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
		s.WaitForShutdown()
	})

	return nc, js
}
