// rss is the Rule Storage Service — owns the rule registry and serves it to
// LTC over NATS.
//
// Source of rules: a directory of YAML files (one rule per file). At startup
// every *.yaml / *.yml under --rules-dir is loaded into the in-memory
// registry and a SnapshotReply is ready to serve. The watcher then keeps
// the registry in sync with on-disk edits and publishes one RuleDelta per
// change on the JetStream RULE_UPDATES stream.
//
// See README.md and CLAUDE.md for the NATS contract; see workspace
// CLAUDE.md for cross-service architecture.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atp/nats"

	"rss/internal/loader"
	"rss/internal/publisher"
	"rss/internal/registry"
	"rss/internal/watcher"
)

func main() {
	var (
		natsURL           = flag.String("nats", nats.DefaultURL, "NATS URL")
		rulesDir          = flag.String("rules-dir", "./rules", "directory of rule YAML files to serve")
		heartbeatInterval = flag.Duration("heartbeat-interval", 0, "override rss.heartbeat publish cadence (0 = use compiled default 5 s; e2e suite passes 500 ms)")
	)
	flag.Parse()

	if *heartbeatInterval > 0 {
		publisher.HeartbeatInterval = *heartbeatInterval
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	files, err := loader.LoadDir(*rulesDir)
	if err != nil {
		slog.Error("initial load failed", "dir", *rulesDir, "err", err)
		os.Exit(1)
	}
	reg := registry.New("disk")
	seed := make(map[string]string, len(files))
	for _, f := range files {
		seed[f.ID] = f.YAML
	}
	n, err := reg.ReplaceAll(seed)
	if err != nil {
		slog.Error("bulk load into registry failed", "err", err)
		os.Exit(1)
	}
	slog.Info("rules loaded", "dir", *rulesDir, "count", n)

	slog.Info("connecting to NATS", "url", *natsURL)
	nc, err := nats.Connect(*natsURL, "rss")
	if err != nil {
		slog.Error("nats connect", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	pub, err := publisher.New(nc, reg)
	if err != nil {
		slog.Error("publisher init", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopPub, err := pub.Run(ctx)
	if err != nil {
		slog.Error("publisher start", "err", err)
		os.Exit(1)
	}

	w := watcher.New(*rulesDir, reg, pub)
	w.Seed(files)
	stopWatcher, err := w.Start(ctx)
	if err != nil {
		stopPub()
		slog.Error("watcher start", "err", err)
		os.Exit(1)
	}

	stop := func() {
		stopWatcher()
		stopPub()
	}
	slog.Info("rss running", "rules", reg.Len(), "source", reg.Source(), "watch", *rulesDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig)
	cancel()

	// Give the publisher's goroutines a moment to drain. Same 5 s budget
	// SDM uses; tested with a real broker, the heartbeat goroutine exits
	// in microseconds.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-drainCtx.Done():
		slog.Warn("publisher drain timed out")
	}
	slog.Info("rss stopped")
}
