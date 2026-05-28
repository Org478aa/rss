# RSS — Rule Storage Service

Serves the ATP rule registry to LTC over NATS. Reads YAML rule files from
disk, replies to `rss.snapshot.request`, broadcasts `rss.heartbeat` every
5 s, and pushes per-rule deltas (upsert / delete) to LTC on the JetStream
`RULE_UPDATES` stream as files change — fsnotify-watched with a 100 ms
debounce so editor temp-write+rename bursts collapse to one delta.

See `CLAUDE.md` for the NATS contract and operational conventions. See
`../CLAUDE.md` for workspace architecture.

## Quick start

```bash
go build -o rss ./cmd/rss
./rss --rules-dir ./data
```

In platform-e2e, the container is started via docker compose — see
`platform-e2e/docker-compose.yml` for the canonical invocation.

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `--nats` | `nats://127.0.0.1:4222` | broker URL |
| `--rules-dir` | `./rules` | directory of `*.yaml` / `*.yml` rule files to serve |
| `--heartbeat-interval` | 5 s | override `rss.heartbeat` publish cadence (e2e uses 500 ms) |

## Layout

See `CLAUDE.md` — single source of truth for the package layout and
conventions.
