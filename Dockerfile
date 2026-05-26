# RSS container image.
#
# Build context MUST be the workspace root (the directory containing ldm/,
# sdm/, ltc/, rss/, nats/) — the shared atp/nats module is reached via the
# `replace atp/nats => ../nats` directive in rss/go.mod.
#
# Typical invocation (from workspace root):
#   docker build -f rss/Dockerfile -t atp/rss:dev .
# Pin to a specific patch — `golang:1.26-alpine` floats and can land on a
# version older than what rss/go.mod declares (1.26.3).
FROM golang:1.26.3-alpine AS builder

WORKDIR /src

COPY nats/ /src/nats/
COPY rss/  /src/rss/

WORKDIR /src/rss
RUN go mod download && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rss ./cmd/rss

FROM alpine:3
# tzdata for symmetry with sibling services even though RSS itself does
# not parse named time zones today; cheap insurance against the
# `unknown time zone America/New_York` foot-gun if scheduling logic ever
# moves here (e.g., a "rules paused outside trading hours" admin command).
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /out/rss /usr/local/bin/rss
ENTRYPOINT ["/usr/local/bin/rss"]
