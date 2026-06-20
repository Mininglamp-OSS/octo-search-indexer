# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /app

# Cache module downloads as a separate layer so code edits don't bust the dep cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build all pipeline binaries:
#   es-indexer        — the long-running Kafka→OpenSearch consumer (default entrypoint).
#   searchetl-producer — the long-running MySQL→Kafka polling ETL producer (realtime write side).
#   backfill          — one-shot historical loader (MySQL shards → OpenSearch, bypass Kafka).
#   reconcile         — MySQL-vs-OpenSearch count/sample correctness gate (exit 2 on mismatch).
# backfill + reconcile are on-demand ops tools the deployment upgrade flow runs as
# one-shot jobs; shipping them in the same image means operators do not need a
# separate Go toolchain or a second image to turn search on. searchetl-producer is
# a separate long-running worker (opt-in via PRODUCER_ENABLED) selected by overriding
# the image entrypoint/command in its own Deployment.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /es-indexer ./cmd/es-indexer \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /searchetl-producer ./cmd/searchetl-producer \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /backfill ./cmd/backfill \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /reconcile ./cmd/reconcile

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata wget \
  && adduser -D -u 10001 appuser

COPY --from=builder /es-indexer /usr/local/bin/es-indexer
COPY --from=builder /searchetl-producer /usr/local/bin/searchetl-producer
COPY --from=builder /backfill /usr/local/bin/backfill
COPY --from=builder /reconcile /usr/local/bin/reconcile

USER appuser

# Both long-running workers (es-indexer consumer + searchetl-producer) serve their
# observability endpoints (healthz/readyz/metrics) on :9090. The default entrypoint
# is es-indexer; the producer Deployment overrides command to /usr/local/bin/searchetl-producer.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/es-indexer"]
