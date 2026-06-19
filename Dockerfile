# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /app

# Cache module downloads as a separate layer so code edits don't bust the dep cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build all three pipeline binaries:
#   es-indexer — the long-running Kafka→OpenSearch consumer (default entrypoint).
#   backfill   — one-shot historical loader (MySQL shards → OpenSearch, bypass Kafka).
#   reconcile  — MySQL-vs-OpenSearch count/sample correctness gate (exit 2 on mismatch).
# backfill + reconcile are on-demand ops tools the deployment upgrade flow runs as
# one-shot jobs; shipping them in the same image means operators do not need a
# separate Go toolchain or a second image to turn search on.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /es-indexer ./cmd/es-indexer \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /backfill ./cmd/backfill \
  && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /reconcile ./cmd/reconcile

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata wget \
  && adduser -D -u 10001 appuser

COPY --from=builder /es-indexer /usr/local/bin/es-indexer
COPY --from=builder /backfill /usr/local/bin/backfill
COPY --from=builder /reconcile /usr/local/bin/reconcile

USER appuser

# es-indexer is a Kafka-consumer worker. Phase 4 adds a self-hosted /metrics
# scrape endpoint (reuse octo pkg/metrics NewScrapeServer); EXPOSE/HEALTHCHECK
# will be wired to that port then. Kept minimal in the scaffold.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/es-indexer"]
