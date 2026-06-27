package producer

import (
	"context"
	"sync/atomic"
	"time"
)

// ETL is the incremental extractor: read message shards → produce Kafka → advance
// the cursor over the confirmed-delivered stable prefix.
//
// Two layers of mutual exclusion:
//   - in-process running CAS: blocks concurrent runs within the same process.
//   - 🔴 Redis run-lock (runLocked): blocks cross-replica concurrency — held
//     across the whole tick, renew-failure aborts the in-flight batch.
//
// The ultimate correctness guarantee is NOT the Redis lock but the
// "produce-then-advance" ordering + the cursor-row CAS (AdvanceCursor's WHERE
// last_id=expected, serialized by the FOR UPDATE read) as a fencing token, plus
// the ES _id idempotent upsert downstream. Even under a lock race the CAS blocks
// out-of-turn advancement, duplicates are deduped by the idempotent sink — no
// gaps, no loss, no double-count.
type ETL struct {
	store         Store
	newSink       func() Sink
	lock          RunLock
	batch         int
	lag           int64
	renewInterval time.Duration
	logf          func(string, ...any)

	// metrics is an optional observer (cursor positions, produced counts).
	metrics *Metrics

	// running is the in-process re-entrancy guard (CAS 0→1).
	running atomic.Bool
}

// ETLDeps bundles the constructed dependencies for NewETL.
type ETLDeps struct {
	Store         Store
	NewSink       func() Sink
	Lock          RunLock
	Batch         int
	Lag           int64
	RenewInterval time.Duration
	Logf          func(string, ...any)
	Metrics       *Metrics
}

// NewETL constructs an ETL.
func NewETL(d ETLDeps) *ETL {
	if d.RenewInterval <= 0 {
		d.RenewInterval = lockRenewInterval()
	}
	if d.Logf == nil {
		d.Logf = func(string, ...any) {}
	}
	return &ETL{
		store:         d.Store,
		newSink:       d.NewSink,
		lock:          d.Lock,
		batch:         d.Batch,
		lag:           d.Lag,
		renewInterval: d.RenewInterval,
		logf:          d.Logf,
		metrics:       d.Metrics,
	}
}

// RunIncremental runs one incremental round under the Redis run-lock.
//
// Mutual exclusion:
//   - in-process running CAS blocks same-process re-entry.
//   - 🔴 Redis run-lock blocks cross-replica concurrency; renew failure cancels
//     lockCtx so produce + cursor advance both stop. Losing the acquire race skips
//     the round cleanly.
func (e *ETL) RunIncremental(ctx context.Context, tables []string) error {
	if !e.running.CompareAndSwap(false, true) {
		e.logf("producer: another incremental run in progress (same process), skip")
		return nil
	}
	defer e.running.Store(false)

	return runLocked(ctx, e.lock, e.renewInterval, e.logf, e.metrics, func(lockCtx context.Context) error {
		return e.runTick(lockCtx, tables)
	})
}

// runTick is the work of one lock-held round: construct a sink, scan each shard,
// produce + advance the cursor in order. ctx is lockCtx — on renew failure the
// per-chunk ctx.Err() check aborts the in-flight loop immediately, never producing
// or advancing after lock loss.
func (e *ETL) runTick(ctx context.Context, tables []string) error {
	sink := e.newSink()
	defer func() {
		if cerr := sink.Close(); cerr != nil {
			e.logf("producer: close sink failed: %v", cerr)
		}
	}()

	nowUnix, err := e.store.DBNowUnix(ctx)
	if err != nil {
		return err
	}
	cutoff := nowUnix - e.lag

	var totalMain, totalDLQ int64
	for _, table := range tables {
		if err = e.store.EnsureCursor(ctx, table); err != nil {
			return err
		}
		for {
			if cerr := ctx.Err(); cerr != nil {
				e.logf("producer: lock lost / ctx cancelled, abort in-flight tick (table=%s): %v", table, cerr)
				return cerr
			}
			plan, n, cerr := runChunk(ctx, e.store, sink, table, cutoff, e.batch, e.logf, e.metrics)
			if cerr != nil {
				return cerr
			}
			totalMain += int64(len(plan.main))
			totalDLQ += int64(len(plan.dlq))
			if plan.advanced && e.metrics != nil {
				e.metrics.SetCursor(table, plan.maxID)
			}
			// Reached the unstable tail (stable prefix shorter than a full batch).
			if n < e.batch {
				break
			}
		}
	}

	if e.metrics != nil {
		e.metrics.AddProduced(totalMain, totalDLQ)
	}
	e.logf("producer: incremental done main_produced=%d dlq_produced=%d lag_seconds=%d",
		totalMain, totalDLQ, e.lag)
	return nil
}

// runChunk processes one chunk of a shard (C2 transaction boundary):
//  1. short read transaction: cursor + a batch (ReadStableBatchTx, releases lock
//     immediately, no Kafka IO inside);
//  2. outside the transaction: stability gate truncation + payload extraction
//     (planChunk, pure compute);
//  3. outside the transaction: produce the whole batch (main + DLQ all confirmed);
//  4. after confirmation: a short transaction advances the cursor to the stable
//     prefix end (AdvanceCursor CAS).
//
// Returns the stable-prefix row count (for the "reached the unstable tail" check).
// Any failure returns an error and leaves the cursor un-advanced.
func runChunk(ctx context.Context, store Store, sink Sink, table string, cutoff int64, batch int, logf func(string, ...any), metrics *Metrics) (chunkPlan, int, error) {
	readStart := time.Now()
	cursor, rows, err := store.ReadStableBatchTx(ctx, table, batch)
	if metrics != nil {
		metrics.ObserveReadBatch(time.Since(readStart))
	}
	if err != nil {
		return chunkPlan{}, 0, err
	}
	if len(rows) == 0 {
		return chunkPlan{}, 0, nil
	}

	// Order tripwire: stablePrefix's no-gap truncation assumes the batch is
	// strictly id-ascending (ORDER BY id ASC). Detect + warn loudly (no change to
	// the correctness path).
	if bad := firstNonAscendingByID(rows); bad >= 0 {
		logf("producer: stable batch not strictly ascending by id (table=%s at=%d prev=%d curr=%d) — silent missed-read risk",
			table, bad, rows[bad-1].ID, rows[bad].ID)
	}

	plan := planChunk(table, rows, cutoff)
	if !plan.advanced {
		// Head row not yet stable: do not advance this round; wait for it to age lag.
		return plan, 0, nil
	}

	// 🔴 C2: produce outside the transaction. main then DLQ; any failure leaves the
	// chunk un-advanced and the whole batch is re-produced next round.
	if err = sink.ProduceBatch(ctx, plan.main); err != nil {
		return plan, 0, err
	}
	if err = sink.ProduceDLQ(ctx, plan.dlq); err != nil {
		return plan, 0, err
	}
	// dlq_total{reason}: count each dead-lettered envelope only after the DLQ
	// produce is confirmed (planChunk stays pure — no side effects there).
	if metrics != nil {
		for i := range plan.dlq {
			metrics.MarkDLQ(plan.dlq[i].Reason)
		}
	}

	// Best-effort early-out on lock loss: stop sooner, do less redundant advancing.
	// Even if this misses (lock lost just after the check), the CAS + ordering +
	// idempotency still guarantee correctness.
	if cerr := ctx.Err(); cerr != nil {
		logf("producer: lock lost after produce, skip cursor advance (re-produce next tick) table=%s to=%d: %v",
			table, plan.maxID, cerr)
		return plan, 0, cerr
	}

	advanced, err := store.AdvanceCursor(ctx, table, cursor, plan.maxID)
	if err != nil {
		return plan, 0, err
	}
	if !advanced {
		// CAS miss: another holder advanced the cursor (safe close of a lock race).
		// Already-produced messages are covered by message_id idempotency + ES _id
		// upsert; next round resumes from the new watermark — no gaps.
		logf("producer: cursor moved by another writer (CAS miss) table=%s expected=%d new=%d",
			table, cursor, plan.maxID)
		return plan, 0, nil
	}
	logf("producer: chunk produced + cursor advanced table=%s from=%d to=%d main=%d dlq=%d",
		table, cursor, plan.maxID, len(plan.main), len(plan.dlq))
	return plan, plan.stableCount, nil
}
