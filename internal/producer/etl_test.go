package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// fakeStore is an in-memory Store: one shard with rows + a cursor, plus a CAS
// advance. Used to validate the chunk pipeline without a real MySQL.
type fakeStore struct {
	mu       sync.Mutex
	now      int64
	rows     map[string][]*srcMessageRow
	cursor   map[string]int64
	readErr  error
	advErr   error
	advCalls int
}

func newFakeStore(now int64) *fakeStore {
	return &fakeStore{now: now, rows: map[string][]*srcMessageRow{}, cursor: map[string]int64{}}
}

func (f *fakeStore) EnsureCursor(_ context.Context, table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cursor[table]; !ok {
		f.cursor[table] = 0
	}
	return nil
}

func (f *fakeStore) DBNowUnix(context.Context) (int64, error) { return f.now, nil }

func (f *fakeStore) ReadStableBatchTx(_ context.Context, table string, batch int) (int64, []*srcMessageRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return 0, nil, f.readErr
	}
	cur := f.cursor[table]
	var out []*srcMessageRow
	for _, r := range f.rows[table] {
		if r.ID > cur {
			out = append(out, r)
			if len(out) >= batch {
				break
			}
		}
	}
	return cur, out, nil
}

func (f *fakeStore) AdvanceCursor(_ context.Context, table string, expected, newID int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advCalls++
	if f.advErr != nil {
		return false, f.advErr
	}
	if f.cursor[table] != expected {
		return false, nil // CAS miss
	}
	f.cursor[table] = newID
	return true, nil
}

// fakeSink records produced batches.
type fakeSink struct {
	mu       sync.Mutex
	main     []searchmsg.Message
	dlq      []searchmsg.Message
	mainErr  error
	dlqErr   error
	closeErr error
}

func (s *fakeSink) ProduceBatch(_ context.Context, msgs []searchmsg.Message) error {
	if s.mainErr != nil {
		return s.mainErr
	}
	s.mu.Lock()
	s.main = append(s.main, msgs...)
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) ProduceDLQ(_ context.Context, msgs []searchmsg.Message) error {
	if s.dlqErr != nil {
		return s.dlqErr
	}
	s.mu.Lock()
	s.dlq = append(s.dlq, msgs...)
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) Close() error { return s.closeErr }

// fakeLock is an always-acquire lock (single-replica path).
type fakeLock struct {
	acquireOK  bool
	acquireErr error
	renewOK    bool
	renewErr   error
	released   bool
}

func (l *fakeLock) Acquire(string) (bool, error) { return l.acquireOK, l.acquireErr }
func (l *fakeLock) Renew(string) (bool, error)   { return l.renewOK, l.renewErr }
func (l *fakeLock) Release(string) error         { l.released = true; return nil }

func textRow(id int64, content string, createdUnix int64) *srcMessageRow {
	return &srcMessageRow{
		ID: id, MessageID: fmt.Sprintf("%d", 1000+id), ChannelType: 2,
		Payload: []byte(`{"type":1,"content":"` + content + `"}`), CreatedUnix: createdUnix,
	}
}

func newTestETL(store Store, sink Sink, lock RunLock) *ETL {
	return NewETL(ETLDeps{
		Store:         store,
		NewSink:       func() Sink { return sink },
		Lock:          lock,
		Batch:         100,
		Lag:           600,
		RenewInterval: time.Hour, // never fires during the short test
	})
}

// TestRunIncremental_ProducesAndAdvances: stable rows produced, cursor advances.
func TestRunIncremental_ProducesAndAdvances(t *testing.T) {
	now := int64(10_000)
	store := newFakeStore(now)
	// 3 stable rows (created well before cutoff = now-600).
	store.rows["message"] = []*srcMessageRow{
		textRow(1, "a", now-1000), textRow(2, "b", now-1000), textRow(3, "c", now-1000),
	}
	sink := &fakeSink{}
	lock := &fakeLock{acquireOK: true, renewOK: true}
	etl := newTestETL(store, sink, lock)

	if err := etl.RunIncremental(context.Background(), []string{"message"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(sink.main) != 3 {
		t.Fatalf("want 3 produced, got %d", len(sink.main))
	}
	if store.cursor["message"] != 3 {
		t.Fatalf("cursor must advance to 3, got %d", store.cursor["message"])
	}
	if !lock.released {
		t.Fatalf("lock must be released after run")
	}
}

// TestRunIncremental_StabilityGate: unstable tail not produced, cursor stops at stable.
func TestRunIncremental_StabilityGate(t *testing.T) {
	now := int64(10_000)
	store := newFakeStore(now)
	cutoff := now - 600
	store.rows["message"] = []*srcMessageRow{
		textRow(1, "a", cutoff-10), // stable
		textRow(2, "b", cutoff-5),  // stable
		textRow(3, "c", cutoff+50), // UNSTABLE (created after cutoff)
	}
	sink := &fakeSink{}
	etl := newTestETL(store, sink, &fakeLock{acquireOK: true, renewOK: true})
	if err := etl.RunIncremental(context.Background(), []string{"message"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(sink.main) != 2 {
		t.Fatalf("want 2 stable produced, got %d", len(sink.main))
	}
	if store.cursor["message"] != 2 {
		t.Fatalf("cursor must stop at last stable id=2, got %d", store.cursor["message"])
	}
}

// TestRunChunk_ProduceFailNoAdvance: produce error → cursor un-advanced (C2).
func TestRunChunk_ProduceFailNoAdvance(t *testing.T) {
	now := int64(10_000)
	store := newFakeStore(now)
	store.rows["message"] = []*srcMessageRow{textRow(1, "a", now-1000)}
	sink := &fakeSink{mainErr: errors.New("kafka down")}
	etl := newTestETL(store, sink, &fakeLock{acquireOK: true, renewOK: true})
	err := etl.RunIncremental(context.Background(), []string{"message"})
	if err == nil {
		t.Fatalf("expected error on produce failure")
	}
	if store.cursor["message"] != 0 {
		t.Fatalf("cursor must NOT advance on produce failure, got %d", store.cursor["message"])
	}
}

// TestRunIncremental_LockHeldByOther: acquire returns false → skip, no produce.
func TestRunIncremental_LockHeldByOther(t *testing.T) {
	now := int64(10_000)
	store := newFakeStore(now)
	store.rows["message"] = []*srcMessageRow{textRow(1, "a", now-1000)}
	sink := &fakeSink{}
	lock := &fakeLock{acquireOK: false, renewOK: true} // another replica holds it
	etl := newTestETL(store, sink, lock)
	if err := etl.RunIncremental(context.Background(), []string{"message"}); err != nil {
		t.Fatalf("skip should be nil error, got %v", err)
	}
	if len(sink.main) != 0 {
		t.Fatalf("must not produce when lock held by another, got %d", len(sink.main))
	}
	if store.cursor["message"] != 0 {
		t.Fatalf("cursor must not move when lock held by another, got %d", store.cursor["message"])
	}
}

// TestRunIncremental_InProcessReentrancy: concurrent same-process run is skipped.
func TestRunIncremental_InProcessReentrancy(t *testing.T) {
	now := int64(10_000)
	store := newFakeStore(now)
	store.rows["message"] = []*srcMessageRow{textRow(1, "a", now-1000)}

	release := make(chan struct{})
	entered := make(chan struct{})
	blockingSink := &blockingSinkT{release: release, entered: entered}
	etl := NewETL(ETLDeps{
		Store: store, NewSink: func() Sink { return blockingSink },
		Lock: &fakeLock{acquireOK: true, renewOK: true}, Batch: 100, Lag: 600, RenewInterval: time.Hour,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := etl.RunIncremental(context.Background(), []string{"message"}); err != nil {
			t.Errorf("first run: %v", err)
		}
	}()
	<-entered // first run is now inside runTick (sink produced, blocking)

	// Second concurrent run must be skipped by the in-process CAS.
	if err := etl.RunIncremental(context.Background(), []string{"message"}); err != nil {
		t.Fatalf("reentrant run should skip with nil, got %v", err)
	}
	close(release)
	wg.Wait()
	if store.cursor["message"] != 1 {
		t.Fatalf("only the first run should advance cursor, got %d", store.cursor["message"])
	}
}

type blockingSinkT struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (b *blockingSinkT) ProduceBatch(context.Context, []searchmsg.Message) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}
func (b *blockingSinkT) ProduceDLQ(context.Context, []searchmsg.Message) error { return nil }
func (b *blockingSinkT) Close() error                                          { return nil }

// TestPlanChunk_DLQRouting: bad-json row routed to dlq, cursor watermark counts it.
func TestPlanChunk_DLQRouting(t *testing.T) {
	now := int64(10_000)
	cutoff := now - 600
	rows := []*srcMessageRow{
		textRow(1, "ok", cutoff-10),
		{ID: 2, MessageID: "1002", ChannelType: 2, Payload: []byte("{bad"), CreatedUnix: cutoff - 10},
	}
	plan := planChunk(rows, cutoff)
	if len(plan.main) != 1 || len(plan.dlq) != 1 {
		t.Fatalf("want 1 main + 1 dlq, got main=%d dlq=%d", len(plan.main), len(plan.dlq))
	}
	if plan.maxID != 2 || !plan.advanced {
		t.Fatalf("watermark must include the DLQ row id=2, got maxID=%d advanced=%v", plan.maxID, plan.advanced)
	}
}
