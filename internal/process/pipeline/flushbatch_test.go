package pipeline

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/aura-studio/tango/internal/dao/store"
)

// writeErrStats counts OnWriteError; every other StatsCollector callback is a
// no-op. (core.NoopStats can't observe the write-error count we assert on.)
type writeErrStats struct{ writeErr int }

func (writeErrStats) OnLine()          {}
func (writeErrStats) OnParseOK()       {}
func (writeErrStats) OnParseError()    {}
func (writeErrStats) OnIdentityError() {}
func (writeErrStats) OnUserWrite()     {}
func (writeErrStats) OnEventWrite()    {}
func (writeErrStats) OnDeadLetter()    {}
func (s *writeErrStats) OnWriteError() { s.writeErr++ }
func (writeErrStats) OnFiltered()      {}
func (writeErrStats) OnFilterError()   {}

// unreachableStore builds a Store pointed at an unreachable MongoDB with a short
// server-selection timeout, so every BulkWrite fails quickly without a live
// server. mongo.Connect is lazy (no ping), so construction itself succeeds.
func unreachableStore(t *testing.T) (*store.Store, func()) {
	t.Helper()
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/llb5").
		SetServerSelectionTimeout(200 * time.Millisecond).
		SetConnectTimeout(200 * time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	st := store.New(client.Database("llb5"), &store.Config{MaxElapsedTime: 300 * time.Millisecond})
	return st, func() { _ = client.Disconnect(context.Background()) }
}

// TestFlushBatch_RetainsBatchOnWriteFailure pins the B5 fix: a persistent write
// failure must NOT drop the batch (the pre-fix code reset it unconditionally),
// and flushBatch must stop retrying when ctx is done rather than hang forever.
// maxStuck is negative here, i.e. the "retry forever" escape hatch, so this test
// keeps asserting the unbounded-backpressure behaviour it was written for; the
// bounded case lives in flushbatch_stuck_test.go.
func TestFlushBatch_RetainsBatchOnWriteFailure(t *testing.T) {
	st, cleanup := unreachableStore(t)
	defer cleanup()

	b := NewBatch(8)
	b.Add(store.EventWriteModel("track", "b5-1", bson.M{"#uuid": "b5-1"}))
	b.Add(store.EventWriteModel("track", "b5-2", bson.M{"#uuid": "b5-2"}))
	want := b.Len()

	stats := &writeErrStats{}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		flushBatch(ctx, st, st.EventCollection(), b, stats, -1)
	}()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("flushBatch did not return after ctx deadline — it hung on a failing write")
	}

	if b.Len() != want {
		t.Fatalf("batch dropped on write failure: len=%d, want %d retained (B5 regression)", b.Len(), want)
	}
	if stats.writeErr == 0 {
		t.Error("expected OnWriteError to fire on a failing bulk write")
	}
}

// TestFlushBatch_ResetsOnEmpty is a fast guard that an empty batch is a no-op
// (no write attempted, no hang) even against an unreachable store.
func TestFlushBatch_EmptyIsNoop(t *testing.T) {
	st, cleanup := unreachableStore(t)
	defer cleanup()

	stats := &writeErrStats{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		flushBatch(context.Background(), st, st.EventCollection(), NewBatch(4), stats, -1)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("flushBatch on an empty batch should return immediately")
	}
	if stats.writeErr != 0 {
		t.Errorf("empty batch must not attempt a write, got %d write errors", stats.writeErr)
	}
}
