package pipeline

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/aura-studio/tango/internal/dao/store"
)

// These tests pin the bounded-backpressure contract that the 2026-10-19 incident
// was missing. Two production shards stopped ingesting for ~50 hours because
// flushBatch retained a batch the server would never accept and retried it every
// second forever; flush() is serial, so the worker never returned and the
// backpressure propagated all the way up to the tailer.
//
// The bound is not merely a stability preference: the source log files live on an
// emptyDir volume and the producer rotates them away after roughly fifteen
// minutes whether or not they were read, so blocking past that window preserves
// nothing and loses everything new.

func stuckBatch(n int) *Batch {
	b := NewBatch(n + 1)
	for i := 0; i < n; i++ {
		b.Add(store.EventWriteModel("track", "stuck-"+string(rune('a'+i)), bson.M{"#uuid": i}))
	}
	return b
}

// TestFlushBatch_GivesUpAfterMaxStuck is the core guarantee: a batch that keeps
// failing is released once the budget elapses, so ingestion resumes instead of
// stalling indefinitely.
func TestFlushBatch_GivesUpAfterMaxStuck(t *testing.T) {
	st, cleanup := unreachableStore(t)
	defer cleanup()

	b := stuckBatch(3)
	stats := &writeErrStats{}

	// ctx is deliberately generous: the give-up must be driven by maxStuck, not
	// by the context ending.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		flushBatch(ctx, st, st.EventCollection(), b, stats, 100*time.Millisecond)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("flushBatch did not give up within maxStuck — the wedge this change exists to remove")
	}
	elapsed := time.Since(start)

	if !b.Empty() {
		t.Errorf("batch still holds %d models after give-up; it must be reset so the worker resumes", b.Len())
	}
	if stats.writeErr == 0 {
		t.Error("expected OnWriteError to fire before giving up")
	}
	// The released documents must be accounted for, or a silent drop is
	// indistinguishable from a healthy pipeline in the stats lines.
	if got := st.Stats().TotalAbandoned(); got != 3 {
		t.Errorf("TotalAbandoned = %d, want 3 (the released batch must be counted)", got)
	}
	// bulkRetryPause is 1s and the store's own backoff budget is 300ms, so the
	// first give-up opportunity comes after roughly one outer retry.
	if elapsed > 10*time.Second {
		t.Errorf("give-up took %v, far longer than maxStuck=100ms plus one retry cycle", elapsed)
	}
	t.Logf("gave up after %v with %d write errors", elapsed, stats.writeErr)
}

// TestFlushBatch_NegativeMaxStuckRetriesForever pins the escape hatch: a negative
// budget restores the pre-fix unbounded behaviour rather than dropping data.
func TestFlushBatch_NegativeMaxStuckRetriesForever(t *testing.T) {
	st, cleanup := unreachableStore(t)
	defer cleanup()

	b := stuckBatch(2)
	stats := &writeErrStats{}

	// Bound the test with ctx, not with maxStuck. flushBatch must still be
	// retrying (batch retained) when the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		flushBatch(ctx, st, st.EventCollection(), b, stats, -1)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("flushBatch hung past the ctx deadline")
	}

	if b.Empty() {
		t.Error("a negative maxStuck must never release the batch")
	}
	if got := st.Stats().TotalAbandoned(); got != 0 {
		t.Errorf("TotalAbandoned = %d, want 0 with unbounded backpressure", got)
	}
	if stats.writeErr < 2 {
		t.Errorf("expected at least 2 retry attempts, got %d", stats.writeErr)
	}
}

// TestFlushBatch_ZeroMaxStuckFallsBackToDefault guards the fail-safe: an unset
// budget must mean "three minutes", not "give up immediately" and not "retry
// forever". RunWorkers is exported and can be handed a Config that never went
// through ApplyDefaults, so flushBatch normalises zero itself.
func TestFlushBatch_ZeroMaxStuckFallsBackToDefault(t *testing.T) {
	if defaultMaxStuckDuration != 3*time.Minute {
		t.Errorf("defaultMaxStuckDuration = %v, want 3m", defaultMaxStuckDuration)
	}

	st, cleanup := unreachableStore(t)
	defer cleanup()

	b := stuckBatch(1)
	stats := &writeErrStats{}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		flushBatch(ctx, st, st.EventCollection(), b, stats, 0)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("flushBatch hung past the ctx deadline")
	}

	// 2.5s is far short of the 3m default, so the batch must still be held and
	// nothing may have been abandoned.
	if b.Empty() {
		t.Error("maxStuck=0 gave the batch up immediately; it must fall back to the 3m default")
	}
	if got := st.Stats().TotalAbandoned(); got != 0 {
		t.Errorf("TotalAbandoned = %d, want 0 before the default budget elapses", got)
	}
}
