package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/aura-studio/tango/internal/dao"
	"github.com/aura-studio/tango/internal/logging"
	"github.com/aura-studio/tango/internal/parser"
	"github.com/aura-studio/tango/internal/process/core"
)

// bulkRetryPause is the coarse cadence between whole-batch write retries while
// MongoDB is unavailable. store.BulkWrite already does fine-grained exponential
// backoff within each attempt (up to MaxElapsedTime); this outer pause keeps
// re-trying the SAME batch — applying backpressure — until it succeeds or
// Config.MaxStuckDuration elapses, at which point flushBatch gives it up rather
// than blocking ingestion indefinitely.
const bulkRetryPause = 1 * time.Second

// statementLogEvery is how often the retry log re-emits the rendered MongoDB
// statement of the stuck batch's first model. With bulkRetryPause at 1s this is
// once a minute: enough to identify the offending document from CloudWatch
// without turning a stall into a firehose of near-duplicate lines.
const statementLogEvery = 60

// drainFlushTimeout bounds the final flush at shutdown: long enough to drain a
// healthy backend, but capped so an unreachable MongoDB cannot hang shutdown
// forever. On give-up the batch is left for re-read on the next start, never
// silently dropped.
const drainFlushTimeout = 30 * time.Second

// RunWorkers launches N workers with affinity-based dispatch and blocks
// until all workers finish. prs carries the parser and its filter holder, whose
// active filter can be hot-swapped while workers run.
func RunWorkers(ctx context.Context, cfg *Config, st *dao.Store,
	prs *parser.Parser,
	lineCh <-chan string, stats core.StatsCollector,
) {
	if stats == nil {
		stats = core.NoopStats{}
	}

	workerCount := cfg.BatchWorkers
	chSize := cfg.ChannelSize()

	// Create per-worker channels for affinity-based routing.
	workerChs := make([]chan string, workerCount)
	for i := range workerChs {
		workerChs[i] = make(chan string, chSize)
	}

	// Launch N workers, each consuming from its own dedicated channel.
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(ch <-chan string) {
			defer wg.Done()
			defer logging.Recover("pipeline worker")
			worker(ctx, cfg, st, prs, ch, stats)
		}(workerChs[i])
	}

	// Dispatcher goroutine: routes lines to workers by user affinity key.
	go func() {
		defer logging.Recover("pipeline dispatch")
		Dispatch(ctx, lineCh, workerChs)
	}()

	wg.Wait()
}

// worker processes lines from a channel, batches them, and flushes to MongoDB.
// Per-line parse/filter/identity/route rules live in core.Processor; the
// worker owns only batching, the dynamic flush cadence, and the affinity-local
// dead-letter logging.
func worker(ctx context.Context, cfg *Config, st *dao.Store,
	prs *parser.Parser,
	lineCh <-chan string, stats core.StatsCollector,
) {
	proc := core.NewProcessor(prs, st, stats)

	userBatch := NewBatch(cfg.MaxBatchSize())
	eventBatch := NewBatch(cfg.MaxBatchSize())
	deadBatch := NewBatch(cfg.DeadLetterCap)

	lastFlush := time.Now()
	flushInterval := cfg.FlushInterval
	invalidCount := 0

	// flush writes accumulated batches to MongoDB and resets them. All batches use
	// unordered writes: the user write models carry their own per-document _ts
	// guard (see store/writemodel.go), so intra-batch ordering no longer affects
	// the final state, and unordered writes let one benign duplicate-key skip not
	// abort the rest of the batch.
	flush := func(flushCtx context.Context) {
		flushBatch(flushCtx, st, st.UserCollection(), userBatch, stats, cfg.MaxStuckDuration)
		flushBatch(flushCtx, st, st.EventCollection(), eventBatch, stats, cfg.MaxStuckDuration)
		flushBatch(flushCtx, st, st.DeadLetterCollection(), deadBatch, stats, cfg.MaxStuckDuration)
		lastFlush = time.Now()
	}

	// drainFlush is the shutdown flush: a fresh, bounded context so the final
	// write can still run after the pipeline ctx is cancelled, yet cannot block
	// shutdown indefinitely if MongoDB is unreachable.
	drainFlush := func() {
		dctx, cancel := context.WithTimeout(context.Background(), drainFlushTimeout)
		defer cancel()
		flush(dctx)
	}

	// Use a ticker to ensure batches are flushed even when no new lines arrive.
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case line, ok := <-lineCh:
			if !ok {
				// Channel closed (tailer stopped): drain remaining data.
				drainFlush()
				return
			}

			res := proc.Process(ctx, line)
			switch res.Kind {
			case core.KindParseError:
				invalidCount++
				if invalidCount%1000 == 0 {
					logging.WithError(res.Err).Warnf("dropped invalid line (total invalid=%d)", invalidCount)
				}
				deadBatch.Add(res.Model)
				if deadBatch.Full() || time.Since(lastFlush) >= flushInterval {
					flush(ctx)
				}
				continue
			case core.KindIdentityError:
				logging.WithError(res.Err).Warn("identity resolve failed, sending to dead letter")
				deadBatch.Add(res.Model)
				continue
			case core.KindFiltered:
				continue
			case core.KindUser:
				userBatch.Add(res.Model)
			case core.KindEvent:
				eventBatch.Add(res.Model)
			}

			// Flush on dynamic batch threshold (derived from current backlog)
			// or time-interval triggers.
			backlog := len(lineCh)
			threshold := ComputeFlushThreshold(
				cfg.MinBatchSize(),
				cfg.BatchSize,
				cfg.MaxBatchSize(),
				backlog,
				cfg.ChannelSize(),
			)
			needFlush := userBatch.Len() >= threshold ||
				eventBatch.Len() >= threshold ||
				time.Since(lastFlush) >= flushInterval
			if needFlush {
				flush(ctx)
			}

		case <-flushTicker.C:
			// Periodic flush to ensure data is written even when idle.
			if !userBatch.Empty() || !eventBatch.Empty() || !deadBatch.Empty() {
				flush(ctx)
			}

		case <-ctx.Done():
			// Final flush after cancellation, on a bounded fresh context.
			drainFlush()
			return
		}
	}
}

// flushBatch writes a batch to the given collection (unordered). On success the
// batch is reset.
//
// On failure the batch is retained and retried, which applies backpressure up the
// pipeline: the worker stops draining its channel, and the dispatcher and tailer
// block behind it. That backpressure is BOUNDED by maxStuck. Once the batch has
// been failing for that long it is handed to Store.GiveUp — isolated into
// dead_letter on a best-effort basis, counted in WriteStats.Abandoned, logged
// with a data preview — and then reset so ingestion resumes. A negative maxStuck
// retries forever, restoring the pre-fix behaviour; it exists only as an escape
// hatch.
//
// The bound is not a compromise, it is the correct behaviour for this
// deployment. The old comment here claimed that blocking causes "log files [to]
// accumulate on disk" and that a restart re-reads them, so nothing is lost. That
// is false as deployed: the source lives on an emptyDir volume and the producer
// rotates files away after roughly fifteen minutes whether or not tango read
// them. Holding a batch past that window therefore preserves nothing while
// preventing every subsequent line from being read — strictly worse than
// releasing it. Three minutes is far longer than a DocumentDB failover and far
// shorter than the retention window.
//
// The ctx-done path is unchanged: at shutdown the batch is still retained rather
// than dropped, because a restart inside the retention window does re-read from
// the head and the write models dedup by #uuid / _ts.
func flushBatch(ctx context.Context, st *dao.Store,
	coll *mongo.Collection, b *Batch, stats core.StatsCollector, maxStuck time.Duration,
) {
	if b.Empty() {
		return
	}
	// Fail safe on an unset budget: RunWorkers is exported and can be handed a
	// Config that never went through ApplyDefaults, and losing the safety net
	// silently is exactly how a wedge runs for two days unnoticed.
	if maxStuck == 0 {
		maxStuck = defaultMaxStuckDuration
	}
	var stuckSince time.Time
	var failures int
	for {
		if err := st.BulkWrite(ctx, coll, b.Models); err == nil {
			b.Reset()
			return
		} else {
			failures++
			stats.OnWriteError()
			if stuckSince.IsZero() {
				stuckSince = time.Now()
			}
			fields := logging.Fields{
				"collection": coll.Name(),
				"batch":      b.Len(),
				"failures":   failures,
				// stuck_for and failures are what make a wedge visible: the
				// 2026-10-19 incident repeated this line ~35,000 times over two
				// days and nothing in it said how long ingestion had been
				// stopped, or which document was being refused.
				"stuck_for": time.Since(stuckSince).Round(time.Second),
			}
			// Name the stuck document on the first failure and then once a minute
			// (bulkRetryPause is 1s). Logging it on every retry would push ~500 B/s
			// per stuck worker into CloudWatch and bury the very signal it exists
			// to provide; the store's own "bulk write failed after retries" line
			// carries it on every backoff exhaustion in the meantime.
			if failures == 1 || failures%statementLogEvery == 0 {
				fields["first_statement"] = st.RenderFirstModel(coll, b.Models)
			}
			logging.WithError(err).WithFields(fields).
				Error("bulk write failed; retaining batch and retrying (pipeline backpressure)")
		}
		if ctx.Err() != nil {
			// Shutdown / drain deadline reached while still failing. Do NOT drop:
			// the retained lines are re-read from the source head on the next
			// start (idempotent by #uuid / _ts), for as long as those files still
			// exist on the shared volume.
			logging.WithField("collection", coll.Name()).
				WithField("batch", b.Len()).
				Warn("bulk write still failing at shutdown; batch left for re-read on restart (not dropped)")
			return
		}
		if maxStuck > 0 && time.Since(stuckSince) >= maxStuck {
			st.GiveUp(ctx, coll, b.Models, fmt.Sprintf(
				"batch stuck for %s (maxStuckDuration=%s)",
				time.Since(stuckSince).Round(time.Second), maxStuck))
			b.Reset()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(bulkRetryPause):
		}
	}
}
