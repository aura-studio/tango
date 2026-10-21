// Package daemon implements the long-running report service, orchestrating the
// tango pipeline: file tailing -> line parsing -> batch accumulation ->
// MongoDB bulk writes.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/aura-studio/tango/internal/cfgsync"
	"github.com/aura-studio/tango/internal/dao"
	"github.com/aura-studio/tango/internal/logging"
	"github.com/aura-studio/tango/internal/parser"
	"github.com/aura-studio/tango/internal/process"
	"github.com/aura-studio/tango/internal/source"
)

// statsReportInterval is how often the report service logs processing statistics
// and runs the fd watchdog check. It is a var (not a const) solely so tests can
// shrink it to avoid a 60s wait; production never overrides it.
var statsReportInterval = 60 * time.Second

// fdWatchdogTriggered reports whether the open-fd watchdog should fire: only when
// a positive threshold is configured AND the current open-fd count strictly
// exceeds it. A non-positive threshold (default/disabled) never fires, and an
// unknown count (openFDs == -1 off Linux) never fires because -1 is not > any
// threshold >= 1.
func fdWatchdogTriggered(openFDs, threshold int) bool {
	return threshold > 0 && openFDs > threshold
}

// Service is the main runtime that connects all components together. It is built
// from the module configs the daemon needs (dao + parser + source + process),
// not from the top-level config package. It depends only on the source package,
// reaching the file-tailing config through srcCfg.Tailer rather than importing
// source/tailer directly.
type Service struct {
	dao        *dao.Dao
	parser     *parser.Parser
	srcCfg     *source.Config
	procCfg    *process.Config
	cfgsyncCfg *cfgsync.Config
}

// New connects to MongoDB and creates a ready-to-run Service from the dao,
// parser, source, process, and cfgsync module configs. cfgsyncCfg may be nil
// (cfgsync disabled). The caller must call Shutdown after Run returns to
// disconnect from MongoDB.
func New(ctx context.Context, daoCfg *dao.Config, parserCfg *parser.Config, srcCfg *source.Config, procCfg *process.Config, cfgsyncCfg *cfgsync.Config) (*Service, error) {
	if srcCfg == nil || srcCfg.Tailer == nil {
		return nil, fmt.Errorf("report: source.tailer configuration is required")
	}
	if procCfg == nil {
		procCfg = &process.Config{}
	}
	procCfg.ApplyDefaults()

	p, err := parserCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("report: %w", err)
	}

	da, err := dao.New(ctx, daoCfg)
	if err != nil {
		return nil, fmt.Errorf("report: %w", err)
	}

	return &Service{dao: da, parser: p, srcCfg: srcCfg, procCfg: procCfg, cfgsyncCfg: cfgsyncCfg}, nil
}

// Shutdown disconnects the MongoDB client. It must be called after Run returns
// to ensure all final flushes complete before the connection is closed.
func (d *Service) Shutdown() error {
	return d.dao.Mongo.Close()
}

// EnsureIndexes creates all required MongoDB indexes (idempotent).
func (d *Service) EnsureIndexes(ctx context.Context) error {
	return d.dao.Store.EnsureIndexes(ctx)
}

// Run starts the daemon pipeline and blocks until ctx is cancelled.
//
// Flow: tailer -> lineCh -> dispatcher (routes by user affinity) -> workerCh[i] -> worker_i -> MongoDB
//
// The dispatcher extracts #account_id (preferred) or #distinct_id from each line
// and consistently hashes it to a fixed worker, so on the common path all
// operations for one user are handled in order by a single worker. This affinity
// is best-effort: under backpressure (the target worker's channel is full) the
// dispatcher spills the line to another worker to avoid head-of-line blocking,
// so strict cross-worker ordering is not guaranteed. Correctness does not rely
// on it — the write models use _ts conditional updates, so an older record can
// never overwrite a newer one regardless of which worker applies it.
func (d *Service) Run(ctx context.Context) error {
	tcfg := d.srcCfg.Tailer
	if len(tcfg.LogPattern) == 0 {
		return errors.New("report: source.tailer.logPattern is required (at least one regex)")
	}

	logging.WithFields(logging.Fields{
		"log_patterns":   tcfg.LogPattern,
		"workers":        d.procCfg.Pipeline.BatchWorkers,
		"batch_size":     d.procCfg.Pipeline.BatchSize,
		"flush_interval": d.procCfg.Pipeline.FlushInterval,
		"tail_mode":      tcfg.TailMode,
	}).Info("report: starting pipeline")

	// Derive a cancellable child context so the fd watchdog (in reportStats) can
	// trigger a graceful self-restart: cancelling runCtx drains and flushes the
	// pipeline, Run returns cleanly, the process exits, and the orchestrator's
	// restart policy recreates the container with a fresh fd table. A SIGTERM on
	// the parent ctx cancels runCtx too, so the normal shutdown path is unchanged.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// Build the tailer source via the source facade; the pipeline uploader runs it.
	t := source.NewTailer(tcfg)

	// Create stats collector for periodic reporting.
	stats := &process.Counters{}
	startTime := time.Now()

	// Launch periodic stats reporter (also hosts the fd watchdog).
	reportDone := make(chan struct{})
	go d.reportStats(runCtx, t, stats, startTime, cancelRun, reportDone)

	// Launch the runtime config-sync watcher (opt-in): it hot-swaps the live
	// reporting filter from the central config document. It shares runCtx, so
	// it stops when the daemon does, and runs under its own panic recover like
	// the stats reporter above.
	//
	// Pull-before-ingest gate: with cfgsync ENABLED the daemon must not ingest a
	// single line before the central config has been pulled and applied —
	// otherwise the startup window (tailer re-reads whole existing logs
	// immediately) would run on the baseline/empty filter and could flood the
	// store unfiltered. Ready() only fires once a published document has been
	// applied, so "nothing published yet" keeps the daemon waiting (fail-closed)
	// with a periodic warning instead of ingesting everything (fail-open).
	if w := d.startCfgsync(runCtx); w != nil {
		logging.Info("report: cfgsync enabled — waiting for the central config before starting ingestion")
		waitLog := time.NewTicker(30 * time.Second)
		waiting := true
		for waiting {
			select {
			case <-w.Ready():
				logging.Info("report: central config applied — starting ingestion")
				waiting = false
			case <-runCtx.Done():
				waitLog.Stop()
				<-reportDone
				d.logFinalStats(stats, startTime)
				return nil
			case <-waitLog.C:
				logging.Warn("report: cfgsync enabled but no central config applied yet — daemon is NOT ingesting; " +
					"publish a config document (gateway POST /config, cli function=config, or api.PublishConfig) or disable cfgsync")
			}
		}
		waitLog.Stop()
	}

	// Drive the async pipeline strategy; it blocks until runCtx is cancelled,
	// then the workers flush and exit.
	procCfg := *d.procCfg
	procCfg.Mode = string(process.ModePipeline)
	up, err := process.New(&procCfg, d.dao, d.parser, stats)
	if err != nil {
		return err
	}
	if err := up.Run(runCtx, t); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	// Wait for the reporter goroutine to exit.
	<-reportDone

	// Log final stats summary.
	d.logFinalStats(stats, startTime)

	return nil
}

// startCfgsync launches the cfgsync Watcher goroutine when cfgsync is enabled
// and returns the Watcher so Run can gate ingestion on its Ready() signal (nil
// when cfgsync is disabled — no gate). The Watcher feeds the central config
// document's filter subtree into the live parser filter (atomic Holder swap),
// keeping last-good on a bad config. The goroutine exits when ctx is cancelled;
// a fatal watcher error (e.g. backend=changestream on an unsupported topology)
// is logged rather than taking the daemon down — note that with the
// pull-before-ingest gate this leaves the daemon waiting (visible via the ERROR
// here plus the gate's periodic warning), which is the intended fail-closed
// behaviour rather than ingesting unfiltered.
func (d *Service) startCfgsync(ctx context.Context) *cfgsync.Watcher {
	if d.cfgsyncCfg == nil || !d.cfgsyncCfg.Enabled {
		return nil
	}
	reg := cfgsync.NewRegistry()
	cfgsync.RegisterFilter(reg, d.parser)
	w := cfgsync.New(d.dao, d.cfgsyncCfg, reg.Apply)
	go func() {
		defer logging.Recover("cfgsync watcher")
		if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logging.WithError(err).Error("cfgsync: watcher exited with error")
		}
	}()
	return w
}

// reportStats periodically logs processing statistics every statsReportInterval.
// It also hosts the fd watchdog: when source.tailer.maxOpenFDs is set and the
// process's open fd count exceeds it, triggerRestart cancels the run context to
// perform a graceful self-restart (see Run). tailerSrc is the live tailer,
// queried for its tailed-file count (a direct proxy for open log fds).
func (d *Service) reportStats(ctx context.Context, tailerSrc source.Source, stats *process.Counters, startTime time.Time, triggerRestart context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	defer logging.Recover("daemon stats reporter")

	ticker := time.NewTicker(statsReportInterval)
	defer ticker.Stop()

	var prev process.Snapshot
	// Store-level counters are read straight off the store rather than plumbed
	// through core.StatsCollector: they are cumulative atomics owned by the store,
	// and growing that interface would force a change in all five of its
	// implementations (including three test fakes) for no benefit.
	var prevQuarantined, prevAbandoned, prevRetries int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := stats.Snapshot()

			uptime := time.Since(startTime).Round(time.Second)

			ws := d.dao.Store.Stats()
			quarantined := ws.TotalQuarantined()
			abandoned := ws.TotalAbandoned()
			retries := ws.TotalRetries()

			logging.WithFields(logging.Fields{
				"uptime":                 uptime,
				"interval_lines":         cur.TotalLines - prev.TotalLines,
				"interval_parsed_ok":     cur.ParsedOK - prev.ParsedOK,
				"interval_parse_err":     cur.ParseErrors - prev.ParseErrors,
				"interval_identity_err":  cur.IdentityErrors - prev.IdentityErrors,
				"interval_user_writes":   cur.UserWrites - prev.UserWrites,
				"interval_event_writes":  cur.EventWrites - prev.EventWrites,
				"interval_dead_letters":  cur.DeadLetters - prev.DeadLetters,
				"interval_write_err":     cur.WriteErrors - prev.WriteErrors,
				"interval_filtered":      cur.Filtered - prev.Filtered,
				"interval_filter_err":    cur.FilterErrors - prev.FilterErrors,
				"interval_quarantined": quarantined - prevQuarantined,
				"interval_abandoned":   abandoned - prevAbandoned,
				"interval_retries":     retries - prevRetries,
			}).Info("report: periodic stats (last 60s)")

			logging.WithFields(logging.Fields{
				"total_lines":        cur.TotalLines,
				"total_parsed_ok":    cur.ParsedOK,
				"total_parse_err":    cur.ParseErrors,
				"total_identity_err": cur.IdentityErrors,
				"total_user_writes":  cur.UserWrites,
				"total_event_writes": cur.EventWrites,
				"total_dead_letters": cur.DeadLetters,
				"total_write_err":    cur.WriteErrors,
				"total_filtered":     cur.Filtered,
				"total_filter_err":   cur.FilterErrors,
				"total_quarantined":  quarantined,
				"total_abandoned":    abandoned,
				"total_retries":      retries,
			}).Info("report: cumulative stats")

			// Runtime / resource stats. open_fds is -1 on non-Linux (and the
			// watchdog is inert there). tailed_files tracks live tail
			// goroutines, the most direct signal of an fd leak.
			goroutines := runtime.NumGoroutine()
			openFDs := openFDCount()
			tailedFiles := -1
			if tc, ok := tailerSrc.(interface{ TailedCount() int }); ok {
				tailedFiles = tc.TailedCount()
			}
			logging.WithFields(logging.Fields{
				"goroutines":   goroutines,
				"open_fds":     openFDs,
				"tailed_files": tailedFiles,
			}).Info("report: runtime stats")

			// fd watchdog: graceful self-restart when fds exceed the threshold.
			if threshold := d.srcCfg.Tailer.MaxOpenFDs; fdWatchdogTriggered(openFDs, threshold) {
				logging.WithFields(logging.Fields{
					"open_fds":     openFDs,
					"threshold":    threshold,
					"goroutines":   goroutines,
					"tailed_files": tailedFiles,
				}).Error("report: open fd count exceeded threshold — triggering graceful restart (drain, flush, exit for orchestrator to recreate the container)")
				triggerRestart()
				return
			}

			prev = cur
			prevQuarantined, prevAbandoned, prevRetries = quarantined, abandoned, retries
		}
	}
}

// logFinalStats logs a final summary when the daemon is shutting down.
func (d *Service) logFinalStats(stats *process.Counters, startTime time.Time) {
	cur := stats.Snapshot()

	duration := time.Since(startTime).Round(time.Second)

	ws := d.dao.Store.Stats()
	quarantined := ws.TotalQuarantined()
	abandoned := ws.TotalAbandoned()

	logging.Info("report: ========== shutdown summary ==========")
	logging.WithFields(logging.Fields{
		"total_lines":        cur.TotalLines,
		"total_parsed_ok":    cur.ParsedOK,
		"total_parse_errors": cur.ParseErrors,
		"total_identity_err": cur.IdentityErrors,
		"total_user_writes":  cur.UserWrites,
		"total_event_writes": cur.EventWrites,
		"total_dead_letters": cur.DeadLetters,
		"total_write_errors": cur.WriteErrors,
		"total_filtered":     cur.Filtered,
		"total_filter_err":   cur.FilterErrors,
		"total_quarantined":  quarantined,
		"total_abandoned":    abandoned,
		"total_retries":      ws.TotalRetries(),
		"uptime":             duration,
	}).Info("report: final stats")

	if cur.TotalLines > 0 && duration.Seconds() > 0 {
		lps := float64(cur.TotalLines) / duration.Seconds()
		logging.WithField("lines_per_second", fmt.Sprintf("%.1f", lps)).Info("report: average throughput")
	}

	// Quarantined and abandoned documents are data the server refused or data we
	// released to protect ingestion; either means the run was not clean even when
	// no write error is currently outstanding.
	if cur.ParseErrors > 0 || cur.IdentityErrors > 0 || cur.WriteErrors > 0 || quarantined > 0 || abandoned > 0 {
		logging.Warn("report: ========== SHUTDOWN WITH ERRORS ==========")
	} else {
		logging.Info("report: ========== SHUTDOWN COMPLETE ==========")
	}
}
