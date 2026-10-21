package daemon

// The stats lines are the only always-on signal an operator has that ingestion is
// unhealthy. The 2026-10-19 wedge ran for ~50 hours across two production shards
// with nothing but a repeating "retaining batch and retrying" error line and a
// rising total_write_err to show for it — no counter said "documents were
// skipped" and nothing said how long ingestion had been stalled.

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/aura-studio/tango/internal/source/tailer"
)

// TestStatsLines_CarryStoreCounters pins that the periodic and cumulative stats
// lines expose the store-level counters, so "data was skipped" is directly
// alertable from CloudWatch without parsing free-text error messages.
func TestStatsLines_CarryStoreCounters(t *testing.T) {
	daoCfg, _, cleanup := freshDaemonDB(t)
	defer cleanup()

	restore := statsReportInterval
	statsReportInterval = 300 * time.Millisecond
	defer func() { statsReportInterval = restore }()

	capture := installCapture(t)

	svc, logPath, _ := buildDaemon(t, daoCfg, &tailer.Config{
		TailMode:       tailer.TailModePoll,
		RescanInterval: 100 * time.Millisecond,
		PollInterval:   25 * time.Millisecond,
	})
	defer svc.Shutdown()

	appendEvents(t, logPath, 5)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(ctx) }()

	deadline := time.Now().Add(6 * time.Second)
	var periodic, cumulative logrus.Entry
	var gotPeriodic, gotCumulative bool
	for time.Now().Before(deadline) {
		periodic, gotPeriodic = capture.find("periodic stats")
		cumulative, gotCumulative = capture.find("cumulative stats")
		if gotPeriodic && gotCumulative {
			break
		}
		time.Sleep(80 * time.Millisecond)
	}
	cancel()
	<-runDone

	if !gotPeriodic {
		t.Fatal("no 'report: periodic stats' line logged within the window")
	}
	if !gotCumulative {
		t.Fatal("no 'report: cumulative stats' line logged within the window")
	}

	for _, field := range []string{"interval_quarantined", "interval_abandoned", "interval_retries"} {
		if _, has := periodic.Data[field]; !has {
			t.Errorf("periodic stats line missing field %q (have %v)", field, periodic.Data)
		}
	}
	for _, field := range []string{"total_quarantined", "total_abandoned", "total_retries"} {
		if _, has := cumulative.Data[field]; !has {
			t.Errorf("cumulative stats line missing field %q (have %v)", field, cumulative.Data)
		}
	}

	// A healthy run must not report skipped data: a non-zero baseline here would
	// make the counters useless as an alert trigger.
	for _, field := range []string{"interval_quarantined", "interval_abandoned"} {
		if v, _ := periodic.Data[field].(int64); v != 0 {
			t.Errorf("%s = %d on a healthy run, want 0", field, v)
		}
	}
}
