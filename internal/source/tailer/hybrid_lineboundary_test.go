package tailer

// End-to-end line-boundary coverage for the HYBRID path — the production default
// (tailer.Config.ApplyDefaults sets TailModeHybrid, and the rocket-nano profile
// sets no tailMode).
//
// lineboundary_repro_test.go pins the same defect through the two polling read
// paths individually: drainByPoll called directly, and readFollowFile driven as
// the long-running poll reader. Neither goes through tailFileHybrid, which is the
// path that actually runs in production and the only place the two mechanisms
// hand a file offset to each other:
//
//	Phase 1  hpcloud/tail  -- advances *lastSize per delivered line
//	   |  ticker sees fi.Size() > *lastSize => "stalled"
//	   v
//	Phase 2  drainByPoll   -- re-seeks to *lastSize and drains what Phase 1 missed
//	   |
//	   +-- loop back to Phase 1 at the updated *lastSize
//
// A defect in either half is invisible to the other half's unit test, and a
// mismatch in how they interpret *lastSize is invisible to both. This file drives
// the whole loop through the exported Run surface.
//
// These are not Linux-gated: nothing here depends on /proc or deleted-but-open
// semantics, so they run — and must pass — on the Windows host too.

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// hybridTestTailer builds a hybrid tailer over dir/*.log with fast discovery.
// hybridPollInterval is a package constant (500ms) and deliberately not tunable,
// so tests that need to observe a Phase 1 -> Phase 2 transition must budget for
// it; everything else is shortened here.
func hybridTestTailer(dir string) *Tailer {
	return New([]string{filepath.Join(dir, "*.log")}, 50*time.Millisecond, TailModeHybrid).
		WithTuning(50*time.Millisecond, 1<<20)
}

func (s *lineSink) countOf(line string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count[line]
}

// TestHybrid_PartialLineIsWithheldAndReassembled is the production-path twin of
// TestReproBug1_*. A writer caught mid-line must not have its fragment forwarded,
// and when it finishes the line the whole record must arrive exactly once.
//
// This is the defect behind the routine production parse errors: one physical line
// emitted as two fragments explains why 780 of the 781 minutes containing parse
// errors contained an EVEN count, almost always exactly 2. A fragment split at an
// arbitrary byte offset can also land inside a multi-byte UTF-8 rune, which is the
// only mechanism found that puts invalid UTF-8 into a line — and that is what made
// a dead letter unwritable and wedged two shards for ~50 hours on 2026-10-19.
func TestHybrid_PartialLineIsWithheldAndReassembled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ta.log")
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatalf("create %s: %v", path, err)
	}

	tl := hybridTestTailer(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := drain(tl.Run(ctx))

	// A complete line, then half of the next one — the ordinary state of a file
	// being appended to, and what a reader sees when it catches the writer mid-line.
	appendRaw(t, path, `{"seq":1}`+"\n")
	sink.waitForLine(t, `{"seq":1}`, 8*time.Second)

	appendRaw(t, path, `{"seq":2,"na`)
	// Span several hybridPollInterval ticks so both Phase 1 (which must keep
	// seeking back over the half-line) and Phase 2 (which must not drain it) get
	// multiple chances to leak the fragment.
	time.Sleep(1500 * time.Millisecond)
	if sink.has(`{"seq":2,"na`) {
		t.Fatal("hybrid forwarded the partial record; it must be withheld until the line is complete")
	}

	// Finish the line and add one more.
	appendRaw(t, path, `me":"x"}`+"\n"+`{"seq":3}`+"\n")
	sink.waitForLine(t, `{"seq":3}`, 8*time.Second)

	if n := sink.countOf(`{"seq":2,"name":"x"}`); n != 1 {
		t.Errorf(`the reassembled line was delivered %d times, want exactly 1`, n)
	}
	if sink.has(`{"seq":2,"na`) {
		t.Error(`the leading fragment was delivered as its own line`)
	}
	if sink.has(`me":"x"}`) {
		t.Error(`the trailing remainder was delivered as its own line — one physical line became two`)
	}
	for _, want := range []string{`{"seq":1}`, `{"seq":3}`} {
		if n := sink.countOf(want); n != 1 {
			t.Errorf("%s delivered %d times, want 1", want, n)
		}
	}
}

// TestHybrid_PartialTailDoesNotLeakWhileWithholding guards the cost of
// withholding. Because *lastSize now stops in FRONT of an unterminated tail, the
// stall test in tailFileHybridEvent (fi.Size() > *lastSize) stays true for a file
// that ends mid-line and stops growing, so the loop cycles Phase 1 -> Phase 2 at
// roughly 2 Hz, rebuilding the hpcloud/tail watcher each time. That churn is
// accepted — it is bounded, silent at the production log level, and it only runs
// while a producer has stopped mid-line, which rotation caps at minutes — but it
// must not accumulate. This asserts the same invariant as TestGoroutine_D4, on the
// churn path instead of the rotation path, and without D4's Linux gate.
func TestHybrid_PartialTailDoesNotLeakWhileWithholding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ta.log")
	// Complete lines plus a fragment the writer never finishes.
	if err := os.WriteFile(path, []byte("s1\ns2\ns3"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	tl := hybridTestTailer(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := drain(tl.Run(ctx))
	sink.waitForLine(t, "s2", 8*time.Second)

	// Let the loop settle into the churn before taking a baseline, so the
	// comparison is steady-state against steady-state.
	time.Sleep(1200 * time.Millisecond)
	runtime.GC()
	baseGo := runtime.NumGoroutine()

	time.Sleep(4 * time.Second) // ~8 more Phase 1 rebuilds

	var nowGo int
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		nowGo = runtime.NumGoroutine()
		if nowGo <= baseGo+5 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if nowGo > baseGo+5 {
		t.Fatalf("goroutines grew from %d to %d while withholding a partial tail: the "+
			"Phase 1 rebuild loop leaks", baseGo, nowGo)
	}

	// The withheld fragment must still be withheld, and nothing duplicated.
	if sink.has("s3") {
		t.Error("the unterminated tail s3 was forwarded")
	}
	for _, want := range []string{"s1", "s2"} {
		if n := sink.countOf(want); n != 1 {
			t.Errorf("%s delivered %d times during the churn, want 1", want, n)
		}
	}

	// And the churn must not have wedged the file: finishing the line delivers it.
	appendRaw(t, path, "-finished\n")
	sink.waitForLine(t, "s3-finished", 8*time.Second)
}
