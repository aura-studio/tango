package tailer

// REGRESSION TESTS — landed intentionally red, now green.
//
// These were committed at 177b7fd as reproduction-only, expected to FAIL, pinning
// three real defects in how the tailer tracks line boundaries. d527267 fixed the
// code (lineScanner plus the offset-commit order in drainByPoll) without touching
// this file, so the "want" values below are now the contract. Do not relax them to
// make a failure go away: each was measured red against the pre-fix implementation,
// and re-mutating the fix — emitting the unterminated tail, or returning
// scanner.Err() before committing *lastSize — turns the corresponding test red
// again.
//
// Coverage split: these drive the two polling read paths in isolation
// (drainByPoll directly, readFollowFile as the long-running poll reader).
// hybrid_lineboundary_test.go covers the production default, where the two
// mechanisms hand *lastSize to each other.
//
// Why these matter: they are the origin of the routine parse errors seen in
// production (one shard logged 602 against 145,912,291 lines, ~1 in 242,379).
// Fleet-wide over 8 days, 781 minutes reported a non-zero interval_parse_err and
// 780 of them reported an EVEN count — almost always exactly 2. One physical log
// line being emitted as two fragments explains that pairing exactly. None of this
// was logged: the split is silent, which is why the tailer emitted zero warnings
// across 30 days and ~13 pods while still producing bad lines.
//
// A fragment split at an arbitrary byte offset can also land in the middle of a
// multi-byte UTF-8 rune, which is the only mechanism found so far that puts
// invalid UTF-8 into a line — and that is what made a dead letter unwritable and
// wedged two production shards for ~50 hours on 2026-10-19.

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestTailer builds a Tailer with a fast poll interval. drainByPoll and
// readFollowFile do not consult patterns or rescanInterval, so those are dummies.
func newTestTailer(maxLineBytes int) *Tailer {
	return New([]string{"unused-*.log"}, time.Hour, TailModePoll).
		WithTuning(10*time.Millisecond, maxLineBytes)
}

func writeRaw(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func appendRaw(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s for append: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

// collectDrained takes everything currently buffered on out. drainByPoll is
// synchronous, so once it returns every line it emitted is already in a
// sufficiently large buffered channel.
func collectDrained(out chan string) []string {
	var got []string
	for {
		select {
		case line := <-out:
			got = append(got, line)
		default:
			return got
		}
	}
}

// ---------------------------------------------------------------------------
// BUG 1 — a partial trailing line is emitted as if complete, and the offset is
// committed past it, so the rest of the line becomes a second "line".
//
// drainByPoll (tailer.go:754-790) scans from *lastSize to EOF and forwards every
// token. bufio.ScanLines returns an unterminated final line as a token at EOF, so
// a half-written record is forwarded — and then lines 784-788 set *lastSize to
// EOF, committing the offset past the fragment. When the writer finishes the
// line, the next read starts after the fragment and emits the remainder on its
// own. One physical line becomes exactly two unparseable fragments.
//
// The comment at tailer.go:485-488 promises the opposite ("Seek back to the
// start of that partial line so the next scanner picks it up as a complete line
// once more data is written") but the code below it is
// f.Seek(0, io.SeekCurrent), a no-op position query. Nothing seeks back.
// ---------------------------------------------------------------------------

func TestReproBug1_DrainByPoll_PartialLineIsEmittedAsComplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ta.log")

	// One complete record, then a second record the writer has only half flushed:
	// no trailing newline yet. This is the ordinary state of a file being
	// appended to by another process.
	writeRaw(t, path, "{\"seq\":1}\n{\"seq\":2,\"na")

	tr := newTestTailer(0)
	out := make(chan string, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lastSize int64
	if err := tr.drainByPoll(ctx, path, out, &lastSize); err != nil {
		t.Fatalf("first drainByPoll: %v", err)
	}
	first := collectDrained(out)

	// The complete record must be forwarded; the half-written one must be held
	// back until its newline arrives.
	if len(first) != 1 || first[0] != "{\"seq\":1}" {
		t.Errorf("first drain emitted %q, want exactly [\"{\\\"seq\\\":1}\"] — the partial record must not be forwarded", first)
	}
	// The offset must stop at the last newline (byte 10), not at EOF (byte 21).
	// Committing EOF is what turns the remainder into a second line.
	if want := int64(len("{\"seq\":1}\n")); lastSize != want {
		t.Errorf("lastSize = %d, want %d (must stop at the last newline, not at EOF)", lastSize, want)
	}

	// The writer now finishes the record.
	appendRaw(t, path, "me\":\"x\"}\n")
	if err := tr.drainByPoll(ctx, path, out, &lastSize); err != nil {
		t.Fatalf("second drainByPoll: %v", err)
	}
	second := collectDrained(out)

	if len(second) != 1 || second[0] != "{\"seq\":2,\"name\":\"x\"}" {
		t.Errorf("second drain emitted %q, want exactly [\"{\\\"seq\\\":2,\\\"name\\\":\\\"x\\\"}\"] — one physical line must yield one line", second)
	}
}

// TestReproBug1_ReadFollowFile_PartialLineIsEmittedAsComplete is the same defect
// on the pure-poll path (tailer.go:426-511), driven end to end through the
// long-running reader rather than by calling the helper directly.
func TestReproBug1_ReadFollowFile_PartialLineIsEmittedAsComplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ta.log")
	writeRaw(t, path, "")

	tr := newTestTailer(0)
	out := make(chan string, 64)
	sink := drain(out)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tr.readFollowFile(ctx, path, out) }()
	defer func() {
		cancel()
		<-done
		close(out)
	}()

	// A complete line plus a half-written one.
	appendRaw(t, path, "complete-1\n")
	sink.waitForLine(t, "complete-1", 3*time.Second)

	appendRaw(t, path, "parti")
	// Give the poll loop several cycles. The fragment must NOT appear.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sink.has("parti") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sink.has("parti") {
		t.Error("poll mode forwarded the partial line \"parti\" as if it were complete")
	}

	// Finish the line; it must arrive whole.
	appendRaw(t, path, "al\n")
	complete := make(chan struct{})
	go func() {
		end := time.Now().Add(3 * time.Second)
		for time.Now().Before(end) {
			if sink.has("partial") {
				close(complete)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	select {
	case <-complete:
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for the reassembled line \"partial\"")
	}
	if sink.has("al") {
		t.Error("the remainder \"al\" was forwarded as its own line; one physical line became two")
	}
}

// ---------------------------------------------------------------------------
// BUG 2 — drainByPoll returns scanner.Err() before committing *lastSize, so
// every line it already forwarded is re-read and re-forwarded next time.
//
// tailer.go:779-781 returns the scan error; the offset update is at 784-788 and
// is never reached. The caller (tailFileHybrid:649-659) logs and loops back with
// the stale offset, so the successfully-forwarded lines are emitted again.
//
// The realistic trigger is bufio.ErrTooLong: a line longer than maxLineBytes.
// doc/v1.5/todo.md item B10 describes exactly this shape ("a line over
// maxLineBytes wedges the whole file ... poll's readFollowFile returns
// scanner.Err() (bufio.ErrTooLong) ... retries from 0 ... hits the same
// over-long line ... infinite loop").
//
// Note: bufio.Scanner only raises ErrTooLong once its buffer fills, and
// drainByPoll hands it a 64 KiB initial buffer, so the offending line has to
// exceed 64 KiB regardless of maxLineBytes. Production's maxLineBytes is 10 MiB.
// ---------------------------------------------------------------------------

func TestReproBug2_DrainByPoll_ScannerErrorLosesTheOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ta.log")

	// Three good records, then one far over the limit. The scanner forwards the
	// good ones and then fails on the long one.
	longLine := strings.Repeat("x", 200*1024)
	writeRaw(t, path, "good-1\ngood-2\ngood-3\n"+longLine+"\n")

	tr := newTestTailer(4096)
	out := make(chan string, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lastSize int64
	err1 := tr.drainByPoll(ctx, path, out, &lastSize)
	if err1 != bufio.ErrTooLong {
		t.Fatalf("first drainByPoll error = %v, want bufio.ErrTooLong (test premise)", err1)
	}
	first := collectDrained(out)
	if len(first) != 3 {
		t.Fatalf("first drain emitted %d lines %q, want the 3 good ones", len(first), first)
	}

	// The three forwarded lines are gone downstream; the offset must account for
	// them even though the call failed. It must be at least past "good-3\n".
	minOffset := int64(len("good-1\ngood-2\ngood-3\n"))
	if lastSize < minOffset {
		t.Errorf("lastSize = %d after forwarding 3 lines, want >= %d; the offset was not committed because scanner.Err() returns before the update", lastSize, minOffset)
	}

	// Second call with the stale offset re-forwards the same three lines.
	_ = tr.drainByPoll(ctx, path, out, &lastSize)
	second := collectDrained(out)
	for _, dup := range second {
		t.Errorf("line %q was forwarded twice — a re-read caused by the lost offset", dup)
	}
	if len(second) != 0 {
		t.Errorf("second drain emitted %d lines %q, want 0 (all were already forwarded)", len(second), second)
	}
}
