package tailer

// Unit tests for lineScanner, the abstraction both polling read paths now use.
//
// lineboundary_repro_test.go pins the three defects end to end, through
// drainByPoll and readFollowFile. These pin the scanner's own contract, which is
// what those paths rely on and which nothing else in the repo covers:
//
//   - CRLF parity with bufio.ScanLines. The polling paths used ScanLines before,
//     so switching to a custom SplitFunc must not change the token text for a
//     CRLF-terminated file. No other test in the repository feeds a CRLF line, so
//     a regression in dropCR would otherwise be invisible: every line would gain a
//     trailing \r that the byte accounting still charges for.
//   - Byte accounting, including on a scan that ends in error. Callers advance a
//     file offset from Consumed(), so it must count exactly the bytes behind the
//     tokens handed out — no more (that would skip data) and no less (that would
//     re-read and duplicate).

import (
	"bufio"
	"strings"
	"testing"
)

// scanAll drives a lineScanner to completion and returns its tokens.
func scanAll(ls *lineScanner) []string {
	var got []string
	for ls.Scan() {
		got = append(got, ls.Text())
	}
	return got
}

// scanAllStdlib does the same with a plain bufio.ScanLines scanner, so the two
// can be compared token for token.
func scanAllStdlib(t *testing.T, input string) []string {
	t.Helper()
	sc := bufio.NewScanner(strings.NewReader(input))
	sc.Split(bufio.ScanLines)
	var got []string
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("stdlib scan: %v", err)
	}
	return got
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestLineScanner_CRLFParityWithScanLines is the backward-compatibility guard: for
// a fully terminated file the custom split must produce exactly what
// bufio.ScanLines produced, while still counting the CR in Consumed (the CR is
// part of the line terminator, so the offset has to move past it).
func TestLineScanner_CRLFParityWithScanLines(t *testing.T) {
	for _, input := range []string{
		"a\r\nb\r\nc\r\n",
		"\r\n\r\n",
		"one\r\ntwo\r\nthree\r\n",
		"\r\n",
		"a\r\n\r\nb\r\n",
	} {
		want := scanAllStdlib(t, input)
		ls := newLineScanner(strings.NewReader(input), defaultMaxLineSize)
		got := scanAll(ls)
		if !equalStrings(got, want) {
			t.Errorf("input %q: tokens = %q, want %q (must match bufio.ScanLines)", input, got, want)
		}
		if err := ls.Err(); err != nil {
			t.Errorf("input %q: Err() = %v, want nil", input, err)
		}
		// Every byte is behind a terminated line, so all of it is consumed.
		if int64(len(input)) != ls.Consumed() {
			t.Errorf("input %q: Consumed() = %d, want %d (the CR belongs to the terminator)",
				input, ls.Consumed(), len(input))
		}
	}
}

// TestLineScanner_LFMatchesScanLines does the same for plain LF input, including
// a file whose last line is unterminated — the one case where the two must
// DIFFER, and the difference is the whole point of the type.
func TestLineScanner_LFMatchesScanLines(t *testing.T) {
	for _, input := range []string{"a\nb\nc\n", "\n\n", "a\n\nb\n", "\n"} {
		want := scanAllStdlib(t, input)
		ls := newLineScanner(strings.NewReader(input), defaultMaxLineSize)
		got := scanAll(ls)
		if !equalStrings(got, want) {
			t.Errorf("input %q: tokens = %q, want %q", input, got, want)
		}
		if int64(len(input)) != ls.Consumed() {
			t.Errorf("input %q: Consumed() = %d, want %d", input, ls.Consumed(), len(input))
		}
	}
}

// TestLineScanner_WithholdsUnterminatedTail pins the property ScanLines lacks: a
// trailing fragment with no newline is not a token, and its bytes are not
// consumed, so a caller that advances an offset by Consumed() stays in front of
// the half-written line and picks it up whole later.
func TestLineScanner_WithholdsUnterminatedTail(t *testing.T) {
	const input = "{\"seq\":1}\n{\"seq\":2,\"na"
	ls := newLineScanner(strings.NewReader(input), defaultMaxLineSize)
	got := scanAll(ls)

	if want := []string{`{"seq":1}`}; !equalStrings(got, want) {
		t.Errorf("tokens = %q, want %q — the unterminated tail must not be emitted", got, want)
	}
	if err := ls.Err(); err != nil {
		t.Errorf("Err() = %v, want nil: withholding a partial line is not an error", err)
	}
	// Only "{\"seq\":1}\n" is consumed; the 11 bytes of the fragment are not.
	const want = int64(len("{\"seq\":1}\n"))
	if ls.Consumed() != want {
		t.Errorf("Consumed() = %d, want %d (must stop at the last newline, not at EOF)",
			ls.Consumed(), want)
	}
}

// TestLineScanner_ConsumedCountsBlankLines checks that a blank line the caller
// chooses not to forward is still charged for. drainByPoll skips empty tokens;
// if Consumed() did not count them the offset would fall behind and the blank
// line would be re-read on every pass, forever.
func TestLineScanner_ConsumedCountsBlankLines(t *testing.T) {
	const input = "a\n\n\nb\n"
	ls := newLineScanner(strings.NewReader(input), defaultMaxLineSize)

	var forwarded int
	for ls.Scan() {
		if len(ls.Text()) > 0 {
			forwarded++
		}
	}
	if forwarded != 2 {
		t.Errorf("forwarded %d non-blank lines, want 2", forwarded)
	}
	if int64(len(input)) != ls.Consumed() {
		t.Errorf("Consumed() = %d, want %d: blank lines must be accounted for too",
			ls.Consumed(), len(input))
	}
}

// TestLineScanner_ConsumedSurvivesErrTooLong pins the accounting behind the
// Bug② fix: when a scan dies partway on an over-long line, the tokens already
// handed out must still be counted, so the caller can commit an offset past them
// instead of re-reading and re-emitting them.
//
// The line has to exceed the 64 KiB initial buffer, not merely maxLineSize:
// bufio.Scanner only raises ErrTooLong once its buffer actually fills.
func TestLineScanner_ConsumedSurvivesErrTooLong(t *testing.T) {
	const prefix = "good-1\ngood-2\ngood-3\n"
	input := prefix + strings.Repeat("x", 200*1024) + "\n"

	ls := newLineScanner(strings.NewReader(input), 4096)
	got := scanAll(ls)

	if err := ls.Err(); err != bufio.ErrTooLong {
		t.Fatalf("Err() = %v, want bufio.ErrTooLong (test premise: the line must be over-long)", err)
	}
	if want := []string{"good-1", "good-2", "good-3"}; !equalStrings(got, want) {
		t.Errorf("tokens = %q, want %q", got, want)
	}
	if ls.Consumed() != int64(len(prefix)) {
		t.Errorf("Consumed() = %d, want %d: the three forwarded lines must still be "+
			"accounted for even though the scan ended in error", ls.Consumed(), len(prefix))
	}
}
