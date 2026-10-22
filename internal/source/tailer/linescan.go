package tailer

import (
	"bufio"
	"bytes"
	"io"
)

// lineScanner wraps bufio.Scanner with two properties the polling read paths
// need and bufio.ScanLines does not provide.
//
//  1. It never emits an unterminated trailing fragment. bufio.ScanLines returns
//     the last non-empty run of bytes as a token even when it has no trailing
//     newline, so a reader that catches the writer mid-line forwards half a
//     record as if it were whole — and then, because the file offset has moved
//     past it, forwards the remainder as a second "line" on the next pass. One
//     physical line becomes two unparseable fragments. That is not hypothetical:
//     across the production fleet, 780 of 781 minutes containing parse errors
//     contained exactly 2 (or 4) of them, never 1 or 3.
//
//  2. It counts exactly how many bytes the tokens it emitted consumed, so the
//     caller can advance a file offset precisely. bufio.Scanner buffers ahead of
//     what it hands out and exposes no consumed-byte count, which is why the
//     previous code fell back to f.Seek(0, io.SeekCurrent) — that returns the
//     physical read position, not the logical line boundary, and a comment
//     claiming it "seeks back to the start of that partial line" was describing
//     behaviour the code never had. Counting also makes the offset correct when
//     scanning stops early with an error: the bytes already emitted are still
//     accounted for, so the next pass neither re-reads nor skips them.
//
// The event-driven path (hpcloud/tail) does not use this: that library already
// rewinds over a half-line instead of emitting it.
type lineScanner struct {
	sc       *bufio.Scanner
	consumed int64
}

// newLineScanner builds a lineScanner reading from r with the same buffer sizing
// the polling paths have always used: 64 KiB initial, maxLineSize ceiling.
func newLineScanner(r io.Reader, maxLineSize int) *lineScanner {
	ls := &lineScanner{sc: bufio.NewScanner(r)}
	ls.sc.Buffer(make([]byte, 0, 64*1024), maxLineSize)
	ls.sc.Split(ls.split)
	return ls
}

// split is a bufio.SplitFunc equivalent to bufio.ScanLines except that an
// unterminated tail at EOF is left unemitted. Returning (0, nil, nil) tells the
// Scanner there is no token and no progress; once the underlying reader is also
// at EOF, Scan returns false with Err() == nil and the tail stays beyond
// ls.consumed, so the caller leaves its offset in front of it and picks the line
// up whole on a later pass.
func (ls *lineScanner) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		ls.consumed += int64(i + 1)
		return i + 1, dropCR(data[:i]), nil
	}
	// No newline in what we have. Whether or not this is EOF, there is nothing
	// safe to emit: at EOF it is a half-written line, and otherwise the Scanner
	// will read more and call us again.
	return 0, nil, nil
}

// dropCR strips one trailing carriage return, matching bufio.ScanLines so that
// CRLF-terminated files yield the same token text. The advance returned by split
// still covers the CR, so byte accounting is unaffected.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[:len(data)-1]
	}
	return data
}

func (ls *lineScanner) Scan() bool   { return ls.sc.Scan() }
func (ls *lineScanner) Text() string { return ls.sc.Text() }
func (ls *lineScanner) Err() error   { return ls.sc.Err() }

// Consumed returns the number of bytes accounted for by the tokens emitted so
// far, including their newline terminators and including blank lines that the
// caller chose not to forward. It does not count an unterminated tail.
func (ls *lineScanner) Consumed() int64 { return ls.consumed }
