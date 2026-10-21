package store

import (
	"strings"
	"unicode/utf8"
)

// Byte-level guards for anything that reaches a BSON string field.
//
// Why this file exists: a log line that fails to parse is stored verbatim in
// dead_letter.line (see DeadLetterModel). TA log lines come off a bufio.Scanner
// with no validation, so they can carry raw control bytes or invalid UTF-8.
// Neither the Go bson encoder nor bsoncore.AppendString checks any of it — the
// bytes go on the wire as-is. DocumentDB then rejects the WHOLE command with a
// command-level error whose text is " Badly formed input data" (no codeName, so
// the driver renders it with a leading space), which is not a per-document
// write error and therefore used to fall through to the retry path and wedge the
// pipeline indefinitely. Sanitizing at the source removes that class of poison
// outright: the dead letter becomes writable, so it lands and can be inspected.
//
// A NUL byte is the interesting case and the reason escaping is load-bearing
// rather than decorative: a NUL is perfectly VALID UTF-8, so strings.ToValidUTF8
// leaves it untouched. It also happens to be BSON's string terminator, so an
// embedded one makes the document unreadable by the server. Only an explicit
// control-byte mapping catches it.
const (
	// maxDeadLetterLineBytes caps the raw log line stored in a dead_letter
	// document. The per-worker dead-letter batch is DeadLetterCap (default 128),
	// so the worst-case command is ~2 MB — well under the 16 MB server limit,
	// which also keeps a giant line from turning into a too-large rejection.
	maxDeadLetterLineBytes = 16 * 1024

	// quarantineModelBytes caps the rendered write model stored by quarantine.
	quarantineModelBytes = 8 * 1024

	// logPreviewBytes caps the inline data preview emitted alongside a
	// permanent-rejection log line, so an operator can see the offending bytes
	// in CloudWatch without querying the database.
	logPreviewBytes = 512

	// truncatedMarker is appended by truncate when payload was cut.
	truncatedMarker = "...(truncated)"
)

// sanitizeForBSON returns a string that is safe to store in a BSON string
// field: valid UTF-8, no C0 control bytes, no DEL, and at most maxBytes long.
// Tab, newline and carriage return are preserved so stored content stays
// readable; use previewForLog when the value is going into a single log line.
//
// The expansion steps (UTF-8 repair, control escaping) can each grow the
// string, so the result is re-clipped at the end to keep maxBytes a hard bound.
func sanitizeForBSON(s string, maxBytes int) string {
	return sanitize(s, maxBytes, false)
}

// previewForLog is sanitizeForBSON for log fields: identical guarantees, but
// newline, carriage return and tab are escaped too so the preview cannot break
// the log line into several (which would defeat a CloudWatch filter).
func previewForLog(s string, maxBytes int) string {
	return sanitize(s, maxBytes, true)
}

func sanitize(s string, maxBytes int, singleLine bool) string {
	if maxBytes <= 0 {
		maxBytes = logPreviewBytes
	}
	// Pre-clip so the (potentially expensive) repair work runs on a bounded
	// prefix rather than on a multi-megabyte line.
	s = truncate(s, maxBytes)
	// Replace each run of invalid UTF-8 with U+FFFD. This mirrors exactly what
	// encoding/json already does to every other string in the pipeline, so
	// dead-letter content is no worse than normally-parsed content.
	s = strings.ToValidUTF8(s, string(utf8.RuneError))
	s = escapeControl(s, singleLine)
	// Both steps above can expand the string; enforce the cap again.
	return truncate(s, maxBytes)
}

// truncate caps s to at most n bytes of payload, appending truncatedMarker when
// anything was cut. The cut is pulled back to a UTF-8 rune boundary: the
// previous implementation sliced at an arbitrary byte offset, which could turn a
// perfectly valid string into an invalid one and reintroduce the very poison
// this package is trying to remove.
func truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncatedMarker
}

// escapeControl rewrites bytes that a BSON string must not carry as readable
// "\xNN" text, preserving the diagnostic value of the original data. When
// singleLine is set, tab/newline/carriage-return are escaped as well.
func escapeControl(s string, singleLine bool) string {
	if !hasControl(s, singleLine) {
		return s // fast path: clean strings are the overwhelming majority
	}
	const hex = "0123456789abcdef"
	var b strings.Builder
	b.Grow(len(s) + 16)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !needsEscape(c, singleLine) {
			b.WriteByte(c)
			continue
		}
		b.WriteString(`\x`)
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}

func hasControl(s string, singleLine bool) bool {
	for i := 0; i < len(s); i++ {
		if needsEscape(s[i], singleLine) {
			return true
		}
	}
	return false
}

func needsEscape(c byte, singleLine bool) bool {
	switch c {
	case '\t', '\n', '\r':
		return singleLine // fine in stored content, fatal to a single log line
	case 0x7f:
		return true // DEL
	}
	return c < 0x20 // remaining C0 controls, NUL included
}
