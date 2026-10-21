package store

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// The production incident these tests pin: a TA log line carrying a raw control
// byte failed to parse, was stored verbatim in dead_letter.line, and DocumentDB
// rejected the whole insert command with " Badly formed input data". That is a
// command-level rejection with no per-document detail, so it was retried forever
// and stalled ingestion for ~50 hours on two shards.

// assertBSONSafe fails if s could not be stored in a BSON string field: it must
// be valid UTF-8 and free of the bytes a BSON string cannot carry. Tab, newline
// and carriage return are legal there and are deliberately preserved by
// sanitizeForBSON, so they are not failures; NUL and the rest of C0 are.
func assertBSONSafe(t *testing.T, what, s string) {
	t.Helper()
	if !utf8.ValidString(s) {
		t.Errorf("%s: not valid UTF-8: %q", what, s)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\t', '\n', '\r':
			continue
		}
		if c < 0x20 || c == 0x7f {
			t.Errorf("%s: contains control byte 0x%02x at %d: %q", what, c, i, s)
			return
		}
	}
}

// assertSingleLine is the stricter contract for a log preview: it must also be
// free of tab/newline/carriage-return so it cannot split a log entry.
func assertSingleLine(t *testing.T, what, s string) {
	t.Helper()
	assertBSONSafe(t, what, s)
	if strings.ContainsAny(s, "\t\n\r") {
		t.Errorf("%s: must be single-line, got %q", what, s)
	}
}

func TestSanitizeForBSON(t *testing.T) {
	repl := string(utf8.RuneError)
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"clean ascii unchanged", `{"#type":"track","a":1}`, `{"#type":"track","a":1}`},
		{"clean utf8 unchanged", "中文·emoji", "中文·emoji"},
		// Whitespace survives in stored content: it is legal in a BSON string and
		// keeps a multi-line payload readable when inspected in the database.
		{"tab and newline kept", "a\tb\nc\rd", "a\tb\nc\rd"},
		// The load-bearing case: a NUL is valid UTF-8 (so ToValidUTF8 cannot help)
		// but is BSON's string terminator, so the server rejects the document.
		{"raw NUL escaped", "a\x00b", `a\x00b`},
		{"raw 0x01 escaped", "a\x01b", `a\x01b`},
		{"raw 0x1f escaped", "a\x1fb", `a\x1fb`},
		{"DEL escaped", "a\x7fb", `a\x7fb`},
		{"multiple controls escaped", "\x00\x01\x1f", `\x00\x01\x1f`},
		// Invalid UTF-8 does not by itself make a line unparseable (encoding/json
		// silently substitutes U+FFFD), but it does make the BSON string invalid,
		// so repair it the same way the JSON decoder would.
		{"invalid utf8 replaced", "a\xffb", "a" + repl + "b"},
		{"invalid utf8 run collapsed", "a\xff\xfe\xfdb", "a" + repl + "b"},
		{"control and invalid utf8 together", "a\x00\xffb", `a\x00` + repl + "b"},
		{"empty stays empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeForBSON(tc.in, 1024)
			if got != tc.want {
				t.Errorf("sanitizeForBSON(%q) = %q, want %q", tc.in, got, tc.want)
			}
			assertBSONSafe(t, "sanitizeForBSON output", got)
		})
	}
}

func TestPreviewForLog(t *testing.T) {
	// A preview goes into a single logrus field; a newline would split the log
	// entry across lines and defeat a CloudWatch `filter @message like ...`.
	got := previewForLog("line1\nline2\ttab\rcr", 1024)
	want := `line1\x0aline2\x09tab\x0dcr`
	if got != want {
		t.Errorf("previewForLog = %q, want %q", got, want)
	}
	assertSingleLine(t, "preview", got)

	// Stored content keeps the same characters readable.
	if kept := sanitizeForBSON("line1\nline2", 1024); kept != "line1\nline2" {
		t.Errorf("sanitizeForBSON should keep newlines, got %q", kept)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Errorf("short string changed: %q", got)
	}
	if got := truncate("0123456789abc", 5); got != "01234...(truncated)" {
		t.Errorf("truncate = %q", got)
	}
	if got := truncate("", 5); got != "" {
		t.Errorf("empty string changed: %q", got)
	}
	// A non-positive cap means "no limit" rather than "delete everything".
	if got := truncate("abc", 0); got != "abc" {
		t.Errorf("n=0 should be a no-op, got %q", got)
	}
}

// TestTruncate_RuneBoundary pins the fix for the old byte-slicing truncate,
// which could cut a multi-byte rune in half and manufacture invalid UTF-8 out
// of a perfectly valid string — reintroducing the poison it was meant to cap.
func TestTruncate_RuneBoundary(t *testing.T) {
	// "日本語" is 3 runes x 3 bytes. A cut at byte 4 lands inside the second rune.
	in := "日本語"
	for n := 1; n < len(in); n++ {
		got := truncate(in, n)
		payload := strings.TrimSuffix(got, truncatedMarker)
		if !utf8.ValidString(payload) {
			t.Errorf("truncate(%q, %d) split a rune: %q", in, n, got)
		}
	}
	// A long run of multi-byte runes must stay valid at every cap.
	long := strings.Repeat("漢", 4096)
	for _, n := range []int{1, 2, 7, 100, 1000, 8191} {
		got := truncate(long, n)
		payload := strings.TrimSuffix(got, truncatedMarker)
		if !utf8.ValidString(payload) {
			t.Errorf("truncate(long, %d) split a rune", n)
		}
	}
}

// TestSanitize_HardBound checks that maxBytes is a hard bound even for input
// that expands under repair: each step (UTF-8 replacement, control escaping,
// the marker) can grow the string, so the final re-clip must catch it.
func TestSanitize_HardBound(t *testing.T) {
	pathological := []struct {
		name string
		in   string
	}{
		{"all NUL", strings.Repeat("\x00", 4096)},
		{"all invalid utf8", strings.Repeat("\xff", 4096)},
		{"alternating control and rune", strings.Repeat("\x00漢", 4096)},
		{"single huge line", strings.Repeat("x", 1<<20)},
	}
	for _, tc := range pathological {
		for _, max := range []int{1, 8, 64, 1024} {
			got := sanitizeForBSON(tc.in, max)
			if len(got) > max+len(truncatedMarker) {
				t.Errorf("%s max=%d: len=%d exceeds bound", tc.name, max, len(got))
			}
			assertBSONSafe(t, tc.name, got)
		}
	}
	// maxBytes <= 0 falls back to the log-preview cap rather than dropping data.
	if got := sanitizeForBSON(strings.Repeat("y", 4096), 0); len(got) > logPreviewBytes+len(truncatedMarker) {
		t.Errorf("maxBytes=0 should fall back to %d, got len=%d", logPreviewBytes, len(got))
	}
}

// TestDeadLetterModel_SanitizesRawLine is the end-to-end pin for the incident:
// the raw line reaching DeadLetterModel must come out the other side as a
// document the server can actually store.
func TestDeadLetterModel_SanitizesRawLine(t *testing.T) {
	// A line that both fails to parse (raw control byte inside a JSON string)
	// and would be rejected by DocumentDB if stored verbatim.
	poison := "{\"#type\":\"track\",\"#uuid\":\"u1\",\"text\":\"a\x00b\xff\"}"

	model := DeadLetterModel(poison, errors.New("invalid character '\\x00' in string"))
	im, ok := model.(*mongo.InsertOneModel)
	if !ok {
		t.Fatalf("DeadLetterModel returned %T, want *mongo.InsertOneModel", model)
	}

	// Marshal exactly what would go on the wire, then read it back.
	raw, err := bson.Marshal(im.Document)
	if err != nil {
		t.Fatalf("bson.Marshal of the dead-letter document failed: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("bson.Unmarshal round-trip failed: %v", err)
	}

	line, ok := doc["line"].(string)
	if !ok {
		t.Fatalf("line field = %T, want string", doc["line"])
	}
	assertBSONSafe(t, "dead_letter.line", line)

	// The diagnostic value must survive: the offending bytes are still visible,
	// rendered as escapes rather than silently deleted.
	if !strings.Contains(line, `\x00`) {
		t.Errorf("line lost the NUL diagnostic: %q", line)
	}
	if !strings.Contains(line, `"#uuid":"u1"`) {
		t.Errorf("line lost identifiable content: %q", line)
	}

	errField, ok := doc["error"].(string)
	if !ok {
		t.Fatalf("error field = %T, want string", doc["error"])
	}
	assertBSONSafe(t, "dead_letter.error", errField)
	if _, has := doc["_ts"]; !has {
		t.Error("dead_letter document lost its _ts field")
	}
}

// TestDeadLetterModel_KeepsCleanLineUnchanged guards against over-sanitizing:
// the overwhelming majority of dead letters are ordinary readable text and must
// not be mangled by the new path.
func TestDeadLetterModel_KeepsCleanLineUnchanged(t *testing.T) {
	const line = `{"#type":"user_set","#user_id":7,"name":"Alice"}`
	model := DeadLetterModel(line, errors.New("not a ThinkingData payload"))
	raw, err := bson.Marshal(model.(*mongo.InsertOneModel).Document)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc["line"] != line {
		t.Errorf("clean line altered:\n got %q\nwant %q", doc["line"], line)
	}
}
