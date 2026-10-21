// Integration test for the bad-bytes wedge: a log line carrying raw control bytes
// fails to parse, and pre-fix the raw line was stored verbatim in dead_letter.line.
// DocumentDB then rejected the whole insert command with a command-level
// " Badly formed input data" — no per-document detail — which fell through to the
// transient branch and was retried forever, stalling ingestion. Two production
// shards sat like that for ~50 hours from 2026-10-19.
//
// Post-fix the line is sanitized on the way into the dead-letter model (rune-safe
// truncation, ToValidUTF8, C0/DEL rendered as readable \xNN escapes), so the dead
// letter is simply writable: it lands, the good events in the same submission
// land, and nothing stalls.
//
// IMPORTANT — server differences, measured not assumed: plain mongod ACCEPTS a
// NUL and invalid UTF-8 inside a BSON string, so against a local server the
// pre-fix code does not wedge and this test's "no wedge" assertions pass either
// way. What it does pin locally is the sanitization: run against df432e0 (pre-fix)
// the stored dead_letter.line comes back as valid-UTF-8-violating, still carrying
// byte 0x00, and the three assertions below fail. Reproducing the actual
// command-level rejection requires DocumentDB — point TANGO_TEST_MONGO_URI at a
// DocumentDB endpoint, where the same line produces
// " Badly formed input data" and the wedge is observable.
//
// Gated on TANGO_TEST_MONGO_URI via freshDB (skips without a reachable server).
package test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/aura-studio/tango/internal/parser"
	"github.com/aura-studio/tango/internal/process"
	"github.com/aura-studio/tango/internal/role/api"
)

// badByteLine builds a TA log line that cannot parse: encoding/json rejects a raw
// control byte inside a string literal ("invalid character '\x00' in string").
// The trailing invalid UTF-8 byte is there too — on its own it would NOT fail the
// parse (json silently substitutes U+FFFD), but it would make the stored BSON
// string invalid, so the line exercises both classes of poison at once.
func badByteLine(uuid string) string {
	return `{"#type":"track","#event_name":"e","#time":"2026-06-26 00:00:00",` +
		`"#uuid":"` + uuid + `","#account_id":"acc-bad","text":"a` +
		"\x00" + `b` + "\xff" + `"}`
}

func TestPipelineBadBytesNoWedge(t *testing.T) {
	daoCfg, db, cleanup := freshDB(t)
	defer cleanup()

	procCfg := &process.Config{Mode: string(process.ModePipeline)}
	procCfg.ApplyDefaults()
	parserCfg := &parser.Config{}
	parserCfg.ApplyDefaults()

	eng, err := api.New(context.Background(), daoCfg, procCfg, parserCfg, nil)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	defer eng.Close()
	if err := eng.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	good1 := eventLine("u-ok-1", "acc-1", nil)
	good2 := eventLine("u-ok-2", "acc-2", nil)
	bad := badByteLine("u-bad-1")

	// Sanity-check the premise: the line really must fail to parse, otherwise this
	// test would silently stop exercising the dead-letter path.
	if err := json.Unmarshal([]byte(bad), &map[string]any{}); err == nil {
		t.Fatalf("the bad line parsed cleanly; it must fail so it reaches dead_letter: %q", bad)
	} else {
		t.Logf("bad line rejected by the parser as expected: %v", err)
	}
	// ...and the good lines must parse, so a failure below is attributable.
	for _, g := range []string{good1, good2} {
		if err := json.Unmarshal([]byte(g), &map[string]any{}); err != nil {
			t.Fatalf("a good line failed to parse: %v", err)
		}
	}

	// A bounded context is the "no wedge" proof: pre-fix the dead_letter flush
	// retried forever and Upload blocked here until the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	res, err := eng.Upload(ctx, []string{good1, bad, good2})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Upload returned error (a bad line must be dead-lettered, not surfaced): %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Upload hit the deadline after %s -> still wedging on the bad line (ctx err=%v)", elapsed, ctx.Err())
	}
	t.Logf("Upload returned in %s: %+v", elapsed, res)

	if got := count(t, db, "event"); got != 2 {
		t.Errorf("event count=%d, want 2 (good docs must land alongside the bad line)", got)
	}

	// The bad line must have been dead-lettered rather than dropped or retried.
	if got := count(t, db, "dead_letter"); got != 1 {
		t.Fatalf("dead_letter count=%d, want 1", got)
	}

	var doc bson.M
	if err := db.Collection("dead_letter").FindOne(context.Background(), bson.M{}).Decode(&doc); err != nil {
		t.Fatalf("read the dead letter back: %v", err)
	}
	line, ok := doc["line"].(string)
	if !ok {
		t.Fatalf("dead_letter.line = %T, want string", doc["line"])
	}

	if !utf8.ValidString(line) {
		t.Errorf("dead_letter.line is not valid UTF-8: %q", line)
	}
	for i := 0; i < len(line); i++ {
		if c := line[i]; c < 0x20 || c == 0x7f {
			t.Errorf("dead_letter.line still carries control byte 0x%02x at %d: %q", c, i, line)
			break
		}
	}
	// The diagnosis must survive the cleaning: the offending byte is still visible
	// as a readable escape, and the record is still identifiable.
	if !strings.Contains(line, `\x00`) {
		t.Errorf("dead_letter.line lost the NUL diagnostic: %q", line)
	}
	if !strings.Contains(line, "u-bad-1") {
		t.Errorf("dead_letter.line lost the identifiable payload: %q", line)
	}
	if errMsg, _ := doc["error"].(string); !strings.Contains(errMsg, "invalid character") {
		t.Errorf("dead_letter.error = %q, want it to name the parse failure", errMsg)
	}
	t.Logf("PASS: 2 events written, bad line dead-lettered as %q, no wedge (%s)", line, elapsed)
}

// TestPipelineBadBytesDoesNotStallLaterBatches checks the property the incident
// actually cost us: after a bad line, subsequent submissions must still be
// written. A wedged worker stops every later batch, so this is the end-to-end
// form of "ingestion resumed".
//
// Note this one only discriminates against DocumentDB. On a local mongod the bad
// line is accepted verbatim, nothing wedges, and the test passes both pre- and
// post-fix; it is kept because it is the assertion that matters in production and
// it costs nothing to run.
func TestPipelineBadBytesDoesNotStallLaterBatches(t *testing.T) {
	daoCfg, db, cleanup := freshDB(t)
	defer cleanup()

	procCfg := &process.Config{Mode: string(process.ModePipeline)}
	procCfg.ApplyDefaults()
	parserCfg := &parser.Config{}
	parserCfg.ApplyDefaults()

	eng, err := api.New(context.Background(), daoCfg, procCfg, parserCfg, nil)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	defer eng.Close()
	if err := eng.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := eng.Upload(ctx, []string{badByteLine("u-bad-first")}); err != nil {
		t.Fatalf("first upload: %v", err)
	}
	// Every one of these would hang behind the wedged batch pre-fix.
	for i := 0; i < 5; i++ {
		if _, err := eng.Upload(ctx, []string{eventLine("u-after-"+string(rune('a'+i)), "acc-after", nil)}); err != nil {
			t.Fatalf("upload %d after the bad line: %v", i, err)
		}
		if ctx.Err() != nil {
			t.Fatalf("ctx exhausted at upload %d -> ingestion stalled behind the bad line", i)
		}
	}

	if got := count(t, db, "event"); got != 5 {
		t.Errorf("event count=%d, want 5 (later batches must still land)", got)
	}
	if got := count(t, db, "dead_letter"); got != 1 {
		t.Errorf("dead_letter count=%d, want 1", got)
	}
}
