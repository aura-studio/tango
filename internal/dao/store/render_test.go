package store

import (
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// The point of renderWriteModel: a bulk-write failure must name the statement
// that failed, in a form an operator can paste into mongosh. The 2026-10-19
// wedge repeated a log line ~35,000 times over two days and nothing in it said
// which document was being refused.

func TestRenderWriteModel_AllModelTypes(t *testing.T) {
	upsert := true
	cases := []struct {
		name       string
		model      mongo.WriteModel
		wantPrefix string
		wantBits   []string
	}{
		{
			name:       "insertOne",
			model:      mongo.NewInsertOneModel().SetDocument(bson.M{"a": "b"}),
			wantPrefix: "db.coll.insertOne(",
			wantBits:   []string{`"a":"b"`},
		},
		{
			name:       "deleteOne",
			model:      mongo.NewDeleteOneModel().SetFilter(bson.M{"#uuid": "u1"}),
			wantPrefix: "db.coll.deleteOne(",
			wantBits:   []string{`"#uuid":"u1"`},
		},
		{
			name:       "deleteMany",
			model:      mongo.NewDeleteManyModel().SetFilter(bson.M{"x": 1}),
			wantPrefix: "db.coll.deleteMany(",
			wantBits:   []string{`"x":{"$numberInt":"1"}`},
		},
		{
			name: "updateOne with upsert",
			model: mongo.NewUpdateOneModel().
				SetFilter(bson.M{"#uuid": "u1"}).
				SetUpdate(bson.M{"$set": bson.M{"a": 1}}).
				SetUpsert(true),
			wantPrefix: "db.coll.updateOne(",
			wantBits:   []string{`"#uuid":"u1"`, `"$set"`, `"upsert":true`},
		},
		{
			name: "updateOne without options omits the third argument",
			model: mongo.NewUpdateOneModel().
				SetFilter(bson.M{"k": "v"}).
				SetUpdate(bson.M{"$inc": bson.M{"n": 2}}),
			wantPrefix: "db.coll.updateOne(",
			// The trailing }}}) proves the call closes right after the update
			// document, i.e. no third options argument was emitted.
			wantBits: []string{`"$inc"`, `"n":{"$numberInt":"2"}}})`},
		},
		{
			name: "updateMany",
			model: mongo.NewUpdateManyModel().
				SetFilter(bson.M{"s": "x"}).
				SetUpdate(bson.M{"$set": bson.M{"s": "y"}}),
			wantPrefix: "db.coll.updateMany(",
			wantBits:   []string{`"s":"x"`, `"s":"y"`},
		},
		{
			name: "replaceOne with upsert",
			model: mongo.NewReplaceOneModel().
				SetFilter(bson.M{"#uuid": "u2"}).
				SetReplacement(bson.M{"#uuid": "u2", "v": 3}).
				SetUpsert(upsert),
			wantPrefix: "db.coll.replaceOne(",
			wantBits:   []string{`"#uuid":"u2"`, `"v":{"$numberInt":"3"}`, `"upsert":true`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderWriteModel("coll", tc.model)
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("got %q, want it to start with %q", got, tc.wantPrefix)
			}
			if !strings.HasSuffix(got, ")") {
				t.Errorf("got %q, want a balanced closing paren", got)
			}
			for _, bit := range tc.wantBits {
				if !strings.Contains(got, bit) {
					t.Errorf("got %q, want it to contain %q", got, bit)
				}
			}
			// A statement that spans lines defeats a single CloudWatch log field.
			if strings.ContainsAny(got, "\n\r") {
				t.Errorf("rendered statement contains a newline: %q", got)
			}
			// And it must not leak a Go pointer address, which is what %+v did.
			if strings.Contains(got, "0xc0") {
				t.Errorf("rendered statement leaks a pointer address: %q", got)
			}
		})
	}
}

// TestRenderWriteModel_RealModels renders the models tango actually builds, so a
// change to any of them shows up here rather than only in production logs.
func TestRenderWriteModel_RealModels(t *testing.T) {
	cases := []struct {
		name     string
		coll     string
		model    mongo.WriteModel
		wantBits []string
	}{
		{
			name:     "event track upsert",
			coll:     "event",
			model:    EventWriteModel("track", "ev-1", bson.M{"#uuid": "ev-1", "_ts": int64(1792599000000000000), "#event_name": "login"}),
			wantBits: []string{"db.event.updateOne(", `"$setOnInsert"`, `"#uuid":"ev-1"`, "$numberLong"},
		},
		{
			name:     "event track_update",
			coll:     "event",
			model:    EventWriteModel("track_update", "ev-2", bson.M{"#uuid": "ev-2", "_ts": int64(5), "a": "v"}),
			wantBits: []string{"db.event.updateOne(", `"$set"`},
		},
		{
			name:     "event track_overwrite",
			coll:     "event",
			model:    EventWriteModel("track_overwrite", "ev-3", bson.M{"#uuid": "ev-3", "_ts": int64(7)}),
			wantBits: []string{"db.event.replaceOne(", `"upsert":true`},
		},
		{
			name:     "user user_set",
			coll:     "user",
			model:    UserWriteModel("user_set", 42, bson.M{"#time": "2026-10-19", "_ts": int64(9), "name": "Alice"}),
			wantBits: []string{"db.user.updateOne(", `"$set"`, `"#user_id"`, `"name":"Alice"`},
		},
		{
			name:     "user user_del",
			coll:     "user",
			model:    UserWriteModel("user_del", 42, bson.M{"_ts": int64(9)}),
			wantBits: []string{"db.user.deleteOne("},
		},
		{
			name:     "dead letter",
			coll:     "dead_letter",
			model:    DeadLetterModel(`{"#type":"track","text":"bad"}`, errors.New("not a ThinkingData payload")),
			wantBits: []string{"db.dead_letter.insertOne(", `"line":`, `"error":"not a ThinkingData payload"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderWriteModel(tc.coll, tc.model)
			for _, bit := range tc.wantBits {
				if !strings.Contains(got, bit) {
					t.Errorf("rendered statement missing %q:\n  %s", bit, got)
				}
			}
			if strings.ContainsAny(got, "\n\r") {
				t.Errorf("rendered statement contains a newline: %q", got)
			}
			t.Logf("%s", previewForLog(got, 300))
		})
	}
}

// TestRenderWriteModel_BadBytesStayRenderable is the case that started all this:
// a model whose document carries a raw NUL and invalid UTF-8 must still render,
// and the rendering must be safe to put in a log field and in a BSON string.
func TestRenderWriteModel_BadBytesStayRenderable(t *testing.T) {
	model := DeadLetterModel("{\"text\":\"a\x00b\xff\"}", errors.New("invalid character"))

	got := renderWriteModel("dead_letter", model)
	if !strings.HasPrefix(got, "db.dead_letter.insertOne(") {
		t.Fatalf("unexpected rendering: %q", got)
	}

	// DeadLetterModel sanitizes on the way in, so the NUL is already the four
	// characters \x00 and the invalid byte is already U+FFFD. The renderer must
	// preserve that diagnosis rather than swallow it.
	if !strings.Contains(got, `x00`) {
		t.Errorf("the NUL diagnostic did not survive rendering: %q", got)
	}

	forLog := previewForLog(got, logPreviewBytes)
	assertSingleLine(t, "log preview", forLog)
	forStore := sanitizeForBSON(got, quarantineModelBytes)
	assertBSONSafe(t, "stored model", forStore)

	// A model that was NOT built through a sanitizing constructor can still carry
	// raw bad bytes; the renderer plus the sanitizer must cope with that too.
	raw := mongo.NewInsertOneModel().SetDocument(bson.M{"line": "a\x00b\xff"})
	rawForLog := previewForLog(renderWriteModel("event", raw), logPreviewBytes)
	assertSingleLine(t, "raw log preview", rawForLog)
	if strings.ContainsRune(rawForLog, 0) {
		t.Errorf("raw NUL survived into the log preview: %q", rawForLog)
	}
	rawForStore := sanitizeForBSON(renderWriteModel("event", raw), quarantineModelBytes)
	assertBSONSafe(t, "raw stored model", rawForStore)
}

func TestRenderWriteModel_DegenerateInputs(t *testing.T) {
	if got := renderWriteModel("coll", nil); got != "db.coll.<nil model>" {
		t.Errorf("nil model rendered as %q", got)
	}
	// NOTE: renderWriteModel's default branch cannot be exercised from a test.
	// mongo.WriteModel's only method is the unexported writeModel(), so no type
	// outside the mongo package can implement it and every reachable value is one
	// of the six driver models. The branch stays as defence against a future
	// driver release adding a seventh.
	if got := firstModelPreview("coll", nil, 512); got != "<empty batch>" {
		t.Errorf("empty batch rendered as %q", got)
	}
	if got := firstModelPreview("coll", []mongo.WriteModel{}, 512); got != "<empty batch>" {
		t.Errorf("zero-length batch rendered as %q", got)
	}
	// A nil document must not panic the Extended JSON marshaller.
	if got := renderWriteModel("coll", mongo.NewInsertOneModel()); !strings.Contains(got, "insertOne(") {
		t.Errorf("nil document rendered as %q", got)
	}
	if got := extJSON(nil); got != "null" {
		t.Errorf("extJSON(nil) = %q, want null", got)
	}
}

// TestRenderWriteModel_Int64Fidelity pins the reason canonical Extended JSON is
// used instead of relaxed. _ts is a UnixNano int64 (~1.8e18), far beyond the 2^53
// exact-integer range of a JavaScript double. Relaxed mode renders it as a bare
// number, so pasting the statement into mongosh would silently round it and the
// "replay" would not reproduce the original write. Canonical emits $numberLong.
func TestRenderWriteModel_Int64Fidelity(t *testing.T) {
	const ts = int64(1792599000000000000) // > 2^53, would lose precision as a double
	got := renderWriteModel("event",
		EventWriteModel("track", "fid-1", bson.M{"#uuid": "fid-1", "_ts": ts}))

	if !strings.Contains(got, `"_ts":{"$numberLong":"1792599000000000000"}`) {
		t.Errorf("_ts was not rendered as $numberLong, so a replay would lose precision:\n  %s", got)
	}
	if strings.Contains(got, `"_ts":1792599000000000000`) {
		t.Errorf("_ts rendered as a bare number (relaxed mode):\n  %s", got)
	}
	// The exact digits must survive; a rounded value would look plausible and be wrong.
	if !strings.Contains(got, "1792599000000000000") {
		t.Errorf("the _ts digits did not survive rendering:\n  %s", got)
	}
}

func TestRenderFirstModel_Exported(t *testing.T) {
	st, cleanup := unitStore(t)
	defer cleanup()

	if got := st.RenderFirstModel(st.EventCollection(), nil); got != "<empty batch>" {
		t.Errorf("empty batch = %q", got)
	}
	models := []mongo.WriteModel{
		EventWriteModel("track", "rf-1", bson.M{"#uuid": "rf-1"}),
		EventWriteModel("track", "rf-2", bson.M{"#uuid": "rf-2"}),
	}
	got := st.RenderFirstModel(st.EventCollection(), models)
	if !strings.Contains(got, "db.event.updateOne(") || !strings.Contains(got, "rf-1") {
		t.Errorf("RenderFirstModel = %q, want the first model of the batch as a statement", got)
	}
	if strings.Contains(got, "rf-2") {
		t.Errorf("RenderFirstModel rendered more than the first model: %q", got)
	}
	assertSingleLine(t, "RenderFirstModel", got)
	if len(got) > logPreviewBytes+len(truncatedMarker) {
		t.Errorf("RenderFirstModel output is %d bytes, want <= %d", len(got), logPreviewBytes+len(truncatedMarker))
	}
}

// TestRenderWriteModel_HugeDocumentIsBounded checks that a document far larger
// than the caps still renders and then gets clipped, so an oversize poison
// document cannot turn a diagnostic log line into a multi-megabyte one.
func TestRenderWriteModel_HugeDocumentIsBounded(t *testing.T) {
	model := mongo.NewInsertOneModel().SetDocument(bson.M{"blob": strings.Repeat("x", 4*1024*1024)})
	got := renderWriteModel("event", model)
	if len(got) < 4*1024*1024 {
		t.Fatalf("rendering looks truncated before sanitizing: %d bytes", len(got))
	}
	if clipped := previewForLog(got, logPreviewBytes); len(clipped) > logPreviewBytes+len(truncatedMarker) {
		t.Errorf("clipped preview is %d bytes, want <= %d", len(clipped), logPreviewBytes+len(truncatedMarker))
	}
	if clipped := sanitizeForBSON(got, quarantineModelBytes); len(clipped) > quarantineModelBytes+len(truncatedMarker) {
		t.Errorf("clipped stored model is %d bytes, want <= %d", len(clipped), quarantineModelBytes+len(truncatedMarker))
	}
}

// TestRenderWriteModel_DeterministicAcrossCalls pins that rendering is a function
// of the model alone. The write models are built from bson.M — a Go map, whose
// iteration order is randomised on every marshal — so without the key-sorting pass
// in extJSON the same model renders differently on each call. That is not
// cosmetic: Store.quarantineID hashes the rendering to derive a stable _id, so a
// nondeterministic render silently defeats the deduplication of isolated
// documents, and an operator comparing two first_statement log values cannot tell
// one recurring document from a stream of distinct ones.
func TestRenderWriteModel_DeterministicAcrossCalls(t *testing.T) {
	// Enough keys that random map ordering would show up almost immediately:
	// 6 keys have 720 permutations, so an unsorted render differs from the first
	// one with probability 719/720 per call.
	model := EventWriteModel("track", "det-1", bson.M{
		"#uuid":       "det-1",
		"#event_name": "login",
		"a":           "1",
		"b":           "2",
		"c":           "3",
		"d":           "4",
	})
	first := renderWriteModel("event", model)
	for i := 0; i < 200; i++ {
		if got := renderWriteModel("event", model); got != first {
			t.Fatalf("render call #%d differs from the first one for the same model:\n  first: %s\n  #%d:   %s",
				i, first, i, got)
		}
	}
	// Keys must actually be sorted, not merely stable by accident. Scoped to the
	// $setOnInsert object: the rendered statement is a mongosh call whose FILTER
	// argument comes first and also contains "#uuid", so searching the whole
	// string would compare a key in one argument against a key in another.
	inner := first[strings.Index(first, `"$setOnInsert":`):]
	for _, pair := range [][2]string{
		{`"#event_name"`, `"#uuid"`},
		{`"#uuid"`, `"_ts"`},
		{`"a"`, `"b"`},
		{`"b"`, `"c"`},
		{`"c"`, `"d"`},
	} {
		i, j := strings.Index(inner, pair[0]), strings.Index(inner, pair[1])
		if i < 0 || j < 0 {
			t.Fatalf("render missing %s or %s: %s", pair[0], pair[1], first)
		}
		if i > j {
			t.Errorf("%s appears after %s; object keys must be sorted: %s", pair[0], pair[1], first)
		}
	}
	t.Logf("deterministic render: %s", first)
}

// TestRenderWriteModel_HTMLOrderingNotEscaped checks the readability half of the
// key-sorting pass: encoding/json escapes <, > and & by default, which would turn
// a log line containing them into backslash-u noise in the very field an operator
// reads to diagnose a failure.
func TestRenderWriteModel_HTMLCharsNotEscaped(t *testing.T) {
	model := mongo.NewInsertOneModel().SetDocument(bson.M{"line": `a < b && c > d`})
	got := renderWriteModel("dead_letter", model)
	if !strings.Contains(got, `a < b && c > d`) {
		t.Errorf("render = %q, want the raw <, > and & preserved rather than escaped", got)
	}
}
