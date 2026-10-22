// Repo-level integration tests for the file face: the one-shot file-import
// source (explicit paths, no glob, no directories) feeding the real engine
// against a real MongoDB/DocumentDB (TANGO_TEST_MONGO_URI), through both entry
// points (Engine.File and the cli role's RunFile). The second halves are the
// idempotency assertion: file import has no checkpoint, so a re-run re-imports
// every line, and the event/user write models must converge — identical
// counts, byte-identical event docs ($setOnInsert), identical user data fields
// (only the $max-guarded _ts ordering meta may advance). Dead letters are
// append-only diagnostics (an invalid line carries no uuid to converge on) and
// grow by one per run.
package test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/aura-studio/tango/internal/process"
	"github.com/aura-studio/tango/internal/role/api"
	clirole "github.com/aura-studio/tango/internal/role/cli"
)

// writeSampleLogs splits sampleLines across two .log files and plants one
// unlisted decoy, returning the explicit paths of the two .log files (the decoy
// is not returned, so it must not import). Expected ingestion result equals
// verifyCounts: 4 events, 1 user, 1 dead letter.
func writeSampleLogs(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	lines := sampleLines()
	write := func(name string, ls []string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(strings.Join(ls, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}
	a := write("a.log", lines[:3])
	b := write("b.log", lines[3:])
	write("decoy.txt", []string{`{"#type":"track","#event_name":"never","#time":"2024-01-01","#uuid":"it-never","#account_id":"acc"}`})
	return []string{a, b}
}

// rawDoc fetches the single document matching filter as raw BSON bytes, the
// strictest "nothing changed" comparator.
func rawDoc(t *testing.T, db *mongo.Database, coll string, filter bson.M) bson.Raw {
	t.Helper()
	raw, err := db.Collection(coll).FindOne(context.Background(), filter).Raw()
	if err != nil {
		t.Fatalf("find %s %v: %v", coll, filter, err)
	}
	return raw
}

// TestFileEngineImportIsIdempotent feeds the engine's File face from on-disk
// files and re-imports the same files: counts and documents must come out
// identical (events upsert by #uuid, user fields are _ts-guarded, dead letters
// dedup on a content-derived _id).
func TestFileEngineImportIsIdempotent(t *testing.T) {
	daoCfg, db, cleanup := freshDB(t)
	defer cleanup()
	ctx := context.Background()
	paths := writeSampleLogs(t)

	eng, err := api.New(ctx, daoCfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	defer eng.Close()
	if err := eng.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	// Path-less call: rejected before any source/database work.
	if _, err := eng.File(ctx, &api.FileConfig{}); err == nil {
		t.Fatal("File with no paths succeeded, want paths-required error")
	}

	res, err := eng.File(ctx, &api.FileConfig{Paths: paths})
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	if res.Lines != 6 || res.EventWrites != 4 || res.UserWrites != 1 || res.DeadLetters != 1 {
		t.Fatalf("first import result = %+v, want lines=6 event=4 user=1 dead=1", res)
	}
	verifyCounts(t, db)

	// The single user doc is keyed by the resolved #user_id; with one user in
	// the fixture an empty filter pins it without re-deriving the resolution.
	eventBefore := rawDoc(t, db, "event", bson.M{"#uuid": "it-e1"})
	userBefore := rawDoc(t, db, "user", bson.M{})

	// Re-import: full re-read (no checkpoint). Nothing may change — the import is
	// now idempotent across all three collections.
	//
	// Dead letters used to be the documented exception here ("append-only
	// diagnostics: an invalid line has no uuid to converge on, so the re-run adds
	// one more"). That was a statement of a missing dedup key, not a wanted
	// behaviour, and DeadLetterModel now supplies one: a deterministic _id derived
	// from the sanitized line and error. The second arrival conflicts on _id, and
	// E11000 is already absorbed as a benign no-op, so the re-run converges.
	//
	// What must NOT change is the rate signal: res.DeadLetters is incremented at
	// parse time, so every occurrence is still reported per invocation even though
	// only one copy is stored. Both halves are pinned below.
	res2, err := eng.File(ctx, &api.FileConfig{Paths: paths})
	if err != nil {
		t.Fatalf("File re-run: %v", err)
	}
	if res2.Lines != 6 {
		t.Fatalf("re-run lines = %d, want 6 (no checkpoint, full re-read)", res2.Lines)
	}
	if res2.DeadLetters != 1 {
		t.Errorf("re-run res.DeadLetters = %d, want 1: the counter must still report every "+
			"occurrence even though the stored copy deduplicates", res2.DeadLetters)
	}
	if got := count(t, db, "event"); got != 4 {
		t.Errorf("events after re-import = %d, want 4 (uuid upsert idempotency)", got)
	}
	if got := count(t, db, "user"); got != 1 {
		t.Errorf("users after re-import = %d, want 1 (_ts-guarded idempotency)", got)
	}
	if got := count(t, db, "dead_letter"); got != 1 {
		t.Errorf("dead_letters after re-import = %d, want 1 (deterministic _id dedups the re-read)", got)
	}

	// Events are $setOnInsert-only: byte-identical across re-imports.
	if eventAfter := rawDoc(t, db, "event", bson.M{"#uuid": "it-e1"}); !bytes.Equal(eventBefore, eventAfter) {
		t.Errorf("event doc changed across re-import:\n before %v\n after  %v", eventBefore, eventAfter)
	}
	// The user doc's _ts is the ingestion-order guard and only advances
	// ($max-protected meta); every data field must be identical.
	userAfter := rawDoc(t, db, "user", bson.M{})
	beforeFields, beforeTS := splitUserTS(t, userBefore)
	afterFields, afterTS := splitUserTS(t, userAfter)
	if afterTS < beforeTS {
		t.Errorf("user _ts went backwards across re-import: %d -> %d", beforeTS, afterTS)
	}
	if !reflect.DeepEqual(beforeFields, afterFields) {
		t.Errorf("user data fields changed across re-import:\n before %v\n after  %v", beforeFields, afterFields)
	}
}

// splitUserTS decodes a user document and splits off the _ts ordering meta,
// returning the remaining fields and the _ts value.
func splitUserTS(t *testing.T, raw bson.Raw) (bson.M, int64) {
	t.Helper()
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal user doc: %v", err)
	}
	ts, ok := doc["_ts"].(int64)
	if !ok {
		t.Fatalf("user doc _ts missing or not int64: %v", doc["_ts"])
	}
	delete(doc, "_ts")
	return doc, ts
}

// TestFileCliRoleAcrossModes runs the cli role's file path (the function=file
// core) across the three upload strategies, re-importing under each to pin the
// idempotent convergence.
func TestFileCliRoleAcrossModes(t *testing.T) {
	for _, mode := range []process.Mode{process.ModeSingle, process.ModeBatch, process.ModePipeline} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			daoCfg, db, cleanup := freshDB(t)
			defer cleanup()
			ctx := context.Background()
			paths := writeSampleLogs(t)
			procCfg := &process.Config{Mode: string(mode)}
			fCfg := &api.FileConfig{Paths: paths}

			for run := 1; run <= 2; run++ {
				res, err := clirole.RunFile(ctx, daoCfg, procCfg, nil, fCfg)
				if err != nil {
					t.Fatalf("run %d: RunFile: %v", run, err)
				}
				if res.Lines != 6 {
					t.Fatalf("run %d: lines = %d, want 6", run, res.Lines)
				}
				if got := count(t, db, "event"); got != 4 {
					t.Errorf("run %d: events = %d, want 4", run, got)
				}
				if got := count(t, db, "user"); got != 1 {
					t.Errorf("run %d: users = %d, want 1", run, got)
				}
				// Dead letters converge across runs rather than appending: the
				// deterministic _id makes run 2's copy of the same bad line a
				// benign E11000 no-op. The per-run counter still reports it.
				if got := count(t, db, "dead_letter"); got != 1 {
					t.Errorf("run %d: dead_letters = %d, want 1 (deterministic _id dedups the re-import)", run, got)
				}
				if res.DeadLetters != 1 {
					t.Errorf("run %d: res.DeadLetters = %d, want 1 (every occurrence is still counted)", run, res.DeadLetters)
				}
			}
		})
	}
}
