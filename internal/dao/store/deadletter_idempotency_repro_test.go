package store

// REGRESSION TEST — landed intentionally red at 177b7fd, green from d527267.
//
// tailFileHybrid re-reads a whole file from offset 0 whenever os.Stat fails or the
// inode changes, and justifies that with a comment claiming re-reading is
// acceptable at-least-once because "the write models dedup events by #uuid".
//
// That reasoning only covered events. DeadLetterModel was a bare
// mongo.NewInsertOneModel() with no dedup key and no _ts guard, so every re-read of
// a line that failed to parse appended another dead letter. The same applied to the
// pipeline worker's outer retry loop, which re-submits the full original batch even
// though bulkWriteRetried narrowed models internally ("models = retry"): event and
// user are idempotent so that was harmless, dead_letter was not.
//
// So the at-least-once argument was false for exactly the collection that
// at-least-once is supposed to protect. This test pins the property the comment
// claimed; DeadLetterModel now carries a deterministic _id (deadLetterID) and the
// tailer comment was corrected to say what makes the claim hold.
//
// The two dead_letter writers that are NOT DeadLetterModel — quarantine and
// GiveUp's isolate — had the same hole and are covered separately in
// quarantine_idempotency_test.go.

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestReproBug3_DeadLetterIsNotIdempotentUnderReread feeds the same content
// twice, exactly as a whole-file re-read would, and compares the two collections.
func TestReproBug3_DeadLetterIsNotIdempotentUnderReread(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	count := func(coll string) int64 {
		t.Helper()
		n, err := db.Collection(coll).CountDocuments(ctx, bson.M{})
		if err != nil {
			t.Fatalf("count %s: %v", coll, err)
		}
		return n
	}

	// --- The case the tailer comment actually covers: an event. ---
	// Written twice with the same #uuid and _ts, as a re-read would. The upsert
	// keyed on #uuid plus the _ts guard must collapse them into one document.
	event := EventWriteModel("track", "idem-event-1", bson.M{
		"#uuid": "idem-event-1", "_ts": int64(1792600000000000000), "#event_name": "login",
	})
	for i := 0; i < 2; i++ {
		if err := st.BulkWrite(ctx, st.EventCollection(), []mongo.WriteModel{event}); err != nil {
			t.Fatalf("event write %d: %v", i, err)
		}
	}
	if n := count("event"); n != 1 {
		t.Errorf("event count = %d after writing the same #uuid twice, want 1", n)
	}

	// --- The case the comment does NOT cover: a dead letter. ---
	// The same unparseable line, read twice. A re-read is documented as safe, so
	// this must also collapse to one document.
	const badLine = `{"#type":"track","truncated":`
	for i := 0; i < 2; i++ {
		dl := DeadLetterModel(badLine, errors.New("invalid JSON: unexpected end of JSON input"))
		if err := st.BulkWrite(ctx, st.DeadLetterCollection(), []mongo.WriteModel{dl}); err != nil {
			t.Fatalf("dead_letter write %d: %v", i, err)
		}
	}
	if n := count("dead_letter"); n != 1 {
		t.Errorf("dead_letter count = %d after re-reading the same bad line twice, want 1: "+
			"DeadLetterModel is a bare InsertOneModel with no dedup key, so the at-least-once "+
			"re-read argument in tailer.go:625-626 does not hold for this collection", n)
	}

	// --- Same asymmetry through the pipeline's outer retry path. ---
	// bulkWriteRetried narrows models to the transient remainder internally, but
	// flushBatch re-submits the full original batch on the next attempt. For
	// dead_letter that duplicates; the two writes below model one such re-submit.
	const anotherBadLine = `{"#type":"user_set",`
	first := DeadLetterModel(anotherBadLine, errors.New("invalid JSON"))
	second := DeadLetterModel(anotherBadLine, errors.New("invalid JSON"))
	if err := st.BulkWrite(ctx, st.DeadLetterCollection(), []mongo.WriteModel{first}); err != nil {
		t.Fatalf("dead_letter re-submit 1: %v", err)
	}
	if err := st.BulkWrite(ctx, st.DeadLetterCollection(), []mongo.WriteModel{first, second}); err != nil {
		t.Fatalf("dead_letter re-submit 2: %v", err)
	}
	if n := count("dead_letter"); n != 2 {
		t.Errorf("dead_letter count = %d, want 2 distinct bad lines (one document each) "+
			"after a batch re-submit that repeated the first", n)
	}
}
