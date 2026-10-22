package store

// Regression tests for the idempotency of the two dead_letter writers that are
// NOT DeadLetterModel: Store.quarantine (a document the server permanently
// refused) and Store.GiveUp -> isolate (a batch abandoned after the backpressure
// budget ran out).
//
// Why these needed their own key. DeadLetterModel gained a deterministic _id so
// that a re-read of the same bad line converges instead of appending. That closed
// the parse-error path but left isolate building a bare bson.M with a
// server-generated ObjectId, so the two paths that actually fired during the
// 2026-10-19 incident — quarantine and abandonment — still appended one copy per
// attempt. Attempts repeat by design: bulkWriteRetried narrows `models` to the
// transient remainder inside its closure, while flushBatch re-submits the full
// original batch on the next pass, and the tailer re-reads a file from the head on
// rotation. So the duplication was not a corner case, it was the normal path.
//
// The rate signal must survive the deduplication, exactly as it does for dead
// letters: Quarantined and Abandoned are incremented per occurrence, before the
// write, so an operator alerting on "documents were dropped" still sees every one
// of them even though the collection stores a single row.

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// poison models standing in for documents the server refused. They are ordinary
// event models: what makes them poison is the error the server returned, not their
// shape, and quarantine only ever renders them.
func rejectedEvents(uuids ...string) []poisonDoc {
	out := make([]poisonDoc, 0, len(uuids))
	for _, u := range uuids {
		out = append(out, poisonDoc{
			model:   EventWriteModel("track", u, bson.M{"#uuid": u, "#event_name": "login"}),
			code:    2,
			message: "BadValue: '$' is not allowed in field names",
		})
	}
	return out
}

// TestQuarantine_RereadIsIdempotent isolates the same two poison documents twice —
// what a re-submitted batch does — and requires one stored row per document while
// the counter still reports both attempts.
func TestQuarantine_RereadIsIdempotent(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	poison := rejectedEvents("q-idem-1", "q-idem-2")
	st.quarantine(ctx, "event", poison)
	st.quarantine(ctx, "event", poison)

	n, err := db.Collection("dead_letter").CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count dead_letter: %v", err)
	}
	if n != 2 {
		t.Errorf("dead_letter count = %d after quarantining the same 2 documents twice, want 2: "+
			"the second attempt must converge on the same _id, not append", n)
	}
	if got := st.Stats().TotalQuarantined(); got != 4 {
		t.Errorf("TotalQuarantined = %d, want 4: the counter must report every occurrence "+
			"even though the stored copy deduplicates", got)
	}
	if got := st.Stats().TotalAbandoned(); got != 0 {
		t.Errorf("TotalAbandoned = %d, want 0 (quarantine must not also count as abandoned)", got)
	}
}

// TestGiveUp_AbandonedIsIdempotent covers the abandonment path, and specifically
// that the varying part of the reason string cannot defeat the key. The worker
// builds "batch stuck for %s (maxStuckDuration=%s)" from a wall-clock duration, so
// two attempts at the same batch never carry the same message; if the _id hashed
// the message, deduplication would fail for exactly the case that recurs.
func TestGiveUp_AbandonedIsIdempotent(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	models := []mongo.WriteModel{
		EventWriteModel("track", "ab-idem-1", bson.M{"#uuid": "ab-idem-1"}),
		EventWriteModel("track", "ab-idem-2", bson.M{"#uuid": "ab-idem-2"}),
		EventWriteModel("track", "ab-idem-3", bson.M{"#uuid": "ab-idem-3"}),
	}
	st.GiveUp(ctx, st.EventCollection(), models, "batch stuck for 3m4s (maxStuckDuration=3m0s)")
	st.GiveUp(ctx, st.EventCollection(), models, "batch stuck for 6m11s (maxStuckDuration=3m0s)")

	n, err := db.Collection("dead_letter").CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count dead_letter: %v", err)
	}
	if n != int64(len(models)) {
		t.Errorf("dead_letter count = %d after abandoning the same %d-model batch twice with "+
			"different stuck durations, want %d", n, len(models), len(models))
	}
	if got := st.Stats().TotalAbandoned(); got != int64(2*len(models)) {
		t.Errorf("TotalAbandoned = %d, want %d (both attempts counted)", got, 2*len(models))
	}

	// Both rows must be recognisable as abandonments, and must carry the reason
	// class rather than the varying message.
	cur, err := db.Collection("dead_letter").Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	defer cur.Close(ctx)
	seen := 0
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			t.Fatalf("decode: %v", err)
		}
		seen++
		if got, _ := doc["_quarantine_reason"].(string); got != "abandoned: backpressure budget exhausted" {
			t.Errorf("_quarantine_reason = %q, want the abandonment class", got)
		}
		id, _ := doc["_id"].(string)
		if !strings.HasPrefix(id, "q-") {
			t.Errorf("_id = %v, want a %q-prefixed quarantine identifier", doc["_id"], "q-")
		}
	}
	if seen != len(models) {
		t.Errorf("decoded %d quarantine rows, want %d", seen, len(models))
	}
}

// TestQuarantine_SameDocumentDifferentClassStaysDistinguishable pins the other
// half of the key design: reason IS part of the identity, so a document first
// refused by the server and later abandoned leaves two rows rather than one. An
// operator reading dead_letter must be able to tell "the server will never accept
// this" from "we ran out of patience", and collapsing them would hide a change in
// the failure mode.
func TestQuarantine_SameDocumentDifferentClassStaysDistinguishable(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	models := []mongo.WriteModel{
		EventWriteModel("track", "both-classes", bson.M{"#uuid": "both-classes"}),
	}
	st.quarantine(ctx, "event", []poisonDoc{{model: models[0], code: 2, message: "BadValue"}})
	st.GiveUp(ctx, st.EventCollection(), models, "batch stuck for 3m0s (maxStuckDuration=3m0s)")

	n, err := db.Collection("dead_letter").CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count dead_letter: %v", err)
	}
	if n != 2 {
		t.Errorf("dead_letter count = %d, want 2: a rejection and an abandonment of the same "+
			"document are different facts and must not collapse", n)
	}
}

// TestQuarantineID_DomainSeparation is the unit-level guard on the key itself. It
// needs no server, so it also runs where the integration tests above skip.
func TestQuarantineID_DomainSeparation(t *testing.T) {
	const stmt = `db.event.insertOne({"#uuid":"x"})`

	base := quarantineID("event", "permanent bulk-write rejection", 2, stmt)
	if !strings.HasPrefix(base, "q-") || len(base) != len("q-")+40 {
		t.Fatalf("quarantineID = %q, want a %q prefix plus 40 hex chars", base, "q-")
	}
	if base != quarantineID("event", "permanent bulk-write rejection", 2, stmt) {
		t.Error("the same inputs produced different identifiers; the key is not deterministic")
	}
	if base == quarantineID("event", "abandoned: backpressure budget exhausted", 2, stmt) {
		t.Error("reason is not part of the identity: a rejection and an abandonment would collapse")
	}
	if base == quarantineID("event", "permanent bulk-write rejection", 121, stmt) {
		t.Error("code is not part of the identity")
	}
	if base == quarantineID("user", "permanent bulk-write rejection", 2, stmt) {
		t.Error("collection is not part of the identity")
	}
	if base == quarantineID("event", "permanent bulk-write rejection", 2, stmt+" ") {
		t.Error("statement is not part of the identity")
	}

	// A quarantine record must never collide with a parse-error dead letter, even
	// for identical content: the two populations share a collection and are told
	// apart by their _id prefix.
	if base == deadLetterID(stmt, "") || strings.HasPrefix(deadLetterID(stmt, ""), "q-") {
		t.Error("quarantineID and deadLetterID are not domain-separated")
	}
}
