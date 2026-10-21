package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/aura-studio/tango/internal/logging"
)

// unitStore builds a Store pointed at an unreachable MongoDB so counter and
// control-flow behaviour can be asserted without a live server. mongo.Connect is
// lazy, so construction itself succeeds and every write fails fast.
func unitStore(t *testing.T) (*Store, func()) {
	t.Helper()
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/llb5").
		SetServerSelectionTimeout(200 * time.Millisecond).
		SetConnectTimeout(200 * time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return New(client.Database("llb5"), &Config{MaxElapsedTime: 200 * time.Millisecond}),
		func() { _ = client.Disconnect(context.Background()) }
}

// TestGiveUp_Counters pins that GiveUp accounts for released documents under
// Abandoned and never double-counts them as Quarantined: the two counters name
// different reasons for reaching the same sink, and an operator alerting on
// "data was dropped" needs them distinguishable.
func TestGiveUp_Counters(t *testing.T) {
	st, cleanup := unitStore(t)
	defer cleanup()

	models := []mongo.WriteModel{
		EventWriteModel("track", "c1", bson.M{"#uuid": "c1"}),
		EventWriteModel("track", "c2", bson.M{"#uuid": "c2"}),
		EventWriteModel("track", "c3", bson.M{"#uuid": "c3"}),
	}
	st.GiveUp(context.Background(), st.EventCollection(), models, "unit test")

	if got := st.Stats().TotalAbandoned(); got != int64(len(models)) {
		t.Errorf("TotalAbandoned = %d, want %d", got, len(models))
	}
	if got := st.Stats().TotalQuarantined(); got != 0 {
		t.Errorf("TotalQuarantined = %d, want 0 (GiveUp must not also count Quarantined)", got)
	}

	// An empty batch is a no-op and must not bump the counter.
	before := st.Stats().TotalAbandoned()
	st.GiveUp(context.Background(), st.EventCollection(), nil, "unit test")
	if got := st.Stats().TotalAbandoned(); got != before {
		t.Errorf("empty batch changed TotalAbandoned: %d -> %d", before, got)
	}
}

// TestGiveUp_IsolatesToDeadLetter is the integration half: a released batch must
// actually land in dead_letter so the data is recoverable, and must not have
// reached its intended collection.
func TestGiveUp_IsolatesToDeadLetter(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	const reason = "abandoned: backpressure budget exhausted"
	models := []mongo.WriteModel{
		EventWriteModel("track", "giveup-1", bson.M{"#uuid": "giveup-1"}),
		EventWriteModel("track", "giveup-2", bson.M{"#uuid": "giveup-2"}),
	}
	st.GiveUp(ctx, st.EventCollection(), models, "integration test")

	if got := st.Stats().TotalAbandoned(); got != 2 {
		t.Errorf("TotalAbandoned = %d, want 2", got)
	}
	n, err := db.Collection("dead_letter").CountDocuments(ctx, bson.M{"_quarantine_reason": reason})
	if err != nil {
		t.Fatalf("count dead_letter: %v", err)
	}
	if n != 2 {
		t.Errorf("dead_letter holds %d isolated records, want 2", n)
	}
	en, err := db.Collection("event").CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count event: %v", err)
	}
	if en != 0 {
		t.Errorf("event holds %d documents, want 0 (GiveUp must not write to the target collection)", en)
	}
}

// TestGiveUp_DeadLetterBatchIsNotReSunk pins the special case: dead-letter
// models are non-idempotent inserts, so a batch of dead letters that is itself
// unwinnable must be logged and released, never written back into dead_letter.
func TestGiveUp_DeadLetterBatchIsNotReSunk(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	models := []mongo.WriteModel{
		DeadLetterModel(`{"a":1}`, errors.New("not a ThinkingData payload")),
		DeadLetterModel(`{"b":2}`, errors.New("not a ThinkingData payload")),
	}
	st.GiveUp(ctx, st.DeadLetterCollection(), models, "integration test")

	if got := st.Stats().TotalAbandoned(); got != 2 {
		t.Errorf("TotalAbandoned = %d, want 2", got)
	}
	n, err := db.Collection("dead_letter").CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count dead_letter: %v", err)
	}
	if n != 0 {
		t.Errorf("dead_letter holds %d documents, want 0: GiveUp must not re-sink dead letters into their own collection", n)
	}
}

// TestQuarantine_PoisonedModelIsStorable is the end-to-end form of the
// 2026-10-19 incident. A dead-letter model carries the offending raw log line;
// quarantine renders it with %+v, which re-emits exactly the bytes that got the
// document rejected. Without sanitization the isolation write fails for the same
// reason and the evidence is dropped.
//
// Note: plain mongod is more permissive than DocumentDB about invalid UTF-8, so
// against a local server this pins "the isolation write succeeded and the stored
// model is clean" rather than reproducing the server-side rejection. Point
// TANGO_TEST_MONGO_URI at DocumentDB for the full reproduction.
func TestQuarantine_PoisonedModelIsStorable(t *testing.T) {
	st, db, cleanup := testSetup(t)
	defer cleanup()
	ctx := context.Background()

	// A line with both a raw NUL and an invalid UTF-8 byte, i.e. the shape that
	// fails json.Unmarshal AND would be refused by DocumentDB.
	poisonLine := "{\"#type\":\"track\",\"text\":\"a\x00b\xff\"}"
	st.quarantine(ctx, "dead_letter", []poisonDoc{{
		model:   DeadLetterModel(poisonLine, errors.New("invalid character in string")),
		code:    0,
		message: " Badly formed input data",
	}})

	if got := st.Stats().TotalQuarantined(); got != 1 {
		t.Errorf("TotalQuarantined = %d, want 1", got)
	}
	if got := st.Stats().TotalAbandoned(); got != 0 {
		t.Errorf("TotalAbandoned = %d, want 0 (quarantine counts Quarantined, not Abandoned)", got)
	}

	var doc bson.M
	if err := db.Collection("dead_letter").FindOne(ctx, bson.M{}).Decode(&doc); err != nil {
		t.Fatalf("the isolation write did not land: %v", err)
	}
	model, ok := doc["model"].(string)
	if !ok {
		t.Fatalf("model field = %T, want string", doc["model"])
	}
	assertBSONSafe(t, "quarantine.model", model)
	if strings.ContainsRune(model, 0) {
		t.Error("quarantine.model still contains a raw NUL")
	}
	if !utf8.ValidString(model) {
		t.Errorf("quarantine.model is not valid UTF-8: %q", model)
	}
	// The diagnosis must survive the cleaning: the payload is still recognisable
	// and the offending byte is still visible, now as a readable escape.
	if !strings.Contains(model, "track") {
		t.Errorf("quarantine.model lost the identifiable payload: %q", model)
	}
	if !strings.Contains(model, `\x00`) {
		t.Errorf("quarantine.model lost the NUL diagnostic: %q", model)
	}
	// And it must be a replayable statement, not a Go struct dump. The old %+v
	// rendering emitted field names and pointer addresses (Upsert:0xc000216100),
	// which told an operator nothing they could act on.
	if !strings.HasPrefix(model, "db.dead_letter.insertOne(") {
		t.Errorf("quarantine.model is not a mongosh statement: %q", model)
	}
	if strings.Contains(model, "0xc0") {
		t.Errorf("quarantine.model leaks a Go pointer address: %q", model)
	}
	if strings.Contains(model, "Document:") {
		t.Errorf("quarantine.model still looks like a Go struct dump: %q", model)
	}
	if msg, _ := doc["_quarantine_message"].(string); !strings.Contains(msg, "Badly formed input data") {
		t.Errorf("_quarantine_message = %q, want it to carry the server's reason", msg)
	}
}

// ---------------------------------------------------------------------------
// log capture — the abandoned-dead-letter path persists nothing, so its log
// lines ARE the record and have to be asserted on directly.
// ---------------------------------------------------------------------------

type captureHook struct {
	mu      sync.Mutex
	entries []logrus.Entry
}

func (c *captureHook) Levels() []logrus.Level { return logrus.AllLevels }

func (c *captureHook) Fire(e *logrus.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := logrus.Entry{Level: e.Level, Message: e.Message, Data: logrus.Fields{}}
	for k, v := range e.Data {
		cp.Data[k] = v
	}
	c.entries = append(c.entries, cp)
	return nil
}

func (c *captureHook) count(msgSubstr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.entries {
		if strings.Contains(e.Message, msgSubstr) {
			n++
		}
	}
	return n
}

func installCapture(t *testing.T) *captureHook {
	t.Helper()
	c := &captureHook{}
	l := logging.L()
	old := logrus.LevelHooks{}
	for lvl, hs := range l.Hooks {
		old[lvl] = hs
	}
	l.AddHook(c)
	t.Cleanup(func() { l.ReplaceHooks(old) })
	return c
}

// TestGiveUp_DeadLetterAbandonLogsEveryLine pins that no abandoned dead letter
// vanishes silently. On this path nothing is persisted anywhere — dead letters
// are not re-sunk into their own collection, and the source files rotate away
// within minutes — so the log line is the only surviving record. An earlier
// revision capped this at maxDetailLogsPerBatch, which would have discarded 123
// of a full 128-model batch with nothing but a counter to show for it.
func TestGiveUp_DeadLetterAbandonLogsEveryLine(t *testing.T) {
	st, cleanup := unitStore(t)
	defer cleanup()
	hook := installCapture(t)

	const n = maxDetailLogsPerBatch * 4 // deliberately well over the isolate cap
	models := make([]mongo.WriteModel, 0, n)
	for i := 0; i < n; i++ {
		models = append(models, DeadLetterModel(fmt.Sprintf(`{"i":%d}`, i), errors.New("parse")))
	}
	st.GiveUp(context.Background(), st.DeadLetterCollection(), models, "test")

	if got := hook.count("dead-letter document abandoned"); got != n {
		t.Errorf("logged %d abandoned dead letters, want %d — every line must survive in the log", got, n)
	}
	if got := st.Stats().TotalAbandoned(); got != int64(n) {
		t.Errorf("TotalAbandoned = %d, want %d", got, n)
	}
	// The overflow summary must NOT fire here: nothing is being withheld, so
	// claiming "further documents not logged individually" would be false.
	if got := hook.count("not logged individually"); got != 0 {
		t.Errorf("abandoned-dead-letter path emitted %d overflow summaries, want 0", got)
	}
}

// TestIsolate_CapsDetailLogsButSummarises pins the other half of the logging
// contract: when documents ARE persisted to dead_letter, per-document logging is
// capped so an outage cannot emit thousands of lines — but the cap must be
// announced, never silent.
func TestIsolate_CapsDetailLogsButSummarises(t *testing.T) {
	st, cleanup := unitStore(t)
	defer cleanup()
	hook := installCapture(t)

	const n = maxDetailLogsPerBatch * 4
	poison := make([]poisonDoc, 0, n)
	for i := 0; i < n; i++ {
		poison = append(poison, poisonDoc{
			model:   DeadLetterModel(fmt.Sprintf(`{"i":%d}`, i), errors.New("parse")),
			code:    2,
			message: "BadValue",
		})
	}
	st.quarantine(context.Background(), "event", poison)

	if got := hook.count("document permanently rejected"); got != maxDetailLogsPerBatch {
		t.Errorf("logged %d detail lines, want the cap %d", got, maxDetailLogsPerBatch)
	}
	if got := hook.count("not logged individually"); got != 1 {
		t.Errorf("overflow summaries = %d, want exactly 1", got)
	}
	if got := st.Stats().TotalQuarantined(); got != int64(n) {
		t.Errorf("TotalQuarantined = %d, want %d (the counter must count ALL of them, not just the logged ones)", got, n)
	}
}
