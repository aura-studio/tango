package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestIsTransientWriteCode(t *testing.T) {
	for _, c := range []int{11600, 11602, 10107, 13435, 13436, 189, 91, 7, 6, 89, 9001, 262, 50, 64, 16500} {
		if !isTransientWriteCode(c) {
			t.Errorf("code %d should be classified transient", c)
		}
	}
	// Permanent / unknown codes must NOT be transient (so they get quarantined).
	// 11000 (dup-key) is handled separately as a benign no-op, not "transient".
	for _, c := range []int{2 /*BadValue*/, 9 /*FailedToParse*/, 121 /*DocumentValidationFailure*/, 10334 /*BSONObjectTooLarge*/, 52, 56, 0, 11000, 99999} {
		if isTransientWriteCode(c) {
			t.Errorf("code %d should NOT be classified transient", c)
		}
	}
}

func TestClassifyBulkWriteError(t *testing.T) {
	// 5 models: index 0 & 4 succeed (no write error), 1 dup-key (benign), 2
	// transient (retry), 3 poison (permanent -> quarantine).
	models := make([]mongo.WriteModel, 5)
	for i := range models {
		models[i] = mongo.NewInsertOneModel().SetDocument(bson.M{"i": i})
	}
	var err error = mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Index: 1, Code: 11000, Message: "dup"}},
			{WriteError: mongo.WriteError{Index: 2, Code: 10107, Message: "not primary"}},
			{WriteError: mongo.WriteError{Index: 3, Code: 2, Message: "$bad prefixed field not valid for storage"}},
		},
	}
	retry, poison, perDoc := classifyBulkWriteError(err, models)
	if !perDoc {
		t.Fatal("perDoc should be true for a BulkWriteException with WriteErrors")
	}
	if len(retry) != 1 {
		t.Errorf("retry=%d, want 1 (the transient 10107)", len(retry))
	}
	if len(poison) != 1 {
		t.Fatalf("poison=%d, want 1 (the permanent code 2)", len(poison))
	}
	if poison[0].code != 2 {
		t.Errorf("poison code=%d, want 2", poison[0].code)
	}

	// A plain (non-bulk) error has no per-document detail -> whole-batch retry.
	if _, _, perDoc := classifyBulkWriteError(errors.New("connection refused"), models); perDoc {
		t.Error("plain error should yield perDoc=false (retry whole batch)")
	}

	// A write-concern error is transient at the operation level -> whole retry.
	var wce error = mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "wc"}}
	if _, _, perDoc := classifyBulkWriteError(wce, models); perDoc {
		t.Error("write-concern error should yield perDoc=false (retry whole batch)")
	}

	// All-poison batch: every failure permanent -> retry empty, all quarantined.
	var allBad error = mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Index: 0, Code: 2, Message: "bad"}},
			{WriteError: mongo.WriteError{Index: 1, Code: 10334, Message: "too large"}},
		},
	}
	retry2, poison2, _ := classifyBulkWriteError(allBad, models)
	if len(retry2) != 0 || len(poison2) != 2 {
		t.Errorf("all-poison: retry=%d poison=%d, want 0 and 2", len(retry2), len(poison2))
	}
}

func TestIsTooLargeError(t *testing.T) {
	// DocumentDB: CommandError code 80 ("Query size ... exceeded maximum query size").
	if !isTooLargeError(mongo.CommandError{Code: 80, Message: "Query size 17825821 exceeded maximum query size 16793600"}) {
		t.Error("CommandError code 80 should be too-large")
	}
	// MongoDB: BSONObjectTooLarge (10334).
	if !isTooLargeError(mongo.CommandError{Code: 10334, Message: "object to insert too large"}) {
		t.Error("code 10334 should be too-large")
	}
	// Message fallback.
	if !isTooLargeError(errors.New("write batch exceeded maximum size")) {
		t.Error("'exceeded maximum' message should be too-large")
	}
	// Not too-large: a transient/other error.
	if isTooLargeError(errors.New("connection refused")) {
		t.Error("connection refused should NOT be too-large")
	}
	if isTooLargeError(nil) {
		t.Error("nil should not be too-large")
	}
}

// TestIsPermanentCommandError covers the error shape the 2026-10-19 incident
// produced: DocumentDB refusing a whole command with no codeName and no usable
// code. classifyBulkWriteError cannot see it (it is not a BulkWriteException)
// and isTooLargeError does not match it, so it fell through to the transient
// branch and was retried forever.
func TestIsPermanentCommandError(t *testing.T) {
	// Reproduce the exact text seen in production. DocumentDB omits codeName, so
	// Name is empty and CommandError.Error() unconditionally prepends a space.
	incident := mongo.CommandError{Message: "Badly formed input data"}
	if got, want := incident.Error(), " Badly formed input data"; got != want {
		t.Errorf("CommandError.Error() = %q, want %q (the leading space proves Name is empty)", got, want)
	}
	if !isPermanentCommandError(incident) {
		t.Error("the incident error must be classified permanent; it was retried forever")
	}
	if !isUnwinnableError(incident) {
		t.Error("the incident error must be unwinnable so bulkWrite releases it")
	}
	if got := commandCode(incident); got != 0 {
		t.Errorf("commandCode = %d, want 0 (DocumentDB sends no code here)", got)
	}

	permanent := []struct {
		name string
		err  error
	}{
		{"BadValue 2", mongo.CommandError{Code: 2, Message: "invalid field name"}},
		{"FailedToParse 9", mongo.CommandError{Code: 9, Message: "unknown field"}},
		{"DocumentValidationFailure 121", mongo.CommandError{Code: 121, Message: "validation failed"}},
		{"unknown code 99999", mongo.CommandError{Code: 99999, Message: "something new"}},
		{"wrapped in another error", fmt.Errorf("bulk write: %w", mongo.CommandError{Code: 2, Message: "bad"})},
	}
	for _, tc := range permanent {
		if !isPermanentCommandError(tc.err) {
			t.Errorf("%s: want permanent", tc.name)
		}
		if !isUnwinnableError(tc.err) {
			t.Errorf("%s: want unwinnable", tc.name)
		}
	}

	// A backend failover or throttle must keep its backpressure: classifying
	// these as permanent would quarantine recoverable data during an outage.
	transient := []struct {
		name string
		err  error
	}{
		{"NotWritablePrimary 10107", mongo.CommandError{Code: 10107, Message: "not primary"}},
		{"ShutdownInProgress 91", mongo.CommandError{Code: 91, Message: "shutting down"}},
		{"InterruptedAtShutdown 11600", mongo.CommandError{Code: 11600, Message: "interrupted"}},
		{"InterruptedDueToReplStateChange 11602", mongo.CommandError{Code: 11602, Message: "repl state"}},
		{"NetworkTimeout 89", mongo.CommandError{Code: 89, Message: "timeout"}},
		{"throttling 16500", mongo.CommandError{Code: 16500, Message: "rate limited"}},
		{"WriteConcernFailed 64", mongo.CommandError{Code: 64, Message: "write concern"}},
	}
	for _, tc := range transient {
		if isPermanentCommandError(tc.err) {
			t.Errorf("%s: must NOT be permanent", tc.name)
		}
		if isUnwinnableError(tc.err) {
			t.Errorf("%s: must NOT be unwinnable", tc.name)
		}
	}

	// Too-large codes are unwinnable, but through isTooLargeError, which owns
	// the split-then-quarantine path; isPermanentCommandError must stay out of it.
	for _, code := range []int32{80, 10334} {
		err := mongo.CommandError{Code: code, Message: "size"}
		if isPermanentCommandError(err) {
			t.Errorf("code %d: too-large owns this path, isPermanentCommandError must be false", code)
		}
		if !isUnwinnableError(err) {
			t.Errorf("code %d: want unwinnable via isTooLargeError", code)
		}
	}
	if got := commandCode(mongo.CommandError{Code: 10334, Message: "x"}); got != 10334 {
		t.Errorf("commandCode = %d, want 10334", got)
	}

	// Errors with no server response behind them must never look permanent.
	if isPermanentCommandError(nil) || isUnwinnableError(nil) {
		t.Error("nil must be neither permanent nor unwinnable")
	}
	if isPermanentCommandError(errors.New("connection refused")) {
		t.Error("a plain error must not be a permanent CommandError")
	}
	if isUnwinnableError(errors.New("connection refused")) {
		t.Error("a plain error must not be unwinnable")
	}
	// A per-document rejection is handled by classifyBulkWriteError, not here.
	perDoc := mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{
			{WriteError: mongo.WriteError{Index: 0, Code: 2, Message: "bad"}},
		},
	}
	if isPermanentCommandError(perDoc) {
		t.Error("a BulkWriteException must not be classified as a permanent CommandError")
	}
	if isUnwinnableError(perDoc) {
		t.Error("a per-document BulkWriteException is not unwinnable; it is partitioned per doc")
	}
	if got := commandCode(errors.New("no code")); got != 0 {
		t.Errorf("commandCode of a codeless error = %d, want 0", got)
	}
}

// TestIsUnwinnableError_RealOutageIsTransient pins the property that matters
// most for safety: a backend that is merely unreachable must NOT be classified
// as unwinnable, or an outage would quarantine recoverable data instead of
// applying backpressure. Port 1 is closed, so the write fails fast with no
// server involved.
func TestIsUnwinnableError_RealOutageIsTransient(t *testing.T) {
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/llb5").
		SetServerSelectionTimeout(300 * time.Millisecond).
		SetConnectTimeout(300 * time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	_, werr := client.Database("llb5").Collection("event").BulkWrite(context.Background(),
		[]mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"a": 1})},
		options.BulkWrite().SetOrdered(false))
	if werr == nil {
		t.Skip("expected the unreachable server to fail the write, but it succeeded")
	}
	t.Logf("outage error: %T: %v", werr, werr)

	if isPermanentCommandError(werr) {
		t.Errorf("an unreachable backend was classified permanent (%T: %v)", werr, werr)
	}
	if isUnwinnableError(werr) {
		t.Errorf("an unreachable backend was classified unwinnable (%T: %v); an outage would quarantine recoverable data", werr, werr)
	}
}
