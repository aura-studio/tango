package store

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
