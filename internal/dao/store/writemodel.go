package store

import (
	"crypto/sha1"
	"encoding/hex"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ---------------------------------------------------------------------------
// Meta / data field separation (used by user write models)
// ---------------------------------------------------------------------------

// metaKeys are TA identifier and timestamp fields managed separately from
// business data fields. _ts is the ingestion timestamp used for ordering.
var metaKeys = map[string]struct{}{
	"#uuid":        {},
	"#type":        {},
	"#time":        {},
	"#user_id":     {},
	"#account_id":  {},
	"#distinct_id": {},
	"_ts":          {},
}

// splitFields separates a document into meta (identifier/timestamp) fields
// and data (business property) fields.
func splitFields(doc bson.M) (meta bson.M, data bson.M) {
	meta = make(bson.M, 6)
	data = make(bson.M, len(doc))
	for k, v := range doc {
		if _, ok := metaKeys[k]; ok {
			meta[k] = v
		} else {
			data[k] = v
		}
	}
	return meta, data
}

// tsGuardOr builds the query-filter fragment that enforces _ts anti-rollback at
// the document level: it matches only when the stored document has no _ts yet or
// its _ts is not newer than the incoming ts. Combined with upsert, a write whose
// target already holds a newer _ts simply fails to match; the upsert's insert
// attempt then hits the unique key — a benign duplicate-key error the store
// treats as a skip. Because every TA record carries a single _ts, this
// whole-document guard is equivalent to the former per-field $cond pipelines,
// but uses plain document-form updates that Amazon DocumentDB accepts (DocumentDB
// rejects aggregation-pipeline updates with "Wrong type for parameter u").
func tsGuardOr(ts any) bson.A {
	return bson.A{
		bson.M{"_ts": bson.M{"$exists": false}},
		bson.M{"_ts": bson.M{"$lte": ts}},
	}
}

// metaMaxUpdate builds a traditional (non-pipeline) update document that uses
// $max for _ts and #time, and $set for other identity meta fields.
// This is used by operations where data fields have their own semantics
// ($inc, $push, $addToSet, $setOnInsert) and only the meta timestamps need protection.
func metaMaxUpdate(meta bson.M) bson.M {
	update := bson.M{}
	setFields := make(bson.M, len(meta))
	maxFields := make(bson.M, 2)

	for k, v := range meta {
		switch k {
		case "_ts":
			maxFields["_ts"] = v
		default:
			setFields[k] = v
		}
	}

	if len(setFields) > 0 {
		update["$set"] = setFields
	}
	if len(maxFields) > 0 {
		update["$max"] = maxFields
	}
	return update
}

// ---------------------------------------------------------------------------
// User write models  (ThinkingData user_* semantics)
// ---------------------------------------------------------------------------

// mergeFields merges meta and data fields into a single bson.M.
func mergeFields(meta, data bson.M) bson.M {
	all := make(bson.M, len(meta)+len(data))
	for k, v := range meta {
		all[k] = v
	}
	for k, v := range data {
		all[k] = v
	}
	return all
}

// upsertWithOperator builds an upsert UpdateOneModel that protects meta
// timestamps via $max/$set and applies the given MongoDB operator to the data.
func upsertWithOperator(filter, meta bson.M, op string, data bson.M) mongo.WriteModel {
	update := metaMaxUpdate(meta)
	if len(data) > 0 {
		update[op] = data
	}
	return mongo.NewUpdateOneModel().
		SetFilter(filter).
		SetUpdate(update).
		SetUpsert(true)
}

// UserWriteModel builds the appropriate MongoDB write model for a user operation.
// The user document is keyed by #user_id (the resolved TA user ID) so that all
// user_* operations for the same user are applied to a single document.
//
// Timestamp-based ordering: all user operations use _ts (ingestion timestamp)
// to prevent out-of-order writes. Older records (lower _ts) cannot overwrite
// fields that were already set by newer records (higher _ts).
func UserWriteModel(typ string, userID int64, doc bson.M) mongo.WriteModel {
	filter := bson.M{"#user_id": userID}
	doc["#user_id"] = userID
	meta, data := splitFields(doc)
	ts := meta["_ts"]

	switch typ {
	case "user_set":
		// Overwrite user properties, guarding against older records via the _ts
		// filter (DocumentDB-compatible document-form update; see tsGuardOr).
		filter["$or"] = tsGuardOr(ts)
		return mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(bson.M{"$set": mergeFields(meta, data)}).
			SetUpsert(true)

	case "user_setOnce":
		// Set properties only on insert; existing values are not overwritten.
		// Meta timestamps use $max to only advance forward.
		return upsertWithOperator(filter, meta, "$setOnInsert", data)

	case "user_add":
		// Increment numeric user properties. $inc is commutative so order doesn't
		// matter for data fields. Meta timestamps use $max.
		return upsertWithOperator(filter, meta, "$inc", data)

	case "user_unset":
		// Remove specified user properties, guarding against older records via the
		// _ts filter. meta (incl. _ts) is $set; the data fields are $unset. The two
		// key sets are disjoint, so no field is both set and unset.
		unset := make(bson.M, len(data))
		for k := range data {
			unset[k] = ""
		}
		filter["$or"] = tsGuardOr(ts)
		update := bson.M{"$set": meta}
		if len(unset) > 0 {
			update["$unset"] = unset
		}
		return mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

	case "user_del":
		// Delete the entire user record. No timestamp check needed.
		return mongo.NewDeleteOneModel().SetFilter(filter)

	case "user_append":
		// Append values to array-type user properties. $push is additive,
		// order doesn't affect correctness. Meta timestamps use $max.
		return upsertWithOperator(filter, meta, "$push", toEachFields(data))

	case "user_uniq_append":
		// Append values to array-type properties with deduplication.
		// $addToSet is idempotent. Meta timestamps use $max.
		return upsertWithOperator(filter, meta, "$addToSet", toEachFields(data))

	default:
		// Fallback: same as user_set.
		filter["$or"] = tsGuardOr(ts)
		return mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(bson.M{"$set": mergeFields(meta, data)}).
			SetUpsert(true)
	}
}

// toEachFields wraps values in $each for $push/$addToSet operations.
func toEachFields(data bson.M) bson.M {
	result := make(bson.M, len(data))
	for k, v := range data {
		switch arr := v.(type) {
		case []any:
			result[k] = bson.M{"$each": arr}
		default:
			result[k] = bson.M{"$each": []any{v}}
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Event write models  (ThinkingData track* semantics)
// ---------------------------------------------------------------------------

// EventWriteModel builds the appropriate MongoDB write model for an event operation.
func EventWriteModel(typ, uuid string, doc bson.M) mongo.WriteModel {
	// Ensure required meta fields are present for ordering and upsert.
	if doc == nil {
		doc = bson.M{}
	}
	if v, ok := doc["#uuid"].(string); !ok || v == "" {
		doc["#uuid"] = uuid
	}
	ts, ok := doc["_ts"]
	if !ok || ts == nil {
		ts = time.Now().UnixNano()
		doc["_ts"] = ts
	}

	switch typ {
	case "track":
		// Upsert by #uuid to ensure idempotency: if the event already exists
		// (e.g. daemon restart re-reads from file beginning), it is skipped.
		// Uses $setOnInsert so existing documents are never modified.
		return mongo.NewUpdateOneModel().
			SetFilter(bson.M{"#uuid": uuid}).
			SetUpdate(bson.M{"$setOnInsert": doc}).
			SetUpsert(true)

	case "track_update":
		// Field-level update with per-#uuid `_ts` ordering protection, enforced via
		// the _ts filter (DocumentDB-compatible document-form update; see tsGuardOr).
		return mongo.NewUpdateOneModel().
			SetFilter(bson.M{"#uuid": uuid, "$or": tsGuardOr(ts)}).
			SetUpdate(bson.M{"$set": doc}).
			SetUpsert(true)

	case "track_overwrite":
		// Full replacement with per-#uuid `_ts` ordering protection, enforced via
		// the _ts filter. ReplaceOne swaps the whole document for `doc`.
		return mongo.NewReplaceOneModel().
			SetFilter(bson.M{"#uuid": uuid, "$or": tsGuardOr(ts)}).
			SetReplacement(doc).
			SetUpsert(true)

	default:
		// Default: same as track, upsert with $setOnInsert for idempotency.
		return mongo.NewUpdateOneModel().
			SetFilter(bson.M{"#uuid": uuid}).
			SetUpdate(bson.M{"$setOnInsert": doc}).
			SetUpsert(true)
	}
}

// ---------------------------------------------------------------------------
// Dead letter write model
// ---------------------------------------------------------------------------

// DeadLetterModel creates an insert model for a failed log line.
//
// Both stored strings are sanitized. `line` is the raw scanner output and is
// the reason this matters: a line carrying a NUL byte or invalid UTF-8 parses
// into a Go string unchanged, the bson encoder writes it unchanged, and the
// server then rejects the entire insert command (" Badly formed input data") —
// a command-level rejection with no per-document detail, which used to be
// retried forever and stalled ingestion. Cleaning here makes the dead letter
// writable, so the bad data is preserved for inspection instead of wedging the
// pipeline. Control bytes are rendered as readable `\xNN` escapes rather than
// dropped, so the diagnosis survives.
//
// The document also carries a deterministic _id derived from the sanitized
// content. Unlike the event and user models, a dead letter has no producer-
// assigned identity to dedup on, so it used to be the one write model that
// re-delivery could duplicate — and re-delivery is normal here, not exceptional:
// the tailer resets its offset and re-reads a file on rotation or a scan error,
// and the pipeline worker re-submits a whole batch after a transient failure.
// With a stable _id the second arrival hits E11000, which bulkWriteRetried
// already treats as a benign no-op (isOnlyDuplicateKey / classifyBulkWriteError),
// so no store change was needed.
//
// The trade-off is deliberate: two genuinely separate occurrences of a
// byte-identical bad line collapse into one dead_letter document. That is
// acceptable because the dead_letters counter is incremented at parse time, not
// at write time, so the RATE signal an operator alerts on still counts both —
// only the stored copy is deduplicated.
func DeadLetterModel(line string, parseErr error) mongo.WriteModel {
	errMsg := ""
	if parseErr != nil {
		errMsg = parseErr.Error()
	}
	safeLine := sanitizeForBSON(line, maxDeadLetterLineBytes)
	safeErr := sanitizeForBSON(errMsg, logPreviewBytes)
	doc := bson.M{
		"_id":   deadLetterID(safeLine, safeErr),
		"_ts":   time.Now().UnixNano(),
		"line":  safeLine,
		"error": safeErr,
	}
	return mongo.NewInsertOneModel().SetDocument(doc)
}

// deadLetterID derives a stable _id from already-sanitized content, so the same
// bad line produces the same identifier no matter when or how many times it is
// read. Hashing the sanitized form rather than the raw line is what makes this
// work: sanitizing is deterministic, so equal inputs still hash equal, and it
// keeps the identifier free of the bytes that made the document unwritable in
// the first place. The domain-separation prefix follows the existing convention
// in internal/backfill/rowdecode.go's synthUserUUID.
func deadLetterID(safeLine, safeErr string) string {
	h := sha1.Sum([]byte("tango/dead_letter\x00" + safeLine + "\x00" + safeErr))
	return "dl-" + hex.EncodeToString(h[:])
}
