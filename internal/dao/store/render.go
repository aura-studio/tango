package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// renderWriteModel renders a write model as a mongosh statement, e.g.
//
//	db.event.updateOne({"#uuid":"u1"}, {"$set":{"a":"v"}}, {"upsert":true})
//	db.dead_letter.insertOne({"_ts":{"$numberLong":"1792..."},"line":"...","error":"..."})
//	db.user.deleteOne({"#user_id":{"$numberLong":"7"}})
//
// Why this exists: when a bulk write fails, the only thing an operator used to
// get was the error text and a collection name. The 2026-10-19 wedge repeated
// such a line ~35,000 times over two days and nothing in it said WHICH document
// was being refused, so the cause could not be identified from the logs at all.
// The quarantine record stored fmt.Sprintf("%+v", model), which is worse than
// useless for replay: it emits Go field names and pointer addresses
// (Upsert:0xc000216100) rather than anything MongoDB understands.
//
// The output is intended to be pasted into mongosh to reproduce a failure, so
// documents are rendered as CANONICAL Extended JSON. Relaxed mode would be
// prettier but renders int64 as a bare JSON number, and a value like _ts
// (UnixNano, ~1.8e18) exceeds 2^53 — mongosh would parse it as a double and
// silently lose precision, so the "replayable" statement would not actually
// reproduce the write. Canonical emits {"$numberLong":"..."} and round-trips
// exactly, at the cost of some noise around small integers.
//
// The result is NOT sanitized or length-capped: callers decide, because a log
// preview needs previewForLog (single line) while a stored field needs
// sanitizeForBSON. Rendering happens only on failure paths, never per document
// on the success path.
//
// Rendering IS deterministic: the same model always produces the same text (see
// extJSON for why that needs an explicit guarantee). Store.quarantineID hashes
// the rendering to give an isolated document a stable _id, so this is a
// correctness property, not a nicety.
func renderWriteModel(collName string, m mongo.WriteModel) string {
	if m == nil {
		return "db." + collName + ".<nil model>"
	}
	switch v := m.(type) {
	case *mongo.InsertOneModel:
		return "db." + collName + ".insertOne(" + extJSON(v.Document) + ")"
	case *mongo.DeleteOneModel:
		return "db." + collName + ".deleteOne(" + extJSON(v.Filter) +
			writeOpts(nil, v.Collation, v.Hint, nil, nil) + ")"
	case *mongo.DeleteManyModel:
		return "db." + collName + ".deleteMany(" + extJSON(v.Filter) +
			writeOpts(nil, v.Collation, v.Hint, nil, nil) + ")"
	case *mongo.ReplaceOneModel:
		return "db." + collName + ".replaceOne(" + extJSON(v.Filter) + ", " + extJSON(v.Replacement) +
			writeOpts(v.Upsert, v.Collation, v.Hint, v.Sort, nil) + ")"
	case *mongo.UpdateOneModel:
		return "db." + collName + ".updateOne(" + extJSON(v.Filter) + ", " + extJSON(v.Update) +
			writeOpts(v.Upsert, v.Collation, v.Hint, v.Sort, v.ArrayFilters) + ")"
	case *mongo.UpdateManyModel:
		// UpdateManyModel has no Sort field in driver v2 (only UpdateOneModel
		// does), so the sort slot is passed as nil.
		return "db." + collName + ".updateMany(" + extJSON(v.Filter) + ", " + extJSON(v.Update) +
			writeOpts(v.Upsert, v.Collation, v.Hint, nil, v.ArrayFilters) + ")"
	default:
		// An unknown model type still deserves *something* readable.
		return fmt.Sprintf("db.%s.<unknown model %T> %+v", collName, m, m)
	}
}

// extJSON renders one document/argument of a write model as canonical Extended
// JSON (see the note on renderWriteModel for why not relaxed). A nil argument
// renders as null and a marshalling failure degrades to a placeholder rather
// than propagating: this runs on error paths, and a renderer that can itself
// fail would defeat the purpose.
//
// The result is then re-marshalled with object keys SORTED, because the write
// models are built from bson.M — a Go map, whose iteration order is randomised on
// every marshal. Without this the SAME model renders with a different key order on
// each call, which breaks three things at once: two identical failures log two
// different first_statement values so an operator cannot tell one recurring
// document from a stream of distinct ones; the stored "model" field is not
// diffable across occurrences; and any hash of the rendering (quarantineID) is
// not stable, so the deduplication it exists to provide silently does not happen.
//
// Canonical Extended JSON wraps every numeric type in an object
// ({"$numberLong":"..."}, {"$numberDouble":"..."}, {"$numberInt":"..."}), so the
// round trip sees only strings, objects, arrays and booleans — there is no float64
// to lose the int64 precision the canonical mode was chosen to preserve.
// UseNumber is set regardless, so a bare number would be carried through verbatim
// rather than reformatted if one ever appeared.
func extJSON(v any) string {
	if v == nil {
		return "null"
	}
	b, err := bson.MarshalExtJSON(v, true, false)
	if err != nil {
		return fmt.Sprintf("<unrenderable %T: %v>", v, err)
	}
	return sortedJSON(string(b))
}

// sortedJSON re-marshalls a JSON document so that object keys come out in sorted
// order, making equal documents always produce equal text. Array order is
// preserved — it is meaningful (an $or list, arrayFilters) — while key order is
// not. HTML escaping is disabled so that a log line containing < or & stays
// readable in the rendered statement instead of turning into \u003c.
//
// It returns the input unchanged if it cannot be parsed. That cannot happen for
// the output of MarshalExtJSON, but a renderer used on error paths must not be
// able to fail.
func sortedJSON(s string) string {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return s
	}
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return s
	}
	return strings.TrimRight(buf.String(), "\n")
}

// writeOpts renders the trailing options argument of an update/replace/delete
// statement, including only the options actually set. It returns "" when there
// are none, so the common case renders as a clean two-argument call.
func writeOpts(upsert *bool, collation *options.Collation, hint, sort any, arrayFilters []any) string {
	opts := bson.M{}
	if upsert != nil {
		opts["upsert"] = *upsert
	}
	if collation != nil {
		opts["collation"] = fmt.Sprintf("%+v", *collation)
	}
	if hint != nil {
		opts["hint"] = hint
	}
	if sort != nil {
		opts["sort"] = sort
	}
	if len(arrayFilters) > 0 {
		opts["arrayFilters"] = arrayFilters
	}
	if len(opts) == 0 {
		return ""
	}
	return ", " + extJSON(opts)
}

// firstModelPreview renders the first model of a batch for a log field, so a
// failure line names the document involved without dumping the whole batch. An
// empty batch renders as a placeholder rather than panicking.
func firstModelPreview(collName string, models []mongo.WriteModel, maxBytes int) string {
	if len(models) == 0 {
		return "<empty batch>"
	}
	return previewForLog(renderWriteModel(collName, models[0]), maxBytes)
}
