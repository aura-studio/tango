// Package store provides MongoDB persistence for ThinkingData records,
// including index management, write-model building, and bulk writes with retry.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aura-studio/tango/internal/logging"

	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Config is the store's own configuration — the subset of settings the store
// actually uses, owned here instead of depending on the top-level config
// package. Callers (e.g. dao) project the loaded configuration onto it.
type Config struct {
	// MaxElapsedTime is the maximum total retry time for a single bulk write.
	MaxElapsedTime time.Duration `mapstructure:"maxElapsedTime"`
}

// RegisterDefaults registers this module's config keys (under prefix) with the
// given setter so env binding works.
func (c *Config) RegisterDefaults(set func(key string, value any), prefix string) {
	set(prefix+".maxElapsedTime", "0s")
}

// ApplyDefaults fills unset store options.
func (c *Config) ApplyDefaults() {
	if c.MaxElapsedTime <= 0 {
		c.MaxElapsedTime = 10 * time.Second
	}
}

// WriteStats holds cumulative statistics for bulk writes.
type WriteStats struct {
	Retries     atomic.Int64 // total retry attempts across all bulk writes
	Quarantined atomic.Int64 // documents permanently rejected and routed to dead_letter
	Abandoned   atomic.Int64 // documents released without being written (see Store.GiveUp)
}

// TotalRetries returns the cumulative retry count.
func (s *WriteStats) TotalRetries() int64 { return s.Retries.Load() }

// TotalQuarantined returns the cumulative count of permanently-rejected
// documents routed to dead_letter (see Store.quarantine).
func (s *WriteStats) TotalQuarantined() int64 { return s.Quarantined.Load() }

// TotalAbandoned returns the cumulative count of documents released without
// ever being written (see Store.GiveUp). Any non-zero value means data was
// deliberately dropped to keep ingestion alive, and should be alerted on.
func (s *WriteStats) TotalAbandoned() int64 { return s.Abandoned.Load() }

// Store manages MongoDB collections for user, event, and dead-letter data.
type Store struct {
	user       *mongo.Collection
	event      *mongo.Collection
	deadLetter *mongo.Collection
	identity   *IdentityResolver
	cfg        *Config
	stats      WriteStats
}

// New creates a Store backed by the given database.
func New(db *mongo.Database, cfg *Config) *Store {
	return &Store{
		user:       db.Collection("user"),
		event:      db.Collection("event"),
		deadLetter: db.Collection("dead_letter"),
		identity:   NewIdentityResolver(db),
		cfg:        cfg,
	}
}

// Identity returns the identity resolver for user ID resolution.
func (s *Store) Identity() *IdentityResolver { return s.identity }

// Stats returns a pointer to the cumulative write statistics.
func (s *Store) Stats() *WriteStats { return &s.stats }

// UserCollection returns the user collection handle.
func (s *Store) UserCollection() *mongo.Collection { return s.user }

// EventCollection returns the event collection handle.
func (s *Store) EventCollection() *mongo.Collection { return s.event }

// DeadLetterCollection returns the dead_letter collection handle.
func (s *Store) DeadLetterCollection() *mongo.Collection { return s.deadLetter }

// BulkWrite executes a batch of write models with exponential-backoff retry.
// It uses unordered writes so individual failures don't block others.
func (s *Store) BulkWrite(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel) error {
	return s.bulkWrite(ctx, coll, models, false)
}

// BulkWriteOrdered executes a batch of write models with exponential-backoff retry.
// It uses ordered writes to guarantee that operations are applied in the order they
// appear in the batch. This is critical for user collections where the same user_id
// may appear multiple times and the final document state must reflect the last operation.
func (s *Store) BulkWriteOrdered(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel) error {
	return s.bulkWrite(ctx, coll, models, true)
}

// bulkWrite writes models, retrying transient failures (backpressure) while
// isolating PERMANENT failures so a single poison document can never wedge the
// pipeline in an infinite retry. Three shapes of permanence are recognised:
//   - per-document rejections (a structurally-invalid doc the server will never
//     accept) are quarantined to dead_letter — see bulkWriteRetried;
//   - a command-level rejection the server answered with a non-transient code
//     ("command too large", DocumentDB's "Badly formed input data", ...) is
//     split — the server applied nothing, so re-submitting each half is safe —
//     and once irreducible to a single document, quarantined;
//   - a batch of dead letters that is itself unwinnable is logged and released
//     rather than split, because dead-letter models are non-idempotent inserts
//     and re-sinking them into their own collection would duplicate.
//
// Connection / failover / write-concern errors carry no permanence signal and
// are retried, applying backpressure. That backpressure is deliberately bounded
// upstream (see pipeline.Config.MaxStuckDuration and Store.GiveUp): the source
// log files live on an emptyDir volume and rotate away in minutes, so holding a
// batch indefinitely does not preserve data — it only stops new data from being
// read while the old source is deleted underneath us.
func (s *Store) bulkWrite(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel, ordered bool) error {
	if len(models) == 0 {
		return nil
	}
	err := s.bulkWriteRetried(ctx, coll, models, ordered)
	if err == nil || !isUnwinnableError(err) {
		return err
	}
	if len(models) == 1 {
		// A lone document the server will never accept — oversize or malformed.
		// Quarantine it rather than retry the unwinnable command forever.
		s.quarantine(ctx, coll.Name(), []poisonDoc{{model: models[0], code: commandCode(err), message: err.Error()}})
		return nil
	}
	if coll.Name() == s.deadLetter.Name() {
		s.GiveUp(ctx, coll, models, "dead_letter batch permanently rejected: "+err.Error())
		return nil
	}
	// The batch contains something the server will never accept: split and write
	// each half. event/user models are additionally idempotent (#uuid upsert with
	// a _ts guard), so re-submitting a half cannot duplicate.
	mid := len(models) / 2
	if e := s.bulkWrite(ctx, coll, models[:mid], ordered); e != nil {
		return e
	}
	return s.bulkWrite(ctx, coll, models[mid:], ordered)
}

// bulkWriteRetried performs the backoff-retried bulk write. Transient errors are
// retried up to MaxElapsedTime; per-document poison is quarantined and dropped;
// an unwinnable command-level rejection (too large, or refused by the server for
// a non-transient reason) is surfaced as a backoff.Permanent for bulkWrite to
// split or quarantine.
func (s *Store) bulkWriteRetried(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel, ordered bool) error {
	var attempt int
	op := func() error {
		attempt++
		_, err := coll.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(ordered))
		if err == nil {
			return nil
		}
		if isOnlyDuplicateKey(err) {
			// Benign: the _ts filter-guard write models (see writemodel.go) skip a
			// record whose target already holds a newer _ts by failing to match,
			// after which the upsert's insert attempt conflicts with the unique key
			// (E11000). These are intended no-ops, not failures.
			return nil
		}
		if retry, poison, perDoc := classifyBulkWriteError(err, models); perDoc && len(poison) > 0 {
			s.quarantine(ctx, coll.Name(), poison)
			models = retry // succeeded docs already committed; keep only transient failures
			if len(models) == 0 {
				return nil // poison was the only failure — nothing left to retry
			}
			return err // retry the transient remainder (backpressure)
		}
		if isUnwinnableError(err) {
			// The server answered and refused. Retrying the same bytes cannot
			// succeed, so stop now instead of burning MaxElapsedTime; bulkWrite
			// splits or quarantines. Surfacing it as backoff.Permanent is what
			// keeps a malformed document from becoming an infinite stall.
			return backoff.Permanent(err)
		}
		return err // transient / unclassified -> backoff retries (backpressure)
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 200 * time.Millisecond
	bo.MaxInterval = 2 * time.Second
	bo.MaxElapsedTime = s.cfg.MaxElapsedTime
	bo.Reset()

	err := backoff.Retry(op, backoff.WithContext(bo, ctx))

	// Track retries (attempts beyond the first one).
	if retries := attempt - 1; retries > 0 {
		s.stats.Retries.Add(int64(retries))
	}

	if err != nil && !isUnwinnableError(err) {
		// Only genuinely transient failures reach here. An unwinnable error was
		// not retried to exhaustion and is reported by quarantine / GiveUp with
		// the data preview, so a "failed after retries" line would mislead.
		logging.WithError(err).WithField("collection", coll.Name()).
			Warn("bulk write failed after retries")
	}
	return err
}

// isOnlyDuplicateKey reports whether a bulk-write error consists solely of
// duplicate-key (E11000) write errors with no write-concern error. The _ts
// filter-guard write models (see writemodel.go) rely on this: when a newer
// document already exists, the guarded upsert fails to match and its insert
// attempt conflicts with the unique key, surfacing as E11000. Such errors mean
// "skipped, a newer record won" — they are intended no-ops, not failures.
func isOnlyDuplicateKey(err error) bool {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return false
	}
	if bwe.WriteConcernError != nil || len(bwe.WriteErrors) == 0 {
		return false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != 11000 {
			return false
		}
	}
	return true
}

// poisonDoc is a write model the server permanently rejected, kept with its
// error so it can be diagnosed in the dead_letter collection.
type poisonDoc struct {
	model   mongo.WriteModel
	code    int
	message string
}

// classifyBulkWriteError inspects a bulk-write error. When the error carries
// per-document write errors (perDoc=true) it partitions the offending models
// into transient failures (retry) and permanent "poison" failures (quarantine);
// documents not referenced by any write error succeeded and appear in neither.
// perDoc=false means the error has no actionable per-document detail (a
// connection / failover / write-concern error), so the caller should retry the
// whole batch — that is the transient-outage backpressure path.
func classifyBulkWriteError(err error, models []mongo.WriteModel) (retry []mongo.WriteModel, poison []poisonDoc, perDoc bool) {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return nil, nil, false
	}
	// A write-concern error means the write may have applied but the ack failed —
	// transient; retry the whole batch rather than reasoning per-document.
	if bwe.WriteConcernError != nil || len(bwe.WriteErrors) == 0 {
		return nil, nil, false
	}
	for _, we := range bwe.WriteErrors {
		if we.Index < 0 || we.Index >= len(models) {
			continue
		}
		m := models[we.Index]
		switch {
		case we.Code == 11000:
			// Duplicate key: benign no-op (see isOnlyDuplicateKey) — drop.
		case isTransientWriteCode(we.Code):
			retry = append(retry, m)
		default:
			poison = append(poison, poisonDoc{model: m, code: we.Code, message: we.Message})
		}
	}
	return retry, poison, true
}

// isTransientWriteCode reports whether a per-document write-error code denotes a
// transient (retryable) failure — primary failover, step-down, shutdown,
// network/timeout, throttling — as opposed to a permanent document defect. The
// classification deliberately errs toward "permanent": any code NOT listed here
// is quarantined, so an unknown poison can never loop forever (at worst a rare
// transient per-document error is routed to dead_letter, which is recoverable —
// far better than an infinite stall). Connection-level / write-concern failures
// carry no per-document code and are handled as whole-batch retries upstream.
func isTransientWriteCode(code int) bool {
	switch code {
	case 11600, // InterruptedAtShutdown
		11602, // InterruptedDueToReplStateChange
		10107, // NotWritablePrimary
		13435, // NotPrimaryNoSecondaryOk
		13436, // NotPrimaryOrSecondary
		189,   // PrimarySteppedDown
		91,    // ShutdownInProgress
		7,     // HostNotFound
		6,     // HostUnreachable
		89,    // NetworkTimeout
		9001,  // SocketException
		262,   // ExceededTimeLimit
		50,    // MaxTimeMSExpired
		64,    // WriteConcernFailed
		16500: // throttling (rate limit)
		return true
	}
	return false
}

// isTooLargeError reports whether a bulk write failed because the command — and
// thus the whole batch, or a single document — exceeds the server's size limit.
// That is permanent for the current batch size (retrying it unchanged never
// helps), so the caller splits a multi-document batch or quarantines a lone
// oversize document. DocumentDB reports it as a CommandError code 80 ("Query
// size N exceeded maximum query size M"); MongoDB as BSONObjectTooLarge (10334).
// A message fallback covers phrasings whose code differs across backends.
func isTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && (ce.Code == 80 || ce.Code == 10334) {
		return true
	}
	var se mongo.ServerError
	if errors.As(err, &se) && se.HasErrorCode(10334) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "exceeded maximum") || strings.Contains(msg, "too large")
}

// isPermanentCommandCode reports whether a server error code means "this document
// or command is malformed and will be refused identically forever" — a DATA
// defect — as opposed to "you are not allowed or not able to do this right now",
// which is an OPERATIONAL condition an operator fixes.
//
// The list is deliberately an allowlist, not "everything not on the transient
// table". Treating unknown codes as permanent destroys good data during an
// operational outage: 13 (Unauthorized) and 18 (AuthenticationFailed) are
// command-level errors that are NOT in isTransientWriteCode, and the driver does
// surface them as mongo.CommandError — auth.Error implements Unwrap, so
// wrapErrors' errors.As(err, &driver.Error{}) reaches the inner coded error and
// rewraps it. Classifying those as permanent would bisect perfectly good
// event/user batches down to single documents and quarantine them, where the old
// behaviour applied backpressure and the data survived once credentials were
// fixed. Unknown codes therefore keep the old backpressure behaviour; the
// pipeline's bounded-backpressure budget is what stops a genuinely-new permanent
// rejection from wedging ingestion, so aggression here buys nothing.
func isPermanentCommandCode(code int32) bool {
	switch code {
	case 2, // BadValue — e.g. "$bad prefixed field not valid for storage"
		9,   // FailedToParse
		121: // DocumentValidationFailure
		return true
	}
	return false
}

// docdbMalformedInput is the message DocumentDB returns when a document carries
// bytes it cannot store. It is matched case-insensitively as a substring.
const docdbMalformedInput = "badly formed input data"

// isPermanentCommandError reports whether the server answered the command and
// refused it for a reason that retrying the same bytes can never fix.
//
// Two gates: the error must be a mongo.CommandError at all (only a real server
// response produces one — connection refused, server-selection timeout and a
// closed client surface as other types, e.g. topology.ServerSelectionError, and
// must keep their backpressure), and then it must be a KNOWN data defect, either
// by code or by DocumentDB's codeless malformed-input text.
//
// The text match exists because the 2026-10-19 incident gave us nothing else:
// DocumentDB rejected a dead_letter insert whose stored line carried a raw
// control byte, answering with a CommandError whose Code is 0 and whose Name is
// empty (no codeName is sent), so it rendered as " Badly formed input data". It
// is neither a per-document BulkWriteException nor too-large, so it fell through
// to the transient branch and was retried forever, stalling ingestion on two
// shards for ~50h. Both ce.Message and err.Error() are checked because
// wrapErrors sets wrappedMsgOnly, which makes Error() render the wrapped
// driver.Error's text instead of the CommandError's own fields.
func isPermanentCommandError(err error) bool {
	if err == nil {
		return false
	}
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return false // no server response: keep applying backpressure
	}
	if isTooLargeError(err) {
		return false // too-large owns the split-then-quarantine path
	}
	if isPermanentCommandCode(ce.Code) {
		return true
	}
	text := strings.ToLower(ce.Message) + " " + strings.ToLower(err.Error())
	return strings.Contains(text, docdbMalformedInput)
}

// isUnwinnableError reports whether retrying this exact batch can never succeed,
// so bulkWrite must split or release it instead of returning it as backpressure.
func isUnwinnableError(err error) bool {
	return isTooLargeError(err) || isPermanentCommandError(err)
}

// commandCode extracts the server error code from a command-level rejection, or
// 0 when the error carries none. DocumentDB's "Badly formed input data" has
// neither a code nor a codeName, so 0 is the expected value there.
func commandCode(err error) int {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return int(ce.Code)
	}
	return 0
}

// quarantine records permanently-rejected write models in the dead_letter
// collection so a poison document is neither lost nor able to wedge the pipeline.
func (s *Store) quarantine(ctx context.Context, collName string, poison []poisonDoc) {
	s.isolate(ctx, collName, poison, "permanent bulk-write rejection",
		// This exact wording predates the bounded-backpressure work and is kept
		// verbatim: it names the specific condition (the SERVER refused the
		// document) and is what any existing grep or alarm would be keyed on.
		"bulk write: document permanently rejected; quarantined to dead_letter (not retried)",
		&s.stats.Quarantined)
}

// GiveUp releases a batch that could not be written and never will be, so that
// holding on to it cannot stall ingestion. It is the store-side half of the
// bounded-backpressure contract: the pipeline worker calls it once a batch has
// been failing for longer than process.pipeline.maxStuckDuration, and bulkWrite
// calls it for an unwinnable batch of dead letters.
//
// The batch is counted in TotalAbandoned and — EXCEPT when the batch is itself
// dead letters — isolated into dead_letter on a best-effort basis, so it stays
// recoverable. A batch that IS dead letters is logged and released without being
// written anywhere: dead-letter models are non-idempotent inserts, so sinking
// them into their own collection again would duplicate whatever partially
// landed. That makes the log line the only surviving record, which is why
// logAbandonedModels is deliberately not capped the way isolate's logging is.
//
// GiveUp never returns an error and never blocks beyond the bounded isolation
// write. Quarantined and Abandoned count different reasons for the same sink, so
// a document is never counted in both.
func (s *Store) GiveUp(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel, reason string) {
	if len(models) == 0 {
		return
	}
	logging.WithFields(logging.Fields{
		"collection": coll.Name(),
		"batch":      len(models),
		"reason":     previewForLog(reason, logPreviewBytes),
	}).Error("bulk write: giving up on batch; documents skipped to keep ingestion running")

	if coll.Name() == s.deadLetter.Name() {
		s.stats.Abandoned.Add(int64(len(models)))
		logAbandonedModels(coll.Name(), models)
		return
	}
	poison := make([]poisonDoc, 0, len(models))
	for _, m := range models {
		poison = append(poison, poisonDoc{model: m, code: 0, message: reason})
	}
	// isolate owns the Abandoned increment, so GiveUp must not add to it here.
	s.isolate(ctx, coll.Name(), poison, "abandoned: backpressure budget exhausted",
		"bulk write: document abandoned after backpressure budget; isolated to dead_letter (not retried)",
		&s.stats.Abandoned)
}

// isolate writes poison records into the dead_letter collection and logs them,
// bumping counter. detailMsg is the per-document log line, which differs between
// a server rejection and a give-up so that either condition stays greppable on
// its own. The model is stored as a STRING, not nested BSON: the very thing that
// made it unwritable (a $-prefixed / dotted field NAME) would otherwise make the
// dead_letter write fail too — as a string value it is harmless. That string is
// additionally passed through sanitizeForBSON: rendering a model with %+v
// re-emits the raw bytes that got it rejected in the first place (a dead-letter
// model carries the offending log line verbatim), so without cleaning the
// isolation write would be refused for the same reason and the evidence lost.
//
// Per-document logging is capped at maxDetailLogsPerBatch here because the full
// content is being persisted anyway; the overflow is summarised so an outage
// cannot emit thousands of lines.
//
// The write is best-effort on a fresh bounded context (so it runs even at
// shutdown and cannot block): if dead_letter itself still rejects it, the error
// is logged and the documents dropped rather than retried, since the alternative
// is the infinite stall this whole mechanism exists to remove.
func (s *Store) isolate(ctx context.Context, collName string, poison []poisonDoc, reason, detailMsg string, counter *atomic.Int64) {
	docs := make([]any, 0, len(poison))
	for i, p := range poison {
		rendered := fmt.Sprintf("%+v", p.model)
		if i < maxDetailLogsPerBatch {
			logging.WithFields(logging.Fields{
				"collection": collName,
				"code":       p.code,
				"message":    previewForLog(p.message, logPreviewBytes),
				"reason":     reason,
				"bytes":      len(rendered),
				"preview":    previewForLog(rendered, logPreviewBytes),
			}).Error(detailMsg)
		}
		docs = append(docs, bson.M{
			"_quarantine_reason":     reason,
			"_quarantine_collection": collName,
			"_quarantine_code":       p.code,
			"_quarantine_message":    sanitizeForBSON(p.message, logPreviewBytes),
			"_ts":                    time.Now().UnixNano(),
			"model":                  sanitizeForBSON(rendered, quarantineModelBytes),
		})
	}
	logOverflow(collName, "rejected documents", len(poison))
	counter.Add(int64(len(poison)))

	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := s.deadLetter.InsertMany(qctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		logging.WithError(err).WithField("collection", collName).
			Error("bulk write: failed to isolate document(s) to dead_letter; dropping to avoid wedge")
	}
}

// logAbandonedModels emits one preview per document for a batch being dropped
// without being written anywhere. Unlike isolate's logging this is NOT capped at
// maxDetailLogsPerBatch: here the log line is the only surviving record of the
// data, since the source files rotate away within minutes and dead letters are
// not re-sunk into their own collection. The batch is bounded by
// pipeline.Config.DeadLetterCap (default 128) and each preview by
// abandonPreviewBytes, so the worst case is ~256 KB of log per give-up — and in
// practice a dead-letter batch holds only a handful of models, because parse
// errors run at roughly one per 240,000 lines in production.
func logAbandonedModels(collName string, models []mongo.WriteModel) {
	for _, m := range models {
		logging.WithFields(logging.Fields{
			"collection": collName,
			"reason":     "abandoned",
			"preview":    previewForLog(fmt.Sprintf("%+v", m), abandonPreviewBytes),
		}).Error("bulk write: dead-letter document abandoned (not re-sunk into dead_letter; this log line is the only record)")
	}
}

// logOverflow records that per-document logging was capped, so a reader of the
// logs knows the batch was larger than the previews shown.
func logOverflow(collName, what string, total int) {
	if total <= maxDetailLogsPerBatch {
		return
	}
	logging.WithFields(logging.Fields{
		"collection": collName,
		"total":      total,
		"logged":     maxDetailLogsPerBatch,
	}).Errorf("bulk write: %d further %s not logged individually", total-maxDetailLogsPerBatch, what)
}
