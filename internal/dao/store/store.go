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
}

// TotalRetries returns the cumulative retry count.
func (s *WriteStats) TotalRetries() int64 { return s.Retries.Load() }

// TotalQuarantined returns the cumulative count of permanently-rejected
// documents routed to dead_letter (see Store.quarantine).
func (s *WriteStats) TotalQuarantined() int64 { return s.Quarantined.Load() }

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
// pipeline in an infinite retry:
//   - per-document rejections (a structurally-invalid doc the server will never
//     accept) are quarantined to dead_letter — see bulkWriteRetried;
//   - a "command too large" failure is split (a batch that is merely too big is
//     written in halves) or, once irreducible to a single oversize document,
//     quarantined.
//
// Connection / failover / write-concern errors carry no such permanence signal
// and are retried, preserving the never-drop-recoverable-data backpressure.
func (s *Store) bulkWrite(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel, ordered bool) error {
	if len(models) == 0 {
		return nil
	}
	err := s.bulkWriteRetried(ctx, coll, models, ordered)
	if err == nil || !isTooLargeError(err) {
		return err
	}
	if len(models) == 1 {
		// A lone document over the server's size limit can never be written —
		// quarantine it rather than retry the unwinnable command forever.
		s.quarantine(ctx, coll.Name(), []poisonDoc{{model: models[0], code: 0, message: err.Error()}})
		return nil
	}
	// The batch is merely too big for one command: split and write each half.
	mid := len(models) / 2
	if e := s.bulkWrite(ctx, coll, models[:mid], ordered); e != nil {
		return e
	}
	return s.bulkWrite(ctx, coll, models[mid:], ordered)
}

// bulkWriteRetried performs the backoff-retried bulk write. Transient errors are
// retried up to MaxElapsedTime; per-document poison is quarantined and dropped; a
// too-large error is surfaced (as a backoff.Permanent) for bulkWrite to split or
// quarantine.
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
		if isTooLargeError(err) {
			return backoff.Permanent(err) // permanent for this batch size; bulkWrite splits/quarantines
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

	if err != nil && !isTooLargeError(err) {
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

// quarantine records permanently-rejected write models in the dead_letter
// collection so a poison document is neither lost nor able to wedge the pipeline.
// The model is stored as a STRING, not nested BSON: the very thing that made it
// unwritable (a $-prefixed / dotted field NAME) would otherwise make the
// dead_letter write fail too — as a string value it is harmless. That string is
// additionally passed through sanitizeForBSON: rendering a model with %+v
// re-emits the raw bytes that got it rejected in the first place (a dead-letter
// model carries the offending log line verbatim), so without cleaning the
// quarantine write would be refused for the same reason and the evidence lost.
// The write is best-effort on a fresh bounded context (so it runs even at
// shutdown and cannot block): if dead_letter itself still rejects it, the error
// is logged and the document dropped rather than retried, since the alternative
// is the infinite stall this whole mechanism exists to remove.
func (s *Store) quarantine(ctx context.Context, collName string, poison []poisonDoc) {
	docs := make([]any, 0, len(poison))
	for _, p := range poison {
		rendered := fmt.Sprintf("%+v", p.model)
		logging.WithFields(logging.Fields{
			"collection": collName,
			"code":       p.code,
			"message":    p.message,
			"reason":     "permanent rejection",
			"bytes":      len(rendered),
			"preview":    previewForLog(rendered, logPreviewBytes),
		}).Error("bulk write: document permanently rejected; quarantined to dead_letter (skipped, not retried)")
		docs = append(docs, bson.M{
			"_quarantine_reason":     "permanent bulk-write rejection",
			"_quarantine_collection": collName,
			"_quarantine_code":       p.code,
			"_quarantine_message":    sanitizeForBSON(p.message, logPreviewBytes),
			"_ts":                    time.Now().UnixNano(),
			"model":                  sanitizeForBSON(rendered, quarantineModelBytes),
		})
	}
	s.stats.Quarantined.Add(int64(len(poison)))

	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := s.deadLetter.InsertMany(qctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		logging.WithError(err).WithField("collection", collName).
			Error("bulk write: failed to quarantine poison document(s) to dead_letter; dropping to avoid wedge")
	}
}
