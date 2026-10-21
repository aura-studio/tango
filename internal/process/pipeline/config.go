package pipeline

import (
	"fmt"
	"time"
)

// Config configures batching and parallel write workers. It is the pipeline
// module's own configuration; the top-level config package references it by
// pointer and the loader unmarshals the pipeline.* keys into it.
type Config struct {
	// BatchSize is the target number of records per bulk-write flush. The
	// adaptive min/max can be overridden via BatchSizeMin/BatchSizeMax.
	// Default 1000.
	BatchSize int `mapstructure:"batchSize"`
	// BatchSizeMin is the adaptive lower bound for batch sizing. When 0, it is
	// auto-derived as BatchSize/4 (minimum 1).
	BatchSizeMin int `mapstructure:"batchSizeMin"`
	// BatchSizeMax is the adaptive upper bound for batch sizing. When 0, it is
	// auto-derived as BatchSize*2.
	BatchSizeMax int `mapstructure:"batchSizeMax"`
	// BatchWorkers is the number of parallel write workers. Default 2.
	BatchWorkers int `mapstructure:"batchWorkers"`
	// FlushInterval is how often workers flush partial batches. Default 1s.
	FlushInterval time.Duration `mapstructure:"flushInterval"`
	// ChannelBuffer is the per-worker line-channel buffer. Default 0 means
	// "derive as BatchSize*2".
	ChannelBuffer int `mapstructure:"channelBuffer"`
	// DeadLetterCap is the per-worker dead-letter batch capacity. Default 128.
	DeadLetterCap int `mapstructure:"deadLetterCap"`
	// MaxStuckDuration bounds how long a worker may hold a batch that keeps
	// failing to write, applying backpressure, before it gives the batch up
	// (isolated to dead_letter, counted in WriteStats.Abandoned) and resumes
	// reading. Default 3m.
	//
	// The bound exists because the backpressure it replaces is not actually
	// protective in this deployment: the source log files live on an emptyDir
	// volume and the producer rotates them away after roughly fifteen minutes
	// whether or not they were read. Holding a batch past that window preserves
	// nothing — it only stops new data from being read while the old source is
	// deleted underneath us. Three minutes is comfortably longer than a
	// DocumentDB failover and comfortably shorter than the retention window.
	//
	// Zero means "use the default"; a negative value means "retry forever",
	// which restores the pre-fix behaviour and is only an escape hatch.
	MaxStuckDuration time.Duration `mapstructure:"maxStuckDuration"`
}

// defaultMaxStuckDuration is the backpressure budget applied when
// Config.MaxStuckDuration is unset. It lives here rather than only in
// ApplyDefaults so flushBatch can also fall back to it: RunWorkers is exported
// and can be handed a hand-built Config that never went through ApplyDefaults,
// and the safety net must not silently disappear in that case.
const defaultMaxStuckDuration = 3 * time.Minute

// Validate checks the explicit adaptive batch bounds are consistent with the
// target batch size. A nil receiver is valid. Note ApplyDefaults clamps these,
// so post-default configs always pass; Validate guards pre-default callers.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.BatchSizeMin > 0 && c.BatchSizeMin > c.BatchSize {
		return fmt.Errorf("batchSizeMin (%d) cannot exceed batchSize (%d)", c.BatchSizeMin, c.BatchSize)
	}
	if c.BatchSizeMax > 0 && c.BatchSize > c.BatchSizeMax {
		return fmt.Errorf("batchSize (%d) cannot exceed batchSizeMax (%d)", c.BatchSize, c.BatchSizeMax)
	}
	return nil
}

// RegisterDefaults registers this module's config keys (under prefix) with the
// given setter so env binding works. The prefix is supplied by the parent
// (process.Config registers it under process.pipeline).
func (c *Config) RegisterDefaults(set func(key string, value any), prefix string) {
	set(prefix+".batchSize", 0)
	set(prefix+".batchSizeMin", 0)
	set(prefix+".batchSizeMax", 0)
	set(prefix+".batchWorkers", 0)
	set(prefix+".flushInterval", "0s")
	set(prefix+".channelBuffer", 0)
	set(prefix+".deadLetterCap", 0)
	set(prefix+".maxStuckDuration", "0s")
}

// ApplyDefaults fills unset pipeline options and clamps explicit batch bounds.
func (c *Config) ApplyDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.BatchSizeMin > 0 && c.BatchSizeMin > c.BatchSize {
		c.BatchSizeMin = c.BatchSize
	}
	if c.BatchSizeMax > 0 && c.BatchSizeMax < c.BatchSize {
		c.BatchSizeMax = c.BatchSize
	}
	if c.BatchWorkers <= 0 {
		c.BatchWorkers = 2
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.DeadLetterCap <= 0 {
		c.DeadLetterCap = 128
	}
	// Only an unset (zero) value takes the default: a negative MaxStuckDuration
	// is a deliberate "retry forever" escape hatch and must survive.
	if c.MaxStuckDuration == 0 {
		c.MaxStuckDuration = defaultMaxStuckDuration
	}
}

// MinBatchSize returns the adaptive lower bound for batch sizing. When
// BatchSizeMin is explicitly set (>0) it is used directly (clamped to
// BatchSize); when 0 it is auto-derived as BatchSize/4 (minimum 1).
func (c *Config) MinBatchSize() int {
	if c.BatchSizeMin > 0 {
		if c.BatchSizeMin > c.BatchSize {
			return c.BatchSize
		}
		return c.BatchSizeMin
	}
	v := c.BatchSize / 4
	if v < 1 {
		return 1
	}
	return v
}

// MaxBatchSize returns the adaptive upper bound for batch sizing. When
// BatchSizeMax is explicitly set (>0) it is used directly (clamped to
// BatchSize); when 0 it is auto-derived as BatchSize*2.
func (c *Config) MaxBatchSize() int {
	if c.BatchSizeMax > 0 {
		if c.BatchSizeMax < c.BatchSize {
			return c.BatchSize
		}
		return c.BatchSizeMax
	}
	return c.BatchSize * 2
}

// ChannelSize returns the per-worker channel buffer size. It honours an
// explicit ChannelBuffer when set, otherwise derives BatchSize*2.
func (c *Config) ChannelSize() int {
	if c.ChannelBuffer > 0 {
		return c.ChannelBuffer
	}
	return c.BatchSize * 2
}
