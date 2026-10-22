// Package tailer discovers log files by glob patterns and tails them,
// streaming new lines into a channel. It periodically rescans for new files.
package tailer

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hpcloud/tail"

	"github.com/aura-studio/tango/internal/logging"
)

// ---------------------------------------------------------------------------
// Cross-platform path helpers (Linux ↔ Windows)
// ---------------------------------------------------------------------------

// driveLetterRe matches a Linux-style path starting with a single drive
// letter, e.g. "/c/..." or "/D/...".
var driveLetterRe = regexp.MustCompile(`^/([a-zA-Z])/`)

// linuxDriveRoot is the directory prefix where Windows drive letters are
// mounted on Linux (e.g. "/mnt/"). Detected once at init time.
// On Windows this is unused; on Linux without drive mounts it defaults
// to "/".
var linuxDriveRoot = detectDriveRoot()

func detectDriveRoot() string {
	if runtime.GOOS == "windows" {
		return "/" // unused on Windows, safe default
	}
	// Probe common Linux mount points; prefer /mnt/c, fall back to /c.
	for _, prefix := range []string{"/mnt/", "/"} {
		if fi, err := os.Stat(prefix + "c"); err == nil && fi.IsDir() {
			return prefix
		}
	}
	return "/"
}

// toWindowsPath converts a Linux-style path to a Windows path.
//
//	/c/xxx/ta.log  →  C:\xxx\ta.log
//	/var/log/...   →  unchanged (not a single-letter drive)
//	*.log          →  unchanged (relative pattern)
func toWindowsPath(path string) string {
	m := driveLetterRe.FindStringSubmatch(path)
	if m == nil {
		return path
	}
	drive := strings.ToUpper(m[1])
	rest := path[len(m[0]):]
	rest = strings.ReplaceAll(rest, "/", `\`)
	return drive + `:\` + rest
}

// normalizeWindowsPath converts a Windows file path to Linux-style format
// so that it can be matched against a Linux-style glob pattern.
//
//	C:\xxx\app.log  →  /c/xxx/app.log
func normalizeWindowsPath(path string) string {
	if len(path) >= 2 && path[1] == ':' {
		drive := strings.ToLower(string(path[0]))
		rest := path[2:]
		path = "/" + drive + rest
	}
	return strings.ReplaceAll(path, `\`, "/")
}

// toLinuxNativePath converts any path to Linux-native format, handling
// Windows drive letters and backslashes. It maps drive letters to the
// detected mount point (e.g. /mnt/c/).
//
//	C:\logs\app.log       →  /mnt/c/logs/app.log   (drives at /mnt/)
//	/c/logs/app.log       →  /mnt/c/logs/app.log   (drives at /mnt/)
//	/c/logs/app.log       →  /c/logs/app.log        (drives at /)
//	logs\app.log          →  logs/app.log
//	/var/log/app.log      →  /var/log/app.log       (unchanged)
func toLinuxNativePath(path string) string {
	// Handle Windows drive letter: C:\...
	if len(path) >= 2 && path[1] == ':' {
		drive := strings.ToLower(string(path[0]))
		rest := strings.ReplaceAll(path[2:], `\`, "/")
		return linuxDriveRoot + drive + rest
	}
	// Handle Linux-style drive letter: /c/...
	// Only remap when drives are at a prefix other than "/" (e.g. /mnt/).
	if linuxDriveRoot != "/" {
		if m := driveLetterRe.FindStringSubmatch(path); m != nil {
			drive := strings.ToLower(m[1])
			rest := path[len(m[0]):]
			return linuxDriveRoot + drive + "/" + rest
		}
	}
	// Just replace backslashes.
	return strings.ReplaceAll(path, `\`, "/")
}

// toNativePath converts a path to OS-native format for use with
// filepath.WalkDir and other filesystem calls.
//
//   - On Windows: /c/logs    →  C:\logs            (via toWindowsPath)
//   - On Linux:   C:\logs    →  /mnt/c/logs        (via toLinuxNativePath)
//   - On Linux:   /c/logs    →  /mnt/c/logs        (drive remapping)
//   - On Linux:   logs\app   →  logs/app           (backslash fix)
//
// Paths already in native format pass through unchanged.
func toNativePath(path string) string {
	if runtime.GOOS == "windows" {
		return toWindowsPath(path)
	}
	return toLinuxNativePath(path)
}

// normalizePath converts any file path to a canonical forward-slash format
// for glob matching. On all platforms, Windows-style paths are converted:
//
//   - On Windows:  C:\logs\app.log  →  /c/logs/app.log
//   - On Linux:    C:\logs\app.log  →  /mnt/c/logs/app.log
//   - On Linux:    /mnt/c/logs/x    →  /mnt/c/logs/x         (unchanged)
//   - Everywhere:  logs/app.log     →  logs/app.log           (unchanged)
func normalizePath(path string) string {
	if runtime.GOOS == "windows" {
		return normalizeWindowsPath(path)
	}
	return toLinuxNativePath(path)
}

// ---------------------------------------------------------------------------
// Glob matching (supports ** for recursive directory matching)
// ---------------------------------------------------------------------------

// globMatch matches name against a glob pattern. Both must use forward
// slashes. Supported wildcards:
//
//   - matches any sequence of non-separator characters
//     **     matches zero or more directory levels
//     ?      matches any single non-separator character
//     [...]  matches a character class
func globMatch(pattern, name string) bool {
	return doGlobMatch(
		strings.Split(pattern, "/"),
		strings.Split(name, "/"),
	)
}

// doGlobMatch performs segment-level recursive glob matching.
func doGlobMatch(patParts, nameParts []string) bool {
	for len(patParts) > 0 {
		seg := patParts[0]

		if seg == "**" {
			patParts = patParts[1:]
			// Consume consecutive ** segments.
			for len(patParts) > 0 && patParts[0] == "**" {
				patParts = patParts[1:]
			}
			if len(patParts) == 0 {
				return true // trailing ** matches everything
			}
			// Try matching remaining pattern at every depth.
			for i := 0; i <= len(nameParts); i++ {
				if doGlobMatch(patParts, nameParts[i:]) {
					return true
				}
			}
			return false
		}

		if len(nameParts) == 0 {
			return false
		}

		matched, _ := filepath.Match(seg, nameParts[0])
		if !matched {
			return false
		}

		patParts = patParts[1:]
		nameParts = nameParts[1:]
	}

	return len(nameParts) == 0
}

// ---------------------------------------------------------------------------
// Tailer
// ---------------------------------------------------------------------------

// defaultPollInterval is how often the tail loop re-reads a file after
// reaching EOF when no explicit interval is configured. A short interval
// avoids the notification-drop race present in event-driven watchers while
// keeping CPU usage negligible.
const defaultPollInterval = 200 * time.Millisecond

// defaultMaxLineSize is the maximum line length (bytes) the scanner accepts
// when none is configured. It matches tailer.Config.MaxLineBytes's default
// (see config.go's ApplyDefaults) so the standalone and config-driven paths agree.
const defaultMaxLineSize = 10 * 1024 * 1024 // 10 MiB

// Tailer watches for log files matching glob patterns and tails them,
// sending new lines to an output channel.
type Tailer struct {
	patterns       []string
	rescanInterval time.Duration
	tailMode       string // TailModePoll or TailModeEvent

	pollInterval time.Duration // EOF re-read cadence (poll/hybrid modes)
	maxLineSize  int           // scanner buffer cap per line

	mu sync.Mutex
	// tailed maps a path to the cancel func of its per-file tail goroutine.
	// Presence means the file is currently being tailed; calling the cancel
	// func stops that goroutine (which then deletes its own entry on exit).
	tailed map[string]context.CancelFunc

	// wg tracks every live per-file tail goroutine. Run waits on it before
	// closing the output channel so a tail goroutine can never execute
	// `out <- line` after `close(out)` (send-on-closed-channel race).
	wg sync.WaitGroup
}

// New creates a Tailer that watches the given glob patterns.
// tailMode selects the file-watching strategy: TailModePoll (default)
// or TailModeEvent. Tuning (poll interval, max line size) defaults to
// sane values; override with WithTuning.
func New(patterns []string, rescanInterval time.Duration, tailMode string) *Tailer {
	if tailMode == "" {
		tailMode = TailModePoll
	}
	return &Tailer{
		patterns:       patterns,
		rescanInterval: rescanInterval,
		tailMode:       tailMode,
		pollInterval:   defaultPollInterval,
		maxLineSize:    defaultMaxLineSize,
		tailed:         make(map[string]context.CancelFunc),
	}
}

// TailedCount returns the number of files currently being tailed (i.e. live
// per-file tail goroutines). With the reaping logic in scanAndTail this tracks
// open log fds closely, so a steadily climbing value signals an fd leak.
func (t *Tailer) TailedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.tailed)
}

// WithTuning overrides the poll interval and max line size. Non-positive
// values keep the current default. Returns the receiver for chaining.
func (t *Tailer) WithTuning(pollInterval time.Duration, maxLineBytes int) *Tailer {
	if pollInterval > 0 {
		t.pollInterval = pollInterval
	}
	if maxLineBytes > 0 {
		t.maxLineSize = maxLineBytes
	}
	return t
}

// Run discovers and tails files, sending lines to the returned channel.
// It blocks until ctx is cancelled, then closes the output channel.
// The caller should drain the channel after ctx cancellation.
func (t *Tailer) Run(ctx context.Context) <-chan string {
	out := make(chan string, 2000)

	go func() {
		defer close(out)
		// Wait for every per-file tail goroutine to finish before closing out,
		// so none can send on the closed channel. Runs after the recover below
		// (deferred LIFO) so a panic in t.run still reaps children first.
		defer t.wg.Wait()
		defer logging.Recover("tailer run")
		t.run(ctx, out)
	}()

	return out
}

func (t *Tailer) run(ctx context.Context, out chan<- string) {
	// Initial file scan.
	t.scanAndTail(ctx, out)

	ticker := time.NewTicker(t.rescanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.scanAndTail(ctx, out)
		}
	}
}

// scanAndTail discovers files matching patterns, reaps tails whose file has
// disappeared, and starts tailing newly-discovered files.
func (t *Tailer) scanAndTail(ctx context.Context, out chan<- string) {
	found := discoverFiles(t.patterns)
	t.reapMissing(found)
	for _, path := range found {
		t.startFile(ctx, path, out)
	}
}

// reapMissing cancels the tail goroutine of every currently-tailed path that
// is no longer present in the freshly-discovered set. discoverFiles only
// returns paths that still exist, so anything tracked but absent has been
// deleted or rotated away. Cancelling its context unblocks the goroutine,
// whose deferred cleanup closes the (possibly already-unlinked) file and drops
// the map entry — bounding fd retention to one rescan interval even when the
// OS never delivers an inotify removal event for a file we hold open.
//
// ⚠️ RELEASE-GATE INVARIANT — do NOT weaken or remove. This is the coarse
// backstop that bounds deleted-but-open fd retention to ≤1 rescan even if the
// per-file event/hybrid ticker is bypassed; together they stop the v1.5.0 fd
// leak. A regression is invisible on Windows (Linux-only semantics) — guarded
// by TestFD_D2/D3, TestReap_*, and the E/G soaks in the Linux container.
func (t *Tailer) reapMissing(found []string) {
	alive := make(map[string]struct{}, len(found))
	for _, p := range found {
		alive[p] = struct{}{}
	}

	t.mu.Lock()
	var stale []string
	var cancels []context.CancelFunc
	for path, cancel := range t.tailed {
		if _, ok := alive[path]; !ok {
			stale = append(stale, path)
			cancels = append(cancels, cancel)
		}
	}
	t.mu.Unlock()

	for i, cancel := range cancels {
		logging.WithField("path", stale[i]).Info("tailer: file gone, stopping tail and releasing fd")
		cancel()
	}
}

// startFile begins tailing a single file if not already tailed. Each file runs
// under its own cancellable sub-context so it can be reaped individually; the
// goroutine removes its own map entry on exit so the file can be re-tailed if
// it later reappears at the same path.
func (t *Tailer) startFile(ctx context.Context, path string, out chan<- string) {
	t.mu.Lock()
	if _, ok := t.tailed[path]; ok {
		t.mu.Unlock()
		return
	}
	fileCtx, cancel := context.WithCancel(ctx)
	t.tailed[path] = cancel
	t.mu.Unlock()

	logging.WithFields(logging.Fields{
		"path":      path,
		"tail_mode": t.tailMode,
	}).Info("tailer: discovered and tailing new file")

	var tailFn func(context.Context, string, chan<- string)
	switch t.tailMode {
	case TailModeEvent:
		tailFn = t.tailFileEvent
	case TailModePoll:
		tailFn = t.tailFile
	default: // hybrid
		tailFn = t.tailFileHybrid
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done() // registered first → runs last, after cleanup below
		defer logging.Recover("tailer " + t.tailMode + ":" + path)
		defer func() {
			t.mu.Lock()
			delete(t.tailed, path)
			t.mu.Unlock()
			cancel() // release sub-context resources (idempotent if reaped)
		}()
		tailFn(fileCtx, path, out)
	}()
}

// tailFile reads a file from the beginning and follows new lines.
// It uses a simple poll-based approach: read until EOF, sleep briefly,
// then retry. This avoids the notification-drop race in hpcloud/tail's
// kqueue/inotify watcher where sendOnlyIfEmpty can silently discard
// a Modified event while the reader is busy, causing the tail to stall
// until the writer's next burst.
//
// File rotation (rename + recreate) is detected by comparing the file's
// inode across polls. When the underlying inode changes, the old file is
// closed and the new file is opened from the beginning.
func (t *Tailer) tailFile(ctx context.Context, path string, out chan<- string) {
	for {
		if err := t.readFollowFile(ctx, path, out); err != nil {
			if ctx.Err() != nil {
				return
			}
			logging.WithError(err).WithField("path", path).Warn("tailer: error reading file, will retry")
		}

		// File was deleted or rotated; wait for it to reappear.
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.pollInterval):
		}
	}
}

// readFollowFile opens path, reads all lines, then polls for new data
// until the file is deleted/rotated or ctx is cancelled.
func (t *Tailer) readFollowFile(ctx context.Context, path string, out chan<- string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	origInfo, err := f.Stat()
	if err != nil {
		return err
	}

	// startOff is f's offset when the current scanner was created. bufio.Scanner
	// reads ahead of what it hands out, so once a scan ends f sits past the last
	// complete line; rewindToConsumed uses startOff plus the bytes actually
	// consumed to put it back on a line boundary.
	var startOff int64

	for {
		// Read all available complete lines. An unterminated trailing line is not
		// one of them: lineScanner withholds it and the rewind below leaves f in
		// front of it, so the next pass reads it whole once the writer finishes
		// it. Emitting the fragment here — which is what the previous code did,
		// despite a comment claiming it seeked back — is what split one physical
		// line into two unparseable ones.
		ls := newLineScanner(f, t.maxLineSize)
		for ls.Scan() {
			line := ls.Text()
			if len(line) == 0 {
				continue
			}
			select {
			case out <- line:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := rewindToConsumed(f, startOff, ls.Consumed()); err != nil {
			return err
		}
		startOff += ls.Consumed()
		// Rewind first, then report: a scan that failed partway must not leave
		// the offset behind the lines already forwarded.
		if err := ls.Err(); err != nil {
			return err
		}

		// EOF reached. Before sleeping, check if the file was rotated
		// (renamed and a new file created at the same path) by comparing
		// the current inode to the original.
		curInfo, err := os.Stat(path)
		if err != nil {
			// File removed; caller will wait and re-open.
			return err
		}
		if !os.SameFile(origInfo, curInfo) {
			// Inode changed → file was rotated. Return so caller reopens.
			return nil
		}

		// Wait before polling again.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.pollInterval):
		}

		// bufio.Scanner is not resumable after Scan returns false, so the next
		// iteration builds a fresh one over the same fd, whose offset the rewind
		// above left exactly on a line boundary.

		// Check if the file was truncated (size shrank below our position).
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if fi.Size() < startOff {
			// File was truncated; rewind to beginning.
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return err
			}
			startOff = 0
		}
	}
}

// rewindToConsumed moves f back to startOff+consumed, undoing bufio.Scanner's
// read-ahead. The scanner fills its buffer in chunks, so f's physical offset
// ends up past the last line the caller actually took — by at least the
// unterminated tail that lineScanner withheld, and by more if the scan stopped
// early with an error. Leaving the offset where the reads stopped would make the
// next scanner resume in the middle of a line.
func rewindToConsumed(f *os.File, startOff, consumed int64) error {
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// consumed can never exceed the bytes read, so back is never negative.
	if back := pos - startOff - consumed; back > 0 {
		if _, err := f.Seek(-back, io.SeekCurrent); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Event-driven tail (hpcloud/tail with kqueue/inotify)
// ---------------------------------------------------------------------------

// stopTail stops an hpcloud/tail handle without deadlocking under backpressure.
// The library's internal tailFileSync goroutine produces lines into tt.Lines;
// when our consumer is blocked on a full out channel it stops draining tt.Lines,
// so that goroutine parks on `tt.Lines <- line`. A plain tt.Stop() (Kill+Wait)
// would then block forever waiting for a goroutine that is itself blocked on a
// send nobody is receiving — and the file's fd (possibly already unlinked) would
// never be released. Draining tt.Lines concurrently lets that goroutine observe
// the kill and exit, so Stop returns and the fd is closed. Lines drained here
// are intentionally discarded: callers stop only on ctx cancellation / rotation,
// where the hybrid path re-reads any gap from the file via drainByPoll.
func stopTail(tt *tail.Tail) {
	drained := make(chan struct{})
	go func() {
		for range tt.Lines {
		}
		close(drained)
	}()
	_ = tt.Stop()
	<-drained
}

// tailFileEvent uses hpcloud/tail's kqueue/inotify watcher to follow a file.
// It offers lower latency than polling but may stall under sustained
// concurrent writes due to the sendOnlyIfEmpty notification-drop race
// in the upstream library.
func (t *Tailer) tailFileEvent(ctx context.Context, path string, out chan<- string) {
	tt, err := tail.TailFile(path, tail.Config{
		Location:    &tail.SeekInfo{Whence: 0, Offset: 0},
		ReOpen:      true,
		Follow:      true,
		MustExist:   false,
		Poll:        false,
		Logger:      tail.DiscardingLogger,
		MaxLineSize: t.maxLineSize,
	})
	if err != nil {
		logging.WithError(err).WithField("path", path).Warn("tailer: failed to start event-mode tailing")
		return
	}

	defer stopTail(tt)

	// ⚠️ RELEASE-GATE INVARIANT — do NOT delete this ticker or its os.Stat /
	// os.SameFile self-check. Event mode had no ticker before v1.5.1 and pinned
	// every deleted-but-open inode forever: that is the leak that filled
	// rocket-nano's EKS overlay ~5GB/h. inotify delivers no IN_DELETE_SELF while
	// WE hold the file open, and hpcloud/tail's ReOpen:true never closes the fd,
	// so this periodic stat is the only fast (~500ms) fd-release path; reapMissing
	// is the coarser ≤1-rescan backstop. A regression here is INVISIBLE on
	// Windows/macOS (deleted-but-open is Linux-only) — only the Linux-container
	// tests catch it: TestReap_C2 (doc/test2.md C2), TestFD_D2, and the E/G soaks.
	origInfo, _ := os.Stat(path)
	ticker := time.NewTicker(hybridPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-tt.Lines:
			if !ok {
				return
			}
			if line == nil || len(line.Text) == 0 {
				continue
			}
			select {
			case out <- line.Text:
			case <-ctx.Done():
				return
			}
		case <-ticker.C:
			fi, err := os.Stat(path)
			if err != nil {
				return // deleted / renamed away — release the fd
			}
			if origInfo != nil && !os.SameFile(origInfo, fi) {
				return // replaced by a new inode at this path — reopen via rescan
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Hybrid tail (event-driven + poll fallback)
// ---------------------------------------------------------------------------

// hybridPollInterval is how often the hybrid mode checks for missed
// notifications. Longer than pure-poll mode because the event watcher
// handles the common case; this is only a safety net.
const hybridPollInterval = 500 * time.Millisecond

// tailFileHybrid uses hpcloud/tail's event-driven watcher as the primary
// mechanism, with a periodic stat-based fallback. When the event watcher
// delivers a line within hybridPollInterval, it is forwarded immediately
// (low latency). If no line arrives before the ticker fires, the fallback
// stats the file: if the size grew since the last known position, it
// stops the stalled event watcher, reads the missed data with a polling
// reader, then restarts the event watcher from the new position.
//
// This combines the low latency of kqueue/inotify with the reliability
// of polling, eliminating the sendOnlyIfEmpty notification-drop race.
func (t *Tailer) tailFileHybrid(ctx context.Context, path string, out chan<- string) {
	// Start from the head of the file (offset 0), matching poll and event modes:
	// a newly-discovered file is read in full from its beginning, so content that
	// already exists when the file is first matched — a backlog written before the
	// tailer attaches, or while the daemon was down — is ingested rather than
	// skipped. lastSize is the byte offset the event watcher has delivered up to;
	// it advances only after a line is forwarded (see tailFileHybridEvent) and is
	// reset to 0 on rotation/truncation so the replacement inode is likewise read
	// from the start. Re-reading the same content on a later restart is acceptable
	// (at-least-once) because every write model dedups: events by #uuid, users by
	// the _ts guard, dead letters by a content-derived _id. The dead-letter key is
	// what makes that claim hold for the whole pipeline — without it, a re-read
	// appended a duplicate diagnostic row for every line that failed to parse.
	var lastSize int64

	for {
		// --- Phase 1: event-driven reading via hpcloud/tail ---
		stalled := t.tailFileHybridEvent(ctx, path, out, &lastSize)
		if ctx.Err() != nil {
			return
		}

		if !stalled {
			// Event watcher exited normally (file deleted / rotated / error).
			// Wait briefly then re-enter the loop to reopen.
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.pollInterval):
			}
			continue
		}

		// --- Phase 2: poll fallback to drain missed data ---
		logging.WithField("path", path).Debug("tailer: hybrid fallback — draining missed data via poll")
		if err := t.drainByPoll(ctx, path, out, &lastSize); err != nil {
			if ctx.Err() != nil {
				return
			}
			logging.WithError(err).WithField("path", path).Warn("tailer: hybrid poll drain error")
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.pollInterval):
			}
		}
		// Loop back to restart the event watcher from the updated position.
	}
}

// tailFileHybridEvent runs the event-driven watcher and returns true if
// the watcher stalled (no data despite file growth), false if it exited
// normally (file rotated, deleted, or error).
func (t *Tailer) tailFileHybridEvent(ctx context.Context, path string, out chan<- string, lastSize *int64) (stalled bool) {
	tt, err := tail.TailFile(path, tail.Config{
		Location:    &tail.SeekInfo{Whence: io.SeekStart, Offset: *lastSize},
		ReOpen:      true,
		Follow:      true,
		MustExist:   false,
		Poll:        false,
		Logger:      tail.DiscardingLogger,
		MaxLineSize: t.maxLineSize,
	})
	if err != nil {
		return false
	}
	defer stopTail(tt)

	// Snapshot the opened inode so the fallback ticker can detect deletion or
	// an in-place inode swap that inotify will not report while we hold the
	// file open (no IN_DELETE_SELF until the last fd closes).
	origInfo, _ := os.Stat(path)

	ticker := time.NewTicker(hybridPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false

		case line, ok := <-tt.Lines:
			if !ok {
				return false
			}
			if line == nil {
				continue
			}
			// Track the consumed offset by byte counting instead of tt.Tell():
			// hpcloud/tail's Tell does an unlocked tail.file.Seek and reads
			// tail.reader, which data-races with the library's own reopen
			// (openReader, fired on delete-with-ReOpen and always on truncate).
			// Byte counting matches Tell's offset for newline-terminated records,
			// and any drift (e.g. CRLF) self-corrects when drainByPoll reseeks.
			//
			// Crucially, advance lastSize only AFTER the line is safely forwarded
			// (or for a skipped blank line). If we are preempted on a full out
			// channel and ctx is cancelled, the in-hand line was NOT delivered,
			// so leaving lastSize put lets the poll fallback / reopen re-read it
			// rather than skipping it (a lost line under backpressure).
			advance := int64(len(line.Text)) + 1
			if len(line.Text) == 0 {
				*lastSize += advance // consumed a blank line; nothing to forward
				continue
			}
			select {
			case out <- line.Text:
				*lastSize += advance
			case <-ctx.Done():
				return false
			}

		case <-ticker.C:
			// Fallback check. First stat the file: detect deletion or an
			// inode swap that inotify won't report for a file we hold open,
			// then detect a stalled watcher (file grew but no event arrived).
			fi, err := os.Stat(path)
			if err != nil {
				// File deleted / renamed away. Reset the position and return
				// so the outer loop reopens a fresh inode; the deferred
				// tt.Stop() releases the (possibly unlinked) fd.
				*lastSize = 0
				return false
			}
			if origInfo != nil && !os.SameFile(origInfo, fi) {
				// Replaced by a new inode at the same path (in-place
				// rotation). Reopen from the start of the new file.
				*lastSize = 0
				return false
			}
			if fi.Size() > *lastSize {
				// Data was written but no event arrived — stalled.
				return true
			}
		}
	}
}

// drainByPoll reads data from *lastSize up to the last newline-terminated line,
// forwarding each one and advancing *lastSize past it. An unterminated tail is
// left alone: *lastSize stops in front of it so a later pass picks the line up
// whole, once the writer has finished it.
func (t *Tailer) drainByPoll(ctx context.Context, path string, out chan<- string, lastSize *int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(*lastSize, io.SeekStart); err != nil {
		return err
	}

	ls := newLineScanner(f, t.maxLineSize)
	// delivered mirrors the event path's rule (see tailFileHybridEvent): advance
	// the offset only once a line is safely forwarded. ls.Consumed() counts a
	// token as soon as the split function produces it, which is BEFORE the send
	// below, so using it directly on the ctx-cancelled path would skip the line
	// we were preempted on.
	var delivered int64
	for ls.Scan() {
		line := ls.Text()
		if len(line) == 0 {
			delivered = ls.Consumed() // blank: nothing to forward, but it is consumed
			continue
		}
		select {
		case out <- line:
			delivered = ls.Consumed()
		case <-ctx.Done():
			*lastSize += delivered
			return ctx.Err()
		}
	}
	// Advance BEFORE checking the error. This function used to return
	// scanner.Err() first and only update *lastSize afterwards, so a scan that
	// failed partway — bufio.ErrTooLong on a line over maxLineSize is the
	// realistic trigger — left the offset behind the lines already forwarded.
	// The caller then looped back with the stale offset and re-read them, which
	// for dead letters meant duplicate rows (see DeadLetterModel's stable _id).
	*lastSize += ls.Consumed()
	return ls.Err()
}

// ---------------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------------

// DiscoverFiles walks directories matching glob patterns and returns
// deduplicated file paths. This is exported for use by the once package.
func DiscoverFiles(patterns []string) []string {
	return discoverFiles(patterns)
}

// discoverFiles walks directories matching glob patterns and returns
// deduplicated file paths.
func discoverFiles(patterns []string) []string {
	seen := make(map[string]struct{})
	var result []string

	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}

		// Strip leading "./" or ".\" from pattern before matching,
		// because filepath.WalkDir never returns paths with "./" prefix.
		matchPattern := pattern
		if strings.HasPrefix(matchPattern, "./") || strings.HasPrefix(matchPattern, `.\`) {
			matchPattern = matchPattern[2:]
		}
		// Normalize the pattern to forward slashes so it matches the
		// normalized (forward-slash) paths returned by normalizePath.
		// Without this, Windows-style patterns like C:\logs\*.log would
		// fail to match against /c/logs/app.log.
		matchPattern = normalizePath(matchPattern)

		base := globBaseDir(pattern)
		logging.WithFields(logging.Fields{
			"pattern":  pattern,
			"walk_dir": base,
		}).Debug("tailer: scanning directory for matching files")

		var matched int
		walkErr := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil // skip inaccessible paths
			}
			if d == nil || d.IsDir() {
				return nil
			}
			// Normalize the OS-native path to forward slashes so that the
			// glob pattern (written in Linux format) can match it.
			normalized := normalizePath(path)
			if globMatch(matchPattern, normalized) {
				if _, ok := seen[path]; !ok {
					seen[path] = struct{}{}
					result = append(result, path)
					matched++
				}
			}
			return nil
		})
		if walkErr != nil {
			logging.WithError(walkErr).WithField("walk_dir", base).Warn("tailer: error walking directory")
		}

		logging.WithFields(logging.Fields{
			"pattern":       pattern,
			"walk_dir":      base,
			"files_matched": matched,
		}).Info("tailer: scan complete")
	}
	return result
}

// globBaseDir derives a walk root directory from a glob pattern by taking
// the literal prefix before the first glob metacharacter (*, ?, [).
//
// It handles both absolute and relative paths:
//
//	/var/log/app/*.log   →  /var/log/app     (absolute, Linux)
//	/c/xxx/ta.*.log      →  C:\xxx           (absolute, Windows via toNativePath)
//	logs/ta.*.log        →  logs             (relative)
//	./logs/*.log         →  logs             (relative with dot prefix)
//	**/*.log             →  .                (no literal prefix → current dir)
//	*.log                →  .                (no literal prefix → current dir)
func globBaseDir(pattern string) string {
	// Handle "./" or ".\" prefix: skip it when scanning for metacharacters
	// so that the leading dot is not mistaken for a glob metacharacter.
	dotPrefix := ""
	scan := pattern
	if strings.HasPrefix(scan, "./") || strings.HasPrefix(scan, `.\`) {
		dotPrefix = scan[:2]
		scan = scan[2:]
	}

	idx := strings.IndexAny(scan, "*?[")

	if idx < 0 {
		// No metacharacters — the entire pattern is a literal path.
		full := dotPrefix + scan
		full = strings.TrimRight(full, `/\`)
		if full == "" || full == "." {
			return "."
		}
		native := toNativePath(full)
		return filepath.Dir(native)
	}

	// Find the last directory separator before the first metacharacter.
	// Everything up to that separator is the walk root.
	prefix := scan[:idx]
	lastSep := strings.LastIndexAny(prefix, `/\`)
	if lastSep >= 0 {
		dir := dotPrefix + prefix[:lastSep]
		if dir == "" {
			return "."
		}
		return toNativePath(dir)
	}

	// No separator before metachar — the metachar is in the first segment.
	if dotPrefix != "" {
		return "."
	}
	return "."
}
