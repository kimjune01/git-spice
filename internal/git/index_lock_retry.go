package git

import (
	"context"
	"errors"

	"go.abhg.dev/gs/internal/retry"
	"go.abhg.dev/gs/internal/xec"
)

// runGitWithIndexLockRetry runs a Git command under the worktree's
// configured lock retry policy.
//
// build must construct a fresh command for each attempt.
// This allows callers to rebuild all command state,
// including stdio wiring, transient config, and output buffers,
// without needing cloning support from xec.
//
// If retry is disabled, build is invoked exactly once
// and the resulting command is run once.
// Otherwise, a fresh command is built and run on each attempt
// until it succeeds, fails terminally,
// or the configured timeout is exhausted.
func (w *Worktree) runGitWithIndexLockRetry(
	ctx context.Context,
	build func() *gitCmd,
) error {
	runAttempt := func(attempt retry.Attempt) error {
		cmd := build()
		observer := cmd.ObserveLock()
		if err := cmd.Run(); err != nil {
			if observer.IsLockErr(err) {
				cmd.log.Debug("Retrying Git command after lock contention",
					"attempt", attempt.Number,
					"error", err,
				)
				return err
			}
			return retry.Fail(err)
		}
		return nil
	}

	return retry.Exponential{
		Timeout: w.indexLockTimeout,
		Delay:   _indexLockRetryDelay,
	}.Do(ctx, runAttempt)
}

// ObserveLock attaches a lock observer
// to the command's stderr stream.
//
// The observer sees the same bytes that would otherwise
// go only to the command's current stderr destination.
func (c *gitCmd) ObserveLock() *lockObserver {
	observer := new(lockObserver)
	c.cmd.TeeStderr(observer)
	return observer
}

// ObserveIndexLock is an alias for [ObserveLock]
// kept for backward compatibility.
func (c *gitCmd) ObserveIndexLock() *lockObserver {
	return c.ObserveLock()
}

// _numLockTokens is the number of lock tokens to match.
const _numLockTokens = 2

// _lockTokens lists the lock file names
// that Git mentions in stderr during lock contention.
var _lockTokens = [_numLockTokens]string{
	"index.lock",
	"HEAD.lock",
}

// lockObserver watches a byte stream
// for Git's lock conflict markers.
//
// Git writes lock file names like "index.lock" or "HEAD.lock"
// in its stderr output when it cannot acquire a lock.
// This observer matches those tokens incrementally
// as bytes arrive from stderr.
//
// The zero value is ready to use.
// It starts with no partial match state
// and reports that no token has been seen.
type lockObserver struct {
	// matchers tracks the match state for each lock token.
	matchers [_numLockTokens]tokenMatcher

	// seen reports whether any lock token
	// has been observed in the stream.
	// Once set, it stays set.
	seen bool
}

// indexLockObserver is an alias for [lockObserver]
// kept for backward compatibility in tests.
type indexLockObserver = lockObserver

// Write consumes stderr bytes
// and updates the observer's match state.
func (o *lockObserver) Write(p []byte) (int, error) {
	if o.seen {
		return len(p), nil
	}
	for _, b := range p {
		for i := range o.matchers {
			if o.matchers[i].writeByte(b, _lockTokens[i]) {
				o.seen = true
				return len(p), nil
			}
		}
	}
	return len(p), nil
}

// Seen reports whether the observer has matched any lock token.
func (o *lockObserver) Seen() bool {
	return o.seen
}

// IsLockErr reports whether err is a non-zero-exit error
// from a command whose stderr stream contained a lock token.
func (o *lockObserver) IsLockErr(err error) bool {
	var exitErr *xec.ExitError
	return errors.As(err, &exitErr) && o.seen
}

// IsIndexLockErr is an alias for [IsLockErr]
// kept for backward compatibility in tests.
func (o *lockObserver) IsIndexLockErr(err error) bool {
	return o.IsLockErr(err)
}

// tokenMatcher tracks incremental match state
// for a single lock token string.
type tokenMatcher struct {
	// state is the number of token bytes
	// matched so far at the end of the stream.
	state int
}

// writeByte advances the matcher by one byte
// and reports whether the full token was matched.
//
// This is a simple prefix-state matcher.
// The tokens have no useful repeated prefix structure,
// so on mismatch we only need to either:
//   - restart from state 1 if this byte can begin a fresh match, or
//   - reset to state 0 otherwise.
//
// That makes matching cheap while still handling cases
// where the token is split across arbitrary write boundaries.
func (m *tokenMatcher) writeByte(b byte, token string) bool {
	switch b {
	case token[0]:
		m.state = 1
	case token[m.state]:
		m.state++
	default:
		m.state = 0
	}

	if m.state == len(token) {
		m.state = 0
		return true
	}
	return false
}
