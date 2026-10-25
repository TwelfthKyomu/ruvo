package ruvo

import (
	"io"
	"time"
)

// Frame is one call in a goroutine stack. Stacks are slices of frames with
// the innermost call first.
type Frame struct {
	Func string
	File string
	Line int
}

// Wakeup sums up how goroutines waiting at one stack were woken during an
// execution trace.
type Wakeup struct {
	// Waker is the stack where the waking goroutine last waited; it is empty
	// unless Cause is CauseGoroutine.
	Waker []Frame
	// Cause is CauseGoroutine, CauseNetwork or CauseTimer.
	Cause string
	// Waiter is the stack where the woken goroutines waited.
	Waiter []Frame
	// Count is the number of wakeups.
	Count int
	// Waiters is the number of distinct goroutines woken.
	Waiters int
	// Wait is the total time they waited before being woken.
	Wait time.Duration
}

// Causes of a wakeup.
const (
	CauseGoroutine = "goroutine"
	CauseNetwork   = "network"
	CauseTimer     = "timer"
)

// TraceReader reads wakeups from a runtime execution trace, as written by
// runtime/trace or served at /debug/pprof/trace. stacks maps goroutine IDs
// to the stacks of a full dump taken while the trace was recorded, for
// goroutines that were already waiting when it started; it may be nil.
type TraceReader interface {
	ReadTrace(trace io.Reader, stacks map[int64][]Frame) ([]Wakeup, error)
}
