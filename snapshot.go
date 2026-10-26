package ruvo

import "time"

// Snapshot is one view of the goroutines of the watched process. It is what
// the JSON API returns and what every view draws. In JSON, durations are
// integer nanoseconds and times are RFC 3339.
type Snapshot struct {
	// Seq orders snapshots: a later snapshot has a larger Seq. It follows At
	// in microseconds since the Unix epoch, but never goes back.
	Seq        int64     `json:"seq"`
	At         time.Time `json:"at"`
	Target     string    `json:"target"`
	Goroutines int       `json:"goroutines"`
	// Interval is how long until the next snapshot; it grows with the cost of taking one.
	Interval time.Duration `json:"interval"`
	// FullAt is when the last full (debug=2) dump was taken; State, CreatedBy and MaxWait come from it.
	FullAt time.Time `json:"fullAt,omitzero"`
	// FullCount is how many goroutines the last full dump held. It is less than
	// Goroutines when the dump was cut at 64 MB; State, CreatedBy and MaxWait
	// then cover only those goroutines.
	FullCount int `json:"fullCount,omitempty"`
	// FullCut reports that the last full dump was cut at 64 MB, so it holds only FullCount goroutines.
	FullCut bool `json:"fullCut,omitempty"`
	// LeaksAt is when the goroutineleak profile was last captured; Leaked comes from it.
	LeaksAt time.Time `json:"leaksAt,omitzero"`
	// TraceAt is when the last execution trace ended; unblock edges come from it.
	TraceAt time.Time `json:"traceAt,omitzero"`
	// TraceFor is how long that trace ran.
	TraceFor time.Duration `json:"traceFor,omitempty"`
	// TraceUnknown is how many wakeups of that trace had no edge because it
	// is not known where the woken goroutines waited.
	TraceUnknown int `json:"traceUnknown,omitempty"`
	// Offline reports that the snapshot comes from a session file opened with
	// OpenSession: nothing reads the target, and the actions are off.
	Offline bool `json:"offline,omitempty"`
	// Error is set when the last attempt to read the target failed; Groups then hold the last good data.
	Error  string  `json:"error,omitempty"`
	Groups []Group `json:"groups"`
	Edges  []Edge  `json:"edges"`
}

// Group is a set of goroutines that wait at the same place and run the same function.
type Group struct {
	ID       string `json:"id"`
	WaitSite string `json:"waitSite"`
	// Entry is the function the goroutines were started with.
	Entry     string `json:"entry"`
	CreatedBy string `json:"createdBy,omitempty"`
	State     string `json:"state,omitempty"`
	Count     int    `json:"count"`
	// MaxWait is the longest any goroutine of the group has been seen waiting.
	// It is meaningful only when InFullDump is true.
	MaxWait time.Duration `json:"maxWait"`
	// InFullDump reports whether the group was in the last full dump, which is
	// where State, CreatedBy and MaxWait come from.
	InFullDump bool `json:"inFullDump"`
	// Growth is the trend of Count per minute over the last two minutes; a
	// change of less than one goroutine over that span counts as zero.
	Growth float64 `json:"growthPerMin"`
	Leaked int     `json:"leaked"`
	// Labels breaks Count down by the pprof labels of the goroutines, most
	// common first and at most 10 sets; goroutines without labels are not
	// counted. Labels come from code that calls pprof.Do or SetGoroutineLabels.
	Labels []LabelCount `json:"labels,omitempty"`
	// Unwoken is how many goroutines of the group waited through the whole
	// last trace: nothing woke them while it ran.
	Unwoken int      `json:"unwoken,omitempty"`
	Stack   []string `json:"stack"`
	// Std reports that the group waits in the standard library, with no code
	// of yours on the stack.
	Std bool `json:"std,omitempty"`
}

// LabelCount is how many goroutines of a group carry one set of pprof labels.
type LabelCount struct {
	Labels map[string]string `json:"labels"`
	Count  int               `json:"count"`
}

// EdgeKind says how two groups are related.
type EdgeKind string

// Edge kinds.
const (
	EdgeSpawn   EdgeKind = "spawn"
	EdgeUnblock EdgeKind = "unblock"
)

// Edge links two groups. It is one of two kinds:
//
//   - EdgeUnblock: goroutines of From woke goroutines of To during the last
//     execution trace, so To was waiting for From. From may also be
//     "network" or "timer" for wakeups no goroutine caused.
//   - EdgeSpawn: goroutines of From created Count goroutines of To that are
//     still alive, according to the latest full dump.
type Edge struct {
	// From is a group ID, or "network" or "timer" on an unblock edge.
	From string `json:"from"`
	// To is a group ID.
	To   string   `json:"to"`
	Kind EdgeKind `json:"kind"`
	// Count is the number of wakeups on an unblock edge and the number of
	// live goroutines of To created by From on a spawn edge.
	Count int `json:"count"`
	// Wait is the total time the woken goroutines waited before being woken.
	// It is set only on unblock edges.
	Wait time.Duration `json:"wait,omitempty"`
	// Waiters is the number of goroutines woken, summed over the wakeups
	// merged into the edge; it is an upper bound on the distinct goroutines.
	// It is set only on unblock edges.
	Waiters int `json:"waiters,omitempty"`
	// Approx reports that an end of the edge was matched to a current group
	// by its wait site or start function rather than exactly: the group it
	// was recorded for is gone.
	Approx bool `json:"approx,omitempty"`
}
