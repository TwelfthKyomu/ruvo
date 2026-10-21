package ruvo

import "time"

// Snapshot is one view of the goroutines of the watched process. It is what
// the JSON API returns and what every view draws. In JSON, durations are
// integer nanoseconds and times are RFC 3339.
type Snapshot struct {
	At         time.Time `json:"at"`
	Target     string    `json:"target"`
	Goroutines int       `json:"goroutines"`
	// Interval is how long until the next snapshot; it grows with the cost of taking one.
	Interval time.Duration `json:"interval"`
	// FullAt is when the last full (debug=2) dump was taken; State, CreatedBy and MaxWait come from it.
	FullAt time.Time `json:"fullAt,omitzero"`
	// LeaksAt is when the goroutineleak profile was last captured; Leaked comes from it.
	LeaksAt time.Time `json:"leaksAt,omitzero"`
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
	// Growth is the change in Count per minute over the last two minutes.
	Growth float64  `json:"growthPerMin"`
	Leaked int      `json:"leaked"`
	Stack  []string `json:"stack"`
}

// EdgeKind says how two groups are related.
type EdgeKind string

// Edge kinds.
const (
	EdgeSpawn   EdgeKind = "spawn"
	EdgeUnblock EdgeKind = "unblock"
)

// Edge links two groups.
type Edge struct {
	From  string   `json:"from"`
	To    string   `json:"to"`
	Kind  EdgeKind `json:"kind"`
	Count int      `json:"count"`
}
