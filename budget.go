package ruvo

import "time"

const (
	frequentMin    = 2 * time.Second
	frequentMax    = time.Minute
	frequentFactor = 200
	fullMin        = time.Minute
	fullMax        = 30 * time.Minute
	fullFactor     = 1000
	historyWindow  = growthSpan
	idleStop       = 10 * time.Minute
	fetchTimeout   = 30 * time.Second
	growthSpan     = 2 * time.Minute
	growthMinSpan  = 30 * time.Second
	growthMinDelta = 1
	waitSlack      = 3 * time.Minute
	actionGap      = 10 * time.Second
	firstFullLimit = 10_000
	traceDefault   = 5 * time.Second
	traceMax       = 30 * time.Second
	traceDumpDelay = 200 * time.Millisecond
	fullDumpCap    = 64 << 20

	eventWriteTimeout = 10 * time.Second
)

func nextInterval(cost time.Duration, factor int, lo, hi time.Duration) time.Duration {
	return min(max(cost*time.Duration(factor), lo), hi)
}
