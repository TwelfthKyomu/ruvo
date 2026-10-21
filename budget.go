package ruvo

import "time"

const (
	frequentMin    = 2 * time.Second
	frequentMax    = time.Minute
	frequentFactor = 200
	fullMin        = time.Minute
	fullMax        = 30 * time.Minute
	fullFactor     = 1000
	historyWindow  = 10 * time.Minute
	idleStop       = 10 * time.Minute
	fetchTimeout   = 30 * time.Second
	growthSpan     = 2 * time.Minute
	growthMinSpan  = 30 * time.Second
	waitSlack      = 3 * time.Minute
	actionGap      = 10 * time.Second
	firstFullLimit = 10_000
)

func nextInterval(cost time.Duration, factor int, lo, hi time.Duration) time.Duration {
	return min(max(cost*time.Duration(factor), lo), hi)
}
