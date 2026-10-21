package ruvo

import "time"

type history struct {
	window  time.Duration
	samples []sample
}

type sample struct {
	at     time.Time
	counts map[string]int
}

func (h *history) add(at time.Time, counts map[string]int) {
	h.samples = append(h.samples, sample{at: at, counts: counts})

	cut := 0

	for cut < len(h.samples) && at.Sub(h.samples[cut].at) > h.window {
		cut++
	}

	h.samples = h.samples[cut:]
}

func (h *history) growth(id string, now time.Time, current int) float64 {
	for _, s := range h.samples {
		age := now.Sub(s.at)
		if age > growthSpan {
			continue
		}

		if age < growthMinSpan {
			return 0
		}

		return float64(current-s.counts[id]) / age.Minutes()
	}

	return 0
}
