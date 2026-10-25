package ruvo

import (
	"math"
	"time"
)

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
	var (
		span             time.Duration
		sx, sy, sxx, sxy float64
		points           = 1.0
	)

	for _, s := range h.samples {
		age := now.Sub(s.at)
		if age > growthSpan {
			continue
		}

		c, ok := s.counts[id]
		if !ok {
			continue
		}

		x := -age.Minutes()
		y := float64(c - current)

		span = max(span, age)
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
		points++
	}

	if span < growthMinSpan {
		return 0
	}

	slope := (points*sxy - sx*sy) / (points*sxx - sx*sx)

	if math.Abs(slope)*span.Minutes() < growthMinDelta {
		return 0
	}

	return slope
}
