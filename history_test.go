package ruvo

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

func TestGrowthPerMinute(t *testing.T) {
	h := history{window: historyWindow}
	h.add(t0, map[string]int{"a": 10})

	if got := h.growth("a", t0.Add(time.Minute), 60); got != 50 {
		t.Fatalf("growth %v, want 50", got)
	}
}

func TestGrowthNeedsThirtySeconds(t *testing.T) {
	h := history{window: historyWindow}
	h.add(t0, map[string]int{"a": 10})

	if got := h.growth("a", t0.Add(10*time.Second), 20); got != 0 {
		t.Fatalf("growth %v, want 0", got)
	}
}

func TestGrowthUsesTwoMinuteSpan(t *testing.T) {
	h := history{window: historyWindow}
	h.add(t0, map[string]int{"a": 0})
	h.add(t0.Add(3*time.Minute), map[string]int{"a": 30})

	if got := h.growth("a", t0.Add(4*time.Minute), 60); got != 30 {
		t.Fatalf("growth %v, want 30", got)
	}
}

func TestHistoryPrunesOld(t *testing.T) {
	h := history{window: 10 * time.Minute}
	h.add(t0, map[string]int{"a": 1})
	h.add(t0.Add(11*time.Minute), map[string]int{"a": 2})

	if len(h.samples) != 1 {
		t.Fatalf("%d samples, want 1", len(h.samples))
	}
}
