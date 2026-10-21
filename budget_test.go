package ruvo

import (
	"testing"
	"time"
)

func TestNextInterval(t *testing.T) {
	for _, c := range []struct {
		cost   time.Duration
		factor int
		lo, hi time.Duration
		want   time.Duration
	}{
		{time.Millisecond, 200, 2 * time.Second, time.Minute, 2 * time.Second},
		{50 * time.Millisecond, 200, 2 * time.Second, time.Minute, 10 * time.Second},
		{time.Second, 200, 2 * time.Second, time.Minute, time.Minute},
		{100 * time.Millisecond, 1000, time.Minute, 10 * time.Minute, 100 * time.Second},
		{2 * time.Second, 1000, time.Minute, 10 * time.Minute, 10 * time.Minute},
	} {
		if got := nextInterval(c.cost, c.factor, c.lo, c.hi); got != c.want {
			t.Errorf("nextInterval(%v, %d) = %v, want %v", c.cost, c.factor, got, c.want)
		}
	}
}
