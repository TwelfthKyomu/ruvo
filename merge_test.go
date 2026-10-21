package ruvo

import (
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

func goroutine(id int64, state string, wait time.Duration, fn string) dump.Goroutine {
	return dump.Goroutine{
		ID:        id,
		State:     state,
		Wait:      wait,
		Frames:    []dump.Frame{{Func: fn, File: "/home/dev/a.go", Line: 7}},
		CreatedBy: dump.Frame{Func: "main.start", File: "/home/dev/a.go", Line: 3},
		Creator:   1,
	}
}

func idOf(fn string) string {
	f := dump.Frame{Func: fn, File: "/home/dev/a.go", Line: 7}

	return groupID(f, f)
}

func TestFullStateMostCommonState(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{
		goroutine(1, "select", 0, "main.x"),
		goroutine(2, "select", 0, "main.x"),
		goroutine(3, "chan receive", 0, "main.x"),
	})

	g := f.groups[idOf("main.x")]
	if g.state != "select" || g.createdBy != "main.start a.go:3" {
		t.Fatalf("group %+v", g)
	}
}

func TestFullStateMaxWaitFromHeader(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{goroutine(1, "chan receive", 7*time.Minute, "main.x")})

	if got := f.groups[idOf("main.x")].maxWait; got != 7*time.Minute {
		t.Fatalf("maxWait %v, want 7m", got)
	}
}

func TestFullStateMaxWaitFromSeenIDs(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{goroutine(5, "select", 0, "main.x")})
	f.update(t0.Add(5*time.Minute), []dump.Goroutine{goroutine(5, "select", 4*time.Minute, "main.x")})

	if got := f.groups[idOf("main.x")].maxWait; got != 5*time.Minute {
		t.Fatalf("maxWait %v, want 5m", got)
	}
}

func TestBusyPoolDoesNotAccumulateWait(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{goroutine(5, "chan receive", 0, "main.x")})
	f.update(t0.Add(4*time.Minute), []dump.Goroutine{goroutine(5, "chan receive", 0, "main.x")})

	if got := f.groups[idOf("main.x")].maxWait; got != 0 {
		t.Fatalf("maxWait %v, want 0: the runtime says this goroutine has waited under a minute", got)
	}
}

func TestSeenWaitIsCappedByRuntime(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{goroutine(5, "select", 0, "main.x")})
	f.update(t0.Add(20*time.Minute), []dump.Goroutine{goroutine(5, "select", 2*time.Minute, "main.x")})

	if got := f.groups[idOf("main.x")].maxWait; got != 5*time.Minute {
		t.Fatalf("maxWait %v, want 5m (2m reported + 3m slack)", got)
	}
}

func TestFullStateResetsWhenMoved(t *testing.T) {
	var f fullState

	f.update(t0, []dump.Goroutine{goroutine(5, "select", 0, "main.x")})
	f.update(t0.Add(90*time.Second), []dump.Goroutine{goroutine(5, "select", 0, "main.y")})

	if got := f.groups[idOf("main.y")].maxWait; got != 0 {
		t.Fatalf("maxWait %v, want 0", got)
	}
}

func TestLeakStateCounts(t *testing.T) {
	var l leakState

	l.update(t0, readProfile(t, "leak.txt"))

	notify := dump.Frame{Func: "main.notify", File: "/home/dev/leaky/pipeline.go", Line: 190}

	if len(l.counts) != 1 || l.counts[groupID(notify, notify)] != 1390 || !l.at.Equal(t0) {
		t.Fatalf("counts %v at %v", l.counts, l.at)
	}
}
