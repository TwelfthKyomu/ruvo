package ruvo

import (
	"maps"
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

func spawned(id, creator int64, fn string) dump.Goroutine {
	g := goroutine(id, "chan receive", 0, fn)
	g.CreatedBy = dump.Frame{Func: "main.server", File: "/home/dev/a.go", Line: 7}
	g.Creator = creator

	return g
}

func TestFullStateSpawns(t *testing.T) {
	server := goroutine(1, "select", 0, "main.server")
	server.Creator = 0

	var f fullState

	f.update(t0, []dump.Goroutine{server, spawned(5, 1, "main.worker"), spawned(6, 1, "main.worker"), spawned(7, 99, "main.worker")})

	want := map[[2]endpoint]int{{endpointOf(server.Frames), endpointOf(spawned(5, 1, "main.worker").Frames)}: 2}

	if !maps.Equal(f.spawns, want) {
		t.Fatalf("spawns %v, want %v", f.spawns, want)
	}
}

func TestFullStateSpawnsSkipOwn(t *testing.T) {
	own := dump.Goroutine{
		ID: 2,
		Frames: []dump.Frame{
			{Func: "runtime.gopark", File: "/usr/local/go/src/runtime/proc.go", Line: 460},
			{Func: "github.com/TwelfthKyomu/ruvo.(*handler).serveEvents", File: "/home/dev/ruvo/handler.go", Line: 150},
		},
		Creator: 1,
	}
	server := goroutine(1, "select", 0, "main.server")
	server.Creator = 0

	var f fullState

	f.update(t0, []dump.Goroutine{server, own, spawned(8, 2, "main.worker")})

	if len(f.spawns) != 0 {
		t.Fatalf("spawns %v, want none: the creator is a goroutine of ruvo", f.spawns)
	}

	spawn := spawned(9, 1, "main.worker")
	spawn.Frames = own.Frames

	f.update(t0, []dump.Goroutine{server, spawn})

	if len(f.spawns) != 0 {
		t.Fatalf("spawns %v, want none: the child is a goroutine of ruvo", f.spawns)
	}
}

func TestFullStateSpawnsFollowLastDump(t *testing.T) {
	server := goroutine(1, "select", 0, "main.server")
	server.Creator = 0

	var f fullState

	f.update(t0, []dump.Goroutine{server, spawned(5, 1, "main.worker")})
	f.update(t0.Add(time.Minute), []dump.Goroutine{server, spawned(5, 77, "main.worker")})

	if len(f.spawns) != 0 {
		t.Fatalf("spawns %v, want none: the last dump has no creator", f.spawns)
	}
}
