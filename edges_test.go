package ruvo

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

var (
	workerStack = []Frame{
		{Func: "time.Sleep", File: "/usr/local/go/src/runtime/time.go", Line: 368},
		{Func: "main.(*pipeline).worker", File: "/home/dev/leaky/pipeline.go", Line: 114},
	}
	handlerStack = []Frame{
		{Func: "main.(*pipeline).handleWork", File: "/home/dev/leaky/pipeline.go", Line: 145},
		{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
	}
)

func traceCollector(t *testing.T, ws ...Wakeup) *collector {
	t.Helper()

	c, _ := newTestCollector(t, newFake(t, "stacks.txt", "full.txt", ""))
	c.reader = &fakeReader{wakeups: ws}
	c.sample(context.Background(), false)

	return c
}

func groupIDs(t *testing.T, s Snapshot) (worker, handler string) {
	t.Helper()

	w, ok := groupByEntry(s, "main.(*pipeline).worker")
	if !ok {
		t.Fatal("no worker group")
	}

	h, ok := groupByEntry(s, "net/http.(*conn).serve")
	if !ok {
		t.Fatal("no handler group")
	}

	return w.ID, h.ID
}

func TestCaptureTraceBuildsEdges(t *testing.T) {
	c := traceCollector(t, Wakeup{Cause: CauseGoroutine, Waker: workerStack, Waiter: handlerStack, Count: 10, Waiters: 1, Wait: 4 * time.Second})

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	worker, handler := groupIDs(t, s)
	want := []Edge{{From: worker, To: handler, Kind: EdgeUnblock, Count: 10, Waiters: 1, Wait: 4 * time.Second}}

	if !slices.Equal(s.Edges, want) {
		t.Fatalf("edges %+v, want %+v", s.Edges, want)
	}

	if !s.TraceAt.Equal(t0) || s.TraceFor != 5*time.Second {
		t.Fatalf("traceAt %v, traceFor %v", s.TraceAt, s.TraceFor)
	}

	if s := c.sample(context.Background(), false); !slices.Equal(s.Edges, want) || !s.TraceAt.Equal(t0) {
		t.Fatalf("next sample: edges %+v, traceAt %v: the trace must outlive one snapshot", s.Edges, s.TraceAt)
	}
}

func TestCaptureTraceNetworkEdge(t *testing.T) {
	c := traceCollector(t, Wakeup{Cause: CauseNetwork, Waiter: workerStack, Count: 3, Waiters: 2, Wait: time.Second})

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	worker, _ := groupIDs(t, s)

	if len(s.Edges) != 1 || s.Edges[0].From != "network" || s.Edges[0].To != worker {
		t.Fatalf("edges %+v, want one from network to %s", s.Edges, worker)
	}
}

func TestCaptureTraceResolvesByEntry(t *testing.T) {
	waker := []Frame{
		{Func: "main.(*pipeline).flush", File: "/home/dev/leaky/pipeline.go", Line: 120},
		{Func: "main.(*pipeline).worker", File: "/home/dev/leaky/pipeline.go", Line: 118},
	}
	c := traceCollector(t, Wakeup{Cause: CauseGoroutine, Waker: waker, Waiter: handlerStack, Count: 1, Waiters: 1, Wait: time.Second})

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	worker, handler := groupIDs(t, s)

	if len(s.Edges) != 1 || s.Edges[0].From != worker || s.Edges[0].To != handler {
		t.Fatalf("edges %+v, want one from %s to %s", s.Edges, worker, handler)
	}
}

func TestCaptureTraceDropsUnknownGroups(t *testing.T) {
	unknown := []Frame{
		{Func: "main.elsewhere", File: "/home/dev/other.go", Line: 3},
		{Func: "main.start", File: "/home/dev/other.go", Line: 9},
	}
	c := traceCollector(t,
		Wakeup{Cause: CauseGoroutine, Waker: workerStack, Waiter: unknown, Count: 1, Waiters: 1, Wait: time.Second},
		Wakeup{Cause: CauseGoroutine, Waker: unknown, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second},
		Wakeup{Cause: CauseTimer, Waiter: unknown, Count: 1, Waiters: 1, Wait: time.Second},
	)

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	if s.Edges == nil || len(s.Edges) != 0 {
		t.Fatalf("edges %+v, want none", s.Edges)
	}
}

func TestCaptureTraceSkipsStdEntry(t *testing.T) {
	waiter := []Frame{
		{Func: "main.(*pipeline).render", File: "/home/dev/leaky/pipeline.go", Line: 150},
		{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
	}
	c := traceCollector(t, Wakeup{Cause: CauseGoroutine, Waker: workerStack, Waiter: waiter, Count: 1, Waiters: 1, Wait: time.Second})

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	if len(s.Edges) != 0 {
		t.Fatalf("edges %+v, want none: a standard-library entry says nothing about the group", s.Edges)
	}
}

func TestTraceEdgesMergeSameGroups(t *testing.T) {
	other := []Frame{
		{Func: "time.Sleep", File: "/usr/local/go/src/runtime/time.go", Line: 370},
		{Func: "main.(*pipeline).worker", File: "/home/dev/leaky/pipeline.go", Line: 114},
	}

	got := traceEdges([]Wakeup{
		{Cause: CauseTimer, Waiter: workerStack, Count: 2, Waiters: 1, Wait: time.Second},
		{Cause: CauseTimer, Waiter: other, Count: 3, Waiters: 2, Wait: 2 * time.Second},
		{Cause: CauseNetwork, Waiter: other, Count: 1, Waiters: 1, Wait: time.Second},
	})

	if len(got) != 2 || got[0].cause != CauseTimer || got[0].count != 5 || got[0].waiters != 3 || got[0].wait != 3*time.Second {
		t.Fatalf("trace edges %+v, want the two timer wakeups merged", got)
	}
}

func TestTraceEdgesDropsOwnAndUnknownCauses(t *testing.T) {
	own := []Frame{
		{Func: "runtime.gopark", File: "/usr/local/go/src/runtime/proc.go", Line: 460},
		{Func: "github.com/TwelfthKyomu/ruvo.(*handler).serveEvents", File: "/home/dev/ruvo/handler.go", Line: 150},
		{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
	}

	got := traceEdges([]Wakeup{
		{Cause: CauseNetwork, Waiter: own, Count: 1},
		{Cause: CauseGoroutine, Waker: own, Waiter: workerStack, Count: 1},
		{Cause: CauseGoroutine, Waiter: workerStack, Count: 1},
		{Cause: "signal", Waiter: workerStack, Count: 1},
		{Cause: CauseTimer, Count: 1},
		{Cause: CauseTimer, Waiter: workerStack, Count: 2},
	})

	if len(got) != 1 || got[0].cause != CauseTimer || got[0].count != 2 {
		t.Fatalf("trace edges %+v, want only the timer wakeup of the worker", got)
	}
}

func TestResolveEndpoint(t *testing.T) {
	idx := newGroupIndex(map[string]*Group{
		"a": {ID: "a", WaitSite: "main.f a.go:1", Entry: "main.run", Count: 3},
		"b": {ID: "b", WaitSite: "main.g a.go:2", Entry: "main.run", Count: 5},
		"c": {ID: "c", WaitSite: "main.g a.go:2", Entry: "main.other", Count: 1},
		"d": {ID: "d", WaitSite: "main.h a.go:3", Entry: "main.tie", Count: 4},
		"e": {ID: "e", WaitSite: "main.i a.go:4", Entry: "main.tie", Count: 4},
		"f": {ID: "f", WaitSite: "main.serve a.go:5", Entry: "net/http.(*conn).serve", Count: 9},
		"g": {ID: "g", WaitSite: "main.deep a.go:6", Entry: "main.top", Count: 2, Stack: []string{"main.deep a.go:6", "main.mid a.go:7", "main.top a.go:8"}},
		"h": {ID: "h", WaitSite: "main.shared a.go:9", Entry: "main.entryB", Count: 6},
	})

	for _, tc := range []struct {
		e     endpoint
		want  string
		exact bool
	}{
		{endpoint{id: "a", site: "main.g a.go:2", entry: "main.other"}, "a", true},
		{endpoint{id: "x", site: "main.f a.go:1", entry: "main.run"}, "a", false},
		{endpoint{id: "x", site: "main.f a.go:1", entry: "main.other"}, "c", false},
		{endpoint{id: "x", site: "main.g a.go:2", entry: "main.other"}, "c", false},
		{endpoint{id: "x", site: "main.g a.go:2", entry: "main.mixed", calls: "\x00main.run\x00main.other\x00main.mixed\x00"}, "", false},
		{endpoint{id: "x", site: "main.z a.go:9", entry: "main.run"}, "", false},
		{endpoint{id: "x", site: "main.z a.go:9", entry: "main.tie"}, "", false},
		{endpoint{id: "x", site: "main.z a.go:9", entry: "main.top"}, "g", false},
		{endpoint{id: "x", site: "main.z a.go:9", entry: "main.none"}, "", false},
		{endpoint{id: "x", site: "main.z a.go:9", entry: "net/http.(*conn).serve"}, "", false},
		{endpoint{id: "x", site: "main.serve a.go:5", entry: "net/http.(*conn).serve"}, "f", false},
		{endpoint{id: "x", site: "main.deep a.go:6", entry: "main.mid"}, "g", false},
		{endpoint{id: "x", site: "main.deep a.go:6", entry: "main.cut", calls: "\x00main.deep\x00main.top\x00main.cut\x00"}, "g", false},
		{endpoint{id: "x", site: "main.shared a.go:9", entry: "main.entryA"}, "", false},
	} {
		got, exact, ok := idx.resolve(tc.e)
		if got != tc.want || ok != (tc.want != "") || exact != tc.exact {
			t.Errorf("resolve(%+v) = %q, exact %v, %v; want %q, exact %v", tc.e, got, exact, ok, tc.want, tc.exact)
		}
	}
}

func TestOldEdgeDoesNotMoveToAnotherEntry(t *testing.T) {
	shared := dump.Frame{Func: "main.sharedWait", File: "/home/dev/a.go", Line: 5}
	old := endpointOf([]dump.Frame{shared, {Func: "main.entryA", File: "/home/dev/a.go", Line: 20}})
	now := []dump.Frame{shared, {Func: "main.entryB", File: "/home/dev/a.go", Line: 30}}
	waker := []dump.Frame{{Func: "main.waker", File: "/home/dev/a.go", Line: 40}}

	groups, _ := buildGroups(dump.Profile{Records: []dump.Record{{Count: 3, Frames: now}, {Count: 1, Frames: waker}}})

	c := &collector{trace: traceState{at: t0, d: traceDefault, edges: []traceEdge{
		{from: endpointOf(waker), to: old, cause: CauseGoroutine, count: 4, waiters: 1, wait: time.Second},
		{to: old, cause: CauseTimer, count: 2, waiters: 1, wait: time.Second},
	}}}

	if got := c.edgesLocked(newGroupIndex(groups)); len(got) != 0 {
		t.Fatalf("edges %+v: an edge of main.entryA must not land on main.entryB", got)
	}
}

func TestEdgesMergeAndOrder(t *testing.T) {
	groups, _ := buildGroups(readProfile(t, "stacks.txt"))

	var worker, handler endpoint

	for _, g := range groups {
		switch g.Entry {
		case "main.(*pipeline).worker":
			worker = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		case "net/http.(*conn).serve":
			handler = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		}
	}

	stale := endpoint{id: "gone", site: worker.site, entry: "main.renamed", calls: "\x00main.(*pipeline).worker\x00main.renamed\x00"}

	c := &collector{trace: traceState{at: t0, d: traceDefault, edges: []traceEdge{
		{from: worker, to: handler, cause: CauseGoroutine, count: 3, waiters: 1, wait: time.Second},
		{from: stale, to: handler, cause: CauseGoroutine, count: 2, waiters: 2, wait: 2 * time.Second},
		{to: worker, cause: CauseNetwork, count: 1, waiters: 1, wait: 3 * time.Second},
		{to: worker, cause: CauseTimer, count: 7, waiters: 1, wait: 3 * time.Second},
		{from: handler, to: handler, cause: CauseGoroutine, count: 9, waiters: 1, wait: 3 * time.Second},
		{to: worker, cause: CauseTimer, count: 4, waiters: 3, wait: time.Second},
		{to: handler, cause: CauseTimer, count: 1, waiters: 1, wait: 3 * time.Second},
	}}}

	want := []Edge{
		{From: "timer", To: worker.id, Kind: EdgeUnblock, Count: 11, Waiters: 4, Wait: 4 * time.Second},
		{From: handler.id, To: handler.id, Kind: EdgeUnblock, Count: 9, Waiters: 1, Wait: 3 * time.Second},
		{From: worker.id, To: handler.id, Kind: EdgeUnblock, Count: 5, Waiters: 3, Wait: 3 * time.Second, Approx: true},
		{From: "network", To: worker.id, Kind: EdgeUnblock, Count: 1, Waiters: 1, Wait: 3 * time.Second},
		{From: "timer", To: handler.id, Kind: EdgeUnblock, Count: 1, Waiters: 1, Wait: 3 * time.Second},
	}

	if got := c.edgesLocked(newGroupIndex(groups)); !slices.Equal(got, want) {
		t.Fatalf("edges\n%+v\nwant\n%+v", got, want)
	}
}

func TestEndpointOfMatchesGroup(t *testing.T) {
	frames := []dump.Frame{
		{Func: "time.Sleep", File: "/usr/local/go/src/runtime/time.go", Line: 368},
		{Func: "main.(*pipeline).worker", File: "/home/dev/leaky/pipeline.go", Line: 114},
	}

	groups, _ := buildGroups(dump.Profile{Records: []dump.Record{{Count: 1, Frames: frames}}})
	e := endpointOf(frames)

	g, ok := groups[e.id]
	if !ok || g.WaitSite != e.site || g.Entry != e.entry {
		t.Fatalf("endpoint %+v, groups %+v", e, groups)
	}
}

func TestEdgesSpawnResolveMergeAndOrder(t *testing.T) {
	groups, _ := buildGroups(readProfile(t, "stacks.txt"))

	var worker, handler endpoint

	for _, g := range groups {
		switch g.Entry {
		case "main.(*pipeline).worker":
			worker = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		case "net/http.(*conn).serve":
			handler = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		}
	}

	stale := endpoint{id: "gone", site: worker.site, entry: "main.renamed", calls: "\x00main.(*pipeline).worker\x00main.renamed\x00"}
	unknown := endpoint{id: "none", site: "main.none none.go:1", entry: "main.none"}

	c := &collector{
		trace: traceState{at: t0, d: traceDefault, edges: []traceEdge{
			{from: worker, to: handler, cause: CauseGoroutine, count: 1, waiters: 1, wait: time.Second},
		}},
		full: fullState{spawns: map[[2]endpoint]int{
			{handler, worker}: 3,
			{handler, stale}:  2,
			{worker, worker}:  1,
			{unknown, worker}: 4,
			{worker, unknown}: 5,
		}},
	}

	want := []Edge{
		{From: worker.id, To: handler.id, Kind: EdgeUnblock, Count: 1, Waiters: 1, Wait: time.Second},
		{From: handler.id, To: worker.id, Kind: EdgeSpawn, Count: 5, Approx: true},
		{From: worker.id, To: worker.id, Kind: EdgeSpawn, Count: 1},
	}

	if got := c.edgesLocked(newGroupIndex(groups)); !slices.Equal(got, want) {
		t.Fatalf("edges\n%+v\nwant\n%+v", got, want)
	}
}

func TestEdgesSpawnOnly(t *testing.T) {
	groups, _ := buildGroups(readProfile(t, "stacks.txt"))

	var worker, handler endpoint

	for _, g := range groups {
		switch g.Entry {
		case "main.(*pipeline).worker":
			worker = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		case "net/http.(*conn).serve":
			handler = endpoint{id: g.ID, site: g.WaitSite, entry: g.Entry}
		}
	}

	c := &collector{full: fullState{spawns: map[[2]endpoint]int{{handler, worker}: 3}}}

	want := []Edge{{From: handler.id, To: worker.id, Kind: EdgeSpawn, Count: 3}}

	if got := c.edgesLocked(newGroupIndex(groups)); !slices.Equal(got, want) {
		t.Fatalf("edges %+v, want %+v", got, want)
	}
}

func TestCaptureTraceCountsUnknownAndUnwoken(t *testing.T) {
	c := traceCollector(t,
		Wakeup{Cause: CauseGoroutine, Count: 7, Waiters: 3},
		Wakeup{Cause: CauseTimer, Count: 2, Waiters: 2},
		Wakeup{Cause: CauseNone, Waiter: workerStack, Count: 8, Waiters: 8, Wait: 40 * time.Second},
		Wakeup{Cause: CauseNetwork, Waiter: handlerStack, Count: 1, Waiters: 1, Wait: time.Second},
	)

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	worker, handler := groupIDs(t, s)
	w, _ := groupByEntry(s, "main.(*pipeline).worker")
	h, _ := groupByEntry(s, "net/http.(*conn).serve")

	if s.TraceUnknown != 9 || w.Unwoken != 8 || h.Unwoken != 0 {
		t.Fatalf("traceUnknown %d, unwoken worker %d handler %d; want 9, 8, 0", s.TraceUnknown, w.Unwoken, h.Unwoken)
	}

	want := []Edge{{From: CauseNetwork, To: handler, Kind: EdgeUnblock, Count: 1, Waiters: 1, Wait: time.Second}}
	if !slices.Equal(s.Edges, want) {
		t.Fatalf("edges %+v, want %+v: goroutines nobody woke are not an edge (worker %s)", s.Edges, want, worker)
	}
}

func TestShortTraceLastsUntilTheDump(t *testing.T) {
	for name, tc := range map[string]struct {
		cost   time.Duration
		recent bool
		ask    time.Duration
		want   time.Duration
	}{
		"slow dump":   {1500 * time.Millisecond, false, time.Second, 3 * time.Second},
		"fast dump":   {100 * time.Millisecond, false, time.Second, time.Second},
		"long enough": {1500 * time.Millisecond, false, 5 * time.Second, 5 * time.Second},
		"no dump":     {1500 * time.Millisecond, true, time.Second, time.Second},
		"huge dump":   {time.Minute, false, time.Second, traceMax},
	} {
		src := newFake(t, "stacks.txt", "full.txt", "")
		c, clk := newTestCollector(t, src)
		c.reader = &fakeReader{}
		c.sample(context.Background(), false)

		clk.set(t0.Add(time.Minute))

		c.mu.Lock()
		c.full.cost = tc.cost
		if tc.recent {
			c.fullTry = attempt{at: c.now()}
		}
		c.mu.Unlock()

		s, err := c.captureTrace(context.Background(), tc.ask)
		if err != nil {
			t.Fatal(err)
		}

		src.mu.Lock()
		got := src.traceD
		src.mu.Unlock()

		if got != tc.want || s.TraceFor != tc.want {
			t.Errorf("%s: recorded %v, TraceFor %v, want %v", name, got, s.TraceFor, tc.want)
		}
	}
}
