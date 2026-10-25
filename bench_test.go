package ruvo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

var benchSizes = []int{1_000, 10_000, 100_000}

func park(n int) func() {
	ch := make(chan struct{})

	var wg sync.WaitGroup

	for range n {
		wg.Add(1)

		go func() {
			wg.Done()
			<-ch
		}()
	}

	wg.Wait()

	return func() { close(ch) }
}

func BenchmarkSample(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("debug1/%dk", n/1000), func(b *testing.B) {
			release := park(n)
			defer release()

			c := newCollector(collect.Local())
			defer c.close()

			ctx := context.Background()
			b.ReportAllocs()

			for b.Loop() {
				c.sample(ctx, false)
			}

			b.ReportMetric(float64(n), "goroutines")
		})
	}

	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("debug2/%dk", n/1000), func(b *testing.B) {
			release := park(n)
			defer release()

			src := collect.Local()
			c := newCollector(src)
			defer c.close()

			ctx := context.Background()
			b.ReportAllocs()

			var walk time.Duration

			for b.Loop() {
				start := time.Now()

				raw, err := src.Goroutines(ctx, 2)
				if err != nil {
					b.Fatal(err)
				}

				walk += time.Since(start)

				gs, err := dump.ParseFull(bytes.NewReader(raw))
				if err != nil {
					b.Fatal(err)
				}

				c.mu.Lock()
				c.applyFullLocked(c.now(), fullDump{gs: gs})
				c.mu.Unlock()
			}

			b.ReportMetric(float64(walk.Microseconds())/1000/float64(b.N), "walk-ms/op")
			b.ReportMetric(float64(n), "goroutines")
		})
	}
}

func BenchmarkGrowth(b *testing.B) {
	const groups = 1000

	ids := make([]string, groups)
	for i := range ids {
		ids[i] = fmt.Sprintf("group-%d", i)
	}

	h := history{window: historyWindow}

	for i := range 60 {
		counts := make(map[string]int, groups)
		for j, id := range ids {
			counts[id] = j + i
		}

		h.add(t0.Add(time.Duration(i)*2*time.Second), counts)
	}

	now := t0.Add(2 * time.Minute)

	b.ReportAllocs()

	for b.Loop() {
		for j, id := range ids {
			h.growth(id, now, j+60)
		}
	}

	b.ReportMetric(groups, "groups")
}

func edgeFixture(n int) (map[string]*Group, []endpoint) {
	gs := make(map[string]*Group, n)
	eps := make([]endpoint, n)

	for i := range n {
		e := endpointOf([]dump.Frame{{Func: fmt.Sprintf("main.f%d", i), File: "/home/dev/a.go", Line: i + 1}})
		gs[e.id] = &Group{ID: e.id, WaitSite: e.site, Entry: e.entry, Count: i + 1}

		if i%2 == 1 {
			e.id = "gone"
		}

		eps[i] = e
	}

	return gs, eps
}

func BenchmarkEdges(b *testing.B) {
	const groups = 1000

	gs, eps := edgeFixture(groups)

	edges := make([]traceEdge, groups)
	for i := range edges {
		edges[i] = traceEdge{from: eps[i], to: eps[(i*7+1)%groups], cause: CauseGoroutine, count: i + 1, waiters: 1, wait: time.Duration(i) * time.Millisecond}
	}

	c := &collector{trace: traceState{at: t0, d: traceDefault, edges: edges}}

	b.ReportAllocs()

	for b.Loop() {
		c.edgesLocked(gs)
	}

	b.ReportMetric(groups, "groups")
	b.ReportMetric(float64(len(edges)), "edges")
}

func BenchmarkSpawnEdges(b *testing.B) {
	const groups = 1000

	gs, eps := edgeFixture(groups)
	spawns := make(map[[2]endpoint]int, groups)

	for i := range groups {
		spawns[[2]endpoint{eps[i], eps[(i*7+1)%groups]}] = i + 1
	}

	c := &collector{full: fullState{spawns: spawns}}

	b.ReportAllocs()

	for b.Loop() {
		c.edgesLocked(gs)
	}

	b.ReportMetric(groups, "groups")
	b.ReportMetric(float64(len(spawns)), "edges")
}
