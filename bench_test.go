package ruvo

import (
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

				gs, err := dump.ParseFull(raw)
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
		c.edgesLocked(newGroupIndex(gs))
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
		c.edgesLocked(newGroupIndex(gs))
	}

	b.ReportMetric(groups, "groups")
	b.ReportMetric(float64(len(spawns)), "edges")
}

func manyStacks(groups, depth int) []byte {
	var b []byte

	b = fmt.Appendf(b, "goroutine profile: total %d\n", groups*3)

	for i := range groups {
		b = fmt.Appendf(b, "3 @ 0x1\n#\t0x1\tinternal/poll.runtime_pollWait+0x1\t/usr/local/go/src/runtime/netpoll.go:351\n")

		for d := range depth {
			b = fmt.Appendf(b, "#\t0x1\tgithub.com/acme/service/internal/layer%d.(*Handler).Step%d+0x1\t/home/dev/service/internal/layer%d/handler.go:%d\n", d, i, d, 10+d)
		}

		b = append(b, '\n')
	}

	return b
}

func BenchmarkPublish(b *testing.B) {
	for _, groups := range []int{100, 1000} {
		raw := manyStacks(groups, 20)

		b.Run(fmt.Sprintf("%dgroups", groups), func(b *testing.B) {
			c := newCollector(collect.Local())
			defer c.close()

			at := t0
			c.now = func() time.Time {
				at = at.Add(frequentMin)

				return at
			}

			_, cancel := c.subscribe()
			defer cancel()

			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()

			for b.Loop() {
				c.pmu.Lock()
				p, err := c.strs.ParseStacks(raw)
				c.pmu.Unlock()

				if err != nil {
					b.Fatal(err)
				}

				c.mu.Lock()
				c.profile = p
				c.publishLocked(c.now(), true, "")
				c.mu.Unlock()
			}
		})
	}
}

func BenchmarkExport(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("%dk", n/1000), func(b *testing.B) {
			release := park(n)
			defer release()

			c := newCollector(collect.Local())
			defer c.close()

			ctx := context.Background()
			c.sample(ctx, false)

			if _, err := c.captureFull(ctx); err != nil {
				b.Fatal(err)
			}

			s := c.session(ctx)
			size := 0

			for _, f := range s.info.Files {
				size += len(f.data)
			}

			b.SetBytes(int64(size))
			b.ReportAllocs()

			var out int64

			for b.Loop() {
				w := &counter{}

				p, err := pack(s)
				if err != nil {
					b.Fatal(err)
				}

				if err := p.write(w); err != nil {
					b.Fatal(err)
				}

				out = w.n
			}

			b.ReportMetric(float64(size)/(1<<20), "raw-MB")
			b.ReportMetric(float64(out)/(1<<20), "zip-MB")
		})
	}
}

type counter struct {
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	c.n += int64(len(p))

	return len(p), nil
}

func BenchmarkEncode(b *testing.B) {
	c := newCollector(collect.Local())
	defer c.close()

	p, err := dump.ParseStacks(manyStacks(1000, 20))
	if err != nil {
		b.Fatal(err)
	}

	c.mu.Lock()
	c.profile = p
	s := c.publishLocked(t0, false, "")
	c.mu.Unlock()

	for name, enc := range map[string]func(message) []byte{"full": message.full, "lean": message.lean} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			var size int

			for b.Loop() {
				size = len(enc(newMessage(s, s)))
			}

			b.ReportMetric(float64(size)/1024, "KB/event")
		})
	}
}
