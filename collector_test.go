package ruvo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
)

func groupByEntry(s Snapshot, entry string) (Group, bool) {
	for _, g := range s.Groups {
		if g.Entry == entry {
			return g, true
		}
	}

	return Group{}, false
}

func TestSampleBuildsGroups(t *testing.T) {
	c, _ := newTestCollector(t, newFake(t, "stacks.txt", "", ""))

	s := c.sample(context.Background(), false)

	if s.Goroutines != 26 || len(s.Groups) != 3 || s.Groups[0].Count != 17 {
		t.Fatalf("goroutines %d, %d groups, first %+v", s.Goroutines, len(s.Groups), s.Groups[0])
	}

	if s.Edges == nil || len(s.Edges) != 0 || s.Target != "fake" || s.Error != "" {
		t.Fatalf("edges %v, target %q, error %q", s.Edges, s.Target, s.Error)
	}
}

func TestSampleMergesFullDump(t *testing.T) {
	c, _ := newTestCollector(t, newFake(t, "stacks.txt", "full.txt", ""))

	s := c.sample(context.Background(), true)

	g, ok := groupByEntry(s, "main.(*pipeline).worker")
	if !ok || g.State != "sleep" || g.CreatedBy != "main.newPipeline pipeline.go:64" || !g.InFullDump {
		t.Fatalf("worker group %+v (found %v)", g, ok)
	}

	if h, ok := groupByEntry(s, "net/http.(*conn).serve"); !ok || h.InFullDump {
		t.Fatalf("handler group %+v (found %v): it is not in the full dump", h, ok)
	}

	if !s.FullAt.Equal(t0) {
		t.Fatalf("fullAt %v, want %v", s.FullAt, t0)
	}
}

func TestCaptureLeaksMarksGroups(t *testing.T) {
	c, _ := newTestCollector(t, newFake(t, "leak.txt", "", "leak.txt"))
	c.sample(context.Background(), false)

	s, err := c.captureLeaks(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	g, ok := groupByEntry(s, "main.notify")
	if !ok || g.Count != 1390 || g.Leaked != 1390 || !s.LeaksAt.Equal(t0) {
		t.Fatalf("notify group %+v (found %v), leaksAt %v", g, ok, s.LeaksAt)
	}
}

func TestCaptureLeaksUnavailable(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	src.leaksErr = fmt.Errorf("%w: GET …: 404 Not Found", collect.ErrNoLeakProfile)

	c, _ := newTestCollector(t, src)
	c.sample(context.Background(), false)

	if _, err := c.captureLeaks(context.Background()); !errors.Is(err, collect.ErrNoLeakProfile) {
		t.Fatalf("err %v, want ErrNoLeakProfile", err)
	}

	if s := c.snapshot(context.Background()); !s.LeaksAt.IsZero() {
		t.Fatalf("leaksAt %v, want zero", s.LeaksAt)
	}
}

func TestSampleErrorKeepsGroups(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	c, _ := newTestCollector(t, src)
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })

	s := c.sample(context.Background(), false)
	if !strings.Contains(s.Error, "connection refused") || len(s.Groups) != 3 {
		t.Fatalf("error %q, %d groups", s.Error, len(s.Groups))
	}

	src.set(func(f *fakeSource) { f.err = nil })

	if s := c.sample(context.Background(), false); s.Error != "" {
		t.Fatalf("error %q after recovery", s.Error)
	}
}

func TestGrowthAcrossSamples(t *testing.T) {
	src := &fakeSource{debug1: stacksText(10)}
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.debug1 = stacksText(70) })

	s := c.sample(context.Background(), false)
	if g, ok := groupByEntry(s, "main.x"); !ok || g.Growth != 60 {
		t.Fatalf("group %+v (found %v), want growth 60", g, ok)
	}
}

func TestUnsubscribe(t *testing.T) {
	c, _ := newTestCollector(t, newFake(t, "stacks.txt", "", ""))

	ch, cancel := c.subscribe()
	s := c.sample(context.Background(), false)

	select {
	case got := <-ch:
		if got.s.Goroutines != s.Goroutines {
			t.Fatalf("got %d goroutines, want %d", got.s.Goroutines, s.Goroutines)
		}
	case <-time.After(time.Second):
		t.Fatal("no snapshot delivered to the subscriber")
	}

	cancel()

	if n := c.subscribers(); n != 0 {
		t.Fatalf("%d subscribers after cancel", n)
	}
}

func TestIdle(t *testing.T) {
	c, clk := newTestCollector(t, newFake(t, "stacks.txt", "", ""))

	c.snapshot(context.Background())

	if c.idle(t0.Add(9*time.Minute)) || !c.idle(t0.Add(11*time.Minute)) {
		t.Fatal("idle should flip after ten minutes without activity")
	}

	clk.set(t0)

	_, cancel := c.subscribe()
	defer cancel()

	if c.idle(t0.Add(11 * time.Minute)) {
		t.Fatal("a subscriber keeps the collector busy")
	}
}

func TestRestartDetectedByGoroutineIDs(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(3 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.debug2 = []byte("goroutine 3 [sleep]:\ntime.Sleep(0x1)\n\t/usr/local/go/src/runtime/time.go:368 +0x1\nmain.(*pipeline).worker(0x1)\n\t/home/dev/leaky/pipeline.go:114 +0x1\ncreated by main.newPipeline in goroutine 1\n\t/home/dev/leaky/pipeline.go:64 +0x1\n")
	})

	s := c.sample(context.Background(), false)

	if !s.LeaksAt.IsZero() || !s.FullAt.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("leaksAt %v, fullAt %v: a restarted target must start from scratch", s.LeaksAt, s.FullAt)
	}
}

func TestTransientFailureKeepsState(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("timeout") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) { f.err = nil })

	if s := c.sample(context.Background(), false); !s.LeaksAt.Equal(t0) {
		t.Fatalf("leaksAt %v, want %v: one failed read must not wipe the state", s.LeaksAt, t0)
	}
}

func TestFirstManualFullDumpCollectsOnce(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)

	if _, err := c.captureFull(context.Background()); err != nil {
		t.Fatal(err)
	}

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for the first press, want 1", n)
	}
}

func TestManualAndAutomaticFullDumpsShareOneCollection(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 50 * time.Millisecond })

	var wg sync.WaitGroup

	wg.Go(func() { c.sample(context.Background(), true) })
	wg.Go(func() { c.captureFull(context.Background()) })
	wg.Wait()

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for an automatic and a manual dump at once, want 1", n)
	}
}

func TestSnapshotRefreshesStaleData(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)

	clk.set(t0.Add(5 * time.Minute))

	if s := c.snapshot(context.Background()); !s.At.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("snapshot at %v, want a fresh one at %v", s.At, t0.Add(5*time.Minute))
	}
}

func TestFirstSampleSkipsFullDumpForLargeTargets(t *testing.T) {
	src := &fakeSource{debug1: stacksText(20_000), debug2: testdata(t, "full.txt")}
	c, _ := newTestCollector(t, src)

	c.snapshot(context.Background())

	if n := src.count("debug2"); n != 0 {
		t.Fatalf("%d full dumps on first open of a 20k-goroutine target, want 0", n)
	}
}

func TestDeepStackMatchesBySite(t *testing.T) {
	src := &fakeSource{
		debug1: []byte("goroutine profile: total 1\n1 @ 0x1\n#\t0x1\tmain.site+0x1\t/home/dev/a.go:7\n#\t0x1\tmain.cut+0x1\t/home/dev/a.go:9\n\n"),
		debug2: []byte("goroutine 5 [select]:\nmain.site()\n\t/home/dev/a.go:7 +0x1\nmain.root()\n\t/home/dev/a.go:20 +0x1\n"),
	}
	c, _ := newTestCollector(t, src)

	s := c.sample(context.Background(), true)
	if len(s.Groups) != 1 || s.Groups[0].State != "select" || !s.Groups[0].InFullDump {
		t.Fatalf("groups %+v: a stack cut by debug=1 must still match its full-dump goroutine by wait site", s.Groups)
	}
}

func TestFullDumpFailureKeepsStacks(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.err2 = errors.New("503 Service Unavailable")

	c, _ := newTestCollector(t, src)

	s := c.sample(context.Background(), true)
	if len(s.Groups) != 3 || !strings.Contains(s.Error, "full dump") {
		t.Fatalf("%d groups, error %q", len(s.Groups), s.Error)
	}

	c.mu.Lock()
	next, down := c.nextFull, c.down
	c.mu.Unlock()

	if !next.After(t0) || down {
		t.Fatalf("nextFull %v (want after %v), down %v: a failed full dump must not be retried at once or reset state", next, t0, down)
	}
}

func TestConcurrentFullDumpsShareOneRun(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 50 * time.Millisecond })

	var wg sync.WaitGroup

	for range 3 {
		wg.Go(func() { c.captureFull(context.Background()) })
	}

	wg.Wait()

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for 3 simultaneous presses, want 1", n)
	}
}

func TestFullDumpCooldown(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)
	c.captureFull(context.Background())

	clk.set(t0.Add(5 * time.Second))
	c.captureFull(context.Background())

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps within 5s, want 1", n)
	}

	clk.set(t0.Add(11 * time.Second))
	c.captureFull(context.Background())

	if n := src.count("debug2"); n != 2 {
		t.Fatalf("%d full dumps after the cooldown, want 2", n)
	}
}

func TestConcurrentFirstOpensShareOneSample(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.delay = 50 * time.Millisecond

	c, _ := newTestCollector(t, src)

	var wg sync.WaitGroup

	for range 4 {
		wg.Go(func() { c.snapshot(context.Background()) })
	}

	wg.Wait()

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for 4 simultaneous first opens, want 1", n)
	}
}
