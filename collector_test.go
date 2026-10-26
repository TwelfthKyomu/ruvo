package ruvo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
	"github.com/TwelfthKyomu/ruvo/internal/dump"
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

var restarted = []byte("goroutine 3 [sleep]:\ntime.Sleep(0x1)\n\t/usr/local/go/src/runtime/time.go:368 +0x1\nmain.(*pipeline).worker(0x1)\n\t/home/dev/leaky/pipeline.go:114 +0x1\ncreated by main.newPipeline in goroutine 1\n\t/home/dev/leaky/pipeline.go:64 +0x1\n")

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
		f.debug2 = restarted
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

func TestRestartDetectedAfterFailedFullDump(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.err2 = errors.New("503 Service Unavailable")
	})

	if s := c.sample(context.Background(), false); !s.LeaksAt.Equal(t0) {
		t.Fatalf("leaksAt %v, want %v: a failed full dump proves no restart", s.LeaksAt, t0)
	}

	now := t0.Add(2*time.Minute + fullMax)
	clk.set(now)
	src.set(func(f *fakeSource) {
		f.err2 = nil
		f.debug2 = restarted
	})

	s := c.sample(context.Background(), false)

	if !s.LeaksAt.IsZero() || !s.FullAt.Equal(now) {
		t.Fatalf("leaksAt %v, fullAt %v: the restart check must survive a failed full dump", s.LeaksAt, s.FullAt)
	}
}

func TestRecoveryRetriesFullDumpOnSchedule(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.err2 = errors.New("503 Service Unavailable")
	})
	c.sample(context.Background(), false)

	c.mu.Lock()
	next := c.nextFull
	c.mu.Unlock()

	clk.set(next.Add(-time.Second))
	c.sample(context.Background(), false)

	if n := src.count("debug2"); n != 2 {
		t.Fatalf("%d full dumps before nextFull %v, want 2: a failed restart check is retried on schedule, not at once", n, next)
	}

	clk.set(next)
	c.sample(context.Background(), false)

	if n := src.count("debug2"); n != 3 {
		t.Fatalf("%d full dumps at nextFull %v, want 3: the restart check is retried on schedule", n, next)
	}
}

func TestLargeTargetWithoutBaselineDumpsOnSchedule(t *testing.T) {
	src := &fakeSource{debug1: stacksText(firstFullLimit + 1), debug2: testdata(t, "full.txt"), err2: errors.New("503 Service Unavailable")}
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)

	clk.set(t0.Add(30 * time.Second))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(40 * time.Second))
	src.set(func(f *fakeSource) { f.err = nil })
	c.sample(context.Background(), false)

	if n := src.count("debug2"); n != 0 {
		t.Fatalf("%d full dumps after the first open and a recovery with no baseline, want 0", n)
	}

	c.mu.Lock()
	next := c.nextFull
	c.mu.Unlock()

	clk.set(next)
	c.sample(context.Background(), true)

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps at nextFull %v, want 1: only the first open skips the dump", n, next)
	}
}

func TestButtonGapHoldsDuringRestartCheck(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.err2 = errors.New("503 Service Unavailable")
	})
	c.sample(context.Background(), false)

	clk.set(t0.Add(2*time.Minute + 5*time.Second))

	if _, err := c.captureFull(context.Background()); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err %v, want the last attempt's 503", err)
	}

	if n := src.count("debug2"); n != 2 {
		t.Fatalf("%d full dumps, want 2: a press 5s after a failed attempt must not reach the target", n)
	}

	c.mu.Lock()
	next := c.nextFull
	c.mu.Unlock()

	clk.set(next)
	src.set(func(f *fakeSource) {
		f.err2 = nil
		f.debug2 = restarted
	})

	if s := c.sample(context.Background(), false); !s.LeaksAt.IsZero() || !s.FullAt.Equal(next) {
		t.Fatalf("leaksAt %v, fullAt %v: the restart check runs on schedule", s.LeaksAt, s.FullAt)
	}
}

func TestRecoveryAfterRecentAttemptWaitsForSchedule(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	if _, err := c.captureFull(context.Background()); err == nil {
		t.Fatal("a full dump of a target that is down succeeded")
	}

	clk.set(t0.Add(time.Minute + 5*time.Second))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.debug2 = restarted
	})

	if s := c.sample(context.Background(), false); s.LeaksAt.IsZero() || !s.FullAt.Equal(t0) {
		t.Fatalf("leaksAt %v, fullAt %v: a recovery within fullMin of a full-dump attempt must wait for the schedule", s.LeaksAt, s.FullAt)
	}

	if n := src.count("debug2"); n != 2 {
		t.Fatalf("%d full dumps, want 2: the recovery must not reach the target", n)
	}

	c.mu.Lock()
	next := c.nextFull
	c.mu.Unlock()

	clk.set(next)

	if s := c.sample(context.Background(), false); !s.LeaksAt.IsZero() || !s.FullAt.Equal(next) {
		t.Fatalf("leaksAt %v, fullAt %v: the restart check runs on schedule", s.LeaksAt, s.FullAt)
	}
}

func TestLoopAndStaleSnapshotShareOneFetch(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	src.delay = 200 * time.Millisecond

	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)

	clk.set(t0.Add(5 * time.Minute))

	var wg sync.WaitGroup

	wg.Go(func() { c.refresh(context.Background(), false) })
	wg.Go(func() { c.snapshot(context.Background()) })
	wg.Wait()

	if n := src.count("debug1"); n != 2 {
		t.Fatalf("%d stack reads, want 2: the loop and a stale snapshot must share one", n)
	}
}

func TestCanceledWaiterReturns(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.delay = 2 * time.Second

	c, _ := newTestCollector(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	c.snapshot(ctx)

	if d := time.Since(start); d >= 500*time.Millisecond {
		t.Fatalf("snapshot returned after %v: a caller that gave up must not wait for the read", d)
	}
}

func TestFirstFailedManualFullDumpCollectsOnce(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.err2 = errors.New("503 Service Unavailable")

	c, _ := newTestCollector(t, src)

	if _, err := c.captureFull(context.Background()); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err %v, want the 503", err)
	}

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for the first failed press, want 1", n)
	}
}

func TestFullDumpPressWhileDownKeepsReadError(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)

	clk.set(t0.Add(time.Second))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(5 * time.Second))

	if s, err := c.captureFull(context.Background()); err != nil || !strings.Contains(s.Error, "connection refused") {
		t.Fatalf("err %v, error %q: a press inside the gap must keep the read error", err, s.Error)
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
		debug2: []byte("goroutine 5 [select]:\nmain.site()\n\t/home/dev/a.go:7 +0x1\nmain.cut()\n\t/home/dev/a.go:9 +0x1\nmain.root()\n\t/home/dev/a.go:20 +0x1\n"),
	}
	c, _ := newTestCollector(t, src)

	s := c.sample(context.Background(), true)
	if len(s.Groups) != 1 || s.Groups[0].State != "select" || !s.Groups[0].InFullDump {
		t.Fatalf("groups %+v: a stack cut by debug=1 must still match its full-dump goroutine by wait site", s.Groups)
	}
}

func TestBySiteNeedsEntryInFullStack(t *testing.T) {
	src := &fakeSource{
		debug1: []byte("goroutine profile: total 1\n1 @ 0x1\n#\t0x1\tmain.site+0x1\t/home/dev/a.go:7\n#\t0x1\tmain.other+0x1\t/home/dev/a.go:30\n\n"),
		debug2: []byte("goroutine 5 [select]:\nmain.site()\n\t/home/dev/a.go:7 +0x1\nmain.root()\n\t/home/dev/a.go:20 +0x1\n"),
	}
	c, _ := newTestCollector(t, src)

	s := c.sample(context.Background(), true)
	if len(s.Groups) != 1 || s.Groups[0].State != "" || s.Groups[0].InFullDump {
		t.Fatalf("groups %+v: a group must not take the full-dump data of another entry at its wait site", s.Groups)
	}
}

func TestFullCountOfCutDump(t *testing.T) {
	src := &fakeSource{debug1: stacksText(3), debug2: []byte("goroutine 5 [select]:\nmain.x()\n\t/home/dev/a.go:7 +0x1\n")}
	c, _ := newTestCollector(t, src)
	c.fullCap = len(src.debug2)

	s := c.sample(context.Background(), true)
	if s.Goroutines != 3 || s.FullCount != 1 || !s.FullCut {
		t.Fatalf("goroutines %d, fullCount %d, fullCut %v, want 3, 1, true", s.Goroutines, s.FullCount, s.FullCut)
	}
}

func TestFullDumpLargerThanLocalCapIsWhole(t *testing.T) {
	src := &fakeSource{debug1: stacksText(1), debug2: []byte("goroutine 5 [select]:\nmain.x()\n\t/home/dev/a.go:7 +0x1\n")}
	c, _ := newTestCollector(t, src)
	c.fullCap = len(src.debug2) - 10

	if s := c.sample(context.Background(), true); s.FullCut || s.FullCount != 1 {
		t.Fatalf("fullCut %v, fullCount %d: a remote body longer than the local buffer was not cut", s.FullCut, s.FullCount)
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

func TestFullDumpGapAfterFailure(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.err2 = errors.New("503 Service Unavailable")

	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)

	for _, d := range []time.Duration{0, 5 * time.Second} {
		clk.set(t0.Add(d))

		if _, err := c.captureFull(context.Background()); err == nil || !strings.Contains(err.Error(), "503") {
			t.Fatalf("press at +%v: err %v, want the 503", d, err)
		}
	}

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps within 5s of a failed one, want 1", n)
	}

	clk.set(t0.Add(11 * time.Second))
	c.captureFull(context.Background())

	if n := src.count("debug2"); n != 2 {
		t.Fatalf("%d full dumps after the gap, want 2", n)
	}
}

func TestLeaksGapAfterFailure(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	src.leaksErr = fmt.Errorf("%w: GET …: 404 Not Found", collect.ErrNoLeakProfile)

	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), false)

	for _, d := range []time.Duration{0, 5 * time.Second} {
		clk.set(t0.Add(d))

		if _, err := c.captureLeaks(context.Background()); !errors.Is(err, collect.ErrNoLeakProfile) {
			t.Fatalf("press at +%v: err %v, want ErrNoLeakProfile", d, err)
		}
	}

	if n := src.count("leaks"); n != 1 {
		t.Fatalf("%d leak profiles within 5s of a failed one, want 1", n)
	}

	clk.set(t0.Add(11 * time.Second))
	c.captureLeaks(context.Background())

	if n := src.count("leaks"); n != 2 {
		t.Fatalf("%d leak profiles after the gap, want 2", n)
	}
}

func TestSlowFullDumpOutlivesSampleDeadline(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.delay = 100 * time.Millisecond

	c, _ := newTestCollector(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if s := c.sample(ctx, true); s.Error != "" || !s.FullAt.Equal(t0) {
		t.Fatalf("error %q, fullAt %v: a full dump that is still running must not be reported as failed", s.Error, s.FullAt)
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

func TestCaptureTracePassesFullDumpStacks(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.trace = []byte("raw trace")

	c, _ := newTestCollector(t, src)
	r := &fakeReader{}
	c.reader = r
	c.sample(context.Background(), false)

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	if string(r.raw) != "raw trace" {
		t.Fatalf("reader got %q, want the recorded trace", r.raw)
	}

	if len(r.stacks) != 5 || !slices.Equal(r.stacks[9], workerStack) {
		t.Fatalf("%d stacks, goroutine 9: %+v", len(r.stacks), r.stacks[9])
	}

	if n := src.count("debug2"); n != 1 || !s.FullAt.Equal(t0) {
		t.Fatalf("%d full dumps, fullAt %v: the dump taken for the trace also updates the list", n, s.FullAt)
	}
}

func TestCaptureTraceWithoutFullDump(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.err2 = errors.New("503 Service Unavailable")

	c, _ := newTestCollector(t, src)
	r := &fakeReader{wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.reader = r
	c.sample(context.Background(), false)

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil || len(s.Edges) != 1 {
		t.Fatalf("err %v, edges %+v: a failed full dump must not fail the trace", err, s.Edges)
	}

	if r.stacks != nil {
		t.Fatalf("stacks %v, want nil when the full dump failed", r.stacks)
	}

	if _, err := c.captureFull(context.Background()); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err %v: the dump taken for the trace counts as the last full dump attempt", err)
	}

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps, want 1", n)
	}
}

func TestTraceFailureSkipsFullDump(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.traceErr = fmt.Errorf("%w: GET …: 404 Not Found", collect.ErrNoTrace)

	c, _ := newTestCollector(t, src)
	c.reader = &fakeReader{}
	c.sample(context.Background(), false)

	if _, err := c.captureTrace(context.Background(), traceDefault); !errors.Is(err, collect.ErrNoTrace) {
		t.Fatalf("err %v, want ErrNoTrace", err)
	}

	if n := src.count("debug2"); n != 0 {
		t.Fatalf("%d full dumps for a trace that failed at once, want 0", n)
	}
}

func TestCaptureTraceReadError(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.reader = &fakeReader{err: errors.New("bad header")}
	c.sample(context.Background(), false)

	if _, err := c.captureTrace(context.Background(), traceDefault); err == nil || err.Error() != "read trace: bad header" {
		t.Fatalf("err %v, want read trace: bad header", err)
	}

	if s := c.lastSnapshot(); !s.TraceAt.IsZero() || len(s.Edges) != 0 {
		t.Fatalf("traceAt %v, edges %+v after a failed read", s.TraceAt, s.Edges)
	}
}

func TestCaptureTraceSharedAndCooldown(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.reader = &fakeReader{}
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 50 * time.Millisecond })

	var wg sync.WaitGroup

	for range 3 {
		wg.Go(func() { c.captureTrace(context.Background(), traceDefault) })
	}

	wg.Wait()

	if n := src.count("trace"); n != 1 {
		t.Fatalf("%d traces for 3 simultaneous presses, want 1", n)
	}

	clk.set(t0.Add(5 * time.Second))

	if _, err := c.captureTrace(context.Background(), traceDefault); err != nil {
		t.Fatal(err)
	}

	if n := src.count("trace"); n != 1 {
		t.Fatalf("%d traces within 5s, want 1", n)
	}

	clk.set(t0.Add(11 * time.Second))
	c.captureTrace(context.Background(), traceDefault)

	if n := src.count("trace"); n != 2 {
		t.Fatalf("%d traces after the cooldown, want 2", n)
	}
}

func TestTraceGapAfterFailure(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.traceErr = errors.New("503 Service Unavailable")

	c, clk := newTestCollector(t, src)
	c.reader = &fakeReader{}
	c.sample(context.Background(), false)

	for _, d := range []time.Duration{0, 5 * time.Second} {
		clk.set(t0.Add(d))

		if _, err := c.captureTrace(context.Background(), traceDefault); err == nil || !strings.Contains(err.Error(), "503") {
			t.Fatalf("press at +%v: err %v, want the 503", d, err)
		}
	}

	if n := src.count("trace"); n != 1 {
		t.Fatalf("%d traces within 5s of a failed one, want 1", n)
	}

	clk.set(t0.Add(11 * time.Second))
	c.captureTrace(context.Background(), traceDefault)

	if n := src.count("trace"); n != 2 {
		t.Fatalf("%d traces after the gap, want 2", n)
	}
}

func TestTraceSurvivesCanceledRequest(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.reader = &fakeReader{wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 300 * time.Millisecond })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, err := c.captureTrace(ctx, traceDefault); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want the request's deadline", err)
	}

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil || len(s.Edges) != 1 {
		t.Fatalf("err %v, edges %+v: the recording must outlive the request that started it", err, s.Edges)
	}

	if n := src.count("trace"); n != 1 {
		t.Fatalf("%d traces, want 1", n)
	}
}

func TestRestartClearsTrace(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.reader = &fakeReader{wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.sample(context.Background(), true)

	if s, err := c.captureTrace(context.Background(), traceDefault); err != nil || len(s.Edges) != 1 {
		t.Fatalf("err %v, edges %+v", err, s.Edges)
	}

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.debug2 = restarted
	})

	s := c.sample(context.Background(), false)

	if !s.FullAt.Equal(t0.Add(2*time.Minute)) || !s.TraceAt.IsZero() || s.TraceFor != 0 || s.Edges == nil || len(s.Edges) != 0 {
		t.Fatalf("fullAt %v, traceAt %v, traceFor %v, edges %+v: a restarted target has no trace", s.FullAt, s.TraceAt, s.TraceFor, s.Edges)
	}
}

func TestTraceReaderPanicIsAnError(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	r := &fakeReader{crash: "boom", wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.reader = r
	c.sample(context.Background(), false)

	if _, err := c.captureTrace(context.Background(), traceDefault); err == nil || err.Error() != "read trace: panic: boom" {
		t.Fatalf("err %v, want read trace: panic: boom", err)
	}

	r.crash = nil
	clk.set(t0.Add(11 * time.Second))

	if s, err := c.captureTrace(context.Background(), traceDefault); err != nil || len(s.Edges) != 1 {
		t.Fatalf("err %v, edges %+v: a press after the gap must work", err, s.Edges)
	}
}

func TestTraceSkipsDumpInsideFullGap(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	r := &fakeReader{stacks: map[int64][]Frame{}}
	c.reader = r
	c.sample(context.Background(), false)

	if _, err := c.captureFull(context.Background()); err != nil {
		t.Fatal(err)
	}

	clk.set(t0.Add(5 * time.Second))

	if _, err := c.captureTrace(context.Background(), traceDefault); err != nil {
		t.Fatal(err)
	}

	if n := src.count("debug2"); n != 1 || r.stacks != nil {
		t.Fatalf("%d full dumps, stacks %v: a trace 5s after a full dump takes none of its own", n, r.stacks)
	}
}

func TestTraceSharesFullDumpInProgress(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.reader = &fakeReader{}
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 100 * time.Millisecond })

	var (
		wg             sync.WaitGroup
		fullErr, trErr error
	)

	wg.Go(func() { _, fullErr = c.captureFull(context.Background()) })
	wg.Go(func() { _, trErr = c.captureTrace(context.Background(), traceDefault) })
	wg.Wait()

	if fullErr != nil || trErr != nil {
		t.Fatalf("full %v, trace %v", fullErr, trErr)
	}

	if n := src.count("debug2"); n != 1 {
		t.Fatalf("%d full dumps for a full and a trace press at once, want 1", n)
	}
}

func TestTraceStacksFromJoinedFullDump(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	r := &fakeReader{}
	c.reader = r
	c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.delay = 300 * time.Millisecond })

	var wg sync.WaitGroup

	wg.Go(func() { c.captureTrace(context.Background(), traceDefault) })
	time.Sleep(100 * time.Millisecond)
	wg.Go(func() { c.captureFull(context.Background()) })
	wg.Wait()

	if n := src.count("debug2"); n != 1 || len(r.stacks) != 5 {
		t.Fatalf("%d full dumps, %d stacks: a trace that joins a running full dump uses its goroutines", n, len(r.stacks))
	}
}

func TestTraceStacksFailureIsNoted(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, _ := newTestCollector(t, src)
	c.reader = &readerFunc{fn: func() { src.set(func(f *fakeSource) { f.err = errors.New("connection refused") }) }}
	c.sample(context.Background(), false)

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil || !strings.Contains(s.Error, "connection refused") || len(s.Groups) != 3 {
		t.Fatalf("err %v, error %q, %d groups: a failed final read keeps the groups and says why", err, s.Error, len(s.Groups))
	}
}

var (
	spawnStacks = []byte("goroutine profile: total 4\n1 @ 0x1\n#\t0x1\tmain.server+0x1\t/home/dev/a.go:10\n\n3 @ 0x1\n#\t0x1\tmain.worker+0x1\t/home/dev/a.go:20\n\n")
	spawnFull   = []byte("goroutine 1 [select]:\nmain.server()\n\t/home/dev/a.go:10 +0x1\n\n" +
		"goroutine 5 [chan receive]:\nmain.worker()\n\t/home/dev/a.go:20 +0x1\ncreated by main.server in goroutine 1\n\t/home/dev/a.go:12 +0x1\n\n" +
		"goroutine 6 [chan receive]:\nmain.worker()\n\t/home/dev/a.go:20 +0x1\ncreated by main.server in goroutine 1\n\t/home/dev/a.go:12 +0x1\n\n" +
		"goroutine 7 [chan receive]:\nmain.worker()\n\t/home/dev/a.go:20 +0x1\ncreated by main.elsewhere in goroutine 99\n\t/home/dev/b.go:5 +0x1\n")
)

func TestSnapshotHasSpawnEdges(t *testing.T) {
	c, _ := newTestCollector(t, &fakeSource{debug1: spawnStacks, debug2: spawnFull})

	s := c.sample(context.Background(), true)

	server, ok := groupByEntry(s, "main.server")
	if !ok {
		t.Fatal("no server group")
	}

	worker, ok := groupByEntry(s, "main.worker")
	if !ok {
		t.Fatal("no worker group")
	}

	want := []Edge{{From: server.ID, To: worker.ID, Kind: EdgeSpawn, Count: 2}}

	if !slices.Equal(s.Edges, want) {
		t.Fatalf("edges %+v, want %+v", s.Edges, want)
	}

	if s := c.sample(context.Background(), false); !slices.Equal(s.Edges, want) {
		t.Fatalf("next sample: edges %+v, want %+v: spawn edges outlive one snapshot", s.Edges, want)
	}
}

func TestSpawnEdgesFollowLatestFullDump(t *testing.T) {
	src := &fakeSource{debug1: spawnStacks, debug2: spawnFull}
	c, clk := newTestCollector(t, src)
	c.sample(context.Background(), true)

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.debug2 = restarted })

	if s := c.sample(context.Background(), true); len(s.Edges) != 0 || !s.FullAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("edges %+v, fullAt %v, want no edges from the new full dump", s.Edges, s.FullAt)
	}
}

func TestEmptyFullKeepsLastGoodDump(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)

	before := c.sample(context.Background(), true)
	if before.FullCount == 0 {
		t.Fatal("no full dump to begin with")
	}

	clk.set(t0.Add(time.Minute))
	src.set(func(f *fakeSource) { f.debug2 = []byte{} })

	if _, err := c.captureFull(context.Background()); err == nil {
		t.Fatal("an empty debug=2 body was taken for a dump")
	}

	s := c.lastSnapshot()
	if s.FullCount != before.FullCount || !s.FullAt.Equal(t0) {
		t.Fatalf("fullCount %d (was %d), fullAt %v: a failed dump must keep the last good one", s.FullCount, before.FullCount, s.FullAt)
	}

	g, ok := groupByEntry(s, "main.(*pipeline).worker")
	if !ok || !g.InFullDump || g.State != "sleep" {
		t.Fatalf("worker group %+v (found %v): it lost what the last good dump said", g, ok)
	}
}

type gatedSource struct {
	*fakeSource
	mu    sync.Mutex
	calls int
	first chan struct{}
}

func (s *gatedSource) Goroutines(ctx context.Context, debug int) ([]byte, error) {
	if debug == 1 {
		s.mu.Lock()
		s.calls++
		n := s.calls
		s.mu.Unlock()

		if n == 1 {
			<-s.first

			return nil, errors.New("old request failed")
		}
	}

	return s.fakeSource.Goroutines(ctx, debug)
}

func TestLateFailedReadDoesNotOverrideNewer(t *testing.T) {
	src := &gatedSource{fakeSource: &fakeSource{debug1: stacksText(20)}, first: make(chan struct{})}
	c := newCollector(src)
	t.Cleanup(c.close)

	old := make(chan Snapshot)

	go func() { old <- c.sample(context.Background(), false) }()

	waitFor(t, time.Second, func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()

		return src.calls == 1
	})

	if s := c.sample(context.Background(), false); s.Goroutines != 20 || s.Error != "" {
		t.Fatalf("newer read: %d goroutines, error %q", s.Goroutines, s.Error)
	}

	close(src.first)
	<-old

	s := c.lastSnapshot()

	c.mu.Lock()
	down := c.down
	c.mu.Unlock()

	if s.Goroutines != 20 || s.Error != "" || down {
		t.Fatalf("%d goroutines, error %q, down %v: an older failure must not override a newer success", s.Goroutines, s.Error, down)
	}
}

func TestTruncatedDumpDoesNotProveRestart(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	c, clk := newTestCollector(t, src)
	c.reader = &fakeReader{wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	if _, err := c.captureTrace(context.Background(), traceDefault); err != nil {
		t.Fatal(err)
	}

	clk.set(t0.Add(2 * time.Minute))
	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(3 * time.Minute))
	src.set(func(f *fakeSource) {
		f.err = nil
		f.debug2 = restarted
	})

	c.mu.Lock()
	c.fullCap = len(restarted)
	c.mu.Unlock()

	s := c.sample(context.Background(), false)

	if !s.FullCut || !s.LeaksAt.Equal(t0) || !s.TraceAt.Equal(t0) {
		t.Fatalf("fullCut %v, leaksAt %v, traceAt %v: a cut dump has no say about restarts", s.FullCut, s.LeaksAt, s.TraceAt)
	}
}

func TestSuccessfulTraceClearsReadOutage(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	c.reader = &fakeReader{wakeups: []Wakeup{{Cause: CauseNetwork, Waiter: workerStack, Count: 1, Waiters: 1, Wait: time.Second}}}
	c.sample(context.Background(), false)

	clk.set(t0.Add(30 * time.Second))
	src.set(func(f *fakeSource) { f.err = errors.New("target offline") })
	c.sample(context.Background(), false)

	clk.set(t0.Add(40 * time.Second))
	src.set(func(f *fakeSource) { f.err = nil })

	s, err := c.captureTrace(context.Background(), traceDefault)
	if err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	down := c.down
	c.mu.Unlock()

	if s.Error != "" || down || len(s.Edges) != 1 {
		t.Fatalf("error %q, down %v, %d edges: a good read after the trace means the target is back", s.Error, down, len(s.Edges))
	}
}

func TestSeqGrowsWithinOneInstant(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	c, _ := newTestCollector(t, src)

	a := c.sample(context.Background(), false)
	b := c.sample(context.Background(), false)

	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })

	e := c.sample(context.Background(), false)

	if !a.At.Equal(b.At) || !(a.Seq < b.Seq && b.Seq < e.Seq) || a.Seq < t0.UnixMicro() {
		t.Fatalf("seq %d, %d, %d at %v, %v: snapshots of one instant must still be ordered", a.Seq, b.Seq, e.Seq, a.At, b.At)
	}
}

func TestUnchangedStacksAreParsedOnce(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	c, _ := newTestCollector(t, src)

	records := func() *dump.Record {
		c.mu.Lock()
		defer c.mu.Unlock()

		return &c.profile.Records[0]
	}

	c.sample(context.Background(), false)
	first := records()

	c.sample(context.Background(), false)

	if records() != first {
		t.Fatal("the same debug=1 text was parsed again")
	}

	src.set(func(f *fakeSource) { f.debug1 = stacksText(4) })
	c.sample(context.Background(), false)

	if records() == first {
		t.Fatal("a changed debug=1 text was not parsed")
	}

	c.mu.Lock()
	c.stopLocked()
	c.mu.Unlock()

	c.pmu.Lock()
	kept := c.parsed != nil
	c.pmu.Unlock()

	if kept {
		t.Fatal("the last debug=1 text is kept after the collector stopped")
	}
}
