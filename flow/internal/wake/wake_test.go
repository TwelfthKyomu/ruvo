package wake

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	rtrace "runtime/trace"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo"
	"golang.org/x/exp/trace"
)

func capture(tb testing.TB, fn func()) []byte {
	tb.Helper()

	var b bytes.Buffer

	if err := rtrace.Start(&b); err != nil {
		tb.Fatal(err)
	}

	fn()
	rtrace.Stop()

	return b.Bytes()
}

func parse(t *testing.T, data []byte, stacks map[int64][]ruvo.Frame) ([]ruvo.Wakeup, int) {
	t.Helper()

	ws, later, err := read(bytes.NewReader(data), stacks)
	if err != nil {
		t.Fatal(err)
	}

	check(t, ws)

	return ws, later
}

func user(fs []ruvo.Frame) bool {
	for _, f := range fs {
		if !strings.HasPrefix(f.Func, "runtime.") && !strings.HasPrefix(f.Func, "internal/runtime/") {
			return true
		}
	}

	return false
}

func kindOf(w ruvo.Wakeup) int {
	switch {
	case len(w.Waiter) == 0:
		return 2
	case w.Cause == ruvo.CauseNone:
		return 1
	}

	return 0
}

func check(t *testing.T, ws []ruvo.Wakeup) {
	t.Helper()

	for i, w := range ws {
		kind := kindOf(w)

		if i > 0 && kindOf(ws[i-1]) > kind {
			t.Fatalf("edges, then goroutines still waiting, then unknown wakeups: %s after %s", show(w), show(ws[i-1]))
		}

		if kind == 2 {
			if len(w.Waker) != 0 || w.Count < 1 || w.Waiters != 0 || w.Wait != 0 || w.Cause == ruvo.CauseNone {
				t.Fatalf("bad count of unknown wakeups: %s", show(w))
			}

			if i > 0 && kindOf(ws[i-1]) == 2 && ws[i-1].Cause >= w.Cause {
				t.Fatalf("unknown wakeups not one per cause in order: %s after %s", show(w), show(ws[i-1]))
			}

			continue
		}

		switch w.Cause {
		case ruvo.CauseGoroutine:
			if !user(w.Waker) {
				t.Fatalf("goroutine wakeup without a user waker stack: %s", show(w))
			}
		case ruvo.CauseNetwork, ruvo.CauseTimer, ruvo.CauseNone:
			if len(w.Waker) != 0 {
				t.Fatalf("%s wakeup with a waker stack: %s", w.Cause, show(w))
			}
		default:
			t.Fatalf("unknown cause: %s", show(w))
		}

		if !user(w.Waiter) {
			t.Fatalf("wakeup without a user waiter stack: %s", show(w))
		}

		if w.Count < 1 || w.Waiters < 1 || w.Waiters > w.Count || w.Wait < 0 {
			t.Fatalf("bad totals: %s", show(w))
		}

		if kind == 1 && w.Waiters != w.Count {
			t.Fatalf("each goroutine still waiting counts once: %s", show(w))
		}

		if i > 0 && kindOf(ws[i-1]) == kind {
			p := ws[i-1]

			if p.Wait < w.Wait || p.Wait == w.Wait && p.Count < w.Count {
				t.Fatalf("not sorted by wait, then count: %s before %s", show(p), show(w))
			}
		}
	}
}

func unknown(ws []ruvo.Wakeup) int {
	n := 0

	for _, w := range ws {
		if len(w.Waiter) == 0 {
			n += w.Count
		}
	}

	return n
}

func has(fs []ruvo.Frame, s string) bool {
	for _, f := range fs {
		if strings.Contains(f.Func, s) {
			return true
		}
	}

	return false
}

func match(w ruvo.Wakeup, cause, waker, waiter string) bool {
	return w.Cause == cause && (waker == "" || has(w.Waker, waker)) && has(w.Waiter, waiter)
}

func find(ws []ruvo.Wakeup, cause, waker, waiter string) (ruvo.Wakeup, bool) {
	for _, w := range ws {
		if match(w, cause, waker, waiter) {
			return w, true
		}
	}

	return ruvo.Wakeup{}, false
}

func total(ws []ruvo.Wakeup, cause, waker, waiter string) (ruvo.Wakeup, bool) {
	sum := ruvo.Wakeup{Cause: cause}
	found := false

	for _, w := range ws {
		if !match(w, cause, waker, waiter) {
			continue
		}

		found = true
		sum.Count += w.Count
		sum.Waiters += w.Waiters
		sum.Wait += w.Wait
	}

	return sum, found
}

func short(fs []ruvo.Frame) string {
	for _, f := range fs {
		if !strings.HasPrefix(f.Func, "runtime.") && !strings.HasPrefix(f.Func, "time.") {
			return f.Func + ":" + strconv.Itoa(f.Line)
		}
	}

	if len(fs) == 0 {
		return "-"
	}

	return fs[0].Func + ":" + strconv.Itoa(fs[0].Line)
}

func trail(fs []ruvo.Frame) string {
	parts := make([]string, len(fs))

	for i, f := range fs {
		parts[i] = f.Func + ":" + strconv.Itoa(f.Line)
	}

	return strings.Join(parts, " < ")
}

func show(w ruvo.Wakeup) string {
	from := w.Cause
	if w.Cause == ruvo.CauseGoroutine {
		from = short(w.Waker)
	}

	return fmt.Sprintf("%s -> %s ×%d (%d waiters, %v)", from, short(w.Waiter), w.Count, w.Waiters, w.Wait)
}

func top(ws []ruvo.Wakeup) string {
	var b strings.Builder

	for i, w := range ws {
		if i == 10 {
			break
		}

		b.WriteString("\n  " + show(w))
	}

	return b.String()
}

func producer(jobs chan<- int, stop <-chan struct{}, delay time.Duration) {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}

		if delay > 0 {
			time.Sleep(delay)
		}

		jobs <- i
	}
}

func worker(jobs <-chan int, delay time.Duration) {
	for range jobs {
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func produceFor(jobs chan<- int, delay, d time.Duration) {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		producer(jobs, stop, delay)
		close(done)
	}()

	time.Sleep(d)
	close(stop)
	<-done
}

func waiter(n int) {
	for range n {
		<-time.After(time.Millisecond)
	}
}

func reader(c net.Conn, n int) {
	buf := make([]byte, 1)

	for range n {
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
	}
}

func ticker(c net.Conn, n int, every time.Duration) {
	for range n {
		time.Sleep(every)

		if _, err := c.Write([]byte{1}); err != nil {
			return
		}
	}
}

func conns(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer ln.Close()

	accepted := make(chan net.Conn, 1)

	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}

		accepted <- c
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	peer, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}

	t.Cleanup(func() {
		c.Close()
		peer.Close()
	})

	return c, peer
}

func waiting(fn, creator string) []int64 {
	buf := make([]byte, 64<<10)

	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}

		buf = make([]byte, 2*len(buf))
	}

	var ids []int64

	for g := range strings.SplitSeq(string(buf), "\n\n") {
		head, body, _ := strings.Cut(g, "\n")
		if !strings.HasPrefix(head, "goroutine ") || !strings.Contains(head, "[chan receive") {
			continue
		}

		if !strings.Contains(body, fn+"(") || !strings.Contains(body, "created by "+creator) {
			continue
		}

		n, _, _ := strings.Cut(strings.TrimPrefix(head, "goroutine "), " ")

		id, err := strconv.ParseInt(n, 10, 64)
		if err == nil {
			ids = append(ids, id)
		}
	}

	return ids
}

func TestSlowProducerWakesWorkers(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	data := capture(t, func() { produceFor(jobs, time.Millisecond, 100*time.Millisecond) })
	close(jobs)

	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseGoroutine, "wake.producer", "wake.worker")
	if !ok || w.Count < 20 || w.Waiters < 2 {
		t.Fatalf("producer -> worker: %s (found %v)%s", show(w), ok, top(ws))
	}

	if w.Wait < 100*time.Millisecond {
		t.Fatalf("4 workers idle for ~100ms waited only %v in total", w.Wait)
	}
}

func TestSlowWorkersWakeProducer(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, time.Millisecond)
	}

	data := capture(t, func() { produceFor(jobs, 0, 100*time.Millisecond) })
	close(jobs)

	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseGoroutine, "wake.worker", "wake.producer")
	if !ok || w.Count < 20 {
		t.Fatalf("worker -> producer: %s (found %v)%s", show(w), ok, top(ws))
	}
}

func TestWakerIsWhereItLastWaited(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	data := capture(t, func() { produceFor(jobs, time.Millisecond, 100*time.Millisecond) })
	close(jobs)

	ws, _ := parse(t, data, nil)

	w, ok := find(ws, ruvo.CauseGoroutine, "wake.producer", "wake.worker")
	if !ok {
		t.Fatalf("no producer -> worker wakeup%s", top(ws))
	}

	if !has(w.Waker, "time.Sleep") || has(w.Waker, "runtime.chansend1") {
		t.Fatalf("waker stack is not where the producer slept: %s", trail(w.Waker))
	}
}

func TestWaitersBlockedBeforeStartUseStacks(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	parked := []ruvo.Frame{{Func: "x.parked", File: "/home/dev/p.go", Line: 1}}
	stacks := map[int64][]ruvo.Frame{}

	data := capture(t, func() {
		for _, id := range waiting("wake.worker", "github.com/TwelfthKyomu/ruvo/flow/internal/wake.TestWaitersBlockedBeforeStartUseStacks") {
			stacks[id] = parked
		}

		for i := range 4 {
			jobs <- i
		}
	})
	close(jobs)

	if len(stacks) != 4 {
		t.Fatalf("found %d waiting workers in the dump, want 4", len(stacks))
	}

	ws, _ := parse(t, data, stacks)

	w, ok := find(ws, ruvo.CauseGoroutine, "", "x.parked")
	if !ok || w.Waiters != 4 || len(w.Waiter) != 1 {
		t.Fatalf("wakeup of workers blocked before the trace: %s (found %v)%s", show(w), ok, top(ws))
	}
}

func TestDumpBeforeTraceIsNotUsed(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	stacks := map[int64][]ruvo.Frame{}

	for _, id := range waiting("wake.worker", "github.com/TwelfthKyomu/ruvo/flow/internal/wake.TestDumpBeforeTraceIsNotUsed") {
		stacks[id] = []ruvo.Frame{{Func: "x.parked", File: "/home/dev/p.go", Line: 1}}
	}

	data := capture(t, func() {
		for i := range 4 {
			jobs <- i
		}
	})
	close(jobs)

	ws, _ := parse(t, data, stacks)

	if w, ok := find(ws, ruvo.CauseGoroutine, "", "x.parked"); ok || unknown(ws) < 4 {
		t.Fatalf("a dump taken before the trace was used: %s (found %v), %d unknown%s", show(w), ok, unknown(ws), top(ws))
	}
}

func waitOnA(a <-chan struct{}) {
	<-a
}

func waitOnB(b <-chan struct{}) {
	<-b
}

func twoWaits(a, b <-chan struct{}) {
	waitOnA(a)
	waitOnB(b)
}

func TestLaterDumpDoesNotRewriteEarlierWait(t *testing.T) {
	a, b := make(chan struct{}), make(chan struct{})
	defer close(b)

	go twoWaits(a, b)
	time.Sleep(10 * time.Millisecond)

	stacks := map[int64][]ruvo.Frame{}

	data := capture(t, func() {
		time.Sleep(25 * time.Millisecond)
		close(a)
		time.Sleep(175 * time.Millisecond)

		for _, id := range waiting("wake.waitOnB", "github.com/TwelfthKyomu/ruvo/flow/internal/wake.TestLaterDumpDoesNotRewriteEarlierWait") {
			stacks[id] = []ruvo.Frame{{Func: "x.atB", File: "/home/dev/p.go", Line: 2}}
		}

		time.Sleep(20 * time.Millisecond)
	})

	if len(stacks) != 1 {
		t.Fatalf("found %d goroutines waiting on B in the dump, want 1", len(stacks))
	}

	ws, _ := parse(t, data, stacks)

	if w, ok := total(ws, ruvo.CauseGoroutine, "", "x.atB"); ok {
		t.Fatalf("the wakeup from A was put on B, where the goroutine waited only later: %s%s", show(w), top(ws))
	}

	if unknown(ws) < 1 {
		t.Fatalf("the wakeup from A is not counted as unknown%s", top(ws))
	}

	if w, ok := find(ws, ruvo.CauseNone, "", "wake.waitOnB"); ok {
		t.Fatalf("the goroutine was woken from A, so it did not sleep through the trace: %s%s", show(w), top(ws))
	}
}

func TestUnwokenWaitersAreReported(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	for range 12 {
		go waitOnB(stop)
	}

	time.Sleep(10 * time.Millisecond)

	data := capture(t, func() { time.Sleep(220 * time.Millisecond) })
	ws, _ := parse(t, data, nil)

	w, ok := find(ws, ruvo.CauseNone, "", "wake.waitOnB")
	if !ok || w.Count != 12 || w.Waiters != 12 || w.Wait < 12*200*time.Millisecond {
		t.Fatalf("goroutines that waited the whole trace: %s (found %v)%s", show(w), ok, top(ws))
	}

	if _, ok := find(ws, ruvo.CauseGoroutine, "", "wake.waitOnB"); ok {
		t.Fatalf("nothing woke them, yet there is an edge%s", top(ws))
	}
}

func TestBusyWaitersAreNotUnwoken(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	data := capture(t, func() { produceFor(jobs, time.Millisecond, 300*time.Millisecond) })
	close(jobs)

	ws, _ := parse(t, data, nil)

	if w, ok := find(ws, ruvo.CauseNone, "", "wake.worker"); ok {
		t.Fatalf("workers woken all along are waiting at the end, but they did not sleep through the trace: %s%s", show(w), top(ws))
	}
}

func TestWaitSiteSurvivesGenerations(t *testing.T) {
	jobs := make(chan int)

	data := capture(t, func() {
		go worker(jobs, 0)

		time.Sleep(10 * time.Millisecond)
		jobs <- 1

		time.Sleep(1500 * time.Millisecond)
		jobs <- 2
	})
	close(jobs)

	ws, later := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseGoroutine, "TestWaitSiteSurvivesGenerations", "wake.worker")
	if !ok || w.Count != 2 {
		t.Fatalf("wakeups of the worker across a generation boundary: %s (found %v)%s", show(w), ok, top(ws))
	}

	if later != 0 {
		t.Fatalf("wait sites lost after the first generation: %d%s", later, top(ws))
	}
}

func TestReasonSurvivesGenerations(t *testing.T) {
	c, peer := conns(t)
	done := make(chan struct{})

	begin := time.Now()
	data := capture(t, func() {
		go func() {
			reader(c, 1)
			close(done)
		}()

		ticker(peer, 1, 2500*time.Millisecond)
		<-done
	})
	elapsed := time.Since(begin)

	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseNetwork, "", "wake.reader")
	if !ok || w.Count != 1 || w.Wait < 2*time.Second || w.Wait > elapsed {
		t.Fatalf("network wakeup after two generation boundaries: %s, capture took %v (found %v)%s", show(w), elapsed, ok, top(ws))
	}
}

func TestNetworkWaitersBlockedBeforeStart(t *testing.T) {
	c, peer := conns(t)
	done := make(chan struct{})

	go func() {
		reader(c, 1)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)

	begin := time.Now()
	data := capture(t, func() {
		ticker(peer, 1, 1500*time.Millisecond)
		<-done
	})
	elapsed := time.Since(begin)

	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseNetwork, "", "wake.reader")
	if !ok || w.Count != 1 || w.Wait < time.Second || w.Wait > elapsed {
		t.Fatalf("network wakeup of a reader blocked before the trace: %s, capture took %v (found %v)%s", show(w), elapsed, ok, top(ws))
	}
}

func TestSystemGoroutinesStayHidden(t *testing.T) {
	data := capture(t, func() {
		time.Sleep(1200 * time.Millisecond)
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	})

	ws, later := parse(t, data, nil)

	if later != 0 {
		t.Fatalf("unknown wait sites after the first generation: %d%s", later, top(ws))
	}
}

func TestTimerWakeups(t *testing.T) {
	data := capture(t, func() { waiter(20) })
	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseTimer, "", "wake.waiter")
	if !ok || w.Count < 10 {
		t.Fatalf("timer -> waiter: %s (found %v)%s", show(w), ok, top(ws))
	}
}

func TestNetworkWakeups(t *testing.T) {
	c, peer := conns(t)

	go ticker(peer, 50, time.Millisecond)

	data := capture(t, func() { reader(c, 50) })
	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseNetwork, "", "wake.reader")
	if !ok || w.Count < 10 {
		t.Fatalf("network -> reader: %s (found %v)%s", show(w), ok, top(ws))
	}
}

func TestReadRejectsNonTrace(t *testing.T) {
	if _, err := Read(strings.NewReader("not a trace"), nil); err == nil {
		t.Fatal("want error for non-trace input")
	}
}

func TestReadTruncatedTrace(t *testing.T) {
	jobs := make(chan int)

	for range 4 {
		go worker(jobs, 0)
	}

	data := capture(t, func() { produceFor(jobs, time.Millisecond, 50*time.Millisecond) })
	close(jobs)

	for _, p := range []int{10, 50, 99} {
		_, _, err := read(bytes.NewReader(data[:len(data)*p/100]), nil)
		if err == nil || errors.Is(err, errMalformed) {
			t.Fatalf("trace cut at %d%%: got %v, want a read error", p, err)
		}
	}
}

func seed(t *testing.T, name string) []byte {
	t.Helper()

	b, err := os.ReadFile(filepath.Join("testdata", "fuzz", "FuzzRead", name))
	if err != nil {
		t.Fatal(err)
	}

	_, lit, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")

	s, err := strconv.Unquote(strings.TrimSuffix(strings.TrimPrefix(lit, "[]byte("), ")"))
	if err != nil {
		t.Fatal(err)
	}

	return []byte(s)
}

func TestReadTurnsTracePanicsIntoErrors(t *testing.T) {
	_, _, err := read(bytes.NewReader(seed(t, "8d85e60df633439b")), nil)
	if !errors.Is(err, errMalformed) {
		t.Fatalf("got %v, want a malformed trace error", err)
	}
}

type panicky struct{}

func (panicky) Read([]byte) (int, error) {
	panic("bug")
}

func TestReadRepanicsOtherPanics(t *testing.T) {
	defer func() {
		if p := recover(); p != "bug" {
			t.Fatalf("recovered %v, want the original panic", p)
		}
	}()

	_, _, err := read(panicky{}, nil)
	t.Fatalf("read returned %v instead of panicking", err)
}

func TestCatchKnowsWherePanicsCameFrom(t *testing.T) {
	var err error

	func() {
		defer catch(&err)

		trace.Event{}.StateTransition()
	}()

	if !errors.Is(err, errMalformed) {
		t.Fatalf("panic in x/exp/trace: got %v, want a malformed trace error", err)
	}

	defer func() {
		if p := recover(); p == nil {
			t.Fatal("panic outside x/exp/trace was swallowed")
		}
	}()

	func() {
		defer catch(&err)

		var fs []ruvo.Frame
		_ = fs[len(fs)]
	}()
}

func TestStressManyWorkers(t *testing.T) {
	n := 2000
	if raceOn {
		n = 500
	}

	jobs := make(chan int)

	for range n {
		go worker(jobs, 0)
	}

	time.Sleep(10 * time.Millisecond)

	data := capture(t, func() {
		var wg sync.WaitGroup

		for range 4 {
			wg.Go(func() { produceFor(jobs, time.Millisecond, time.Second) })
		}

		wg.Wait()
	})
	close(jobs)

	ws, _ := parse(t, data, nil)

	w, ok := total(ws, ruvo.CauseGoroutine, "wake.producer", "wake.worker")
	if !ok || w.Count < 100 {
		t.Fatalf("producer -> worker with %d workers: %s (found %v)%s", n, show(w), ok, top(ws))
	}
}

func FuzzRead(f *testing.F) {
	jobs := make(chan int)

	for range 2 {
		go worker(jobs, 0)
	}

	data := capture(f, func() { produceFor(jobs, time.Millisecond, 5*time.Millisecond) })
	close(jobs)

	f.Add(data)

	for _, p := range []int{1, 10, 50, 90, 99} {
		f.Add(data[:len(data)*p/100])
	}

	stacks := map[int64][]ruvo.Frame{}

	for id := range int64(64) {
		stacks[id] = []ruvo.Frame{{Func: "x.parked", File: "/home/dev/p.go", Line: int(id)}}
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		ws, _, err := read(bytes.NewReader(b), stacks)
		if err == nil {
			check(t, ws)
		}
	})
}

func BenchmarkRead(b *testing.B) {
	jobs := make(chan int)

	for range 8 {
		go worker(jobs, 100*time.Microsecond)
	}

	data := capture(b, func() { produceFor(jobs, 0, time.Second) })
	close(jobs)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for b.Loop() {
		if _, err := Read(bytes.NewReader(data), nil); err != nil {
			b.Fatal(err)
		}
	}
}

func xReceiver(x <-chan int) {
	<-x
}

func wakeThenWait(a, b <-chan struct{}, x chan<- int) {
	waitOnA(a)
	x <- 1
	waitOnB(b)
}

func TestWakerFutureDumpIsNotUsed(t *testing.T) {
	a, b, x := make(chan struct{}), make(chan struct{}), make(chan int)
	defer close(b)

	go wakeThenWait(a, b, x)
	time.Sleep(10 * time.Millisecond)

	stacks := map[int64][]ruvo.Frame{}

	data := capture(t, func() {
		go xReceiver(x)
		time.Sleep(25 * time.Millisecond)
		close(a)
		time.Sleep(175 * time.Millisecond)

		for _, id := range waiting("wake.waitOnB", "github.com/TwelfthKyomu/ruvo/flow/internal/wake.TestWakerFutureDumpIsNotUsed") {
			stacks[id] = []ruvo.Frame{{Func: "x.atB", File: "/home/dev/p.go", Line: 2}}
		}

		time.Sleep(20 * time.Millisecond)
	})

	if len(stacks) != 1 {
		t.Fatalf("found %d goroutines waiting on B in the dump, want 1", len(stacks))
	}

	ws, _ := parse(t, data, stacks)

	w, ok := find(ws, ruvo.CauseGoroutine, "", "wake.xReceiver")
	if !ok || has(w.Waker, "x.atB") || !has(w.Waker, "wake.wakeThenWait") {
		t.Fatalf("waker of the receiver: %s (found %v): it must not come from a dump taken after the wakeup%s", trail(w.Waker), ok, top(ws))
	}
}

func TestWakerWaitFromEarlierDump(t *testing.T) {
	a, b, x := make(chan struct{}), make(chan struct{}), make(chan int)
	defer close(b)

	go wakeThenWait(a, b, x)
	time.Sleep(10 * time.Millisecond)

	stacks := map[int64][]ruvo.Frame{}

	data := capture(t, func() {
		go xReceiver(x)
		time.Sleep(10 * time.Millisecond)

		for _, id := range waiting("wake.waitOnA", "github.com/TwelfthKyomu/ruvo/flow/internal/wake.TestWakerWaitFromEarlierDump") {
			stacks[id] = []ruvo.Frame{{Func: "x.atA", File: "/home/dev/p.go", Line: 1}}
		}

		time.Sleep(90 * time.Millisecond)
		close(a)
		time.Sleep(20 * time.Millisecond)
	})

	if len(stacks) != 1 {
		t.Fatalf("found %d goroutines waiting on A in the dump, want 1", len(stacks))
	}

	ws, _ := parse(t, data, stacks)

	w, ok := find(ws, ruvo.CauseGoroutine, "", "wake.xReceiver")
	if !ok || !has(w.Waker, "x.atA") {
		t.Fatalf("waker of the receiver: %s (found %v): the dump proves where it last waited%s", trail(w.Waker), ok, top(ws))
	}
}
