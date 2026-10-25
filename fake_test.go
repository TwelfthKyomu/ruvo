package ruvo

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu       sync.Mutex
	debug1   []byte
	debug2   []byte
	leaks    []byte
	err      error
	err2     error
	leaksErr error
	trace    []byte
	traceErr error
	traceD   time.Duration
	delay    time.Duration
	calls    map[string]int
}

func testdata(t *testing.T, name string) []byte {
	t.Helper()

	if name == "" {
		return nil
	}

	b, err := os.ReadFile("internal/dump/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

func newFake(t *testing.T, debug1, debug2, leaks string) *fakeSource {
	return &fakeSource{debug1: testdata(t, debug1), debug2: testdata(t, debug2), leaks: testdata(t, leaks)}
}

func stacksText(n int) []byte {
	return fmt.Appendf(nil, "goroutine profile: total %d\n%d @ 0x1\n#\t0x1\tmain.x+0x1\t/home/dev/a.go:7\n\n", n, n)
}

func (f *fakeSource) set(fn func(*fakeSource)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(f)
}

func (f *fakeSource) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[name]
}

func (f *fakeSource) enter(name string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.calls == nil {
		f.calls = map[string]int{}
	}

	f.calls[name]++

	return f.delay
}

func (f *fakeSource) Goroutines(ctx context.Context, debug int) ([]byte, error) {
	time.Sleep(f.enter(fmt.Sprintf("debug%d", debug)))

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	if debug == 2 {
		return f.debug2, f.err2
	}

	return f.debug1, nil
}

func (f *fakeSource) Leaks(ctx context.Context) ([]byte, error) {
	time.Sleep(f.enter("leaks"))

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.leaksErr != nil {
		return nil, f.leaksErr
	}

	return f.leaks, nil
}

func (f *fakeSource) Trace(ctx context.Context, d time.Duration) ([]byte, error) {
	time.Sleep(f.enter("trace"))

	f.mu.Lock()
	defer f.mu.Unlock()

	f.traceD = d

	if f.traceErr != nil {
		return nil, f.traceErr
	}

	return f.trace, nil
}

func (f *fakeSource) Name() string {
	return "fake"
}

type fakeReader struct {
	wakeups []Wakeup
	err     error
	crash   any
	raw     []byte
	stacks  map[int64][]Frame
}

func (r *fakeReader) ReadTrace(trace io.Reader, stacks map[int64][]Frame) ([]Wakeup, error) {
	raw, err := io.ReadAll(trace)
	if err != nil {
		return nil, err
	}

	r.raw = raw
	r.stacks = stacks

	if r.crash != nil {
		panic(r.crash)
	}

	return r.wakeups, r.err
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = t
}

func newTestCollector(t *testing.T, src *fakeSource) (*collector, *clock) {
	t.Helper()

	clk := &clock{t: t0}

	c := newCollector(src)
	c.now = clk.now

	t.Cleanup(c.close)

	return c, clk
}

type readerFunc struct {
	fn func()
}

func (r *readerFunc) ReadTrace(trace io.Reader, stacks map[int64][]Frame) ([]Wakeup, error) {
	r.fn()

	return nil, nil
}
