package ruvo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestStressFlappingTarget(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	c, clk := newTestCollector(t, src)
	down := errors.New("connection refused")

	c.sample(context.Background(), true)

	dumps := []time.Time{clk.now()}
	seen := src.count("debug2")

	for i := range 500 {
		failing := i%2 == 1

		clk.set(t0.Add(time.Duration(i+1) * 2 * time.Second))
		src.set(func(f *fakeSource) {
			f.err = nil

			if failing {
				f.err = down
			}
		})

		want := ""
		if failing {
			want = down.Error()
		}

		if s := c.sample(context.Background(), false); s.Error != want {
			t.Fatalf("poll %d: error %q, want %q", i, s.Error, want)
		}

		if n := src.count("debug2"); n != seen {
			dumps = append(dumps, clk.now())
			seen = n
		}
	}

	if len(dumps) < 2 {
		t.Fatalf("%d full dumps: the restart check must still run", len(dumps))
	}

	for i := 1; i < len(dumps); i++ {
		if gap := dumps[i].Sub(dumps[i-1]); gap < fullMin {
			t.Fatalf("full dumps %v and %v are %v apart, want at least %v", dumps[i-1], dumps[i], gap, fullMin)
		}
	}

	c.mu.Lock()
	n := len(c.hist.samples)
	c.mu.Unlock()

	if n > 301 {
		t.Fatalf("%d history samples, want at most 301", n)
	}

	if s := c.lastSnapshot(); s.Error != down.Error() {
		t.Fatalf("last error %q, want %q: the target is down", s.Error, down)
	}
}

func TestStressConcurrentActions(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	src.delay = 100 * time.Millisecond
	srv, _ := serve(t, src, readerView{})

	paths := []string{"/api/full", "/api/leaks", "/api/trace?seconds=1"}
	codes := make([]int, 64)
	errs := make([]error, 64)

	var wg sync.WaitGroup

	for i := range codes {
		wg.Add(1)

		go func() {
			defer wg.Done()

			resp, err := http.Post(srv.URL+paths[i%len(paths)], "", nil)
			if err != nil {
				errs[i] = err
				return
			}

			defer resp.Body.Close()

			_, errs[i] = io.Copy(io.Discard, resp.Body)
			codes[i] = resp.StatusCode
		}()
	}

	wg.Wait()

	for i, code := range codes {
		if errs[i] != nil || code != http.StatusOK {
			t.Fatalf("request %d to %s: status %d, error %v", i, paths[i%len(paths)], code, errs[i])
		}
	}

	for _, name := range []string{"debug2", "leaks", "trace"} {
		if n := src.count(name); n != 1 {
			t.Fatalf("%d %s calls, want 1: concurrent presses share one run", n, name)
		}
	}
}
