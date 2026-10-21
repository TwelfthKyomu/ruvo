package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"
)

var client = &http.Client{Timeout: 2 * time.Second}

func dump() string {
	var b bytes.Buffer

	pprof.Lookup("goroutine").WriteTo(&b, 2)

	return b.String()
}

func leakTotal(t *testing.T) int {
	t.Helper()

	var b bytes.Buffer

	pprof.Lookup("goroutineleak").WriteTo(&b, 1)

	header, _, _ := strings.Cut(b.String(), "\n")
	fields := strings.Fields(header)

	n, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil {
		t.Fatalf("goroutineleak header %q: %v", header, err)
	}

	return n
}

func blocked(d, state string, fns ...string) int {
	n := 0

	for _, g := range strings.Split(d, "\n\n") {
		if !strings.Contains(g, "["+state) {
			continue
		}

		all := true

		for _, fn := range fns {
			if !strings.Contains(g, fn) {
				all = false
				break
			}
		}

		if all {
			n++
		}
	}

	return n
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func fire(url string, n int) {
	for range n {
		go func() {
			resp, err := http.Get(url)
			if err != nil {
				return
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, string(body)
}

func start(t *testing.T, m mode) (*pipeline, string) {
	t.Helper()

	up := httptest.NewServer(http.HandlerFunc(handleUpstream))
	t.Cleanup(up.Close)

	p := newPipeline(8, up.URL)

	srv := httptest.NewServer(http.HandlerFunc(p.handleWork))
	t.Cleanup(srv.Close)
	t.Cleanup(p.stop)

	p.setMode(m)

	return p, srv.URL
}

func TestHealthyServesWork(t *testing.T) {
	_, url := start(t, modeNone)

	for i := range 20 {
		if code, body := get(t, url); code != http.StatusOK {
			t.Fatalf("request %d: status %d, body %q", i, code, body)
		}
	}
}

func TestLeakLeavesNotifyBlocked(t *testing.T) {
	_, url := start(t, modeLeak)
	base := blocked(dump(), "chan send", "leaky.notify")

	for i := range 20 {
		if code, body := get(t, url); code != http.StatusServiceUnavailable || body != "timeout\n" {
			t.Fatalf("request %d: status %d, body %q", i, code, body)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		return blocked(dump(), "chan send", "leaky.notify") >= base+20
	})
}

func TestLeakIsSeenByGoroutineLeakProfile(t *testing.T) {
	_, url := start(t, modeLeak)
	base := leakTotal(t)

	for range 20 {
		get(t, url)
	}

	waitFor(t, 2*time.Second, func() bool {
		return leakTotal(t) >= base+20
	})
}

func TestSlowLoaderStarvesWorkers(t *testing.T) {
	_, url := start(t, modeSlowLoader)
	fire(url, 20)

	waitFor(t, 2*time.Second, func() bool {
		d := dump()

		return blocked(d, "sleep", "leaky.(*pipeline).loader") == 1 &&
			blocked(d, "chan receive", "leaky.(*pipeline).worker") >= 7 &&
			blocked(d, "select", "leaky.(*pipeline).handleWork") >= 10
	})
}

func TestSlowWorkersBlockLoader(t *testing.T) {
	_, url := start(t, modeSlowWorkers)
	fire(url, 80)

	cond := func() bool {
		d := dump()

		return blocked(d, "chan send", "leaky.(*pipeline).loader") >= 1 &&
			blocked(d, "sleep", "leaky.(*pipeline).worker") >= 6
	}

	waitFor(t, 2*time.Second, cond)
	time.Sleep(50 * time.Millisecond)

	if !cond() {
		t.Fatal("loader stopped waiting on workers after 50ms")
	}
}

func TestDeadlockLeavesLockPairsBlocked(t *testing.T) {
	_, url := start(t, modeDeadlock)
	base := blocked(dump(), "sync.Mutex.Lock", "leaky.lockPair")

	for i := range 10 {
		if code, body := get(t, url); code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status %d, body %q", i, code, body)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		return blocked(dump(), "sync.Mutex.Lock", "leaky.lockPair") >= base+16
	})
}

func TestNetWaitLoaderWaitsOnUpstream(t *testing.T) {
	_, url := start(t, modeNetWait)
	fire(url, 10)

	waitFor(t, 2*time.Second, func() bool {
		d := dump()

		return blocked(d, "IO wait", "net/http.(*persistConn).readLoop") >= 1 &&
			blocked(d, "select", "leaky.(*pipeline).loader", "net/http.(*Client)") >= 1
	})
}

func TestRecoversAfterModeReset(t *testing.T) {
	p, url := start(t, modeSlowWorkers)
	fire(url, 40)

	waitFor(t, 2*time.Second, func() bool {
		return blocked(dump(), "chan send", "leaky.(*pipeline).loader") >= 1
	})

	p.setMode(modeNone)
	time.Sleep(100 * time.Millisecond)

	for i := range 5 {
		t0 := time.Now()
		code, body := get(t, url)

		if d := time.Since(t0); code != http.StatusOK || d > 15*time.Millisecond {
			t.Fatalf("request %d after reset: status %d in %v, body %q", i, code, d, body)
		}
	}
}

func TestStopReleasesWorkers(t *testing.T) {
	p := newPipeline(8, "http://127.0.0.1:1")

	waitFor(t, 2*time.Second, func() bool {
		return blocked(dump(), "chan receive", "leaky.(*pipeline).worker") >= 8
	})

	p.stop()

	waitFor(t, 2*time.Second, func() bool {
		return blocked(dump(), "chan receive", "leaky.(*pipeline).worker") == 0
	})
}
