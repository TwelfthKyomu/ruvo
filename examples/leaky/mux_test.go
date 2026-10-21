package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func startMux(t *testing.T) (*pipeline, string) {
	t.Helper()

	up := httptest.NewServer(http.HandlerFunc(handleUpstream))
	t.Cleanup(up.Close)

	p := newPipeline(8, up.URL)

	srv := httptest.NewServer(newMux(p))
	t.Cleanup(srv.Close)
	t.Cleanup(p.stop)

	return p, srv.URL
}

func TestMuxServesWork(t *testing.T) {
	_, url := startMux(t)

	if code, body := get(t, url+"/work"); code != http.StatusOK {
		t.Fatalf("status %d, body %q", code, body)
	}
}

func TestBreakSwitchesMode(t *testing.T) {
	p, url := startMux(t)

	if code, body := get(t, url+"/break?mode=slow-loader"); code != http.StatusOK || body != "slow-loader\n" {
		t.Fatalf("set: status %d, body %q", code, body)
	}

	if code, body := get(t, url+"/break"); code != http.StatusOK || body != "slow-loader\n" {
		t.Fatalf("read: status %d, body %q", code, body)
	}

	if got := p.currentMode(); got != modeSlowLoader {
		t.Fatalf("pipeline mode %q, want %q", got, modeSlowLoader)
	}
}

func TestBreakRejectsUnknownMode(t *testing.T) {
	_, url := startMux(t)

	if code, body := get(t, url+"/break?mode=bogus"); code != http.StatusBadRequest {
		t.Fatalf("set: status %d, body %q", code, body)
	}

	if code, body := get(t, url+"/break"); code != http.StatusOK || body != "none\n" {
		t.Fatalf("read: status %d, body %q", code, body)
	}
}

func TestPprofEndpointsServed(t *testing.T) {
	_, url := startMux(t)

	for _, path := range []string{
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/goroutineleak?debug=1",
		"/debug/pprof/trace?seconds=1",
	} {
		if code, body := get(t, url+path); code != http.StatusOK || body == "" {
			t.Fatalf("%s: status %d, %d bytes", path, code, len(body))
		}
	}
}
