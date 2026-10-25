package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo"
)

var client = &http.Client{Timeout: time.Minute}

func pprofServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func TestParseTarget(t *testing.T) {
	for in, want := range map[string]string{
		"localhost:6060":                     "http://localhost:6060",
		"http://h:6060/":                     "http://h:6060",
		"https://h/debug/pprof/":             "https://h",
		"http://h:6060/internal/debug/pprof": "http://h:6060/internal",
		"http://u:secret@h:6060/":            "http://u:secret@h:6060",
	} {
		if got, err := parseTarget(in); err != nil || got != want {
			t.Errorf("parseTarget(%q) = %q, %v; want %q", in, got, err, want)
		}
	}

	for _, in := range []string{"", "http://"} {
		if _, err := parseTarget(in); err == nil {
			t.Errorf("parseTarget(%q): want error", in)
		}
	}
}

func TestParseArgsFlagsAfterTarget(t *testing.T) {
	target, listen, err := parseArgs([]string{"http://h:1", "-listen", "x:1"}, io.Discard)
	if err != nil || target != "http://h:1" || listen != "x:1" {
		t.Fatalf("target %q, listen %q, err %v", target, listen, err)
	}
}

func TestParseArgsUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"a", "b"}} {
		if _, _, err := parseArgs(args, io.Discard); err != errUsage {
			t.Errorf("parseArgs(%q): err %v, want errUsage", args, err)
		}
	}
}

func TestHelpExitsZero(t *testing.T) {
	for _, flagName := range []string{"-h", "--help"} {
		if code := run([]string{flagName}, io.Discard, io.Discard); code != 0 {
			t.Errorf("run(%s) = %d, want 0", flagName, code)
		}
	}
}

func TestRedactUnparsableURL(t *testing.T) {
	got := redact("http://u:secret@host/%zz")
	if strings.Contains(got, "secret") || !strings.HasPrefix(got, "http://u:") || !strings.HasSuffix(got, "@host/%zz") {
		t.Fatalf("redact = %q", got)
	}
}

func TestShownEmptyHost(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4zero, Port: 7070}

	if got := shown(":7070", addr); got != "localhost:7070" {
		t.Fatalf("shown = %q", got)
	}
}

func TestCheckHidesPassword(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	err := check(context.Background(), strings.Replace(srv.URL, "http://", "http://u:secret@", 1))
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err %v: the password must not be printed", err)
	}
}

func TestCheckOK(t *testing.T) {
	if err := check(context.Background(), pprofServer(t).URL); err != nil {
		t.Fatal(err)
	}
}

func TestCheckNoPprof(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	err := check(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "net/http/pprof") {
		t.Fatalf("err %v, want a hint about net/http/pprof", err)
	}
}

func TestRemoteHandlerServesSnapshot(t *testing.T) {
	target := pprofServer(t)

	srv := httptest.NewServer(newHandler(target.URL))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)

	var s ruvo.Snapshot

	if err := json.Unmarshal(b, &s); err != nil || resp.StatusCode != http.StatusOK || s.Goroutines == 0 || s.Target != target.URL {
		t.Fatalf("status %d, err %v, snapshot %.200s", resp.StatusCode, err, b)
	}
}

func traceProducer(jobs chan<- int, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(jobs)

	for {
		select {
		case <-stop:
			return
		case jobs <- 1:
		}

		time.Sleep(2 * time.Millisecond)
	}
}

func traceWorker(jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
	}
}

func startPipeline(t *testing.T, workers int) {
	t.Helper()

	jobs := make(chan int)
	stop := make(chan struct{})

	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)

		go traceWorker(jobs, &wg)
	}

	wg.Add(1)

	go traceProducer(jobs, stop, &wg)

	t.Cleanup(func() {
		close(stop)
		wg.Wait()
	})
}

func idsByEntry(s ruvo.Snapshot, suffix string) map[string]bool {
	ids := map[string]bool{}

	for _, g := range s.Groups {
		if strings.HasSuffix(g.Entry, suffix) {
			ids[g.ID] = true
		}
	}

	return ids
}

func TestCLIRecordsTrace(t *testing.T) {
	target := pprofServer(t)
	startPipeline(t, 4)

	srv := httptest.NewServer(newHandler(target.URL))
	defer srv.Close()

	resp, err := client.Post(srv.URL+"/flow/api/trace?seconds=1", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)

	var s ruvo.Snapshot

	if err := json.Unmarshal(b, &s); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, err %v, body %.200s", resp.StatusCode, err, b)
	}

	if s.TraceFor != time.Second {
		t.Fatalf("traceFor %v, want 1s", s.TraceFor)
	}

	from, to := idsByEntry(s, ".traceProducer"), idsByEntry(s, ".traceWorker")
	if len(from) == 0 || len(to) == 0 {
		t.Fatalf("groups: %d producers, %d workers", len(from), len(to))
	}

	for _, e := range s.Edges {
		if e.Kind == ruvo.EdgeUnblock && from[e.From] && to[e.To] && e.Count > 0 {
			return
		}
	}

	t.Fatalf("no unblock edge from the producer to the workers: %+v", s.Edges)
}
