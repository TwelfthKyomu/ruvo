package flow_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo"
	"github.com/TwelfthKyomu/ruvo/flow"
)

var client = &http.Client{Timeout: time.Minute}

func newServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(ruvo.NewHandler(ruvo.Local(), flow.View()))
	t.Cleanup(srv.Close)

	return srv
}

func call(t *testing.T, method, url string) (int, []byte, http.Header) {
	t.Helper()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, b, resp.Header
}

func snapshot(t *testing.T, method, url string) ruvo.Snapshot {
	t.Helper()

	code, body, hdr := call(t, method, url)
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		t.Fatalf("status %d, content type %q: %s", code, hdr.Get("Content-Type"), body)
	}

	var s ruvo.Snapshot

	if err := json.Unmarshal(body, &s); err != nil {
		t.Fatalf("%v: %.200s", err, body)
	}

	return s
}

type pipeline struct {
	jobs chan int
	stop chan struct{}
	wg   sync.WaitGroup
}

func newPipeline(t *testing.T, workers int) *pipeline {
	t.Helper()

	p := &pipeline{jobs: make(chan int), stop: make(chan struct{})}

	for range workers {
		p.wg.Add(1)

		go worker(p.jobs, &p.wg)
	}

	t.Cleanup(func() {
		close(p.stop)
		p.wg.Wait()
	})

	return p
}

func (p *pipeline) produce(delay time.Duration, n int) {
	p.wg.Add(1)

	go producer(p.jobs, p.stop, &p.wg, delay, n)
}

func producer(jobs chan<- int, stop <-chan struct{}, wg *sync.WaitGroup, delay time.Duration, n int) {
	defer wg.Done()
	defer close(jobs)

	time.Sleep(delay)

	for i := 0; i != n; i++ {
		select {
		case <-stop:
			return
		default:
		}

		jobs <- i

		time.Sleep(2 * time.Millisecond)
	}

	<-stop
}

func worker(jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	for range jobs {
	}
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

func TestPageAndAPI(t *testing.T) {
	srv := newServer(t)

	code, body, hdr := call(t, http.MethodGet, srv.URL+"/flow/")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html; charset=utf-8") {
		t.Fatalf("status %d, content type %q", code, hdr.Get("Content-Type"))
	}

	if !strings.Contains(string(body), `id="graph"`) {
		t.Fatal("the page has no graph")
	}

	if s := snapshot(t, http.MethodGet, srv.URL+"/flow/api/snapshot"); s.Goroutines == 0 {
		t.Fatal("snapshot without goroutines")
	}

	for _, path := range []string{"/flow/x", "/flow/api", "/flow/ui/index.html"} {
		if code, _, _ := call(t, http.MethodGet, srv.URL+path); code != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", path, code)
		}
	}
}

func TestRecordTraceFindsHandoff(t *testing.T) {
	srv := newServer(t)
	p := newPipeline(t, 4)
	p.produce(2*time.Millisecond, -1)

	s := snapshot(t, http.MethodPost, srv.URL+"/flow/api/trace?seconds=1")
	if s.TraceFor != time.Second || s.TraceAt.IsZero() {
		t.Fatalf("traceFor %v, traceAt %v", s.TraceFor, s.TraceAt)
	}

	from, to := idsByEntry(s, "flow_test.producer"), idsByEntry(s, "flow_test.worker")
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

func TestRecordTraceSeesWaitersBlockedBeforeStart(t *testing.T) {
	srv := newServer(t)
	p := newPipeline(t, 4)

	time.Sleep(100 * time.Millisecond)
	p.produce(600*time.Millisecond, 4)

	s := snapshot(t, http.MethodPost, srv.URL+"/flow/api/trace?seconds=1")
	to := idsByEntry(s, "flow_test.worker")

	for _, e := range s.Edges {
		if e.Kind == ruvo.EdgeUnblock && to[e.To] && e.Waiters >= 4 {
			return
		}
	}

	t.Fatalf("no edge into the workers with 4 waiters: %+v", s.Edges)
}
