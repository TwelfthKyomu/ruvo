package ruvo

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
)

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

func serve(t *testing.T, src *fakeSource, views ...View) (*httptest.Server, *collector) {
	t.Helper()

	h := NewHandler(Source{src: src}, views...).(*handler)
	t.Cleanup(h.c.close)

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	return srv, h.c
}

func call(t *testing.T, method, url string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, string(b), resp.Header
}

func TestSnapshotJSON(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	code, body, hdr := call(t, http.MethodGet, srv.URL+"/api/snapshot")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		t.Fatalf("status %d, content type %q", code, hdr.Get("Content-Type"))
	}

	if !strings.Contains(body, `"goroutines":26`) || !strings.Contains(body, `"edges":[]`) {
		t.Fatalf("body %s", body)
	}
}

func TestEventsStreamsSnapshots(t *testing.T) {
	srv, c := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	got := make(chan string, 1)

	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)

		for sc.Scan() {
			if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				got <- data
				return
			}
		}
	}()

	select {
	case data := <-got:
		var s Snapshot

		if err := json.Unmarshal([]byte(data), &s); err != nil || s.Goroutines != 26 {
			t.Fatalf("event %v: %.80s", err, data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot event within 2s")
	}

	cancel()

	waitFor(t, 2*time.Second, func() bool { return c.subscribers() == 0 })
}

func TestLeaksEndpoint(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "full.txt", "leak.txt"))

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/leaks"); code != http.StatusOK || !strings.Contains(body, `"leaksAt"`) {
		t.Fatalf("POST: status %d, body %.120s", code, body)
	}

	if code, _, _ := call(t, http.MethodGet, srv.URL+"/api/leaks"); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: status %d, want 405", code)
	}
}

func TestLeaksUnavailableIs502(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	src.leaksErr = fmt.Errorf("%w: GET …: 404 Not Found", collect.ErrNoLeakProfile)

	srv, _ := serve(t, src)

	code, body, _ := call(t, http.MethodPost, srv.URL+"/api/leaks")
	if code != http.StatusBadGateway || !strings.Contains(body, "goroutineleak") {
		t.Fatalf("status %d, body %q", code, body)
	}
}

func TestFullEndpoint(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/full"); code != http.StatusOK || !strings.Contains(body, `"fullAt"`) {
		t.Fatalf("status %d, body %.120s", code, body)
	}
}

func TestMountedUnderPrefix(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")

	h := NewHandler(Source{src: src}).(*handler)
	t.Cleanup(h.c.close)

	mux := http.NewServeMux()
	mux.Handle("/debug/ruvo/", h)
	mux.Handle("/x/", http.StripPrefix("/x", h))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	if code, _, _ := call(t, http.MethodGet, srv.URL+"/debug/ruvo/api/snapshot"); code != http.StatusOK {
		t.Fatalf("prefixed api: status %d", code)
	}

	if code, _, hdr := call(t, http.MethodGet, srv.URL+"/debug/ruvo/"); code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("prefixed page: status %d, %q", code, hdr.Get("Content-Type"))
	}

	if code, _, _ := call(t, http.MethodGet, srv.URL+"/x/api/snapshot"); code != http.StatusOK {
		t.Fatalf("StripPrefix api: status %d", code)
	}
}

type probeView struct {
	path   chan string
	gotAPI chan bool
}

func (probeView) Name() string { return "probe" }

func (p probeView) Handler(api http.Handler) http.Handler {
	p.gotAPI <- api != nil

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.path <- r.URL.Path
	})
}

func TestViewsMounted(t *testing.T) {
	v := probeView{path: make(chan string, 1), gotAPI: make(chan bool, 1)}
	srv, _ := serve(t, newFake(t, "stacks.txt", "", ""), v)

	if !<-v.gotAPI {
		t.Fatal("view got a nil api handler")
	}

	call(t, http.MethodGet, srv.URL+"/probe/")

	if got := <-v.path; got != "/" {
		t.Fatalf("view saw path %q, want /", got)
	}
}

func TestUnknownPath404(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "", ""))

	if code, _, _ := call(t, http.MethodGet, srv.URL+"/nope"); code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", code)
	}
}

func parked(ch chan struct{}) {
	<-ch
}

func TestLocalHandlerSeesParked(t *testing.T) {
	ch := make(chan struct{})
	defer close(ch)

	for range 5 {
		go parked(ch)
	}

	h := Handler().(*handler)
	t.Cleanup(h.c.close)

	srv := httptest.NewServer(h)
	defer srv.Close()

	_, body, _ := call(t, http.MethodGet, srv.URL+"/api/snapshot")

	var s Snapshot

	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatal(err)
	}

	for _, g := range s.Groups {
		if strings.Contains(g.WaitSite, "ruvo.parked") && g.Count >= 5 {
			return
		}
	}

	t.Fatalf("no group for parked goroutines in %d groups", len(s.Groups))
}

func TestCrossOriginPostRejected(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	post := func(site string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/full", nil)
		req.Header.Set("Sec-Fetch-Site", site)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		resp.Body.Close()

		return resp.StatusCode
	}

	if code := post("cross-site"); code != http.StatusForbidden {
		t.Fatalf("cross-site POST: status %d, want 403", code)
	}

	if code := post("same-origin"); code != http.StatusOK {
		t.Fatalf("same-origin POST: status %d, want 200", code)
	}
}

func TestStrippedMounts(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")

	h := NewHandler(Source{src: src}).(*handler)
	t.Cleanup(h.c.close)

	mux := http.NewServeMux()
	mux.Handle("/y/", http.StripPrefix("/y/", h))
	mux.Handle("/api/", http.StripPrefix("/api", h))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{"/y/api/snapshot", "/api/api/snapshot"} {
		if code, _, _ := call(t, http.MethodGet, srv.URL+path); code != http.StatusOK {
			t.Errorf("GET %s: status %d, want 200", path, code)
		}
	}
}

func TestEventsSurviveWriteTimeout(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")

	h := NewHandler(Source{src: src}).(*handler)
	t.Cleanup(h.c.close)

	srv := httptest.NewUnstartedServer(h)
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.Start()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	events := make(chan struct{}, 4)

	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)

		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data: ") {
				events <- struct{}{}
			}
		}
	}()

	<-events
	time.Sleep(300 * time.Millisecond)

	for len(events) > 0 {
		<-events
	}

	h.c.sample(context.Background(), false)

	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("the stream was cut by the server's WriteTimeout")
	}
}

func TestZeroSourceIsLocal(t *testing.T) {
	h := NewHandler(Source{}).(*handler)
	t.Cleanup(h.c.close)

	srv := httptest.NewServer(h)
	defer srv.Close()

	if code, body, _ := call(t, http.MethodGet, srv.URL+"/api/snapshot"); code != http.StatusOK || !strings.Contains(body, `"target":"this process"`) {
		t.Fatalf("status %d, body %.120s", code, body)
	}
}
