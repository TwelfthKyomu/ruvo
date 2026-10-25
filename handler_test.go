package ruvo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

func TestEventsDoNotRepeatFirstSnapshot(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	ats := make(chan time.Time, 4)

	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)

		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}

			var s Snapshot

			if json.Unmarshal([]byte(data), &s) == nil {
				ats <- s.At
			}
		}
	}()

	var first time.Time

	select {
	case first = <-ats:
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot event within 2s")
	}

	select {
	case at := <-ats:
		if at.Equal(first) {
			t.Fatalf("the first snapshot (at %v) was sent twice", first)
		}
	case <-time.After(300 * time.Millisecond):
	}
}

func TestEventsHeadDoesNotStream(t *testing.T) {
	srv, c := serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); resp.StatusCode != http.StatusOK || ct != "text/event-stream" {
		t.Fatalf("status %d, content type %q", resp.StatusCode, ct)
	}

	if n := c.subscribers(); n != 0 {
		t.Fatalf("%d subscribers after HEAD, want 0", n)
	}

	if code, _, hdr := call(t, http.MethodPost, srv.URL+"/api/events"); code != http.StatusMethodNotAllowed || hdr.Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST: status %d, Allow %q", code, hdr.Get("Allow"))
	}
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

func TestIndexLinksViews(t *testing.T) {
	v := probeView{path: make(chan string, 1), gotAPI: make(chan bool, 1)}
	srv, _ := serve(t, newFake(t, "stacks.txt", "", ""), v)

	code, body, hdr := call(t, http.MethodGet, srv.URL+"/")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("status %d, content type %q", code, hdr.Get("Content-Type"))
	}

	if !strings.Contains(body, `<nav id="views"><a href="./probe/">probe</a></nav>`) {
		t.Fatal("no link to the view in the nav")
	}
}

type namedView string

func (v namedView) Name() string { return string(v) }

func (namedView) Handler(api http.Handler) http.Handler { return http.NotFoundHandler() }

func TestIndexEscapesViewNames(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "", ""), namedView("b"), namedView(`a<"&`))

	_, body, _ := call(t, http.MethodGet, srv.URL+"/")
	if !strings.Contains(body, `<nav id="views"><a href="./a%3C%22&amp;/">a&lt;&#34;&amp;</a><a href="./b/">b</a></nav>`) {
		t.Fatal("view names are not escaped or not sorted in the nav")
	}
}

func TestIndexWithoutViewsKeepsEmptyNav(t *testing.T) {
	srv, _ := serve(t, newFake(t, "stacks.txt", "", ""))

	_, body, _ := call(t, http.MethodGet, srv.URL+"/")
	if !strings.Contains(body, `<nav id="views"></nav>`) {
		t.Fatal("the empty nav is gone")
	}
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

func noRedirect() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func location(t *testing.T, url string) (int, string) {
	t.Helper()

	resp, err := noRedirect().Get(url)
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	loc, err := resp.Location()
	if err != nil {
		return resp.StatusCode, ""
	}

	return resp.StatusCode, loc.RequestURI()
}

func TestViewWithoutSlashRedirects(t *testing.T) {
	v := probeView{path: make(chan string, 8), gotAPI: make(chan bool, 1)}

	h := NewHandler(Source{src: newFake(t, "stacks.txt", "", "")}, v).(*handler)
	t.Cleanup(h.c.close)

	mux := http.NewServeMux()
	mux.Handle("/", h)
	mux.Handle("/debug/ruvo/", h)
	mux.Handle("/x/", http.StripPrefix("/x", h))
	mux.Handle("/y/", http.StripPrefix("/y/", h))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	for in, want := range map[string]string{
		"/probe":             "/probe/",
		"/probe?a=1&b=":      "/probe/?a=1&b=",
		"/debug/ruvo/probe":  "/debug/ruvo/probe/",
		"/x/probe?seconds=3": "/x/probe/?seconds=3",
		"/y/probe":           "/y/probe/",
	} {
		code, loc := location(t, srv.URL+in)
		if (code != http.StatusMovedPermanently && code != http.StatusPermanentRedirect) || loc != want {
			t.Errorf("GET %s: status %d, location %q; want a redirect to %q", in, code, loc, want)
		}
	}

	if code, _, _ := call(t, http.MethodGet, srv.URL+"/probe"); code != http.StatusOK {
		t.Fatalf("following the redirect: status %d", code)
	}

	select {
	case got := <-v.path:
		if got != "/" {
			t.Fatalf("view saw path %q, want /", got)
		}
	default:
		t.Fatal("the view was not reached")
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

func TestLocalHandlerHidesItself(t *testing.T) {
	h := Handler().(*handler)
	t.Cleanup(h.c.close)

	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1<<20)

	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			break
		}
	}

	h.c.sample(context.Background(), false)

	_, body, _ := call(t, http.MethodGet, srv.URL+"/api/snapshot")

	var s Snapshot

	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatal(err)
	}

	if len(s.Groups) == 0 {
		t.Fatal("no groups at all")
	}

	for _, g := range s.Groups {
		for _, line := range g.Stack {
			if strings.HasPrefix(line, "github.com/TwelfthKyomu/ruvo.(*handler)") || strings.HasPrefix(line, "github.com/TwelfthKyomu/ruvo.(*collector)") {
				t.Fatalf("ruvo's own goroutine is in group %s: %v", g.WaitSite, g.Stack)
			}
		}
	}
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

func readEvents(body io.Reader) <-chan struct{} {
	events := make(chan struct{}, 64)

	go func() {
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 64*1024), 8<<20)

		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "data: ") {
				events <- struct{}{}
			}
		}

		close(events)
	}()

	return events
}

func TestEventsSurviveIdleHTTP2(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")

	h := NewHandler(Source{src: src}).(*handler)
	h.writeTimeout = 100 * time.Millisecond
	t.Cleanup(h.c.close)

	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol %s, want HTTP/2", resp.Proto)
	}

	events := readEvents(resp.Body)
	<-events

	time.Sleep(400 * time.Millisecond)

	src.set(func(f *fakeSource) { f.debug1 = stacksText(3) })
	h.c.sample(context.Background(), false)

	select {
	case _, ok := <-events:
		if !ok {
			t.Fatal("the idle HTTP/2 stream was closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event after an idle spell")
	}
}

func manyGroups(n int) []byte {
	var b strings.Builder

	fmt.Fprintf(&b, "goroutine profile: total %d\n", n)

	for i := range n {
		fmt.Fprintf(&b, "1 @ 0x1\n#\t0x1\tmain.wait%d+0x1\t/home/dev/a.go:%d\n#\t0x1\tmain.entry%d+0x1\t/home/dev/b.go:%d\n\n", i, i+1, i, i+1)
	}

	return []byte(b.String())
}

func TestStuckReaderIsDropped(t *testing.T) {
	src := &fakeSource{debug1: manyGroups(5000)}

	h := NewHandler(Source{src: src}).(*handler)
	h.writeTimeout = 200 * time.Millisecond
	t.Cleanup(h.c.close)

	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return h.c.subscribers() == 1 })

	for i := 0; h.c.subscribers() > 0; i++ {
		if i == 200 {
			t.Fatal("a reader that never reads is still subscribed")
		}

		src.set(func(f *fakeSource) { f.debug1 = manyGroups(5000 + i%2) })
		h.c.sample(context.Background(), false)
		time.Sleep(20 * time.Millisecond)
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

type readerView struct {
	err error
}

func (readerView) Name() string { return "reader" }

func (readerView) Handler(api http.Handler) http.Handler { return http.NotFoundHandler() }

func (v readerView) ReadTrace(trace io.Reader, stacks map[int64][]Frame) ([]Wakeup, error) {
	return nil, v.err
}

func TestTraceEndpoint(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	srv, _ := serve(t, src, readerView{})

	code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace?seconds=2")
	if code != http.StatusOK {
		t.Fatalf("status %d, body %.120s", code, body)
	}

	var s Snapshot

	if err := json.Unmarshal([]byte(body), &s); err != nil || s.TraceFor != 2*time.Second || !strings.Contains(body, `"traceFor":2000000000`) {
		t.Fatalf("%v: body %.200s", err, body)
	}

	src.mu.Lock()
	d := src.traceD
	src.mu.Unlock()

	if d != 2*time.Second {
		t.Fatalf("trace asked for %v, want 2s", d)
	}

	src = newFake(t, "stacks.txt", "full.txt", "")
	srv, _ = serve(t, src, readerView{})

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace"); code != http.StatusOK || !strings.Contains(body, `"traceFor":5000000000`) {
		t.Fatalf("no seconds: status %d, body %.200s", code, body)
	}

	src.mu.Lock()
	d = src.traceD
	src.mu.Unlock()

	if d != 5*time.Second {
		t.Fatalf("trace asked for %v, want the 5s default", d)
	}

	if code, _, hdr := call(t, http.MethodGet, srv.URL+"/api/trace"); code != http.StatusMethodNotAllowed || hdr.Get("Allow") != http.MethodPost {
		t.Fatalf("GET: status %d, Allow %q", code, hdr.Get("Allow"))
	}
}

func TestTraceEndpointErrors(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	srv, _ := serve(t, src, readerView{})

	for _, q := range []string{"0", "31", "abc", "-1", "2.5", "9223372036854775807", "18446744074"} {
		if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace?seconds="+q); code != http.StatusBadRequest {
			t.Errorf("seconds=%s: status %d, body %q, want 400", q, code, body)
		}
	}

	if n := src.count("trace"); n != 0 {
		t.Fatalf("%d traces for bad requests, want 0", n)
	}

	srv, _ = serve(t, newFake(t, "stacks.txt", "full.txt", ""))

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace"); code != http.StatusNotFound || !strings.Contains(body, "no view reads traces: mount flow.View()") {
		t.Fatalf("no reader: status %d, body %q", code, body)
	}

	src = newFake(t, "stacks.txt", "full.txt", "")
	src.traceErr = fmt.Errorf("%w: GET …/debug/pprof/trace?seconds=5: 404 Not Found", collect.ErrNoTrace)
	srv, _ = serve(t, src, readerView{})

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace"); code != http.StatusBadGateway || !strings.Contains(body, "trace profile is not available") {
		t.Fatalf("trace error: status %d, body %q", code, body)
	}

	srv, _ = serve(t, newFake(t, "stacks.txt", "full.txt", ""), readerView{err: errors.New("bad header")})

	if code, body, _ := call(t, http.MethodPost, srv.URL+"/api/trace"); code != http.StatusBadGateway || !strings.Contains(body, "read trace: bad header") {
		t.Fatalf("reader error: status %d, body %q", code, body)
	}
}
