package collect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime/trace"
	"strings"
	"testing"
	"time"
)

func TestLocalGoroutines(t *testing.T) {
	ctx := context.Background()

	b1, err := Local().Goroutines(ctx, 1)
	if err != nil || !strings.HasPrefix(string(b1), "goroutine profile: total ") {
		t.Fatalf("debug 1: %v, %.40q", err, b1)
	}

	b2, err := Local().Goroutines(ctx, 2)
	if err != nil || !strings.HasPrefix(string(b2), "goroutine ") || strings.HasPrefix(string(b2), "goroutine profile") {
		t.Fatalf("debug 2: %v, %.40q", err, b2)
	}
}

func TestLocalLeaks(t *testing.T) {
	b, err := Local().Leaks(context.Background())
	if err != nil || !strings.HasPrefix(string(b), "goroutineleak profile: total ") {
		t.Fatalf("leaks: %v, %.40q", err, b)
	}
}

func TestRemoteGoroutines(t *testing.T) {
	var uri string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uri = r.URL.RequestURI()
		io.WriteString(w, "goroutine profile: total 1\n")
	}))
	defer srv.Close()

	b, err := Remote(srv.URL+"/").Goroutines(context.Background(), 1)
	if err != nil || string(b) != "goroutine profile: total 1\n" {
		t.Fatalf("body %q, err %v", b, err)
	}

	if uri != "/debug/pprof/goroutine?debug=1" {
		t.Fatalf("requested %q", uri)
	}
}

func TestRemoteNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := Remote(srv.URL).Goroutines(context.Background(), 2)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err %v, want a 500 error", err)
	}
}

func TestRemoteNoLeakProfile(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	if _, err := Remote(srv.URL).Leaks(context.Background()); !errors.Is(err, ErrNoLeakProfile) {
		t.Fatalf("err %v, want ErrNoLeakProfile", err)
	}
}

func TestRemoteClientHasNoTimeout(t *testing.T) {
	if got := Remote("http://h:6060").(*remote).client.Timeout; got != 0 {
		t.Fatalf("client timeout %v, want none: the context sets the deadline", got)
	}
}

func TestRemoteName(t *testing.T) {
	if got := Remote("http://h:6060/").Name(); got != "http://h:6060" {
		t.Fatalf("name %q", got)
	}
}

func TestLocalFullDumpSizesBufferFromLastDump(t *testing.T) {
	const need = 3 << 20

	var sizes []int

	l := &local{stack: func(buf []byte, all bool) int {
		sizes = append(sizes, len(buf))

		return min(len(buf), need)
	}}

	b, err := l.Goroutines(context.Background(), 2)
	if err != nil || len(b) != need || !reflect.DeepEqual(sizes, []int{1 << 20, 2 << 20, 4 << 20}) {
		t.Fatalf("first dump: %d bytes, err %v, buffers %v", len(b), err, sizes)
	}

	sizes = nil

	if _, err := l.Goroutines(context.Background(), 2); err != nil || !reflect.DeepEqual(sizes, []int{need * 5 / 4}) {
		t.Fatalf("second dump: err %v, buffers %v — want one stop-the-world walk", err, sizes)
	}
}

func TestLocalFirstFullDumpEstimatesBuffer(t *testing.T) {
	const need = 2_500_000

	var sizes []int

	l := &local{
		count: func() int { return 3000 },
		stack: func(buf []byte, all bool) int {
			sizes = append(sizes, len(buf))

			return min(len(buf), need)
		},
	}

	if _, err := l.Goroutines(context.Background(), 2); err != nil || !reflect.DeepEqual(sizes, []int{3000 * 1024}) {
		t.Fatalf("err %v, buffers %v — the first dump should be sized from the goroutine count", err, sizes)
	}
}

func TestRemoteKeepsPathPrefix(t *testing.T) {
	var uri string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uri = r.URL.RequestURI()
		io.WriteString(w, "goroutine profile: total 1\n")
	}))
	defer srv.Close()

	if _, err := Remote(srv.URL+"/internal/").Goroutines(context.Background(), 1); err != nil || uri != "/internal/debug/pprof/goroutine?debug=1" {
		t.Fatalf("err %v, requested %q", err, uri)
	}
}

func TestRemoteSendsBasicAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "u" || p != "secret" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}

		io.WriteString(w, "goroutine profile: total 1\n")
	}))
	defer srv.Close()

	target := strings.Replace(srv.URL, "http://", "http://u:secret@", 1)

	if _, err := Remote(target).Goroutines(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteNameHidesPassword(t *testing.T) {
	if got := Remote("http://u:secret@h:6060/").Name(); strings.Contains(got, "secret") || !strings.HasPrefix(got, "http://u:") {
		t.Fatalf("name %q", got)
	}
}

func TestRemoteLimitsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat("x", 2000))
	}))
	defer srv.Close()

	r := Remote(srv.URL).(*remote)
	r.limit = 1000

	if _, err := r.Goroutines(context.Background(), 2); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("err %v, want a size-limit error", err)
	}
}

func TestLocalTrace(t *testing.T) {
	b, err := Local().Trace(context.Background(), 100*time.Millisecond)
	if err != nil || !strings.HasPrefix(string(b), "go 1.") {
		t.Fatalf("trace: %v, %.40q", err, b)
	}

	if trace.IsEnabled() {
		t.Fatal("tracing is still enabled")
	}
}

func TestLocalTraceBusy(t *testing.T) {
	if err := trace.Start(io.Discard); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(trace.Stop)

	if _, err := Local().Trace(context.Background(), 50*time.Millisecond); err == nil {
		t.Fatal("want an error while another trace is running")
	}

	if !trace.IsEnabled() {
		t.Fatal("the foreign trace was stopped")
	}
}

func TestLocalTraceCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()

	_, err := Local().Trace(ctx, 10*time.Second)
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatalf("err %v after %v, want context.Canceled right after the cancel", err, time.Since(start))
	}

	if trace.IsEnabled() {
		t.Fatal("tracing is still enabled")
	}
}

func TestLocalTraceCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Local().Trace(ctx, 10*time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}

	if trace.IsEnabled() {
		t.Fatal("tracing is still enabled")
	}

	if err := trace.Start(io.Discard); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(trace.Stop)

	if _, err := Local().Trace(ctx, 10*time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled without trying to start a trace", err)
	}

	if !trace.IsEnabled() {
		t.Fatal("the foreign trace was stopped")
	}
}

func TestLocalTraceLimit(t *testing.T) {
	l := &local{limit: 1024}

	start := time.Now()

	_, err := l.Trace(context.Background(), 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), "larger than 1024 bytes") || time.Since(start) > 2*time.Second {
		t.Fatalf("err %v after %v, want a size-limit error well before the duration ends", err, time.Since(start))
	}

	if trace.IsEnabled() {
		t.Fatal("tracing is still enabled")
	}
}

func TestRemoteTrace(t *testing.T) {
	var path, seconds string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, seconds = r.URL.Path, r.URL.Query().Get("seconds")
		io.WriteString(w, "go 1.27 trace")
	}))
	defer srv.Close()

	b, err := Remote(srv.URL).Trace(context.Background(), 2*time.Second)
	if err != nil || string(b) != "go 1.27 trace" {
		t.Fatalf("body %q, err %v", b, err)
	}

	if path != "/debug/pprof/trace" || seconds != "2" {
		t.Fatalf("requested %q, seconds %q", path, seconds)
	}
}

func TestRemoteTraceRoundsSecondsUp(t *testing.T) {
	var seconds string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seconds = r.URL.Query().Get("seconds")
	}))
	defer srv.Close()

	for _, c := range []struct {
		d    time.Duration
		want string
	}{
		{100 * time.Millisecond, "1"},
		{time.Second, "1"},
		{1500 * time.Millisecond, "2"},
		{30 * time.Second, "30"},
	} {
		if _, err := Remote(srv.URL).Trace(context.Background(), c.d); err != nil || seconds != c.want {
			t.Fatalf("d %v: seconds %q, err %v, want %q", c.d, seconds, err, c.want)
		}
	}
}

func TestRemoteTraceMissing(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	if _, err := Remote(srv.URL).Trace(context.Background(), time.Second); !errors.Is(err, ErrNoTrace) {
		t.Fatalf("err %v, want ErrNoTrace", err)
	}
}

func TestRemoteTraceLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat("x", 11))
	}))
	defer srv.Close()

	r := Remote(srv.URL).(*remote)
	r.limit = 10

	if _, err := r.Trace(context.Background(), time.Second); err == nil || !strings.Contains(err.Error(), "larger than 10 bytes") {
		t.Fatalf("err %v, want a size-limit error", err)
	}
}

func TestRemoteErrorShowsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "profile duration exceeds server's WriteTimeout", http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := Remote(srv.URL).Trace(context.Background(), time.Second)

	want := "GET " + srv.URL + "/debug/pprof/trace?seconds=1: 400 Bad Request: profile duration exceeds server's WriteTimeout"
	if err == nil || err.Error() != want {
		t.Fatalf("err %v, want %q", err, want)
	}
}

func TestRemoteErrorBodyIsShort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, strings.Repeat("x", 1000), http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := Remote(srv.URL).Goroutines(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), strings.Repeat("x", 200)) || strings.Contains(err.Error(), strings.Repeat("x", 201)) {
		t.Fatalf("err %v, want a body cut to 200 bytes", err)
	}
}

func TestRemoteErrorWithoutBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	_, err := Remote(srv.URL).Goroutines(context.Background(), 1)

	want := "GET " + srv.URL + "/debug/pprof/goroutine?debug=1: 502 Bad Gateway"
	if err == nil || err.Error() != want {
		t.Fatalf("err %v, want %q", err, want)
	}
}

func TestRemoteNameHidesPasswordOfUnparsableURL(t *testing.T) {
	got := Remote("http://u:secret@host/%zz").Name()
	if strings.Contains(got, "secret") || !strings.HasPrefix(got, "http://u:") || !strings.HasSuffix(got, "@host/%zz") {
		t.Fatalf("name = %q", got)
	}
}

func TestRemoteErrorsHidePassword(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()

	for _, base := range []string{"http://u:secret@host/%zz", strings.Replace(srv.URL, "http://", "http://u:secret@", 1)} {
		_, err := Remote(base).Goroutines(context.Background(), 1)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: err %v must not contain the password", base, err)
		}
	}
}
