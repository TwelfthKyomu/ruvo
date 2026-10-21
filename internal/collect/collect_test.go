package collect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
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
