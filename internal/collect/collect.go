package collect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Source interface {
	Goroutines(ctx context.Context, debug int) ([]byte, error)
	Leaks(ctx context.Context) ([]byte, error)
	Trace(ctx context.Context, d time.Duration) ([]byte, error)
	Name() string
}

var (
	ErrNoLeakProfile = errors.New("goroutineleak profile is not available")
	ErrNoTrace       = errors.New("trace profile is not available")
)

const (
	minStackBuf     = 1 << 20
	maxStackBuf     = 64 << 20
	stackPerRoutine = 1024
	maxBody         = 80 << 20
	maxErrBody      = 200
	drainErrBody    = 4096
)

func Local() Source {
	return &local{stack: runtime.Stack, count: runtime.NumGoroutine, limit: maxBody}
}

type local struct {
	stack func(buf []byte, all bool) int
	count func() int
	limit int64

	mu   sync.Mutex
	last int
}

func (l *local) Goroutines(ctx context.Context, debug int) ([]byte, error) {
	if debug < 2 {
		return write("goroutine", debug)
	}

	return l.full(), nil
}

func (l *local) full() []byte {
	l.mu.Lock()
	size := l.last * 5 / 4
	l.mu.Unlock()

	if size == 0 && l.count != nil {
		size = l.count() * stackPerRoutine
	}

	size = min(max(size, minStackBuf), maxStackBuf)

	for {
		buf := make([]byte, size)

		n := l.stack(buf, true)
		if n < len(buf) || size >= maxStackBuf {
			l.mu.Lock()
			l.last = n
			l.mu.Unlock()

			return buf[:n]
		}

		size = min(size*2, maxStackBuf)
	}
}

func (l *local) Leaks(ctx context.Context) ([]byte, error) {
	return write("goroutineleak", 1)
}

func (l *local) Trace(ctx context.Context, d time.Duration) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w := &capWriter{limit: l.limit, full: make(chan struct{})}

	if err := trace.Start(w); err != nil {
		return nil, err
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-w.full:
	case <-ctx.Done():
		trace.Stop()

		return nil, ctx.Err()
	}

	trace.Stop()

	return w.result()
}

func (l *local) Name() string {
	return "this process"
}

func (l *local) GoVersion() string {
	return runtime.Version()
}

type capWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	limit int64
	over  bool
	full  chan struct{}
}

func (w *capWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.over {
		return len(p), nil
	}

	if int64(w.buf.Len()+len(p)) > w.limit {
		w.over = true
		close(w.full)

		return len(p), nil
	}

	return w.buf.Write(p)
}

func (w *capWriter) result() ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.over {
		return nil, fmt.Errorf("trace larger than %d bytes", w.limit)
	}

	return w.buf.Bytes(), nil
}

func write(profile string, debug int) ([]byte, error) {
	p := pprof.Lookup(profile)
	if p == nil {
		return nil, ErrNoLeakProfile
	}

	var b bytes.Buffer

	if err := p.WriteTo(&b, debug); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func Remote(baseURL string) Source {
	base := strings.TrimRight(baseURL, "/")

	name := hidePassword(base)
	if u, err := url.Parse(base); err == nil {
		name = u.Redacted()
	}

	return &remote{
		base:   base,
		name:   name,
		client: &http.Client{},
		limit:  maxBody,
	}
}

func hidePassword(s string) string {
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok {
		return s
	}

	authority := rest
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority = rest[:i]
	}

	at := strings.LastIndexByte(authority, '@')
	if at < 0 {
		return s
	}

	colon := strings.IndexByte(authority[:at], ':')
	if colon < 0 {
		return s
	}

	return scheme + "://" + rest[:colon+1] + "xxxxx" + rest[at:]
}

type remote struct {
	base   string
	name   string
	client *http.Client
	limit  int64
}

func (r *remote) Goroutines(ctx context.Context, debug int) ([]byte, error) {
	return r.get(ctx, fmt.Sprintf("/debug/pprof/goroutine?debug=%d", debug))
}

func (r *remote) Leaks(ctx context.Context) ([]byte, error) {
	b, err := r.get(ctx, "/debug/pprof/goroutineleak?debug=1")

	var se statusError
	if errors.As(err, &se) && se.code == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %v", ErrNoLeakProfile, err)
	}

	return b, err
}

func (r *remote) Trace(ctx context.Context, d time.Duration) ([]byte, error) {
	seconds := max(1, int(math.Ceil(d.Seconds())))

	b, err := r.get(ctx, "/debug/pprof/trace?seconds="+strconv.Itoa(seconds))

	var se statusError
	if errors.As(err, &se) && se.code == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %v", ErrNoTrace, err)
	}

	return b, err
}

func (r *remote) Name() string {
	return r.name
}

func (r *remote) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base+path, nil)
	if err != nil {
		return nil, fmt.Errorf("GET %s: invalid URL", r.name+path)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, fmt.Errorf("%s %s: %w", ue.Op, r.name+path, ue.Err)
		}

		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, drainErrBody))
		b = b[:min(len(b), maxErrBody)]

		return nil, statusError{url: r.name + path, code: resp.StatusCode, status: resp.Status, body: strings.TrimSpace(string(b))}
	}

	b, err := io.ReadAll(io.LimitReader(resp.Body, r.limit+1))
	if err != nil {
		return nil, err
	}

	if int64(len(b)) > r.limit {
		return nil, fmt.Errorf("GET %s: response larger than %d bytes", r.name+path, r.limit)
	}

	return b, nil
}

type statusError struct {
	url    string
	code   int
	status string
	body   string
}

func (e statusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("GET %s: %s", e.url, e.status)
	}

	return fmt.Sprintf("GET %s: %s: %s", e.url, e.status, e.body)
}
