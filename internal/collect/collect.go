package collect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type Source interface {
	Goroutines(ctx context.Context, debug int) ([]byte, error)
	Leaks(ctx context.Context) ([]byte, error)
	Name() string
}

var ErrNoLeakProfile = errors.New("goroutineleak profile is not available")

const (
	minStackBuf     = 1 << 20
	maxStackBuf     = 64 << 20
	stackPerRoutine = 1024
	maxBody         = 80 << 20
)

func Local() Source {
	return &local{stack: runtime.Stack, count: runtime.NumGoroutine}
}

type local struct {
	stack func(buf []byte, all bool) int
	count func() int

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

func (l *local) Name() string {
	return "this process"
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

	name := base
	if u, err := url.Parse(base); err == nil {
		name = u.Redacted()
	}

	return &remote{
		base:   base,
		name:   name,
		client: &http.Client{Timeout: 30 * time.Second},
		limit:  maxBody,
	}
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

func (r *remote) Name() string {
	return r.name
}

func (r *remote) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base+path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

		return nil, statusError{url: r.name + path, code: resp.StatusCode, status: resp.Status}
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
}

func (e statusError) Error() string {
	return fmt.Sprintf("GET %s: %s", e.url, e.status)
}
