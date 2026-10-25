package wake

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/TwelfthKyomu/ruvo"
	"golang.org/x/exp/trace"
)

var errMalformed = errors.New("malformed trace")

var systemReasons = map[string]bool{
	"system goroutine wait":        true,
	"GC background sweeper wait":   true,
	"GC mark assist wait for work": true,
	"wait until GC ends":           true,
	"GC weak to strong wait":       true,
}

type stack struct {
	frames []ruvo.Frame
	key    string
	system bool
	net    bool
}

type goroutine struct {
	blocked bool
	system  bool
	wait    *stack
	reason  string
	since   trace.Time
	home    *stack
}

type edgeKey struct {
	cause         string
	waker, waiter *stack
}

type edge struct {
	count   int
	waiters map[trace.GoID]struct{}
	wait    time.Duration
}

type parser struct {
	stacks map[int64][]ruvo.Frame
	dumped map[int64]*stack
	traced map[trace.Stack]*stack
	keys   map[string]*stack
	buf    []byte
	gs     map[trace.GoID]*goroutine
	edges  map[edgeKey]*edge
	start  trace.Time
	syncs  int
	later  int
}

func Read(r io.Reader, stacks map[int64][]ruvo.Frame) ([]ruvo.Wakeup, error) {
	ws, _, err := read(r, stacks)

	return ws, err
}

func read(r io.Reader, stacks map[int64][]ruvo.Frame) (ws []ruvo.Wakeup, unknownLater int, err error) {
	defer catch(&err)

	tr, err := trace.NewReader(r)
	if err != nil {
		return nil, 0, err
	}

	a := &parser{
		stacks: stacks,
		dumped: map[int64]*stack{},
		traced: map[trace.Stack]*stack{},
		keys:   map[string]*stack{},
		gs:     map[trace.GoID]*goroutine{},
		edges:  map[edgeKey]*edge{},
	}

	for {
		ev, err := tr.ReadEvent()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, 0, err
		}

		a.event(ev)
	}

	return a.wakeups(), a.later, nil
}

func (a *parser) event(ev trace.Event) {
	switch ev.Kind() {
	case trace.EventSync:
		if a.syncs == 0 {
			a.start = ev.Time()
		}

		a.syncs++
		clear(a.traced)

		return
	case trace.EventStateTransition:
	default:
		return
	}

	st := ev.StateTransition()
	if st.Resource.Kind != trace.ResourceGoroutine {
		return
	}

	id := st.Resource.Goroutine()
	from, to := st.Goroutine()

	switch {
	case to == trace.GoNotExist:
		delete(a.gs, id)
	case to == trace.GoWaiting && from == trace.GoNotExist:
		a.block(a.track(id), nil, "", ev.Time())
	case to == trace.GoWaiting && (from == trace.GoUndetermined || from == trace.GoWaiting):
		a.status(a.track(id), a.fromTrace(st.Stack))
	case to == trace.GoWaiting:
		a.block(a.track(id), a.fromTrace(st.Stack), st.Reason, ev.Time())
	case from == trace.GoWaiting && to == trace.GoRunnable:
		a.wake(id, ev)
	}
}

func (a *parser) track(id trace.GoID) *goroutine {
	g := a.gs[id]
	if g == nil {
		g = &goroutine{}
		a.gs[id] = g
	}

	return g
}

func (a *parser) status(g *goroutine, s *stack) {
	if g.system {
		return
	}

	if !g.blocked {
		a.block(g, s, "", a.start)
		return
	}

	if g.wait == nil && s != nil {
		a.block(g, s, g.reason, g.since)
	}
}

func (a *parser) block(g *goroutine, s *stack, reason string, since trace.Time) {
	if systemReasons[reason] || s != nil && s.system {
		g.blocked, g.system = false, true
		return
	}

	g.blocked, g.system = true, false
	g.wait, g.reason, g.since = s, reason, since

	if s != nil {
		g.home = s
	}
}

func (a *parser) wake(id trace.GoID, ev trace.Event) {
	g := a.track(id)
	if g.system {
		return
	}

	waiter, reason, since := g.wait, g.reason, g.since
	g.blocked, g.wait, g.reason, g.since = false, nil, "", 0

	if waiter == nil {
		waiter = a.fromDump(id)
	}

	if waiter == nil {
		if a.syncs > 1 {
			a.later++
		}

		return
	}

	if waiter.system {
		return
	}

	cause := ruvo.CauseTimer

	var waker *stack

	switch w := ev.Goroutine(); {
	case w != trace.NoGoroutine:
		if waker = a.waker(w, ev); waker == nil || waker.system {
			return
		}

		cause = ruvo.CauseGoroutine
	case reason == "network", reason == "" && waiter.net:
		cause = ruvo.CauseNetwork
	}

	k := edgeKey{cause: cause, waker: waker, waiter: waiter}

	e := a.edges[k]
	if e == nil {
		e = &edge{waiters: map[trace.GoID]struct{}{}}
		a.edges[k] = e
	}

	e.count++
	e.waiters[id] = struct{}{}

	if d := ev.Time().Sub(max(since, a.start)); d > 0 {
		e.wait += d
	}
}

func (a *parser) waker(id trace.GoID, ev trace.Event) *stack {
	if g := a.gs[id]; g != nil && g.home != nil {
		return g.home
	}

	if s := a.fromDump(id); s != nil {
		return s
	}

	return a.fromTrace(ev.Stack())
}

func (a *parser) fromDump(id trace.GoID) *stack {
	fs := a.stacks[int64(id)]
	if len(fs) == 0 {
		return nil
	}

	s, ok := a.dumped[int64(id)]
	if !ok {
		s = a.intern(fs)
		a.dumped[int64(id)] = s
	}

	return s
}

func (a *parser) fromTrace(ts trace.Stack) *stack {
	if ts == trace.NoStack {
		return nil
	}

	if s, ok := a.traced[ts]; ok {
		return s
	}

	var fs []ruvo.Frame

	for f := range ts.Frames() {
		fs = append(fs, ruvo.Frame{Func: f.Func, File: f.File, Line: int(f.Line)})
	}

	s := a.intern(fs)
	a.traced[ts] = s

	return s
}

func (a *parser) intern(fs []ruvo.Frame) *stack {
	if len(fs) == 0 {
		return nil
	}

	a.buf = a.buf[:0]

	for i, f := range fs {
		if i > 0 {
			a.buf = append(a.buf, '|')
		}

		a.buf = append(a.buf, f.Func...)
		a.buf = append(a.buf, ':')
		a.buf = strconv.AppendInt(a.buf, int64(f.Line), 10)
	}

	if s, ok := a.keys[string(a.buf)]; ok {
		return s
	}

	s := &stack{frames: fs, key: string(a.buf), system: true}

	for _, f := range fs {
		if !strings.HasPrefix(f.Func, "runtime.") && !strings.HasPrefix(f.Func, "internal/runtime/") {
			s.system = false
		}

		if f.Func == "internal/poll.runtime_pollWait" {
			s.net = true
		}
	}

	a.keys[s.key] = s

	return s
}

func (a *parser) wakeups() []ruvo.Wakeup {
	type pair struct {
		k edgeKey
		e *edge
	}

	ps := make([]pair, 0, len(a.edges))

	for k, e := range a.edges {
		ps = append(ps, pair{k, e})
	}

	slices.SortFunc(ps, func(x, y pair) int {
		return cmp.Or(
			cmp.Compare(y.e.wait, x.e.wait),
			cmp.Compare(y.e.count, x.e.count),
			strings.Compare(x.k.cause, y.k.cause),
			strings.Compare(key(x.k.waker), key(y.k.waker)),
			strings.Compare(key(x.k.waiter), key(y.k.waiter)),
		)
	})

	ws := make([]ruvo.Wakeup, len(ps))

	for i, p := range ps {
		ws[i] = ruvo.Wakeup{
			Cause:   p.k.cause,
			Waiter:  p.k.waiter.frames,
			Count:   p.e.count,
			Waiters: len(p.e.waiters),
			Wait:    p.e.wait,
		}

		if p.k.waker != nil {
			ws[i].Waker = p.k.waker.frames
		}
	}

	return ws
}

func key(s *stack) string {
	if s == nil {
		return ""
	}

	return s.key
}

func catch(err *error) {
	p := recover()
	if p == nil {
		return
	}

	if !fromTraceReader() {
		panic(p)
	}

	*err = fmt.Errorf("%w: %v", errMalformed, p)
}

func fromTraceReader() bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs)])
	panicking := false

	for {
		f, more := frames.Next()

		switch {
		case f.Function == "runtime.gopanic":
			panicking = true
		case !panicking, strings.HasPrefix(f.Function, "runtime."):
		default:
			return strings.HasPrefix(f.Function, "golang.org/x/exp/trace.") || strings.HasPrefix(f.Function, "golang.org/x/exp/trace/")
		}

		if !more {
			return false
		}
	}
}
