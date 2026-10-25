package ruvo

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

var errNoTraceReader = errors.New("no view reads traces: mount flow.View()")

type collector struct {
	src    collect.Source
	reader TraceReader
	now    func() time.Time

	mu          sync.Mutex
	hist        history
	full        fullState
	leaks       leakState
	trace       traceState
	profile     dump.Profile
	profileAt   time.Time
	haveProfile bool
	down        bool
	readErr     string
	restart     bool
	last        Snapshot
	subs        map[chan message]struct{}
	running     bool
	active      time.Time
	interval    time.Duration
	fullEvery   time.Duration
	nextFull    time.Time
	fullTry     attempt
	leaksTry    attempt
	traceTry    attempt
	fullCap     int

	done      chan struct{}
	closeOnce sync.Once

	fmu     sync.Mutex
	flights map[string]*flight
}

type message struct {
	s    Snapshot
	data []byte
}

type flight struct {
	done chan struct{}
	val  any
	err  error
}

type attempt struct {
	at  time.Time
	err error
}

func (a attempt) within(now time.Time, d time.Duration) bool {
	return !a.at.IsZero() && now.Sub(a.at) < d
}

func (a attempt) recent(now time.Time) bool {
	return a.within(now, actionGap)
}

type fullDump struct {
	gs   []dump.Goroutine
	cost time.Duration
	cut  bool
}

func newCollector(src collect.Source) *collector {
	return &collector{
		src:       src,
		now:       time.Now,
		hist:      history{window: historyWindow},
		subs:      map[chan message]struct{}{},
		interval:  frequentMin,
		fullEvery: fullMin,
		fullCap:   fullDumpCap,
		done:      make(chan struct{}),
	}
}

func (c *collector) sample(ctx context.Context, withFull bool) Snapshot {
	at := c.now()
	p, cost, err := c.fetchStacks(ctx)
	if err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()

		return c.failLocked(c.now(), err)
	}

	c.mu.Lock()
	now := c.now()
	recovered := c.down
	c.down = false

	if recovered {
		c.restart = c.full.maxID > 0
	}

	if c.restart && (!now.Before(c.nextFull) || (recovered && !c.fullTry.within(now, fullMin))) {
		withFull = true
	}

	if withFull && c.nextFull.IsZero() && p.Total > firstFullLimit {
		withFull = false
		c.nextFull = now.Add(fullMin)
	}

	c.mu.Unlock()

	note := ""

	if withFull {
		if err := c.refreshFull(context.WithoutCancel(ctx), false); err != nil {
			note = "full dump: " + err.Error()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.interval = nextInterval(cost, frequentFactor, frequentMin, frequentMax)

	return c.publishLocked(c.now(), c.setProfileLocked(p, at), note)
}

func (c *collector) setProfileLocked(p dump.Profile, at time.Time) bool {
	if at.Before(c.profileAt) {
		return false
	}

	c.profile = p
	c.profileAt = at
	c.haveProfile = true

	return true
}

func (c *collector) refreshFull(ctx context.Context, manual bool) error {
	_, err := c.fullFlight(ctx, manual)

	return err
}

func (c *collector) fullFlight(ctx context.Context, manual bool) (fullDump, error) {
	return shared(ctx, c, "full", fetchTimeout, func(ctx context.Context) (fullDump, error) {
		c.mu.Lock()
		last := c.fullTry
		skip := last.recent(c.now()) && (manual || !c.restart)
		c.mu.Unlock()

		if skip {
			return fullDump{}, last.err
		}

		fd, err := c.fetchFull(ctx)

		c.mu.Lock()
		defer c.mu.Unlock()

		c.fullDoneLocked(fd, err)

		return fd, err
	})
}

func (c *collector) fullDoneLocked(fd fullDump, err error) {
	c.fullTry = attempt{at: c.now(), err: err}

	if err != nil {
		c.fullEvery = min(2*c.fullEvery, fullMax)
		c.nextFull = c.now().Add(c.fullEvery)

		return
	}

	c.applyFullLocked(c.now(), fd)
}

func (c *collector) captureFull(ctx context.Context) (Snapshot, error) {
	if !c.hasProfile() {
		c.refresh(ctx, true)
	}

	if err := c.refreshFull(ctx, true); err != nil {
		return c.lastSnapshot(), err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.publishLocked(c.now(), false, ""), nil
}

func (c *collector) captureLeaks(ctx context.Context) (Snapshot, error) {
	if !c.hasProfile() {
		c.refresh(ctx, true)
	}

	return shared(ctx, c, "leaks", fetchTimeout, func(ctx context.Context) (Snapshot, error) {
		c.mu.Lock()
		last := c.leaksTry
		recent := last.recent(c.now())
		s := c.last
		c.mu.Unlock()

		if recent {
			return s, last.err
		}

		p, err := c.fetchLeaks(ctx)

		c.mu.Lock()
		defer c.mu.Unlock()

		c.leaksTry = attempt{at: c.now(), err: err}

		if err != nil {
			return c.last, err
		}

		c.leaks.update(c.now(), p)

		return c.publishLocked(c.now(), false, ""), nil
	})
}

func (c *collector) captureTrace(ctx context.Context, d time.Duration) (Snapshot, error) {
	if c.reader == nil {
		return Snapshot{}, errNoTraceReader
	}

	if !c.hasProfile() {
		c.refresh(ctx, false)
	}

	return shared(ctx, c, "trace", d+fetchTimeout, func(ctx context.Context) (Snapshot, error) {
		c.mu.Lock()
		last := c.traceTry
		recent := last.recent(c.now())
		s := c.last
		c.mu.Unlock()

		if recent {
			return s, last.err
		}

		return c.recordTrace(ctx, d)
	})
}

func (c *collector) recordTrace(ctx context.Context, d time.Duration) (Snapshot, error) {
	var (
		raw  []byte
		terr error
	)

	done := make(chan struct{})

	go func() {
		defer close(done)

		raw, terr = c.src.Trace(ctx, d)
	}()

	timer := time.NewTimer(min(traceDumpDelay, d/2))
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-done:
		if terr != nil {
			return c.traceFailed(terr)
		}
	}

	stacks := c.traceStacks(ctx)

	<-done

	if terr != nil {
		return c.traceFailed(terr)
	}

	ws, err := readTrace(c.reader, raw, stacks)
	if err != nil {
		return c.traceFailed(fmt.Errorf("read trace: %w", err))
	}

	edges := traceEdges(ws)
	at := c.now()
	p, _, err := c.fetchStacks(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.traceTry = attempt{at: now}
	c.trace = traceState{at: now, d: d, edges: edges}

	if err != nil {
		return c.publishLocked(now, false, err.Error()), nil
	}

	return c.publishLocked(now, c.setProfileLocked(p, at), ""), nil
}

func readTrace(r TraceReader, raw []byte, stacks map[int64][]Frame) (ws []Wakeup, err error) {
	defer func() {
		if p := recover(); p != nil {
			ws, err = nil, fmt.Errorf("panic: %v", p)
		}
	}()

	return r.ReadTrace(bytes.NewReader(raw), stacks)
}

func (c *collector) traceStacks(ctx context.Context) map[int64][]Frame {
	fd, err := c.fullFlight(ctx, true)
	if err != nil || len(fd.gs) == 0 {
		return nil
	}

	stacks := make(map[int64][]Frame, len(fd.gs))

	for _, g := range fd.gs {
		stacks[g.ID] = traceFrames(g.Frames)
	}

	return stacks
}

func (c *collector) traceFailed(err error) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.traceTry = attempt{at: c.now(), err: err}

	return c.last, err
}

func (c *collector) snapshot(ctx context.Context) Snapshot {
	c.mu.Lock()
	now := c.now()
	c.active = now
	c.startLocked()

	have := c.haveProfile || c.last.Error != ""
	stale := have && now.Sub(c.last.At) >= 2*c.interval
	s := c.last
	c.mu.Unlock()

	switch {
	case !have:
		return c.refresh(ctx, true)
	case stale:
		return c.refresh(ctx, false)
	}

	return s
}

func (c *collector) refresh(ctx context.Context, withFull bool) Snapshot {
	s, err := shared(ctx, c, "refresh", fetchTimeout, func(ctx context.Context) (Snapshot, error) {
		return c.sample(ctx, withFull), nil
	})
	if err != nil {
		return c.lastSnapshot()
	}

	return s
}

func (c *collector) hasProfile() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.haveProfile
}

func shared[T any](ctx context.Context, c *collector, key string, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	c.fmu.Lock()

	f, ok := c.flights[key]
	if !ok {
		if c.flights == nil {
			c.flights = map[string]*flight{}
		}

		f = &flight{done: make(chan struct{})}
		c.flights[key] = f

		run, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)

		go func() {
			defer cancel()

			f.val, f.err = fn(run)

			c.fmu.Lock()
			delete(c.flights, key)
			c.fmu.Unlock()
			close(f.done)
		}()
	}

	c.fmu.Unlock()

	select {
	case <-f.done:
		v, _ := f.val.(T)

		return v, f.err
	case <-ctx.Done():
		var zero T

		return zero, ctx.Err()
	}
}

func (c *collector) subscribe() (<-chan message, func()) {
	ch := make(chan message, 1)

	c.mu.Lock()
	c.subs[ch] = struct{}{}
	c.active = c.now()
	c.startLocked()
	c.mu.Unlock()

	cancel := func() {
		c.mu.Lock()
		delete(c.subs, ch)
		c.active = c.now()
		c.mu.Unlock()
	}

	return ch, cancel
}

func (c *collector) idle(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.idleLocked(now)
}

func (c *collector) subscribers() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.subs)
}

func (c *collector) close() {
	c.closeOnce.Do(func() { close(c.done) })
}

func (c *collector) lastSnapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.last
}

func (c *collector) idleLocked(now time.Time) bool {
	return len(c.subs) == 0 && now.Sub(c.active) > idleStop
}

func (c *collector) startLocked() {
	if c.running {
		return
	}

	c.running = true

	go c.loop()
}

func (c *collector) loop() {
	for {
		c.mu.Lock()
		d := c.interval
		c.mu.Unlock()

		timer := time.NewTimer(d)

		select {
		case <-c.done:
			timer.Stop()
			return
		case <-timer.C:
		}

		c.mu.Lock()
		now := c.now()

		if c.idleLocked(now) {
			c.running = false
			c.mu.Unlock()

			return
		}

		withFull := !now.Before(c.nextFull)
		c.mu.Unlock()

		c.refresh(context.Background(), withFull)
	}
}

func (c *collector) fetchStacks(ctx context.Context) (dump.Profile, time.Duration, error) {
	start := time.Now()

	raw, err := c.src.Goroutines(ctx, 1)
	if err != nil {
		return dump.Profile{}, 0, err
	}

	p, err := dump.ParseStacks(bytes.NewReader(raw))

	return p, time.Since(start), err
}

func (c *collector) fetchFull(ctx context.Context) (fullDump, error) {
	start := time.Now()

	raw, err := c.src.Goroutines(ctx, 2)
	if err != nil {
		return fullDump{}, err
	}

	gs, err := dump.ParseFull(bytes.NewReader(raw))

	return fullDump{gs: gs, cost: time.Since(start), cut: len(raw) >= c.fullCap}, err
}

func (c *collector) fetchLeaks(ctx context.Context) (dump.Profile, error) {
	raw, err := c.src.Leaks(ctx)
	if err != nil {
		return dump.Profile{}, err
	}

	return dump.ParseStacks(bytes.NewReader(raw))
}

func (c *collector) applyFullLocked(now time.Time, fd fullDump) {
	if c.restart && maxID(fd.gs) < c.full.maxID {
		c.hist = history{window: historyWindow}
		c.leaks = leakState{}
		c.full = fullState{}
		c.trace = traceState{}
	}

	c.restart = false
	c.full.update(now, fd.gs)
	c.full.cut = fd.cut
	c.fullEvery = nextInterval(fd.cost, fullFactor, fullMin, fullMax)
	c.nextFull = now.Add(c.fullEvery)
}

func maxID(gs []dump.Goroutine) int64 {
	var m int64

	for _, g := range gs {
		m = max(m, g.ID)
	}

	return m
}

func (c *collector) failLocked(now time.Time, err error) Snapshot {
	c.down = true
	c.readErr = err.Error()
	c.last.At = now
	c.last.Target = c.src.Name()
	c.last.Interval = c.interval
	c.last.Error = c.readErr

	if c.last.Groups == nil {
		c.last.Groups = []Group{}
	}

	if c.last.Edges == nil {
		c.last.Edges = []Edge{}
	}

	c.broadcastLocked(c.last)

	return c.last
}

func (c *collector) publishLocked(now time.Time, record bool, note string) Snapshot {
	groups, total := buildGroups(c.profile)
	counts := make(map[string]int, len(groups))
	out := make([]Group, 0, len(groups))

	for id, g := range groups {
		counts[id] = g.Count
		g.Growth = c.hist.growth(id, now, g.Count)
		g.Leaked = c.leaks.counts[id]

		fg, ok := c.full.groups[id]
		if !ok {
			fg, ok = c.full.bySite[g.WaitSite]
			ok = ok && fg.calls(g.Entry)
		}

		if ok {
			g.State = fg.state
			g.CreatedBy = fg.createdBy
			g.MaxWait = fg.maxWait
			g.InFullDump = true
		}

		out = append(out, *g)
	}

	if record {
		c.hist.add(now, counts)
	}

	if note == "" && c.down {
		note = c.readErr
	}

	slices.SortFunc(out, func(a, b Group) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.ID, b.ID))
	})

	c.last = Snapshot{
		At:         now,
		Target:     c.src.Name(),
		Goroutines: total,
		Interval:   c.interval,
		FullAt:     c.full.at,
		FullCount:  c.full.count,
		FullCut:    c.full.cut,
		LeaksAt:    c.leaks.at,
		TraceAt:    c.trace.at,
		TraceFor:   c.trace.d,
		Error:      note,
		Groups:     out,
		Edges:      c.edgesLocked(groups),
	}

	c.broadcastLocked(c.last)

	return c.last
}

func (c *collector) broadcastLocked(s Snapshot) {
	if len(c.subs) == 0 {
		return
	}

	data, err := json.Marshal(s)
	if err != nil {
		return
	}

	m := message{s: s, data: data}

	for ch := range c.subs {
		select {
		case ch <- m:
			continue
		default:
		}

		select {
		case <-ch:
		default:
		}

		select {
		case ch <- m:
		default:
		}
	}
}
