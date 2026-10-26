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

var (
	errNoTraceReader = errors.New("no view reads traces: mount flow.View()")
	errOffline       = errors.New("this session was opened from a file: ruvo is not reading the target")
)

type collector struct {
	src    collect.Source
	reader TraceReader
	now    func() time.Time

	mu          sync.Mutex
	hist        history
	groups      groupCache
	full        fullState
	leaks       leakState
	trace       traceState
	profile     dump.Profile
	raw         []byte
	rawAt       time.Time
	haveProfile bool
	reads       uint64
	applied     uint64
	seq         int64
	down        bool
	readErr     string
	restart     bool
	last        Snapshot
	subs        map[chan message]struct{}
	running     bool
	frozen      bool
	loaded      session
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

	pmu           sync.Mutex
	strs          dump.Strings
	parsed        []byte
	parsedProfile dump.Profile
}

type message struct {
	s   Snapshot
	enc *encoded
}

type encoded struct {
	fullOnce sync.Once
	leanOnce sync.Once
	full     []byte
	lean     []byte
}

func newMessage(s Snapshot) message {
	return message{s: s, enc: &encoded{}}
}

func (m message) full() []byte {
	m.enc.fullOnce.Do(func() { m.enc.full = encodeJSON(m.s) })

	return m.enc.full
}

func (m message) lean() []byte {
	m.enc.leanOnce.Do(func() { m.enc.lean = encodeJSON(leanSnapshot(m.s)) })

	return m.enc.lean
}

func leanSnapshot(s Snapshot) Snapshot {
	groups := slices.Clone(s.Groups)

	for i := range groups {
		groups[i].Stack = nil
	}

	s.Groups = groups

	return s
}

var encoders = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func encodeJSON(v any) []byte {
	b := encoders.Get().(*bytes.Buffer)
	b.Reset()

	defer func() {
		if b.Cap() <= 16<<20 {
			encoders.Put(b)
		}
	}()

	if err := json.NewEncoder(b).Encode(v); err != nil {
		return nil
	}

	return bytes.Clone(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
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
	raw  []byte
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
	seq := c.beginRead()
	p, raw, cost, err := c.fetchStacks(ctx)

	c.mu.Lock()
	now := c.now()

	if !c.applyReadLocked(seq, now, p, raw, err) {
		defer c.mu.Unlock()

		return c.last
	}

	if err != nil {
		defer c.mu.Unlock()

		return c.failLocked(now)
	}

	if c.restart && !now.Before(c.nextFull) {
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

	return c.publishLocked(c.now(), true, note)
}

func (c *collector) beginRead() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reads++

	return c.reads
}

func (c *collector) applyReadLocked(seq uint64, now time.Time, p dump.Profile, raw []byte, err error) bool {
	if seq < c.applied {
		return false
	}

	c.applied = seq

	if err != nil {
		c.down = true
		c.readErr = err.Error()

		return true
	}

	if c.down {
		c.restart = c.full.maxID > 0

		if c.restart && !c.fullTry.within(now, fullMin) {
			c.nextFull = now
		}
	}

	c.down = false
	c.readErr = ""
	c.profile = p
	c.raw = raw
	c.rawAt = now
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
	if c.frozen {
		return c.lastSnapshot(), errOffline
	}

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
	if c.frozen {
		return c.lastSnapshot(), errOffline
	}

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

		p, raw, err := c.fetchLeaks(ctx)

		c.mu.Lock()
		defer c.mu.Unlock()

		c.leaksTry = attempt{at: c.now(), err: err}

		if err != nil {
			return c.last, err
		}

		c.leaks.update(c.now(), p)
		c.leaks.raw = raw

		return c.publishLocked(c.now(), false, ""), nil
	})
}

func (c *collector) captureTrace(ctx context.Context, d time.Duration) (Snapshot, error) {
	if c.frozen {
		return c.lastSnapshot(), errOffline
	}

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

	stacks, dumped := c.traceStacks(ctx)

	<-done

	if terr != nil {
		return c.traceFailed(terr)
	}

	ws, err := readTrace(c.reader, raw, stacks)
	if err != nil {
		return c.traceFailed(fmt.Errorf("read trace: %w", err))
	}

	ts := readWakeups(ws)
	ts.raw, ts.dump = raw, dumped
	seq := c.beginRead()
	p, praw, _, err := c.fetchStacks(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	c.traceTry = attempt{at: now}
	ts.at, ts.d = now, d
	c.trace = ts
	fresh := c.applyReadLocked(seq, now, p, praw, err)

	return c.publishLocked(now, fresh && err == nil, ""), nil
}

func readTrace(r TraceReader, raw []byte, stacks map[int64][]Frame) (ws []Wakeup, err error) {
	defer func() {
		if p := recover(); p != nil {
			ws, err = nil, fmt.Errorf("panic: %v", p)
		}
	}()

	return r.ReadTrace(bytes.NewReader(raw), stacks)
}

func (c *collector) traceStacks(ctx context.Context) (map[int64][]Frame, []byte) {
	fd, err := c.fullFlight(ctx, true)
	if err != nil || len(fd.gs) == 0 {
		return nil, nil
	}

	stacks := make(map[int64][]Frame, len(fd.gs))

	for _, g := range fd.gs {
		stacks[g.ID] = traceFrames(g.Frames)
	}

	return stacks, fd.raw
}

func (c *collector) traceFailed(err error) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.traceTry = attempt{at: c.now(), err: err}

	return c.last, err
}

func (c *collector) snapshot(ctx context.Context) Snapshot {
	c.mu.Lock()

	if c.frozen {
		defer c.mu.Unlock()

		return c.last
	}

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

func (c *collector) stopLocked() {
	c.running = false
	c.raw = nil
	c.rawAt = time.Time{}
	c.full.raw = nil
	c.leaks.raw = nil
	c.trace.raw = nil
	c.trace.dump = nil

	c.pmu.Lock()
	c.parsed, c.parsedProfile = nil, dump.Profile{}
	c.pmu.Unlock()
}

func (c *collector) idleLocked(now time.Time) bool {
	return len(c.subs) == 0 && now.Sub(c.active) > idleStop
}

func (c *collector) startLocked() {
	if c.running || c.frozen {
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
			c.stopLocked()
			c.mu.Unlock()

			return
		}

		withFull := !now.Before(c.nextFull)
		c.mu.Unlock()

		c.refresh(context.Background(), withFull)
	}
}

func (c *collector) fetchStacks(ctx context.Context) (dump.Profile, []byte, time.Duration, error) {
	start := time.Now()

	raw, err := c.src.Goroutines(ctx, 1)
	if err != nil {
		return dump.Profile{}, nil, 0, err
	}

	c.pmu.Lock()
	defer c.pmu.Unlock()

	if c.parsed != nil && bytes.Equal(raw, c.parsed) {
		return c.parsedProfile, raw, time.Since(start), nil
	}

	p, err := c.strs.ParseStacks(raw)
	if err == nil {
		c.parsed, c.parsedProfile = raw, p
	}

	return p, raw, time.Since(start), err
}

func (c *collector) fetchFull(ctx context.Context) (fullDump, error) {
	start := time.Now()

	raw, err := c.src.Goroutines(ctx, 2)
	if err != nil {
		return fullDump{}, err
	}

	gs, err := dump.ParseFull(raw)

	return fullDump{gs: gs, raw: raw, cost: time.Since(start), cut: len(raw) >= c.fullCap}, err
}

func (c *collector) fetchLeaks(ctx context.Context) (dump.Profile, []byte, error) {
	raw, err := c.src.Leaks(ctx)
	if err != nil {
		return dump.Profile{}, nil, err
	}

	p, err := dump.ParseStacks(raw)

	return p, raw, err
}

func (c *collector) applyFullLocked(now time.Time, fd fullDump) {
	if c.restart && !fd.cut && maxID(fd.gs) < c.full.maxID {
		c.hist = history{window: historyWindow}
		c.leaks = leakState{}
		c.full = fullState{}
		c.trace = traceState{}
	}

	c.restart = false
	c.full.update(now, fd.gs)
	c.full.cut = fd.cut
	c.full.raw = fd.raw
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

func (c *collector) failLocked(now time.Time) Snapshot {
	c.seq = max(c.seq+1, now.UnixMicro())
	c.last.Seq = c.seq
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
	groups, total := c.groups.build(c.profile)
	idx := newGroupIndex(groups)
	edges := c.edgesLocked(idx)
	c.markUnwokenLocked(idx)
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

	c.seq = max(c.seq+1, now.UnixMicro())
	c.last = Snapshot{
		Seq:          c.seq,
		At:           now,
		Target:       c.src.Name(),
		Goroutines:   total,
		Interval:     c.interval,
		FullAt:       c.full.at,
		FullCount:    c.full.count,
		FullCut:      c.full.cut,
		LeaksAt:      c.leaks.at,
		TraceAt:      c.trace.at,
		TraceFor:     c.trace.d,
		TraceUnknown: c.trace.unknown,
		Error:        note,
		Groups:       out,
		Edges:        edges,
	}

	c.broadcastLocked(c.last)

	return c.last
}

func (c *collector) broadcastLocked(s Snapshot) {
	if len(c.subs) == 0 {
		return
	}

	m := newMessage(s)

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
