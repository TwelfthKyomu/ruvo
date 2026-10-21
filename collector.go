package ruvo

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

type collector struct {
	src collect.Source
	now func() time.Time

	mu          sync.Mutex
	hist        history
	full        fullState
	leaks       leakState
	profile     dump.Profile
	haveProfile bool
	down        bool
	last        Snapshot
	subs        map[chan message]struct{}
	running     bool
	active      time.Time
	interval    time.Duration
	fullEvery   time.Duration
	nextFull    time.Time

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

type fullDump struct {
	gs   []dump.Goroutine
	cost time.Duration
}

func newCollector(src collect.Source) *collector {
	return &collector{
		src:       src,
		now:       time.Now,
		hist:      history{window: historyWindow},
		subs:      map[chan message]struct{}{},
		interval:  frequentMin,
		fullEvery: fullMin,
		done:      make(chan struct{}),
	}
}

func (c *collector) sample(ctx context.Context, withFull bool) Snapshot {
	p, cost, err := c.fetchStacks(ctx)
	if err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()

		return c.failLocked(c.now(), err)
	}

	c.mu.Lock()
	recovering := c.down
	c.down = false

	if withFull && !recovering && c.full.at.IsZero() && p.Total > firstFullLimit {
		withFull = false
		c.nextFull = c.now().Add(fullMin)
	}

	c.mu.Unlock()

	note := ""

	if withFull || recovering {
		if err := c.refreshFull(ctx, recovering); err != nil {
			note = "full dump: " + err.Error()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.interval = nextInterval(cost, frequentFactor, frequentMin, frequentMax)
	c.profile = p
	c.haveProfile = true

	return c.publishLocked(c.now(), true, note)
}

func (c *collector) refreshFull(ctx context.Context, recovering bool) error {
	_, err := shared(ctx, c, "full", func(ctx context.Context) (struct{}, error) {
		c.mu.Lock()
		recent := !c.full.at.IsZero() && c.now().Sub(c.full.at) < actionGap
		c.mu.Unlock()

		if recent && !recovering {
			return struct{}{}, nil
		}

		fd, err := c.fetchFull(ctx)

		c.mu.Lock()
		defer c.mu.Unlock()

		if err != nil {
			c.fullEvery = min(2*c.fullEvery, fullMax)
			c.nextFull = c.now().Add(c.fullEvery)

			return struct{}{}, err
		}

		c.applyFullLocked(c.now(), fd, recovering)

		return struct{}{}, nil
	})

	return err
}

func (c *collector) captureFull(ctx context.Context) (Snapshot, error) {
	if !c.hasProfile() {
		c.first(ctx)
	}

	if err := c.refreshFull(ctx, false); err != nil {
		return c.lastSnapshot(), err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.publishLocked(c.now(), false, ""), nil
}

func (c *collector) captureLeaks(ctx context.Context) (Snapshot, error) {
	if !c.hasProfile() {
		c.first(ctx)
	}

	return shared(ctx, c, "leaks", func(ctx context.Context) (Snapshot, error) {
		c.mu.Lock()
		recent := !c.leaks.at.IsZero() && c.now().Sub(c.leaks.at) < actionGap
		last := c.last
		c.mu.Unlock()

		if recent {
			return last, nil
		}

		raw, err := c.src.Leaks(ctx)
		if err != nil {
			return c.lastSnapshot(), err
		}

		p, err := dump.ParseStacks(bytes.NewReader(raw))
		if err != nil {
			return c.lastSnapshot(), err
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		c.leaks.update(c.now(), p)

		return c.publishLocked(c.now(), false, ""), nil
	})
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
		return c.first(ctx)
	case stale:
		s, _ = shared(ctx, c, "refresh", func(ctx context.Context) (Snapshot, error) {
			return c.sample(ctx, false), nil
		})
	}

	return s
}

func (c *collector) first(ctx context.Context) Snapshot {
	s, _ := shared(ctx, c, "first", func(ctx context.Context) (Snapshot, error) {
		return c.sample(ctx, true), nil
	})

	return s
}

func (c *collector) hasProfile() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.haveProfile
}

func shared[T any](ctx context.Context, c *collector, key string, fn func(context.Context) (T, error)) (T, error) {
	c.fmu.Lock()

	if f, ok := c.flights[key]; ok {
		c.fmu.Unlock()
		<-f.done

		v, _ := f.val.(T)

		return v, f.err
	}

	if c.flights == nil {
		c.flights = map[string]*flight{}
	}

	f := &flight{done: make(chan struct{})}
	c.flights[key] = f
	c.fmu.Unlock()

	run, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
	v, err := fn(run)
	cancel()

	f.val, f.err = v, err

	c.fmu.Lock()
	delete(c.flights, key)
	c.fmu.Unlock()
	close(f.done)

	return v, err
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

		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		c.sample(ctx, withFull)
		cancel()
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

	return fullDump{gs: gs, cost: time.Since(start)}, err
}

func (c *collector) applyFullLocked(now time.Time, fd fullDump, recovering bool) {
	if recovering && c.full.maxID > 0 && maxID(fd.gs) < c.full.maxID {
		c.hist = history{window: historyWindow}
		c.leaks = leakState{}
		c.full = fullState{}
	}

	c.full.update(now, fd.gs)
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
	c.last.At = now
	c.last.Target = c.src.Name()
	c.last.Interval = c.interval
	c.last.Error = err.Error()

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

	slices.SortFunc(out, func(a, b Group) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.ID, b.ID))
	})

	c.last = Snapshot{
		At:         now,
		Target:     c.src.Name(),
		Goroutines: total,
		Interval:   c.interval,
		FullAt:     c.full.at,
		LeaksAt:    c.leaks.at,
		Error:      note,
		Groups:     out,
		Edges:      []Edge{},
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
