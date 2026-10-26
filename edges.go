package ruvo

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

type endpoint struct {
	id    string
	site  string
	entry string
	calls string
}

func endpointOf(frames []dump.Frame) endpoint {
	site, entry := groupKey(frames)

	return endpoint{id: groupID(site, entry), site: site.String(), entry: entry.Func, calls: callList(frames)}
}

func callList(frames []dump.Frame) string {
	var b strings.Builder

	b.WriteByte(0)

	for _, f := range frames {
		b.WriteString(f.Func)
		b.WriteByte(0)
	}

	return b.String()
}

func (e endpoint) callsFunc(fn string) bool {
	return strings.Contains(e.calls, "\x00"+fn+"\x00")
}

type traceEdge struct {
	from, to endpoint
	cause    string
	count    int
	waiters  int
	wait     time.Duration
}

type traceState struct {
	at      time.Time
	d       time.Duration
	edges   []traceEdge
	waiting []traceEdge
	unknown int
	raw     []byte
	dump    []byte
}

func readWakeups(ws []Wakeup) traceState {
	var t traceState

	for _, w := range ws {
		switch {
		case len(w.Waiter) == 0:
			t.unknown += w.Count
		case w.Cause == CauseNone:
			if to, ok := traceEndpoint(w.Waiter); ok {
				t.waiting = append(t.waiting, traceEdge{to: to, cause: CauseNone, count: w.Count, waiters: w.Waiters, wait: w.Wait})
			}
		}
	}

	t.edges = traceEdges(ws)

	return t
}

func traceEdges(ws []Wakeup) []traceEdge {
	out := make([]traceEdge, 0, len(ws))
	pos := make(map[traceKey]int, len(ws))

	for _, w := range ws {
		to, ok := traceEndpoint(w.Waiter)
		if !ok || w.Cause == CauseNone {
			continue
		}

		var from endpoint

		switch w.Cause {
		case CauseGoroutine:
			if from, ok = traceEndpoint(w.Waker); !ok {
				continue
			}
		case CauseNetwork, CauseTimer:
		default:
			continue
		}

		k := traceKey{from: from.id, to: to.id, cause: w.Cause}

		i, seen := pos[k]
		if !seen {
			i = len(out)
			pos[k] = i
			out = append(out, traceEdge{from: from, to: to, cause: w.Cause})
		}

		out[i].count += w.Count
		out[i].waiters += w.Waiters
		out[i].wait += w.Wait
	}

	return out
}

type traceKey struct {
	from, to, cause string
}

func traceEndpoint(frames []Frame) (endpoint, bool) {
	fs := make([]dump.Frame, len(frames))

	for i, f := range frames {
		fs[i] = dump.Frame(f)
	}

	if len(fs) == 0 || isOwn(fs) {
		return endpoint{}, false
	}

	return endpointOf(fs), true
}

func traceFrames(frames []dump.Frame) []Frame {
	out := make([]Frame, len(frames))

	for i, f := range frames {
		out[i] = Frame(f)
	}

	return out
}

type groupIndex struct {
	groups  map[string]*Group
	bySite  map[string][]string
	byEntry map[string]string
}

func newGroupIndex(groups map[string]*Group) groupIndex {
	x := groupIndex{
		groups:  groups,
		bySite:  make(map[string][]string, len(groups)),
		byEntry: make(map[string]string, len(groups)),
	}

	for id, g := range groups {
		x.bySite[g.WaitSite] = append(x.bySite[g.WaitSite], id)

		if _, ok := x.byEntry[g.Entry]; ok {
			x.byEntry[g.Entry] = ""
		} else {
			x.byEntry[g.Entry] = id
		}
	}

	return x
}

func (x groupIndex) resolve(e endpoint) (id string, exact, ok bool) {
	if _, ok := x.groups[e.id]; ok {
		return e.id, true, true
	}

	n := 0

	for _, cand := range x.bySite[e.site] {
		if compatible(e, x.groups[cand]) {
			id = cand
			n++
		}
	}

	switch {
	case n == 1:
		return id, false, true
	case n > 1, isStd(e.entry):
		return "", false, false
	}

	id = x.byEntry[e.entry]

	return id, false, id != ""
}

func compatible(e endpoint, g *Group) bool {
	if g.Entry == e.entry || e.callsFunc(g.Entry) {
		return true
	}

	return slices.ContainsFunc(g.Stack, func(line string) bool {
		fn, _, _ := strings.Cut(line, " ")

		return fn == e.entry
	})
}

func (c *collector) edgesLocked(idx groupIndex) []Edge {
	n := len(c.trace.edges) + len(c.full.spawns)
	if n == 0 {
		return []Edge{}
	}

	set := edgeSet{pos: make(map[edgeKey]int, n), out: make([]Edge, 0, n)}

	for _, te := range c.trace.edges {
		to, exact, ok := idx.resolve(te.to)
		if !ok {
			continue
		}

		from := te.cause
		if te.cause == CauseGoroutine {
			var fromExact bool

			if from, fromExact, ok = idx.resolve(te.from); !ok {
				continue
			}

			exact = exact && fromExact
		}

		e := set.edge(from, to, EdgeUnblock)
		e.Count += te.count
		e.Waiters += te.waiters
		e.Wait += te.wait
		e.Approx = e.Approx || !exact
	}

	for pair, count := range c.full.spawns {
		from, fromExact, ok := idx.resolve(pair[0])
		if !ok {
			continue
		}

		to, toExact, ok := idx.resolve(pair[1])
		if !ok {
			continue
		}

		e := set.edge(from, to, EdgeSpawn)
		e.Count += count
		e.Approx = e.Approx || !fromExact || !toExact
	}

	slices.SortFunc(set.out, func(a, b Edge) int {
		return cmp.Or(
			cmp.Compare(kindOrder(a.Kind), kindOrder(b.Kind)),
			cmp.Compare(b.Wait, a.Wait),
			cmp.Compare(b.Count, a.Count),
			cmp.Compare(a.From, b.From),
			cmp.Compare(a.To, b.To),
		)
	})

	return set.out
}

func (c *collector) markUnwokenLocked(idx groupIndex) {
	for _, te := range c.trace.waiting {
		if id, _, ok := idx.resolve(te.to); ok {
			idx.groups[id].Unwoken += te.count
		}
	}
}

type edgeSet struct {
	pos map[edgeKey]int
	out []Edge
}

func (s *edgeSet) edge(from, to string, kind EdgeKind) *Edge {
	k := edgeKey{from: from, to: to, kind: kind}

	i, seen := s.pos[k]
	if !seen {
		i = len(s.out)
		s.pos[k] = i
		s.out = append(s.out, Edge{From: from, To: to, Kind: kind})
	}

	return &s.out[i]
}

type edgeKey struct {
	from, to string
	kind     EdgeKind
}

func kindOrder(k EdgeKind) int {
	if k == EdgeUnblock {
		return 0
	}

	return 1
}
