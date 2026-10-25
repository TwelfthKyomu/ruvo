package ruvo

import (
	"cmp"
	"slices"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

type endpoint struct {
	id    string
	site  string
	entry string
}

func endpointOf(frames []dump.Frame) endpoint {
	site, entry := groupKey(frames)

	return endpoint{id: groupID(site, entry), site: site.String(), entry: entry.Func}
}

type traceEdge struct {
	from, to endpoint
	cause    string
	count    int
	waiters  int
	wait     time.Duration
}

type traceState struct {
	at    time.Time
	d     time.Duration
	edges []traceEdge
}

func traceEdges(ws []Wakeup) []traceEdge {
	out := make([]traceEdge, 0, len(ws))
	pos := make(map[traceKey]int, len(ws))

	for _, w := range ws {
		to, ok := traceEndpoint(w.Waiter)
		if !ok {
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
	bySite  map[string]string
	byEntry map[string]string
}

func newGroupIndex(groups map[string]*Group) groupIndex {
	x := groupIndex{
		groups:  groups,
		bySite:  make(map[string]string, len(groups)),
		byEntry: make(map[string]string, len(groups)),
	}
	ambiguous := map[string]bool{}

	for id, g := range groups {
		if _, dup := x.bySite[g.WaitSite]; dup {
			ambiguous[g.WaitSite] = true
		}

		x.bySite[g.WaitSite] = id

		best, ok := x.byEntry[g.Entry]
		if !ok || g.Count > groups[best].Count || (g.Count == groups[best].Count && id < best) {
			x.byEntry[g.Entry] = id
		}
	}

	for site := range ambiguous {
		delete(x.bySite, site)
	}

	return x
}

func (x groupIndex) resolve(e endpoint) (string, bool) {
	if _, ok := x.groups[e.id]; ok {
		return e.id, true
	}

	if id, ok := x.bySite[e.site]; ok {
		return id, true
	}

	if isStd(e.entry) {
		return "", false
	}

	id, ok := x.byEntry[e.entry]

	return id, ok
}

func (c *collector) edgesLocked(groups map[string]*Group) []Edge {
	n := len(c.trace.edges) + len(c.full.spawns)
	if n == 0 {
		return []Edge{}
	}

	idx := newGroupIndex(groups)
	set := edgeSet{pos: make(map[edgeKey]int, n), out: make([]Edge, 0, n)}

	for _, te := range c.trace.edges {
		to, ok := idx.resolve(te.to)
		if !ok {
			continue
		}

		from := te.cause
		if te.cause == CauseGoroutine {
			if from, ok = idx.resolve(te.from); !ok {
				continue
			}
		}

		e := set.edge(from, to, EdgeUnblock)
		e.Count += te.count
		e.Waiters += te.waiters
		e.Wait += te.wait
	}

	for pair, count := range c.full.spawns {
		from, ok := idx.resolve(pair[0])
		if !ok {
			continue
		}

		to, ok := idx.resolve(pair[1])
		if !ok {
			continue
		}

		set.edge(from, to, EdgeSpawn).Count += count
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
