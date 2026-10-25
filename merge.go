package ruvo

import (
	"slices"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

type fullState struct {
	at     time.Time
	groups map[string]fullGroup
	bySite map[string]fullGroup
	seen   map[int64]seenAt
	maxID  int64
	spawns map[[2]endpoint]int
	count  int
	cut    bool
}

type fullGroup struct {
	state     string
	createdBy string
	maxWait   time.Duration
	frames    []dump.Frame
}

func (g fullGroup) calls(fn string) bool {
	return slices.ContainsFunc(g.frames, func(f dump.Frame) bool { return f.Func == fn })
}

type seenAt struct {
	group string
	at    time.Time
}

func (f *fullState) update(at time.Time, gs []dump.Goroutine) {
	states := map[string]map[string]int{}
	groups := map[string]fullGroup{}
	ends := map[string]endpoint{}
	seen := map[int64]seenAt{}

	var top int64

	count := 0

	for _, g := range gs {
		top = max(top, g.ID)

		if isOwn(g.Frames) {
			continue
		}

		count++

		site, entry := groupKey(g.Frames)
		id := groupID(site, entry)

		if _, ok := ends[id]; !ok {
			ends[id] = endpoint{id: id, site: site.String(), entry: entry.Func}
		}

		since := at
		if prev, ok := f.seen[g.ID]; ok && prev.group == id && g.Wait > 0 {
			since = prev.at
		}

		seen[g.ID] = seenAt{group: id, at: since}
		waited := min(at.Sub(since), g.Wait+waitSlack)

		if states[id] == nil {
			states[id] = map[string]int{}
		}

		states[id][g.State]++

		fg := groups[id]
		if fg.createdBy == "" {
			fg.createdBy = g.CreatedBy.String()
		}

		if fg.frames == nil {
			fg.frames = g.Frames
		}

		fg.maxWait = max(fg.maxWait, g.Wait, waited)
		groups[id] = fg
	}

	bySite := map[string]fullGroup{}
	shared := map[string]bool{}

	for id, fg := range groups {
		fg.state = mostCommon(states[id])
		groups[id] = fg

		site := ends[id].site

		if _, dup := bySite[site]; dup {
			shared[site] = true
		}

		bySite[site] = fg
	}

	for site := range shared {
		delete(bySite, site)
	}

	f.at = at
	f.groups = groups
	f.bySite = bySite
	f.seen = seen
	f.maxID = top
	f.count = count
	f.spawns = spawnPairs(gs, seen, ends)
}

func spawnPairs(gs []dump.Goroutine, seen map[int64]seenAt, ends map[string]endpoint) map[[2]endpoint]int {
	pairs := map[[2]string]int{}

	for _, g := range gs {
		if g.Creator == 0 {
			continue
		}

		child, ok := seen[g.ID]
		if !ok {
			continue
		}

		if creator, ok := seen[g.Creator]; ok {
			pairs[[2]string{creator.group, child.group}]++
		}
	}

	spawns := make(map[[2]endpoint]int, len(pairs))

	for p, n := range pairs {
		spawns[[2]endpoint{ends[p[0]], ends[p[1]]}] = n
	}

	return spawns
}

func mostCommon(counts map[string]int) string {
	best, n := "", 0

	for s, c := range counts {
		if c > n || (c == n && s < best) {
			best, n = s, c
		}
	}

	return best
}

type leakState struct {
	at     time.Time
	counts map[string]int
}

func (l *leakState) update(at time.Time, p dump.Profile) {
	counts := map[string]int{}

	for _, r := range p.Records {
		if isOwn(r.Frames) {
			continue
		}

		site, entry := groupKey(r.Frames)
		counts[groupID(site, entry)] += r.Count
	}

	l.at = at
	l.counts = counts
}
