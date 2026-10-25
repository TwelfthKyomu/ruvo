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
	ids    []string
	maxID  int64
	spawns map[[2]endpoint]int
	count  int
	cut    bool
	raw    []byte
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
	group int32
	at    int64
}

type stackInfo struct {
	own   bool
	group int32
}

func (f *fullState) update(at time.Time, gs []dump.Goroutine) {
	type tally struct {
		fg     fullGroup
		end    endpoint
		states map[string]int
	}

	var (
		tallies []tally
		top     int64
	)

	index := map[string]int32{}
	infos := map[*dump.Frame]stackInfo{}
	seen := make(map[int64]seenAt, len(gs))
	count := 0
	now := at.UnixNano()

	describe := func(frames []dump.Frame) stackInfo {
		if isOwn(frames) {
			return stackInfo{own: true}
		}

		site, entry := groupKey(frames)
		id := groupID(site, entry)

		i, ok := index[id]
		if !ok {
			i = int32(len(tallies))
			index[id] = i
			tallies = append(tallies, tally{
				fg:     fullGroup{frames: frames},
				end:    endpoint{id: id, site: site.String(), entry: entry.Func, calls: callList(frames)},
				states: map[string]int{},
			})
		}

		return stackInfo{group: i}
	}

	for _, g := range gs {
		top = max(top, g.ID)

		var info stackInfo

		if len(g.Frames) == 0 {
			info = describe(g.Frames)
		} else if c, ok := infos[&g.Frames[0]]; ok {
			info = c
		} else {
			info = describe(g.Frames)
			infos[&g.Frames[0]] = info
		}

		if info.own {
			continue
		}

		count++

		t := &tallies[info.group]
		since := now

		if prev, ok := f.seen[g.ID]; ok && g.Wait > 0 && f.ids[prev.group] == t.end.id {
			since = prev.at
		}

		seen[g.ID] = seenAt{group: info.group, at: since}
		waited := min(time.Duration(now-since), g.Wait+waitSlack)

		t.states[g.State]++

		if t.fg.createdBy == "" {
			t.fg.createdBy = g.CreatedBy.String()
		}

		t.fg.maxWait = max(t.fg.maxWait, g.Wait, waited)
	}

	groups := make(map[string]fullGroup, len(tallies))
	bySite := make(map[string]fullGroup, len(tallies))
	ids := make([]string, len(tallies))
	shared := map[string]bool{}

	for i, t := range tallies {
		t.fg.state = mostCommon(t.states)
		groups[t.end.id] = t.fg
		ids[i] = t.end.id

		if _, dup := bySite[t.end.site]; dup {
			shared[t.end.site] = true
		}

		bySite[t.end.site] = t.fg
	}

	for site := range shared {
		delete(bySite, site)
	}

	ends := make([]endpoint, len(tallies))
	for i, t := range tallies {
		ends[i] = t.end
	}

	f.at = at
	f.groups = groups
	f.bySite = bySite
	f.seen = seen
	f.ids = ids
	f.maxID = top
	f.count = count
	f.spawns = spawnPairs(gs, seen, ends)
}

func spawnPairs(gs []dump.Goroutine, seen map[int64]seenAt, ends []endpoint) map[[2]endpoint]int {
	pairs := map[[2]int32]int{}

	for _, g := range gs {
		if g.Creator == 0 {
			continue
		}

		child, ok := seen[g.ID]
		if !ok {
			continue
		}

		if creator, ok := seen[g.Creator]; ok {
			pairs[[2]int32{creator.group, child.group}]++
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
	raw    []byte
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
