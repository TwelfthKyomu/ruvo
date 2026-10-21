package ruvo

import (
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

type fullState struct {
	at     time.Time
	groups map[string]fullGroup
	bySite map[string]fullGroup
	seen   map[int64]seenAt
	maxID  int64
}

type fullGroup struct {
	state     string
	createdBy string
	maxWait   time.Duration
}

type seenAt struct {
	group string
	at    time.Time
}

func (f *fullState) update(at time.Time, gs []dump.Goroutine) {
	states := map[string]map[string]int{}
	groups := map[string]fullGroup{}
	sites := map[string]string{}
	seen := map[int64]seenAt{}

	var top int64

	for _, g := range gs {
		top = max(top, g.ID)

		if isDumpWriter(g.Frames) {
			continue
		}

		site, entry := groupKey(g.Frames)
		id := groupID(site, entry)
		sites[id] = site.String()

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

		fg.maxWait = max(fg.maxWait, g.Wait, waited)
		groups[id] = fg
	}

	bySite := map[string]fullGroup{}
	shared := map[string]bool{}

	for id, fg := range groups {
		fg.state = mostCommon(states[id])
		groups[id] = fg

		if _, dup := bySite[sites[id]]; dup {
			shared[sites[id]] = true
		}

		bySite[sites[id]] = fg
	}

	for site := range shared {
		delete(bySite, site)
	}

	f.at = at
	f.groups = groups
	f.bySite = bySite
	f.seen = seen
	f.maxID = top
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
		if isDumpWriter(r.Frames) {
			continue
		}

		site, entry := groupKey(r.Frames)
		counts[groupID(site, entry)] += r.Count
	}

	l.at = at
	l.counts = counts
}
