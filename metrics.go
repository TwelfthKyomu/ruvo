package ruvo

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const metricGroupsMax = 200

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func (h *handler) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "use GET", http.StatusMethodNotAllowed)

		return
	}

	s := h.c.snapshot(r.Context())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	if r.Method == http.MethodHead {
		return
	}

	writeMetrics(w, s, h.c.reachable())
}

func (c *collector) reachable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.frozen {
		return c.last.Error == ""
	}

	return !c.down
}

type series struct {
	wait, entry string
	count       int
	growth      float64
	leaked      int
}

func groupSeries(groups []Group) []series {
	at := map[[2]string]int{}

	var out []series

	for _, g := range groups {
		wait := g.WaitSite
		if i := strings.LastIndexByte(wait, ' '); i > 0 {
			wait = wait[:i]
		}

		k := [2]string{wait, g.Entry}

		i, ok := at[k]
		if !ok {
			i = len(out)
			at[k] = i
			out = append(out, series{wait: wait, entry: g.Entry})
		}

		out[i].count += g.Count
		out[i].growth += g.Growth
		out[i].leaked += g.Leaked
	}

	slices.SortFunc(out, func(a, b series) int {
		return cmp.Or(cmp.Compare(b.count, a.count), cmp.Compare(a.wait, b.wait), cmp.Compare(a.entry, b.entry))
	})

	return out
}

func writeMetrics(w io.Writer, s Snapshot, up bool) error {
	var b bytes.Buffer

	metric := func(name, kind, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}

	value := func(name string, v float64) {
		fmt.Fprintf(&b, "%s %s\n", name, strconv.FormatFloat(v, 'g', -1, 64))
	}

	all := groupSeries(s.Groups)
	shown := all[:min(len(all), metricGroupsMax)]

	perGroup := func(name, help string, v func(series) float64) {
		metric(name, "gauge", help)

		for _, g := range shown {
			fmt.Fprintf(&b, "%s{wait=\"%s\",entry=\"%s\"} %s\n", name, labelEscaper.Replace(g.wait), labelEscaper.Replace(g.entry), strconv.FormatFloat(v(g), 'g', -1, 64))
		}
	}

	flag := func(on bool) float64 {
		if on {
			return 1
		}

		return 0
	}

	metric("ruvo_target_up", "gauge", "1 if the last read of the target succeeded, 0 if it failed; from a session file, as it was when the file was saved.")
	value("ruvo_target_up", flag(up))

	metric("ruvo_offline", "gauge", "1 if the numbers come from a session file opened with ruvo -open rather than from a live target.")
	value("ruvo_offline", flag(s.Offline))

	metric("ruvo_goroutines", "gauge", "Goroutines of the target, not counting ruvo's own.")
	value("ruvo_goroutines", float64(s.Goroutines))

	metric("ruvo_groups", "gauge", "Groups of goroutines that wait at the same place and run the same function.")
	value("ruvo_groups", float64(len(s.Groups)))

	metric("ruvo_group_series_omitted", "gauge", "Pairs of wait and start function left out of the ruvo_group_* series, which hold only the largest 200.")
	value("ruvo_group_series_omitted", float64(len(all)-len(shown)))

	perGroup("ruvo_group_goroutines", "Goroutines that wait in one function and were started by one function; the largest 200 such pairs.", func(g series) float64 {
		return float64(g.count)
	})

	perGroup("ruvo_group_growth_per_minute", "Trend of ruvo_group_goroutines per minute over the last two minutes.", func(g series) float64 {
		return g.growth
	})

	if !s.LeaksAt.IsZero() {
		leaked := 0
		for _, g := range all {
			leaked += g.leaked
		}

		metric("ruvo_leaked_goroutines", "gauge", "Goroutines the last goroutineleak profile found blocked forever, in all groups.")
		value("ruvo_leaked_goroutines", float64(leaked))

		perGroup("ruvo_group_leaked", "Goroutines of ruvo_group_goroutines the last goroutineleak profile found blocked forever.", func(g series) float64 {
			return float64(g.leaked)
		})
	}

	for _, t := range []struct {
		name, help string
		at         time.Time
	}{
		{"ruvo_snapshot_timestamp_seconds", "When the numbers were taken.", s.At},
		{"ruvo_full_dump_timestamp_seconds", "When the last full dump was taken.", s.FullAt},
		{"ruvo_leaks_timestamp_seconds", "When the goroutineleak profile last ran.", s.LeaksAt},
		{"ruvo_trace_timestamp_seconds", "When the last execution trace ended.", s.TraceAt},
	} {
		if !t.at.IsZero() {
			metric(t.name, "gauge", t.help)
			value(t.name, float64(t.at.UnixMilli())/1000)
		}
	}

	_, err := w.Write(b.Bytes())

	return err
}
