package ruvo

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
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

	return c.frozen || !c.down
}

func writeMetrics(w io.Writer, s Snapshot, up bool) error {
	var b bytes.Buffer

	metric := func(name, kind, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}

	value := func(name string, v float64) {
		fmt.Fprintf(&b, "%s %s\n", name, strconv.FormatFloat(v, 'g', -1, 64))
	}

	groups := s.Groups[:min(len(s.Groups), metricGroupsMax)]

	perGroup := func(name, help string, v func(Group) (float64, bool)) {
		metric(name, "gauge", help)

		for _, g := range groups {
			if x, ok := v(g); ok {
				fmt.Fprintf(&b, "%s{id=\"%s\",site=\"%s\",entry=\"%s\"} %s\n", name, g.ID, labelEscaper.Replace(g.WaitSite), labelEscaper.Replace(g.Entry), strconv.FormatFloat(x, 'g', -1, 64))
			}
		}
	}

	upValue := 0.0
	if up {
		upValue = 1
	}

	metric("ruvo_target_up", "gauge", "1 if the last read of the target succeeded, 0 if it failed.")
	value("ruvo_target_up", upValue)

	metric("ruvo_goroutines", "gauge", "Goroutines of the target, not counting ruvo's own.")
	value("ruvo_goroutines", float64(s.Goroutines))

	metric("ruvo_groups", "gauge", "Groups of goroutines that wait at the same place and run the same function.")
	value("ruvo_groups", float64(len(s.Groups)))

	perGroup("ruvo_group_goroutines", "Goroutines in a group; the largest 200 groups.", func(g Group) (float64, bool) {
		return float64(g.Count), true
	})

	perGroup("ruvo_group_growth_per_minute", "Trend of a group's size per minute over the last two minutes.", func(g Group) (float64, bool) {
		return g.Growth, true
	})

	perGroup("ruvo_group_leaked", "Goroutines of a group the last goroutineleak profile found blocked forever.", func(g Group) (float64, bool) {
		return float64(g.Leaked), !s.LeaksAt.IsZero()
	})

	for _, t := range []struct {
		name, help string
		at         time.Time
	}{
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
