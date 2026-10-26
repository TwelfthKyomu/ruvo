package ruvo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

var sampleLine = regexp.MustCompile(`^[a-z_]+(\{wait="([^"\\]|\\.)*",entry="([^"\\]|\\.)*"\})? -?[0-9.e+-]+$`)

func checkExposition(t *testing.T, body string) map[string]string {
	t.Helper()

	samples := map[string]string{}

	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
			continue
		}

		if !sampleLine.MatchString(line) {
			t.Fatalf("not a Prometheus sample: %q", line)
		}

		i := strings.LastIndexByte(line, ' ')
		samples[line[:i]] = line[i+1:]
	}

	return samples
}

func TestMetricsEndpoint(t *testing.T) {
	src := newFake(t, "stacks.txt", "", "")
	srv, c := serve(t, src)

	code, body, hdr := call(t, http.MethodGet, srv.URL+"/api/metrics")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("status %d, content type %q", code, hdr.Get("Content-Type"))
	}

	m := checkExposition(t, body)

	if m["ruvo_target_up"] != "1" || m["ruvo_goroutines"] != "26" || m["ruvo_groups"] != "3" {
		t.Fatalf("samples %v", m)
	}

	key := `ruvo_group_goroutines{wait="main.(*pipeline).worker",entry="main.(*pipeline).worker"}`
	if m[key] != "8" {
		t.Fatalf("%s = %q in %v", key, m[key], m)
	}

	if strings.Contains(body, "ruvo_group_leaked{") || strings.Contains(body, "ruvo_trace_timestamp_seconds ") {
		t.Fatal("leaked counts or trace time before anything was measured")
	}

	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	if _, body, _ := call(t, http.MethodGet, srv.URL+"/api/metrics"); checkExposition(t, body)["ruvo_target_up"] != "0" {
		t.Fatal("a failed read must report the target down")
	}
}

func TestMetricsEscapeLabels(t *testing.T) {
	var b bytes.Buffer

	s := Snapshot{Groups: []Group{{ID: "a\"b", WaitSite: `main.f"x\y a.go:1`, Entry: "main.g\nh", Count: 2}}}

	if err := writeMetrics(&b, s, true); err != nil {
		t.Fatal(err)
	}

	m := checkExposition(t, b.String())

	if m[`ruvo_group_goroutines{wait="main.f\"x\\y",entry="main.g\nh"}`] != "2" {
		t.Fatalf("samples %v", m)
	}
}

func TestMetricsSeriesSurviveLineChanges(t *testing.T) {
	group := func(id, site string, n int, leaked int) Group {
		return Group{ID: id, WaitSite: site, Entry: "main.serve", Count: n, Growth: 1, Leaked: leaked}
	}

	before := Snapshot{LeaksAt: t0, Groups: []Group{group("a1", "main.wait w.go:10", 5, 1), group("a2", "main.wait w.go:20", 3, 0)}}
	after := Snapshot{LeaksAt: t0, Groups: []Group{group("b1", "main.wait w.go:12", 8, 1)}}

	for i := range 300 {
		before.Groups = append(before.Groups, group(fmt.Sprintf("c%d", i), fmt.Sprintf("main.small%d s.go:1", i), 1, 1))
	}

	var b1, b2 bytes.Buffer

	writeMetrics(&b1, before, true)
	writeMetrics(&b2, after, true)

	m1, m2 := checkExposition(t, b1.String()), checkExposition(t, b2.String())
	key := `ruvo_group_goroutines{wait="main.wait",entry="main.serve"}`

	if m1[key] != "8" || m2[key] != "8" {
		t.Fatalf("%s: %q before and %q after a line moved, want one series of 8", key, m1[key], m2[key])
	}

	if m1[`ruvo_group_growth_per_minute{wait="main.wait",entry="main.serve"}`] != "2" {
		t.Fatalf("growth of merged groups %v", m1)
	}

	if m1["ruvo_group_series_omitted"] != "101" || m1["ruvo_leaked_goroutines"] != "301" {
		t.Fatalf("omitted %q, leaked %q: want 101 left out and leaks counted in all groups", m1["ruvo_group_series_omitted"], m1["ruvo_leaked_goroutines"])
	}
}

func TestMetricsOfOpenedSession(t *testing.T) {
	src, c, good := exported(t)

	src.set(func(f *fakeSource) { f.err = errors.New("connection refused") })
	c.sample(context.Background(), false)

	p, err := pack(c.session(context.Background()))
	if err != nil {
		t.Fatal(err)
	}

	var failed bytes.Buffer

	p.write(&failed)

	for name, tc := range map[string]struct {
		data []byte
		up   string
	}{
		"good":   {good, "1"},
		"failed": {failed.Bytes(), "0"},
	} {
		h, err := OpenSession(bytes.NewReader(tc.data), int64(len(tc.data)))
		if err != nil {
			t.Fatal(err)
		}

		srv := httptest.NewServer(h)
		_, body, _ := call(t, http.MethodGet, srv.URL+"/api/metrics")
		srv.Close()

		m := checkExposition(t, body)

		if m["ruvo_target_up"] != tc.up || m["ruvo_offline"] != "1" {
			t.Errorf("%s: target_up %q offline %q, want %s and 1", name, m["ruvo_target_up"], m["ruvo_offline"], tc.up)
		}
	}
}
