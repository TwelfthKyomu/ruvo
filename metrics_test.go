package ruvo

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

var sampleLine = regexp.MustCompile(`^[a-z_]+(\{id="[0-9a-f]*",site="([^"\\]|\\.)*",entry="([^"\\]|\\.)*"\})? -?[0-9.e+-]+$`)

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

	worker, ok := groupByEntry(c.lastSnapshot(), "main.(*pipeline).worker")
	if !ok {
		t.Fatal("no worker group")
	}

	key := `ruvo_group_goroutines{id="` + worker.ID + `",site="main.(*pipeline).worker pipeline.go:114",entry="main.(*pipeline).worker"}`
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

	s := Snapshot{Groups: []Group{{ID: "ab", WaitSite: `main.f "x" a\b.go:1`, Entry: "main.g\nh", Count: 2}}}

	if err := writeMetrics(&b, s, true); err != nil {
		t.Fatal(err)
	}

	m := checkExposition(t, b.String())

	if m[`ruvo_group_goroutines{id="ab",site="main.f \"x\" a\\b.go:1",entry="main.g\nh"}`] != "2" {
		t.Fatalf("samples %v", m)
	}
}
