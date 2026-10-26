package ruvo

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

type tracingView struct {
	ws []Wakeup
}

func (tracingView) Name() string { return "tracing" }

func (tracingView) Handler(api http.Handler) http.Handler { return api }

func (v tracingView) ReadTrace(io.Reader, map[int64][]Frame) ([]Wakeup, error) {
	return v.ws, nil
}

var sessionWakeups = []Wakeup{
	{Cause: CauseGoroutine, Waker: workerStack, Waiter: handlerStack, Count: 3, Waiters: 1, Wait: time.Second},
	{Cause: CauseNone, Waiter: workerStack, Count: 2, Waiters: 2, Wait: 10 * time.Second},
	{Cause: CauseTimer, Count: 4},
}

func unzip(t *testing.T, b []byte) map[string][]byte {
	t.Helper()

	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}

	out := map[string][]byte{}

	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}

		data, err := io.ReadAll(rc)
		rc.Close()

		if err != nil {
			t.Fatal(err)
		}

		out[f.Name] = data
	}

	return out
}

func jsonOf(t *testing.T, s Snapshot) string {
	t.Helper()

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}

func exported(t *testing.T) (*fakeSource, *collector, []byte) {
	t.Helper()

	src := newFake(t, "stacks.txt", "full.txt", "leak.txt")
	src.trace = []byte("raw trace bytes")
	srv, c := serve(t, src, tracingView{ws: sessionWakeups})

	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/api/snapshot"},
		{http.MethodPost, "/api/leaks"},
		{http.MethodPost, "/api/trace?seconds=1"},
	} {
		if code, body, _ := call(t, req.method, srv.URL+req.path); code != http.StatusOK {
			t.Fatalf("%s %s: status %d, %.120s", req.method, req.path, code, body)
		}
	}

	code, body, hdr := call(t, http.MethodGet, srv.URL+"/api/export")
	if code != http.StatusOK || hdr.Get("Content-Type") != "application/zip" || !strings.Contains(hdr.Get("Content-Disposition"), `filename="ruvo-fake-`) {
		t.Fatalf("status %d, headers %v", code, hdr)
	}

	return src, c, []byte(body)
}

func TestExportHoldsWhatWasRead(t *testing.T) {
	src, c, b := exported(t)
	files := unzip(t, b)

	want := []string{fileReadme, fileSession, fileSnapshot, fileStacks, fileFull, fileLeaks, fileTrace}
	if got := slices.Sorted(maps.Keys(files)); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("files %v, want %v", got, want)
	}

	for name, data := range map[string][]byte{fileStacks: src.debug1, fileFull: src.debug2, fileLeaks: src.leaks, fileTrace: src.trace} {
		if !bytes.Equal(files[name], data) {
			t.Errorf("%s holds %d bytes, not the %d that were read", name, len(files[name]), len(data))
		}
	}

	var snap Snapshot

	if err := json.Unmarshal(files[fileSnapshot], &snap); err != nil {
		t.Fatal(err)
	}

	last := c.lastSnapshot()
	if jsonOf(t, snap) != jsonOf(t, last) || len(snap.Edges) != 1 || snap.TraceUnknown != 4 {
		t.Fatalf("snapshot.json\n%s\nwant\n%s", jsonOf(t, snap), jsonOf(t, last))
	}

	var info sessionInfo

	if err := json.Unmarshal(files[fileSession], &info); err != nil {
		t.Fatal(err)
	}

	if info.Format != sessionFormat || info.Target != "fake" || len(info.Files) != 4 || info.Files[3].For != time.Second || info.Files[1].At.IsZero() {
		t.Fatalf("session.json %+v", info)
	}

	if !bytes.Contains(files[fileReadme], []byte("go tool trace trace.out")) {
		t.Fatalf("README.txt:\n%s", files[fileReadme])
	}
}

func TestOpenSessionServesExport(t *testing.T) {
	_, c, b := exported(t)
	last := c.lastSnapshot()

	h, err := OpenSession(bytes.NewReader(b), int64(len(b)), tracingView{})
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, path := range []string{"/api/snapshot", "/tracing/api/snapshot"} {
		_, body, _ := call(t, http.MethodGet, srv.URL+path)

		var s Snapshot

		if err := json.Unmarshal([]byte(body), &s); err != nil {
			t.Fatal(err)
		}

		if !s.Offline {
			t.Fatalf("GET %s: not marked offline", path)
		}

		s.Offline = false

		if jsonOf(t, s) != jsonOf(t, last) {
			t.Fatalf("GET %s:\n%s\nwant\n%s", path, jsonOf(t, s), jsonOf(t, last))
		}
	}

	for _, path := range []string{"/api/full", "/api/leaks", "/api/trace"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		resp.Body.Close()

		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("POST %s: status %d, want 409", path, resp.StatusCode)
		}
	}

	if _, again, _ := call(t, http.MethodGet, srv.URL+"/api/export"); !bytes.Equal([]byte(again), b) {
		t.Fatal("exporting an opened session must give back the same file")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/events", nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1<<20)

	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			if !strings.Contains(data, `"offline":true`) {
				t.Fatalf("event %.120s", data)
			}

			return
		}
	}

	t.Fatal("no event")
}

func TestOpenSessionRejectsBadFiles(t *testing.T) {
	_, _, good := exported(t)
	files := unzip(t, good)

	rezip := func(edit func(map[string][]byte)) []byte {
		m := maps.Clone(files)
		edit(m)

		var b bytes.Buffer

		zw := zip.NewWriter(&b)

		for name, data := range m {
			w, _ := zw.Create(name)
			w.Write(data)
		}

		zw.Close()

		return b.Bytes()
	}

	for name, b := range map[string][]byte{
		"not a zip":   []byte("goroutine profile: total 3\n"),
		"no snapshot": rezip(func(m map[string][]byte) { delete(m, fileSnapshot) }),
		"no session":  rezip(func(m map[string][]byte) { delete(m, fileSession) }),
		"newer":       rezip(func(m map[string][]byte) { m[fileSession] = []byte(`{"format":2}`) }),
		"lost file":   rezip(func(m map[string][]byte) { delete(m, fileTrace) }),
	} {
		if _, err := OpenSession(bytes.NewReader(b), int64(len(b))); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestExportTakesFirstSnapshot(t *testing.T) {
	src := newFake(t, "stacks.txt", "full.txt", "")
	srv, _ := serve(t, src)

	_, body, _ := call(t, http.MethodGet, srv.URL+"/api/export")
	files := unzip(t, []byte(body))

	if !bytes.Equal(files[fileStacks], src.debug1) || !bytes.Equal(files[fileFull], src.debug2) {
		t.Fatalf("files %v: the export must read the target when nothing was read yet", slices.Sorted(maps.Keys(files)))
	}
}

func TestRawDroppedWhenIdle(t *testing.T) {
	c, _ := newTestCollector(t, newFake(t, "stacks.txt", "full.txt", "leak.txt"))
	c.sample(context.Background(), true)
	c.captureLeaks(context.Background())

	if n := len(c.session(context.Background()).info.Files); n != 3 {
		t.Fatalf("%d raw files, want 3", n)
	}

	c.mu.Lock()
	c.stopLocked()
	c.mu.Unlock()

	if files := c.session(context.Background()).info.Files; len(files) != 0 {
		t.Fatalf("raw files %+v kept after the collector stopped", files)
	}
}

func TestSessionFileName(t *testing.T) {
	at := time.Date(2026, 10, 26, 15, 4, 5, 0, time.UTC)

	for target, want := range map[string]string{
		"http://user:xxxxx@api.internal:6060/base": "ruvo-api.internal-6060-20261026-150405.zip",
		"this process": "ruvo-this-process-20261026-150405.zip",
		"":             "ruvo-session-20261026-150405.zip",
	} {
		s := session{info: sessionInfo{Target: target, Exported: at}}
		if got := s.fileName(); got != want {
			t.Errorf("%q: %q, want %q", target, got, want)
		}
	}
}

func manifestZip(t *testing.T, names []string, sizes map[string]int) []byte {
	t.Helper()

	var b bytes.Buffer

	zw := zip.NewWriter(&b)

	put := func(name string, data []byte) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}

		w.Write(data)
	}

	put(fileSnapshot, []byte(`{"target":"x"}`))

	for name, n := range sizes {
		put(name, make([]byte, n))
	}

	files := make([]sessionFile, len(names))
	for i, name := range names {
		files[i] = sessionFile{Name: name}
	}

	info, _ := json.Marshal(sessionInfo{Format: sessionFormat, Files: files})
	put(fileSession, info)
	zw.Close()

	return b.Bytes()
}

func TestOpenSessionRejectsRepeatedFiles(t *testing.T) {
	b := manifestZip(t, []string{fileStacks, fileStacks, fileStacks}, map[string]int{fileStacks: 1 << 20})

	if _, err := readSession(bytes.NewReader(b), int64(len(b))); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err %v: a file listed many times must not be unpacked many times", err)
	}
}

func TestOpenSessionLimitsSize(t *testing.T) {
	limits := sessionLimits
	t.Cleanup(func() { sessionLimits = limits })

	sessionLimits.file, sessionLimits.total = 1<<20, 3<<20

	for name, tc := range map[string]struct {
		names []string
		sizes map[string]int
		ok    bool
	}{
		"fits":      {[]string{fileStacks, fileFull}, map[string]int{fileStacks: 1 << 20, fileFull: 1 << 20}, true},
		"file":      {[]string{fileFull}, map[string]int{fileFull: 1<<20 + 1}, false},
		"total":     {[]string{fileStacks, fileFull, fileLeaks}, map[string]int{fileStacks: 1 << 20, fileFull: 1 << 20, fileLeaks: 1 << 20}, false},
		"snapshots": {nil, nil, true},
	} {
		b := manifestZip(t, tc.names, tc.sizes)

		if _, err := readSession(bytes.NewReader(b), int64(len(b))); (err == nil) != tc.ok {
			t.Errorf("%s: err %v, want ok %v", name, err, tc.ok)
		}
	}
}

func TestExportFitsWhatOpenSessionReads(t *testing.T) {
	limits := sessionLimits
	t.Cleanup(func() { sessionLimits = limits })

	sessionLimits.file, sessionLimits.total = 1<<20, 3<<20

	file := func(name string, n int) sessionFile {
		return sessionFile{Name: name, What: name, data: bytes.Repeat([]byte{'x'}, n)}
	}

	for name, tc := range map[string]struct {
		files   []sessionFile
		omitted []string
	}{
		"fits":      {[]sessionFile{file(fileStacks, 1<<20), file(fileFull, 1<<20)}, nil},
		"total":     {[]sessionFile{file(fileStacks, 1<<20), file(fileFull, 1<<20), file(fileLeaks, 1<<20), file(fileTrace, 1<<20)}, []string{fileTrace, fileLeaks}},
		"one file":  {[]sessionFile{file(fileFull, 1<<20+1), file(fileStacks, 10)}, []string{fileFull}},
		"at limits": {[]sessionFile{file(fileStacks, 1<<20), file(fileFull, 1<<20), file(fileLeaks, 1<<20-4096)}, nil},
	} {
		s := session{
			info: sessionInfo{Format: sessionFormat, Target: "x", Exported: t0, Files: tc.files},
			snap: Snapshot{Target: "x", Groups: []Group{}, Edges: []Edge{}},
		}

		p, err := pack(s)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		var b bytes.Buffer

		if err := p.write(&b); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		got, err := readSession(bytes.NewReader(b.Bytes()), int64(b.Len()))
		if err != nil {
			t.Fatalf("%s: an exported session does not open: %v", name, err)
		}

		if !slices.Equal(got.info.Omitted, tc.omitted) {
			t.Errorf("%s: omitted %q, want %q", name, got.info.Omitted, tc.omitted)
		}

		if len(tc.omitted) > 0 && !bytes.Contains(unzip(t, b.Bytes())[fileReadme], []byte("Left out")) {
			t.Errorf("%s: README does not say what was left out", name)
		}
	}
}

func sessionZip(t *testing.T, snapshot string, extra ...string) []byte {
	t.Helper()

	var b bytes.Buffer

	zw := zip.NewWriter(&b)

	put := func(name, data string) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}

		io.WriteString(w, data)
	}

	info, _ := json.Marshal(sessionInfo{Format: sessionFormat})
	put(fileSession, string(info))
	put(fileSnapshot, snapshot)

	for _, name := range extra {
		put(name, "")
	}

	zw.Close()

	return b.Bytes()
}

func TestOpenSessionRejectsRepeatedEntries(t *testing.T) {
	b := sessionZip(t, `{"target":"first"}`, fileSnapshot)

	if _, err := readSession(bytes.NewReader(b), int64(len(b))); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err %v: a session with two %s must not open", err, fileSnapshot)
	}
}

func TestOpenSessionLimitsEntries(t *testing.T) {
	names := make([]string, 3000)
	for i := range names {
		names[i] = fmt.Sprintf("x%d", i)
	}

	b := sessionZip(t, `{"target":"x"}`, names...)

	if _, err := readSession(bytes.NewReader(b), int64(len(b))); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("err %v: an archive with thousands of entries must not open", err)
	}
}

func TestOpenSessionChecksSnapshot(t *testing.T) {
	group := `{"id":"%s","waitSite":"main.w w.go:1","entry":"main.e","count":%d,"labels":[{"labels":null,"count":1},{"labels":{"k":"v"},"count":2}]}`

	for name, tc := range map[string]struct {
		snap string
		ok   bool
	}{
		"good":      {`{"groups":[` + fmt.Sprintf(group, "3a66f15dd8c64d0e", 3) + `],"edges":[{"from":"3a66f15dd8c64d0e","to":"gone","kind":"unblock","count":1},{"from":"network","to":"3a66f15dd8c64d0e","kind":"unblock","count":1}]}`, true},
		"quote":     {`{"groups":[` + fmt.Sprintf(group, `a\"b`, 3) + `]}`, false},
		"newline":   {`{"groups":[` + fmt.Sprintf(group, `a\nb`, 3) + `]}`, false},
		"empty":     {`{"groups":[` + fmt.Sprintf(group, "", 3) + `]}`, false},
		"repeated":  {`{"groups":[` + fmt.Sprintf(group, "ab", 3) + `,` + fmt.Sprintf(group, "ab", 1) + `]}`, false},
		"negative":  {`{"groups":[` + fmt.Sprintf(group, "ab", -1) + `]}`, false},
		"no groups": {`{"target":"x"}`, true},
	} {
		b := sessionZip(t, tc.snap)

		s, err := readSession(bytes.NewReader(b), int64(len(b)))
		if (err == nil) != tc.ok {
			t.Errorf("%s: err %v, want ok %v", name, err, tc.ok)
		}

		if err != nil {
			continue
		}

		if s.snap.Groups == nil || s.snap.Edges == nil {
			t.Errorf("%s: nil groups or edges", name)
		}

		for _, g := range s.snap.Groups {
			if len(g.Labels) != 1 || g.Labels[0].Labels == nil || g.Stack == nil {
				t.Errorf("%s: group %+v keeps a label set without labels or a nil stack", name, g)
			}
		}

		for _, e := range s.snap.Edges {
			if e.To == "gone" {
				t.Errorf("%s: an edge to a group that is not there was kept", name)
			}
		}

		if name == "good" && len(s.snap.Edges) != 1 {
			t.Errorf("%s: edges %+v, want the network edge only", name, s.snap.Edges)
		}
	}
}
