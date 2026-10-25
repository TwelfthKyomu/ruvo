package ruvo

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"runtime/trace"
	"strings"
	"testing"
	"time"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

func readProfile(t *testing.T, name string) dump.Profile {
	t.Helper()

	f, err := os.Open("internal/dump/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	p, err := dump.ParseStacks(f)
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func TestIsStd(t *testing.T) {
	for fn, want := range map[string]bool{
		"main.f":                         false,
		"net/http.(*conn).serve":         true,
		"internal/poll.runtime_pollWait": true,
		"github.com/a/b.F":               false,
		"time.Sleep":                     true,
	} {
		if got := isStd(fn); got != want {
			t.Errorf("isStd(%q) = %v, want %v", fn, got, want)
		}
	}
}

func TestGroupKeyPrefersUserFrame(t *testing.T) {
	site, entry := groupKey([]dump.Frame{
		{Func: "internal/poll.runtime_pollWait", File: "/usr/local/go/src/runtime/netpoll.go", Line: 351},
		{Func: "main.(*pipeline).handleWork", File: "/home/dev/leaky/pipeline.go", Line: 145},
		{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
	})

	if site.Func != "main.(*pipeline).handleWork" || entry.Func != "net/http.(*conn).serve" {
		t.Fatalf("site %v, entry %v", site, entry)
	}
}

func TestGroupKeyStdOnlyUsesEntry(t *testing.T) {
	p := readProfile(t, "stacks.txt")

	site, entry := groupKey(p.Records[0].Frames)
	if site.Func != "net/http.(*connReader).backgroundRead" || entry.Func != site.Func {
		t.Fatalf("site %v, entry %v", site, entry)
	}
}

func TestBuildGroupsMergesSameKey(t *testing.T) {
	top := dump.Frame{Func: "main.worker", File: "/home/dev/w.go", Line: 10}
	bottom := dump.Frame{Func: "main.run", File: "/home/dev/w.go", Line: 40}

	p := dump.Profile{Total: 7, Records: []dump.Record{
		{Count: 3, Frames: []dump.Frame{top, {Func: "main.a", File: "/home/dev/w.go", Line: 20}, bottom}},
		{Count: 4, Frames: []dump.Frame{top, {Func: "main.b", File: "/home/dev/w.go", Line: 30}, bottom}},
	}}

	groups, total := buildGroups(p)
	if total != 7 || len(groups) != 1 {
		t.Fatalf("total %d, %d groups", total, len(groups))
	}

	for _, g := range groups {
		if g.Count != 7 || g.WaitSite != "main.worker w.go:10" || g.Entry != "main.run" {
			t.Fatalf("group %+v", g)
		}
	}
}

func TestBuildGroupsMarksStd(t *testing.T) {
	read := dump.Frame{Func: "net/http.(*connReader).backgroundRead", File: "/usr/local/go/src/net/http/server.go", Line: 690}
	poll := dump.Frame{Func: "internal/poll.runtime_pollWait", File: "/usr/local/go/src/runtime/netpoll.go", Line: 351}
	user := dump.Frame{Func: "main.(*pipeline).handleWork", File: "/home/dev/leaky/pipeline.go", Line: 135}

	p := dump.Profile{Total: 5, Records: []dump.Record{
		{Count: 2, Frames: []dump.Frame{poll, read}},
		{Count: 3, Frames: []dump.Frame{poll, user, read}},
	}}

	groups, _ := buildGroups(p)
	if len(groups) != 2 {
		t.Fatalf("%d groups", len(groups))
	}

	for _, g := range groups {
		if want := !strings.HasPrefix(g.WaitSite, "main."); g.Std != want {
			t.Errorf("group %q: Std %v, want %v", g.WaitSite, g.Std, want)
		}
	}
}

func TestBuildGroupsDropsDumpWriter(t *testing.T) {
	p := dump.Profile{Total: 6, Records: []dump.Record{
		{Count: 1, Frames: []dump.Frame{{Func: "runtime/pprof.writeGoroutineStacks", File: "/usr/local/go/src/runtime/pprof/pprof.go", Line: 816}}},
		{Count: 5, Frames: []dump.Frame{{Func: "main.worker", File: "/home/dev/w.go", Line: 10}}},
	}}

	groups, total := buildGroups(p)
	if total != 5 || len(groups) != 1 {
		t.Fatalf("total %d, %d groups", total, len(groups))
	}
}

func TestSnapshotJSONOmitsUnsetTimes(t *testing.T) {
	b, err := json.Marshal(Snapshot{Edges: []Edge{}})
	if err != nil {
		t.Fatal(err)
	}

	s := string(b)
	if !strings.Contains(s, `"edges":[]`) || strings.Contains(s, `"fullAt"`) || strings.Contains(s, `"leaksAt"`) {
		t.Fatalf("json %s", s)
	}
}

func TestGroupKeySkipsRuntimeEntry(t *testing.T) {
	stacks := []dump.Frame{
		{Func: "internal/poll.runtime_pollWait", File: "/usr/local/go/src/runtime/netpoll.go", Line: 351},
		{Func: "main.main", File: "/home/dev/leaky/main.go", Line: 31},
		{Func: "runtime.main", File: "/usr/local/go/src/runtime/proc.go", Line: 283},
	}
	full := stacks[:2]

	site1, entry1 := groupKey(stacks)
	site2, entry2 := groupKey(full)

	if entry1.Func != "main.main" || groupID(site1, entry1) != groupID(site2, entry2) {
		t.Fatalf("debug=1 key %v/%v, debug=2 key %v/%v", site1, entry1, site2, entry2)
	}
}

func TestBuildGroupsSkipsEmptyStacks(t *testing.T) {
	groups, total := buildGroups(dump.Profile{Total: 3, Records: []dump.Record{
		{Count: 2},
		{Count: 1, Frames: []dump.Frame{{Func: "main.x", File: "/home/dev/a.go", Line: 7}}},
	}})

	if total != 3 || len(groups) != 1 {
		t.Fatalf("total %d, %d groups", total, len(groups))
	}

	for _, g := range groups {
		if g.WaitSite != "main.x a.go:7" || g.Count != 1 {
			t.Fatalf("group %+v", g)
		}
	}
}

func TestBuildGroupsDropsOwn(t *testing.T) {
	p := dump.Profile{Total: 9, Records: []dump.Record{
		{Count: 3, Frames: []dump.Frame{
			{Func: "runtime.gopark", File: "/usr/local/go/src/runtime/proc.go", Line: 460},
			{Func: "github.com/TwelfthKyomu/ruvo.(*handler).serveEvents", File: "/home/dev/ruvo/handler.go", Line: 150},
			{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
		}},
		{Count: 1, Frames: []dump.Frame{
			{Func: "github.com/TwelfthKyomu/ruvo.(*collector).loop", File: "/home/dev/ruvo/collector.go", Line: 330},
		}},
		{Count: 5, Frames: []dump.Frame{{Func: "main.worker", File: "/home/dev/w.go", Line: 10}}},
	}}

	groups, total := buildGroups(p)
	if total != 5 || len(groups) != 1 {
		t.Fatalf("total %d, %d groups", total, len(groups))
	}
}

func TestIsOwnHidesRuvo(t *testing.T) {
	for fn, want := range map[string]bool{
		"github.com/TwelfthKyomu/ruvo.(*handler).serveEvents":          true,
		"github.com/TwelfthKyomu/ruvo.(*collector).loop":               true,
		"github.com/TwelfthKyomu/ruvo.shared[...].func1":               true,
		"github.com/TwelfthKyomu/ruvo/internal/collect.(*local).Trace": true,
		"github.com/TwelfthKyomu/ruvo/internal/collect.(*local).full":  true,
		"runtime/pprof.writeGoroutine":                                 true,
		"runtime/pprof.(*Profile).WriteTo":                             true,
		"runtime/trace.(*traceMultiplexer).startLocked.func1":          true,
		"runtime/trace.StartRegion":                                    false,
		"net/http/pprof.Trace":                                         true,
		"net/http/pprof.Index":                                         false,
		"github.com/TwelfthKyomu/ruvo.parked":                          false,
		"github.com/TwelfthKyomu/ruvo/examples/leaky.notify":           false,
		"main.notify":                   false,
		"runtime/pprof.StartCPUProfile": false,
	} {
		if got := isOwn([]dump.Frame{{Func: fn, File: "/home/dev/a.go", Line: 1}}); got != want {
			t.Errorf("isOwn(%q) = %v, want %v", fn, got, want)
		}
	}

	if isOwn(nil) {
		t.Error("isOwn(nil) = true: a record without frames is not ruvo's own")
	}

	deep := []dump.Frame{
		{Func: "runtime.gopark", File: "/usr/local/go/src/runtime/proc.go", Line: 460},
		{Func: "github.com/TwelfthKyomu/ruvo.(*handler).serveAPI", File: "/home/dev/ruvo/handler.go", Line: 100},
		{Func: "net/http.(*conn).serve", File: "/usr/local/go/src/net/http/server.go", Line: 2157},
	}

	if !isOwn(deep) {
		t.Error("isOwn must look at every frame, not only the top one")
	}
}

func TestIsStdDotlessModule(t *testing.T) {
	for _, fn := range []string{"myservice.handler", "myservice/internal/db.Query", "myservice/api.(*Server).serve"} {
		if isStd(fn) {
			t.Errorf("isStd(%q) = true: a module named without a dot is user code", fn)
		}
	}
}

func TestLocalTraceReaderHidden(t *testing.T) {
	var b bytes.Buffer

	if err := trace.Start(&b); err != nil {
		t.Fatal(err)
	}

	defer trace.Stop()

	var p dump.Profile

	waitFor(t, time.Second, func() bool {
		raw, err := Local().src.Goroutines(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		p, err = dump.ParseStacks(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}

		for _, r := range p.Records {
			for _, f := range r.Frames {
				if strings.HasPrefix(f.Func, "runtime/trace.") {
					return true
				}
			}
		}

		return false
	})

	groups, _ := buildGroups(p)

	for _, g := range groups {
		for _, line := range g.Stack {
			if strings.HasPrefix(line, "runtime/trace.") {
				t.Fatalf("the trace reader goroutine is in group %s: %v", g.WaitSite, g.Stack)
			}
		}
	}
}
