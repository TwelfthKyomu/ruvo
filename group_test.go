package ruvo

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

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

func TestBuildGroupsNamesEmptyStacks(t *testing.T) {
	groups, total := buildGroups(dump.Profile{Total: 2, Records: []dump.Record{{Count: 2}}})

	for _, g := range groups {
		if total != 2 || g.WaitSite != "(runtime)" || g.Count != 2 {
			t.Fatalf("total %d, group %+v", total, g)
		}
	}
}

func TestIsDumpWriterSeesOwnFullDump(t *testing.T) {
	if !isDumpWriter([]dump.Frame{{Func: "github.com/TwelfthKyomu/ruvo/internal/collect.(*local).full", File: "/home/dev/collect.go", Line: 67}}) {
		t.Fatal("the goroutine writing ruvo's own full dump must not become a group")
	}
}

func TestIsStdDotlessModule(t *testing.T) {
	for _, fn := range []string{"myservice.handler", "myservice/internal/db.Query", "myservice/api.(*Server).serve"} {
		if isStd(fn) {
			t.Errorf("isStd(%q) = true: a module named without a dot is user code", fn)
		}
	}
}
