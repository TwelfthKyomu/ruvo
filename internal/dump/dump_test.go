package dump

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func open(t *testing.T, name string) *os.File {
	t.Helper()

	f, err := os.Open("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { f.Close() })

	return f
}

func TestParseStacks(t *testing.T) {
	p, err := ParseStacks(open(t, "stacks.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if p.Total != 26 || len(p.Records) != 3 || p.Records[0].Count != 17 {
		t.Fatalf("total %d, %d records, first count %d", p.Total, len(p.Records), p.Records[0].Count)
	}

	want := Frame{"internal/poll.runtime_pollWait", "/usr/local/go/src/runtime/netpoll.go", 351}
	if got := p.Records[0].Frames[0]; got != want {
		t.Fatalf("first frame %+v, want %+v", got, want)
	}

	if got := p.Records[0].Frames[2].Func; got != "net/http.(*connReader).backgroundRead" {
		t.Fatalf("entry %q", got)
	}

	wantLast := []Frame{
		{"main.(*pipeline).handleWork", "/home/dev/leaky/pipeline.go", 145},
		{"net/http.(*conn).serve", "/usr/local/go/src/net/http/server.go", 2157},
	}
	if got := p.Records[2].Frames; !reflect.DeepEqual(got, wantLast) {
		t.Fatalf("labelled record frames %+v", got)
	}
}

func TestParseStacksLeakHeader(t *testing.T) {
	p, err := ParseStacks(open(t, "leak.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if p.Total != 1390 || p.Records[0].Frames[0].Func != "main.notify" {
		t.Fatalf("total %d, records %+v", p.Total, p.Records)
	}
}

func TestParseStacksRejectsGarbage(t *testing.T) {
	if _, err := ParseStacks(strings.NewReader("<html>404 page not found</html>")); err == nil {
		t.Fatal("want error for a non-profile body")
	}
}

func TestParseFull(t *testing.T) {
	gs, err := ParseFull(open(t, "full.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if len(gs) != 5 {
		t.Fatalf("got %d goroutines, want 5", len(gs))
	}

	if gs[0].ID != 652 || gs[0].State != "running" {
		t.Fatalf("goroutine 0: %+v", gs[0])
	}

	worker := Frame{"main.(*pipeline).worker", "/home/dev/leaky/pipeline.go", 114}
	creator := Frame{"main.newPipeline", "/home/dev/leaky/pipeline.go", 64}

	if g := gs[1]; g.ID != 9 || g.State != "sleep" || g.Wait != 0 || g.Frames[1] != worker || g.CreatedBy != creator || g.Creator != 1 {
		t.Fatalf("goroutine 1: %+v", g)
	}

	if g := gs[2]; g.ID != 35 || g.State != "chan receive" || g.Wait != 7*time.Minute || g.Frames[0].Func != "main.main.func1" {
		t.Fatalf("goroutine 2: %+v", g)
	}

	if g := gs[3]; g.ID != 18 || g.State != "IO wait" || g.Wait != 2*time.Minute || len(g.Frames) != 2 || g.Creator != 17 {
		t.Fatalf("goroutine 3: %+v", g)
	}

	lock := Frame{"sync.(*Mutex).Lock", "/usr/local/go/src/sync/mutex.go", 73}
	if g := gs[4]; g.Frames[0] != lock {
		t.Fatalf("goroutine 4 first frame %+v, want %+v", g.Frames[0], lock)
	}
}

func TestParseFullTruncated(t *testing.T) {
	gs, err := ParseFull(open(t, "full_truncated.txt"))
	if err != nil {
		t.Fatal(err)
	}

	var ids []int64

	for _, g := range gs {
		ids = append(ids, g.ID)
	}

	if !reflect.DeepEqual(ids, []int64{652, 9}) {
		t.Fatalf("ids %v, want [652 9]", ids)
	}
}

func TestParseFullRejectsGarbage(t *testing.T) {
	if _, err := ParseFull(strings.NewReader("goroutine profile: total 3\n")); err == nil {
		t.Fatal("want error for a debug=1 body")
	}
}

func TestFrameString(t *testing.T) {
	if got := (Frame{"main.worker", "/home/dev/leaky/pipeline.go", 114}).String(); got != "main.worker pipeline.go:114" {
		t.Fatalf("got %q", got)
	}

	if got := (Frame{}).String(); got != "" {
		t.Fatalf("empty frame: got %q", got)
	}
}
