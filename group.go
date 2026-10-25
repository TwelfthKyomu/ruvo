package ruvo

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/TwelfthKyomu/ruvo/internal/dump"
)

var stdRoots = map[string]bool{
	"archive": true, "arena": true, "bufio": true, "builtin": true, "bytes": true, "cmp": true,
	"compress": true, "container": true, "context": true, "crypto": true, "database": true,
	"debug": true, "embed": true, "encoding": true, "errors": true, "expvar": true, "flag": true,
	"fmt": true, "go": true, "hash": true, "html": true, "image": true, "index": true,
	"internal": true, "io": true, "iter": true, "log": true, "maps": true, "math": true,
	"mime": true, "net": true, "os": true, "path": true, "plugin": true, "reflect": true,
	"regexp": true, "runtime": true, "simd": true, "slices": true, "sort": true, "strconv": true,
	"strings": true, "structs": true, "sync": true, "syscall": true, "testing": true, "text": true,
	"time": true, "unicode": true, "unique": true, "unsafe": true, "uuid": true, "vendor": true,
	"weak": true,
}

func isStd(fn string) bool {
	slash := strings.LastIndexByte(fn, '/')

	pkg := fn
	if dot := strings.IndexByte(fn[slash+1:], '.'); dot >= 0 {
		pkg = fn[:slash+1+dot]
	}

	first, _, _ := strings.Cut(pkg, "/")

	return stdRoots[first]
}

func groupKey(frames []dump.Frame) (site, entry dump.Frame) {
	if len(frames) == 0 {
		return dump.Frame{}, dump.Frame{}
	}

	last := len(frames) - 1

	for last > 0 && strings.HasPrefix(frames[last].Func, "runtime.") {
		last--
	}

	entry = frames[last]

	for _, f := range frames {
		if !isStd(f.Func) {
			return f, entry
		}
	}

	return entry, entry
}

func groupID(site, entry dump.Frame) string {
	h := fnv.New64a()
	h.Write([]byte(site.String() + "|" + entry.Func))

	return fmt.Sprintf("%016x", h.Sum64())
}

var ownPrefixes = []string{
	"github.com/TwelfthKyomu/ruvo.(*handler).",
	"github.com/TwelfthKyomu/ruvo.(*collector).",
	"github.com/TwelfthKyomu/ruvo.shared[",
	"github.com/TwelfthKyomu/ruvo/internal/collect.",
	"runtime/pprof.write",
	"runtime/trace.(*traceMultiplexer).",
	"net/http/pprof.Trace",
}

func isOwn(frames []dump.Frame) bool {
	for _, f := range frames {
		if f.Func == "runtime/pprof.(*Profile).WriteTo" {
			return true
		}

		for _, p := range ownPrefixes {
			if strings.HasPrefix(f.Func, p) {
				return true
			}
		}
	}

	return false
}

func buildGroups(p dump.Profile) (map[string]*Group, int) {
	groups := map[string]*Group{}
	total := 0

	for _, r := range p.Records {
		if isOwn(r.Frames) {
			continue
		}

		total += r.Count

		if len(r.Frames) == 0 {
			continue
		}

		site, entry := groupKey(r.Frames)
		id := groupID(site, entry)

		g := groups[id]
		if g == nil {
			g = &Group{ID: id, WaitSite: site.String(), Entry: entry.Func, Stack: stack(r.Frames), Std: isStd(site.Func)}
			groups[id] = g
		}

		g.Count += r.Count
	}

	return groups, total
}

func stack(frames []dump.Frame) []string {
	out := make([]string, len(frames))

	for i, f := range frames {
		out[i] = f.String()
	}

	return out
}
