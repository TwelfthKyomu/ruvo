package ruvo

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strconv"
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

var ownPrefixes = ownFuncs(modulePath)

func ownFuncs(pkg string) []string {
	return []string{
		pkg + ".(*handler).",
		pkg + ".(*collector).",
		pkg + ".shared[",
		pkg + "/internal/collect.",
		"runtime/pprof.write",
		"runtime/trace.(*traceMultiplexer).",
		"net/http/pprof.Trace",
	}
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

const groupCacheMax = 10_000

type stackGroup struct {
	own   bool
	id    string
	site  string
	entry string
	stack []string
	std   bool
}

type groupCache struct {
	m      map[string]stackGroup
	key    []byte
	labels map[string]map[string]string
}

func buildGroups(p dump.Profile) (map[string]*Group, int) {
	var gc groupCache

	return gc.build(p)
}

func (gc *groupCache) build(p dump.Profile) (map[string]*Group, int) {
	groups := map[string]*Group{}
	labeled := map[string]map[string]int{}
	total := 0

	for _, r := range p.Records {
		sg := gc.lookup(r.Frames)
		if sg.own {
			continue
		}

		total += r.Count

		if len(r.Frames) == 0 {
			continue
		}

		g := groups[sg.id]
		if g == nil {
			g = &Group{ID: sg.id, WaitSite: sg.site, Entry: sg.entry, Stack: sg.stack, Std: sg.std}
			groups[sg.id] = g
		}

		g.Count += r.Count

		if r.Labels != "" {
			if labeled[sg.id] == nil {
				labeled[sg.id] = map[string]int{}
			}

			labeled[sg.id][r.Labels] += r.Count
		}
	}

	for id, counts := range labeled {
		groups[id].Labels = gc.topLabels(counts)
	}

	return groups, total
}

const (
	labelSetsMax  = 10
	labelCacheMax = 10_000
)

func (gc *groupCache) topLabels(counts map[string]int) []LabelCount {
	texts := slices.SortedFunc(maps.Keys(counts), func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), strings.Compare(a, b))
	})

	out := make([]LabelCount, 0, min(len(texts), labelSetsMax))

	for _, text := range texts {
		if len(out) == labelSetsMax {
			break
		}

		if set := gc.labelSet(text); set != nil {
			out = append(out, LabelCount{Labels: set, Count: counts[text]})
		}
	}

	return out
}

func (gc *groupCache) labelSet(text string) map[string]string {
	if set, ok := gc.labels[text]; ok {
		return set
	}

	set := parseLabels(text)

	if gc.labels == nil || len(gc.labels) >= labelCacheMax {
		gc.labels = map[string]map[string]string{}
	}

	gc.labels[text] = set

	return set
}

func parseLabels(text string) map[string]string {
	s, ok := strings.CutPrefix(text, "{")
	if !ok {
		return nil
	}

	if s, ok = strings.CutSuffix(s, "}"); !ok {
		return nil
	}

	set := map[string]string{}

	for s != "" {
		k, rest, ok := unquotePrefix(s)
		if !ok {
			return nil
		}

		if rest, ok = strings.CutPrefix(rest, ":"); !ok {
			return nil
		}

		v, rest, ok := unquotePrefix(rest)
		if !ok {
			return nil
		}

		set[k] = v
		s = strings.TrimPrefix(rest, ", ")
	}

	if len(set) == 0 {
		return nil
	}

	return set
}

func unquotePrefix(s string) (string, string, bool) {
	q, err := strconv.QuotedPrefix(s)
	if err != nil {
		return "", "", false
	}

	v, err := strconv.Unquote(q)

	return v, s[len(q):], err == nil
}

func (gc *groupCache) lookup(frames []dump.Frame) stackGroup {
	gc.key = gc.key[:0]

	for _, f := range frames {
		gc.key = append(gc.key, f.Func...)
		gc.key = append(gc.key, 0)
		gc.key = append(gc.key, f.File...)
		gc.key = strconv.AppendInt(append(gc.key, 0), int64(f.Line), 10)
		gc.key = append(gc.key, '\n')
	}

	if sg, ok := gc.m[string(gc.key)]; ok {
		return sg
	}

	sg := stackGroup{own: isOwn(frames)}

	if !sg.own && len(frames) > 0 {
		site, entry := groupKey(frames)
		sg.id = groupID(site, entry)
		sg.site = site.String()
		sg.entry = entry.Func
		sg.stack = stack(frames)
		sg.std = isStd(site.Func)
	}

	if gc.m == nil || len(gc.m) >= groupCacheMax {
		gc.m = map[string]stackGroup{}
	}

	gc.m[string(gc.key)] = sg

	return sg
}

func stack(frames []dump.Frame) []string {
	out := make([]string, len(frames))

	for i, f := range frames {
		out[i] = f.String()
	}

	return out
}
