package ruvo

import (
	"archive/zip"
	"bytes"
	"cmp"
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"slices"
	"strings"
	"time"
)

const (
	sessionFormat = 1
	modulePath    = "github.com/TwelfthKyomu/ruvo"

	fileReadme    = "README.txt"
	fileSession   = "session.json"
	fileSnapshot  = "snapshot.json"
	fileStacks    = "goroutines.txt"
	fileFull      = "goroutines-full.txt"
	fileLeaks     = "goroutineleak.txt"
	fileTrace     = "trace.out"
	fileTraceFull = "trace-goroutines-full.txt"
)

var rawFiles = []string{fileStacks, fileFull, fileLeaks, fileTrace, fileTraceFull}

var sessionLimits = struct {
	file  int64
	total int64
}{file: 80 << 20, total: 320 << 20}

type session struct {
	info sessionInfo
	snap Snapshot
}

type sessionInfo struct {
	Format   int           `json:"format"`
	Ruvo     string        `json:"ruvo"`
	Go       string        `json:"go,omitempty"`
	Target   string        `json:"target"`
	Exported time.Time     `json:"exported"`
	Files    []sessionFile `json:"files"`
}

type sessionFile struct {
	Name string        `json:"name"`
	What string        `json:"what"`
	At   time.Time     `json:"at,omitzero"`
	For  time.Duration `json:"for,omitempty"`
	Cut  bool          `json:"cut,omitempty"`
	data []byte
}

type frozenSource struct {
	name string
}

func (s frozenSource) Goroutines(context.Context, int) ([]byte, error) { return nil, errOffline }

func (s frozenSource) Leaks(context.Context) ([]byte, error) { return nil, errOffline }

func (s frozenSource) Trace(context.Context, time.Duration) ([]byte, error) { return nil, errOffline }

func (s frozenSource) Name() string { return s.name }

// OpenSession serves a session saved with the Export link of a ruvo page
// (api/export): the pages show the snapshot it holds as it was, and nothing
// reads a target, so Full dump, Find leaks and Record trace answer 409. The
// files of the session are read into memory.
func OpenSession(r io.ReaderAt, size int64, views ...View) (http.Handler, error) {
	s, err := readSession(r, size)
	if err != nil {
		return nil, err
	}

	return newHandler(newFrozenCollector(s), views), nil
}

func newFrozenCollector(s session) *collector {
	c := newCollector(frozenSource{name: s.snap.Target})
	c.frozen = true
	c.loaded = s
	c.last = s.snap
	c.last.Offline = true
	c.haveProfile = true

	return c
}

func (c *collector) session(ctx context.Context) session {
	if c.frozen {
		return c.loaded
	}

	c.snapshot(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	var files []sessionFile

	add := func(f sessionFile) {
		if len(f.data) > 0 {
			files = append(files, f)
		}
	}

	add(sessionFile{Name: fileStacks, What: "goroutine profile, debug=1", At: c.rawAt, data: c.raw})
	add(sessionFile{Name: fileFull, What: "full goroutine dump, debug=2", At: c.full.at, Cut: c.full.cut, data: c.full.raw})
	add(sessionFile{Name: fileLeaks, What: "goroutineleak profile, debug=1", At: c.leaks.at, data: c.leaks.raw})
	add(sessionFile{Name: fileTrace, What: "execution trace", At: c.trace.at, For: c.trace.d, data: c.trace.raw})

	if !sameBytes(c.trace.dump, c.full.raw) {
		add(sessionFile{Name: fileTraceFull, What: "full goroutine dump taken while the trace was recorded", data: c.trace.dump})
	}

	snap := c.last
	snap.Offline = false

	return session{
		info: sessionInfo{
			Format:   sessionFormat,
			Ruvo:     ruvoVersion(),
			Go:       goVersion(c.src),
			Target:   snap.Target,
			Exported: c.now(),
			Files:    files,
		},
		snap: snap,
	}
}

func sameBytes(a, b []byte) bool {
	return len(a) > 0 && len(a) == len(b) && &a[0] == &b[0]
}

func ruvoVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}

	mods := append([]*debug.Module{&bi.Main}, bi.Deps...)

	for _, m := range mods {
		if m.Path != modulePath {
			continue
		}

		if m.Replace != nil || m.Version == "" {
			return "(devel)"
		}

		return m.Version
	}

	return "(devel)"
}

func goVersion(src any) string {
	if v, ok := src.(interface{ GoVersion() string }); ok {
		return v.GoVersion()
	}

	return ""
}

func (h *handler) serveExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "use GET", http.StatusMethodNotAllowed)

		return
	}

	s := h.c.session(r.Context())

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+s.fileName()+`"`)

	if r.Method == http.MethodHead {
		return
	}

	writeSession(w, s)
}

func (s session) fileName() string {
	name := s.info.Target
	if u, err := url.Parse(name); err == nil && u.Host != "" {
		name = u.Host
	}

	slug := strings.Trim(strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.':
			return r
		}

		return '-'
	}, name), "-.")

	if len(slug) > 60 {
		slug = slug[:60]
	}

	return "ruvo-" + cmp.Or(slug, "session") + "-" + s.info.Exported.Format("20060102-150405") + ".zip"
}

func writeSession(w io.Writer, s session) error {
	zw := zip.NewWriter(w)
	zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestSpeed)
	})

	put := func(name string, at time.Time, data []byte) error {
		if at.IsZero() {
			at = s.info.Exported
		}

		f, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Deflate, Modified: at})
		if err != nil {
			return err
		}

		_, err = f.Write(data)

		return err
	}

	info, err := json.MarshalIndent(s.info, "", "  ")
	if err != nil {
		return err
	}

	snap, err := json.MarshalIndent(s.snap, "", "  ")
	if err != nil {
		return err
	}

	if err := put(fileReadme, time.Time{}, readme(s.info)); err != nil {
		return err
	}

	if err := put(fileSession, time.Time{}, info); err != nil {
		return err
	}

	if err := put(fileSnapshot, s.snap.At, snap); err != nil {
		return err
	}

	for _, f := range s.info.Files {
		if err := put(f.Name, f.At, f.data); err != nil {
			return err
		}
	}

	return zw.Close()
}

func readme(info sessionInfo) []byte {
	var b bytes.Buffer

	fmt.Fprintf(&b, "ruvo session\n\nTarget:   %s\nExported: %s\nruvo:     %s\n", info.Target, info.Exported.Format(time.RFC3339), info.Ruvo)

	if info.Go != "" {
		fmt.Fprintf(&b, "Go:       %s\n", info.Go)
	}

	b.WriteString("\nOpen it again with the ruvo command: ruvo -open <this file>\n\nFiles:\n")
	fmt.Fprintf(&b, "  %-26s what the ruvo page showed\n", fileSnapshot)
	fmt.Fprintf(&b, "  %-26s this list, for programs\n", fileSession)

	for _, f := range info.Files {
		line := f.What

		if f.For > 0 {
			line += ", " + f.For.String()
		}

		if !f.At.IsZero() {
			line += ", " + f.At.Format(time.RFC3339)
		}

		if f.Cut {
			line += ", cut at 64 MB"
		}

		if f.Name == fileTrace {
			line += " (go tool trace " + fileTrace + ")"
		}

		fmt.Fprintf(&b, "  %-26s %s\n", f.Name, line)
	}

	b.WriteString("\nThe text files are what net/http/pprof serves at /debug/pprof/goroutine\nand /debug/pprof/goroutineleak; any tool that reads Go tracebacks opens\nthe full dumps.\n")

	return b.Bytes()
}

func readSession(r io.ReaderAt, size int64) (session, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return session{}, fmt.Errorf("not a ruvo session: %w", err)
	}

	files := map[string]*zip.File{}
	for _, f := range zr.File {
		files[f.Name] = f
	}

	var total int64

	read := func(name string) ([]byte, error) {
		f, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("not a ruvo session: no %s", name)
		}

		limit := min(sessionLimits.file, sessionLimits.total-total)

		if f.UncompressedSize64 > uint64(limit) {
			return nil, fmt.Errorf("ruvo session: %s is too large", name)
		}

		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("ruvo session: %s: %w", name, err)
		}

		defer rc.Close()

		b, err := io.ReadAll(io.LimitReader(rc, limit+1))
		if err != nil {
			return nil, fmt.Errorf("ruvo session: %s: %w", name, err)
		}

		if int64(len(b)) > limit {
			return nil, fmt.Errorf("ruvo session: %s is too large", name)
		}

		total += int64(len(b))

		return b, nil
	}

	var s session

	raw, err := read(fileSession)
	if err != nil {
		return session{}, err
	}

	if err := json.Unmarshal(raw, &s.info); err != nil {
		return session{}, fmt.Errorf("ruvo session: %s: %w", fileSession, err)
	}

	if s.info.Format != sessionFormat {
		return session{}, fmt.Errorf("ruvo session: format %d, this ruvo reads format %d", s.info.Format, sessionFormat)
	}

	if raw, err = read(fileSnapshot); err != nil {
		return session{}, err
	}

	if err := json.Unmarshal(raw, &s.snap); err != nil {
		return session{}, fmt.Errorf("ruvo session: %s: %w", fileSnapshot, err)
	}

	kept := s.info.Files[:0]
	seen := map[string]bool{}

	for _, f := range s.info.Files {
		if !slices.Contains(rawFiles, f.Name) {
			continue
		}

		if seen[f.Name] {
			return session{}, fmt.Errorf("ruvo session: %s lists %s twice", fileSession, f.Name)
		}

		seen[f.Name] = true

		if f.data, err = read(f.Name); err != nil {
			return session{}, err
		}

		kept = append(kept, f)
	}

	s.info.Files = kept

	if s.snap.Groups == nil {
		s.snap.Groups = []Group{}
	}

	if s.snap.Edges == nil {
		s.snap.Edges = []Edge{}
	}

	return s, nil
}
