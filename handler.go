package ruvo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const ping = 15 * time.Second

var emptyNav = []byte(`<nav id="views"></nav>`)

var errBadSeconds = fmt.Errorf("seconds must be a whole number from 1 to %d", int(traceMax/time.Second))

type handler struct {
	c     *collector
	views map[string]http.Handler
	cop   *http.CrossOriginProtection
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rel := relPath(r)

	switch {
	case rel == "/":
		h.serveIndex(w, r)
	case strings.HasPrefix(rel, "/api/"):
		h.serveAPI(w, r, rel)
	default:
		h.serveView(w, r, rel)
	}
}

func relPath(r *http.Request) string {
	rel := r.URL.Path

	if u, err := url.ParseRequestURI(r.RequestURI); err != nil || u.Path == rel {
		rel = strings.TrimPrefix(rel, mountPrefix(r))
	}

	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}

	return rel
}

func mountPrefix(r *http.Request) string {
	p := r.Pattern

	if i := strings.IndexByte(p, ' '); i >= 0 {
		p = p[i+1:]
	}

	i := strings.IndexByte(p, '/')
	if i < 0 || strings.Contains(p, "{") {
		return ""
	}

	return strings.TrimSuffix(p[i:], "/")
}

func (h *handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	page, err := fs.ReadFile(uiFS, "ui/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(bytes.Replace(page, emptyNav, h.nav(), 1))
}

func (h *handler) nav() []byte {
	var b bytes.Buffer

	b.WriteString(`<nav id="views">`)

	for _, name := range slices.Sorted(maps.Keys(h.views)) {
		fmt.Fprintf(&b, `<a href="./%s/">%s</a>`, html.EscapeString(url.PathEscape(name)), html.EscapeString(name))
	}

	b.WriteString("</nav>")

	return b.Bytes()
}

func (h *handler) serveView(w http.ResponseWriter, r *http.Request, rel string) {
	name, _, slash := strings.Cut(strings.TrimPrefix(rel, "/"), "/")

	v, ok := h.views[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	if !slash {
		redirectToView(w, r, name)
		return
	}

	r2 := new(http.Request)
	*r2 = *r

	u := *r.URL
	u.Path, u.RawPath = rel, ""
	r2.URL = &u

	http.StripPrefix("/"+name, v).ServeHTTP(w, r2)
}

func redirectToView(w http.ResponseWriter, r *http.Request, name string) {
	loc := "./" + url.PathEscape(name) + "/"

	if r.URL.RawQuery != "" {
		loc += "?" + r.URL.RawQuery
	}

	w.Header().Set("Location", loc)
	w.WriteHeader(http.StatusPermanentRedirect)
}

func (h *handler) serveAPI(w http.ResponseWriter, r *http.Request, rel string) {
	switch rel {
	case "/api/snapshot":
		writeJSON(w, h.c.snapshot(r.Context()))
	case "/api/events":
		h.serveEvents(w, r)
	case "/api/leaks":
		h.serveAction(w, r, func(r *http.Request) (Snapshot, error) { return h.c.captureLeaks(r.Context()) })
	case "/api/full":
		h.serveAction(w, r, func(r *http.Request) (Snapshot, error) { return h.c.captureFull(r.Context()) })
	case "/api/trace":
		h.serveAction(w, r, h.captureTrace)
	default:
		http.NotFound(w, r)
	}
}

func (h *handler) serveAction(w http.ResponseWriter, r *http.Request, act func(*http.Request) (Snapshot, error)) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)

		return
	}

	if err := h.cop.Check(r); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	s, err := act(r)
	if err != nil {
		http.Error(w, err.Error(), errorStatus(err))
		return
	}

	writeJSON(w, s)
}

func (h *handler) captureTrace(r *http.Request) (Snapshot, error) {
	d := traceDefault

	if q := r.URL.Query().Get("seconds"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 || n > int(traceMax/time.Second) {
			return Snapshot{}, errBadSeconds
		}

		d = time.Duration(n) * time.Second
	}

	return h.c.captureTrace(r.Context(), d)
}

func errorStatus(err error) int {
	switch {
	case errors.Is(err, errBadSeconds):
		return http.StatusBadRequest
	case errors.Is(err, errNoTraceReader):
		return http.StatusNotFound
	}

	return http.StatusBadGateway
}

func (h *handler) serveEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "use GET", http.StatusMethodNotAllowed)

		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	rc := http.NewResponseController(w)

	ch, cancel := h.c.subscribe()
	defer cancel()

	s := h.c.snapshot(r.Context())

	first, err := json.Marshal(s)
	if err != nil || send(rc, func() error { return writeEvent(w, first) }) != nil {
		return
	}

	last := s.At
	tick := time.NewTicker(ping)
	defer tick.Stop()

	for {
		var write func() error

		select {
		case <-r.Context().Done():
			return
		case m := <-ch:
			if m.s.At.Equal(last) {
				continue
			}

			last = m.s.At
			write = func() error { return writeEvent(w, m.data) }
		case <-tick.C:
			write = func() error { return writePing(w) }
		}

		if send(rc, write) != nil {
			return
		}
	}
}

func send(rc *http.ResponseController, write func() error) error {
	if err := rc.SetWriteDeadline(time.Now().Add(eventWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}

	if err := write(); err != nil {
		return err
	}

	return rc.Flush()
}

func writePing(w io.Writer) error {
	_, err := io.WriteString(w, ": ping\n\n")

	return err
}

func writeEvent(w io.Writer, data []byte) error {
	_, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)

	return err
}

func writeJSON(w http.ResponseWriter, s Snapshot) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}
