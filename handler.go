package ruvo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const ping = 15 * time.Second

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
	w.Write(page)
}

func (h *handler) serveView(w http.ResponseWriter, r *http.Request, rel string) {
	name, _, _ := strings.Cut(strings.TrimPrefix(rel, "/"), "/")

	v, ok := h.views[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	r2 := new(http.Request)
	*r2 = *r

	u := *r.URL
	u.Path, u.RawPath = rel, ""
	r2.URL = &u

	http.StripPrefix("/"+name, v).ServeHTTP(w, r2)
}

func (h *handler) serveAPI(w http.ResponseWriter, r *http.Request, rel string) {
	switch rel {
	case "/api/snapshot":
		writeJSON(w, h.c.snapshot(r.Context()))
	case "/api/events":
		h.serveEvents(w, r)
	case "/api/leaks":
		h.serveAction(w, r, h.c.captureLeaks)
	case "/api/full":
		h.serveAction(w, r, h.c.captureFull)
	default:
		http.NotFound(w, r)
	}
}

func (h *handler) serveAction(w http.ResponseWriter, r *http.Request, act func(context.Context) (Snapshot, error)) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)

		return
	}

	if err := h.cop.Check(r); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	s, err := act(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	writeJSON(w, s)
}

func (h *handler) serveEvents(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	rc.SetWriteDeadline(time.Time{})

	ch, cancel := h.c.subscribe()
	defer cancel()

	first, err := json.Marshal(h.c.snapshot(r.Context()))
	if err != nil || writeEvent(w, first) != nil || rc.Flush() != nil {
		return
	}

	tick := time.NewTicker(ping)
	defer tick.Stop()

	for {
		var err error

		select {
		case <-r.Context().Done():
			return
		case m := <-ch:
			err = writeEvent(w, m.data)
		case <-tick.C:
			_, err = io.WriteString(w, ": ping\n\n")
		}

		if err != nil || rc.Flush() != nil {
			return
		}
	}
}

func writeEvent(w io.Writer, data []byte) error {
	_, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)

	return err
}

func writeJSON(w http.ResponseWriter, s Snapshot) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}
