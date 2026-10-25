// Package ruvo shows where goroutines are stuck and what they are waiting for.
//
// Mount the handler in a service to watch that service:
//
//	mux.Handle("/debug/ruvo/", ruvo.Handler())
//
// or watch any service that exposes net/http/pprof with the ruvo command.
package ruvo

import (
	"net/http"

	"github.com/TwelfthKyomu/ruvo/internal/collect"
)

// View is an extra page that draws snapshots its own way. Its handler is
// mounted at /<Name>/ next to the built-in list; api serves the JSON API
// (api/snapshot, api/events, api/leaks, api/full, api/trace) relative to the
// view. A view that also implements TraceReader turns on api/trace, which
// records an execution trace of the target and adds unblock edges to the
// snapshot; if several views do, the first one reads the traces.
type View interface {
	Name() string
	Handler(api http.Handler) http.Handler
}

// Source is where ruvo reads goroutine dumps from. Create one with Local or
// Remote; the zero Source reads the current process.
type Source struct {
	src collect.Source
}

// Local reads the goroutines of the current process.
func Local() Source {
	return Source{src: collect.Local()}
}

// Remote reads goroutine dumps over HTTP from a service that serves
// net/http/pprof. baseURL is the address in front of /debug/pprof, such as
// "http://localhost:6060" or "https://user:password@host/internal"; the
// password is hidden wherever ruvo shows the address.
func Remote(baseURL string) Source {
	return Source{src: collect.Remote(baseURL)}
}

// Handler serves the ruvo page and API for the current process. Mount it
// under a path that ends in a slash, on a listener that only you can reach:
// the page shows every stack and can trigger full dumps and execution
// traces. It starts reading goroutines on the first request and stops ten
// minutes after the last viewer leaves.
func Handler(views ...View) http.Handler {
	return NewHandler(Local(), views...)
}

// NewHandler is like Handler but reads goroutines from src.
func NewHandler(src Source, views ...View) http.Handler {
	if src.src == nil {
		src = Local()
	}

	h := &handler{c: newCollector(src.src), views: map[string]http.Handler{}, cop: http.NewCrossOriginProtection()}
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.serveAPI(w, r, r.URL.Path) })

	for _, v := range views {
		h.views[v.Name()] = v.Handler(api)

		if r, ok := v.(TraceReader); ok && h.c.reader == nil {
			h.c.reader = r
		}
	}

	return h
}
