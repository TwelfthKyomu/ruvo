// Package flow is a ruvo view that draws goroutine groups as nodes and handoffs as lines.
package flow

import (
	"embed"
	"io"
	"io/fs"
	"net/http"
	"strings"

	"github.com/TwelfthKyomu/ruvo"
	"github.com/TwelfthKyomu/ruvo/flow/internal/wake"
)

//go:embed ui
var uiFS embed.FS

type view struct{}

// View returns the flow view: goroutine groups as nodes and who wakes whom as
// arrows. It also reads execution traces, which turns on Record trace.
func View() ruvo.View { return view{} }

func (view) Name() string { return "flow" }

func (view) Handler(api http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/"):
			api.ServeHTTP(w, r)
		case r.URL.Path == "/":
			servePage(w)
		default:
			http.NotFound(w, r)
		}
	})
}

func (view) ReadTrace(trace io.Reader, stacks map[int64][]ruvo.Frame) ([]ruvo.Wakeup, error) {
	return wake.Read(trace, stacks)
}

func servePage(w http.ResponseWriter) {
	page, err := fs.ReadFile(uiFS, "ui/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(page)
}
