package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
)

func newMux(p *pipeline) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/work", p.handleWork)
	mux.HandleFunc("/break", p.handleBreak)
	mux.HandleFunc("/upstream", handleUpstream)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

func (p *pipeline) handleBreak(w http.ResponseWriter, r *http.Request) {
	if s := r.URL.Query().Get("mode"); s != "" {
		m, err := parseMode(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		p.setMode(m)
	}

	fmt.Fprintln(w, p.currentMode())
}
