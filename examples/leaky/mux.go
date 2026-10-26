package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	rpprof "runtime/pprof"
)

func newMux(p *pipeline) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/work", labeled(p, http.HandlerFunc(p.handleWork)))
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

func labeled(p *pipeline, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labels := rpprof.Labels("route", r.URL.Path, "mode", string(p.currentMode()))

		rpprof.Do(r.Context(), labels, func(ctx context.Context) {
			h.ServeHTTP(w, r.WithContext(ctx))
		})
	})
}
