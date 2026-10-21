// Package flow is a ruvo view that draws goroutine groups as nodes and handoffs as lines.
package flow

import (
	"net/http"

	"github.com/TwelfthKyomu/ruvo"
)

type view struct{}

// View returns the flow view. It is a placeholder until v0.2 and serves 404.
func View() ruvo.View { return view{} }

func (view) Name() string { return "flow" }

func (view) Handler(api http.Handler) http.Handler { return http.NotFoundHandler() }
