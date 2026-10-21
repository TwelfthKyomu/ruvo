package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"strings"
	"testing"

	"github.com/TwelfthKyomu/ruvo"
)

func pprofServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

func TestParseTarget(t *testing.T) {
	for in, want := range map[string]string{
		"localhost:6060":                     "http://localhost:6060",
		"http://h:6060/":                     "http://h:6060",
		"https://h/debug/pprof/":             "https://h",
		"http://h:6060/internal/debug/pprof": "http://h:6060/internal",
		"http://u:secret@h:6060/":            "http://u:secret@h:6060",
	} {
		if got, err := parseTarget(in); err != nil || got != want {
			t.Errorf("parseTarget(%q) = %q, %v; want %q", in, got, err, want)
		}
	}

	for _, in := range []string{"", "http://"} {
		if _, err := parseTarget(in); err == nil {
			t.Errorf("parseTarget(%q): want error", in)
		}
	}
}

func TestParseArgsFlagsAfterTarget(t *testing.T) {
	target, listen, err := parseArgs([]string{"http://h:1", "-listen", "x:1"}, io.Discard)
	if err != nil || target != "http://h:1" || listen != "x:1" {
		t.Fatalf("target %q, listen %q, err %v", target, listen, err)
	}
}

func TestParseArgsUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"a", "b"}} {
		if _, _, err := parseArgs(args, io.Discard); err != errUsage {
			t.Errorf("parseArgs(%q): err %v, want errUsage", args, err)
		}
	}
}

func TestShownEmptyHost(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4zero, Port: 7070}

	if got := shown(":7070", addr); got != "localhost:7070" {
		t.Fatalf("shown = %q", got)
	}
}

func TestCheckHidesPassword(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	err := check(context.Background(), strings.Replace(srv.URL, "http://", "http://u:secret@", 1))
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err %v: the password must not be printed", err)
	}
}

func TestCheckOK(t *testing.T) {
	if err := check(context.Background(), pprofServer(t).URL); err != nil {
		t.Fatal(err)
	}
}

func TestCheckNoPprof(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	err := check(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "net/http/pprof") {
		t.Fatalf("err %v, want a hint about net/http/pprof", err)
	}
}

func TestRemoteHandlerServesSnapshot(t *testing.T) {
	target := pprofServer(t)

	srv := httptest.NewServer(ruvo.NewHandler(ruvo.Remote(target.URL)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)

	var s ruvo.Snapshot

	if err := json.Unmarshal(b, &s); err != nil || resp.StatusCode != http.StatusOK || s.Goroutines == 0 || s.Target != target.URL {
		t.Fatalf("status %d, err %v, snapshot %.200s", resp.StatusCode, err, b)
	}
}
