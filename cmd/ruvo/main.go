// Command ruvo shows where the goroutines of a running Go service are stuck.
// Point it at any service that exposes net/http/pprof:
//
//	ruvo http://localhost:6060
//
// It serves the goroutine list at / and the flow view, which draws who wakes
// whom, at /flow/.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TwelfthKyomu/ruvo"
	"github.com/TwelfthKyomu/ruvo/flow"
)

const usage = "usage: ruvo [-listen addr] http://host:port"

var errUsage = errors.New(usage)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	raw, listen, err := parseArgs(args, stderr)

	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintln(stderr, usage)
		return 2
	case err != nil:
		return 2
	}

	target, err := parseTarget(raw)
	if err != nil {
		fmt.Fprintln(stderr, "ruvo:", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := check(ctx, target); err != nil {
		fmt.Fprintln(stderr, "ruvo:", err)
		return 1
	}

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		fmt.Fprintln(stderr, "ruvo:", err)
		return 1
	}

	srv := &http.Server{Handler: newHandler(target), ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()

		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srv.Shutdown(shutdown)
	}()

	fmt.Fprintf(stdout, "ruvo: watching %s\nruvo: open http://%s/flow/\n", redact(target), shown(listen, ln.Addr()))

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(stderr, "ruvo:", err)
		return 1
	}

	return 0
}

func newHandler(target string) http.Handler {
	return ruvo.NewHandler(ruvo.Remote(target), flow.View())
}

func parseArgs(args []string, stderr io.Writer) (string, string, error) {
	flags := flag.NewFlagSet("ruvo", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "localhost:7070", "address to serve the ruvo page on")

	flags.Usage = func() {
		fmt.Fprintln(stderr, usage)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return "", "", err
	}

	rest := flags.Args()
	if len(rest) == 0 {
		return "", "", errUsage
	}

	if err := flags.Parse(rest[1:]); err != nil {
		return "", "", err
	}

	if flags.NArg() != 0 {
		return "", "", errUsage
	}

	return rest[0], *listen, nil
}

func parseTarget(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("no target: want http://host:port")
	}

	if !strings.Contains(s, "://") {
		s = "http://" + s
	}

	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("bad target %q: want http://host:port", redact(s))
	}

	u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/debug/pprof")
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""

	return strings.TrimRight(u.String(), "/"), nil
}

func redact(target string) string {
	if u, err := url.Parse(target); err == nil {
		return u.Redacted()
	}

	return hidePassword(target)
}

func hidePassword(s string) string {
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok {
		return s
	}

	authority := rest
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority = rest[:i]
	}

	at := strings.LastIndexByte(authority, '@')
	if at < 0 {
		return s
	}

	colon := strings.IndexByte(authority[:at], ':')
	if colon < 0 {
		return s
	}

	return scheme + "://" + rest[:colon+1] + "xxxxx" + rest[at:]
}

func check(ctx context.Context, target string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	u := target + "/debug/pprof/goroutine?debug=1"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err = errors.New(resp.Status)
		}
	}

	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}

		return fmt.Errorf("cannot read %s/debug/pprof/goroutine: %v — is net/http/pprof enabled?", redact(target), err)
	}

	return nil
}

func shown(listen string, addr net.Addr) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || port == "0" {
		return addr.String()
	}

	if host == "" {
		host = "localhost"
	}

	return net.JoinHostPort(host, port)
}
