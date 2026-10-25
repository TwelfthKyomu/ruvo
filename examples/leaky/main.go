package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
)

func main() {
	addr := flag.String("addr", "localhost:6061", "listen address")
	workers := flag.Int("workers", 8, "number of workers")
	initial := flag.String("break", string(modeNone), "initial mode")
	load := flag.String("load", "", "do not serve: send requests to /work of the leaky at this address until Ctrl+C")
	clients := flag.Int("clients", 32, "number of clients with -load")

	flag.Parse()

	if *load != "" {
		runLoad(*load, *clients)
		return
	}

	m, err := parseMode(*initial)
	if err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}

	p := newPipeline(*workers, "http://"+ln.Addr().String()+"/upstream")
	p.setMode(m)

	log.Printf("leaky on http://%s, mode %s, modes %v", ln.Addr(), m, modes)

	log.Fatal(http.Serve(ln, newMux(p)))
}

func runLoad(addr string, clients int) {
	if clients < 1 {
		log.Fatal("-clients must be at least 1")
	}

	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("sending requests to %s/work from %d clients, Ctrl+C to stop", addr, clients)

	drive(ctx, strings.TrimSuffix(addr, "/"), clients)
}
