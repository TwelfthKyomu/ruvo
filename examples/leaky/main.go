package main

import (
	"flag"
	"log"
	"net"
	"net/http"
)

func main() {
	addr := flag.String("addr", "localhost:6061", "listen address")
	workers := flag.Int("workers", 8, "number of workers")
	initial := flag.String("break", string(modeNone), "initial mode")

	flag.Parse()

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
