package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type mode string

const (
	modeNone        mode = "none"
	modeLeak        mode = "leak"
	modeSlowLoader  mode = "slow-loader"
	modeSlowWorkers mode = "slow-workers"
	modeDeadlock    mode = "deadlock"
	modeNetWait     mode = "net-wait"
)

var modes = []mode{modeNone, modeLeak, modeSlowLoader, modeSlowWorkers, modeDeadlock, modeNetWait}

func parseMode(s string) (mode, error) {
	for _, m := range modes {
		if string(m) == s {
			return m, nil
		}
	}

	return "", fmt.Errorf("unknown mode %q", s)
}

type job struct {
	n    int
	done chan int
}

type pipeline struct {
	mode     atomic.Pointer[mode]
	requests chan *job
	jobs     chan *job
	quit     chan struct{}
	stopOnce sync.Once
	upstream string
	client   *http.Client
	n        atomic.Int64
}

func newPipeline(workers int, upstream string) *pipeline {
	p := &pipeline{
		requests: make(chan *job),
		jobs:     make(chan *job),
		quit:     make(chan struct{}),
		upstream: upstream,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
	p.setMode(modeNone)

	go p.loader()

	for range workers {
		go p.worker()
	}

	return p
}

func (p *pipeline) setMode(m mode) {
	p.mode.Store(&m)
}

func (p *pipeline) currentMode() mode {
	return *p.mode.Load()
}

func (p *pipeline) stop() {
	p.stopOnce.Do(func() { close(p.quit) })
}

func (p *pipeline) loader() {
	defer close(p.jobs)

	for {
		select {
		case j := <-p.requests:
			p.load()
			p.jobs <- j
		case <-p.quit:
			return
		}
	}
}

func (p *pipeline) load() {
	switch p.currentMode() {
	case modeSlowLoader:
		time.Sleep(20 * time.Millisecond)
	case modeNetWait:
		resp, err := p.client.Get(p.upstream)
		if err != nil {
			return
		}

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func (p *pipeline) worker() {
	for j := range p.jobs {
		if p.currentMode() == modeSlowWorkers {
			time.Sleep(20 * time.Millisecond)
		} else {
			time.Sleep(time.Millisecond)
		}

		j.done <- j.n * 2
	}
}

func (p *pipeline) handleWork(w http.ResponseWriter, r *http.Request) {
	switch p.currentMode() {
	case modeLeak:
		handleLeak(w)
		return
	case modeDeadlock:
		handleDeadlock(w)
		return
	}

	j := &job{n: int(p.n.Add(1)), done: make(chan int, 1)}

	select {
	case p.requests <- j:
	case <-r.Context().Done():
		timeout(w)
		return
	case <-p.quit:
		timeout(w)
		return
	}

	select {
	case v := <-j.done:
		fmt.Fprintln(w, v)
	case <-r.Context().Done():
		timeout(w)
	case <-p.quit:
		timeout(w)
	}
}

func handleLeak(w http.ResponseWriter) {
	res := make(chan int)
	go notify(res)

	select {
	case v := <-res:
		fmt.Fprintln(w, v)
	case <-time.After(time.Millisecond):
		timeout(w)
	}
}

func handleDeadlock(w http.ResponseWriter) {
	var a, b sync.Mutex

	done := make(chan struct{}, 2)
	go lockPair(&a, &b, done)
	go lockPair(&b, &a, done)

	deadline := time.After(10 * time.Millisecond)

	for range 2 {
		select {
		case <-done:
		case <-deadline:
			timeout(w)
			return
		}
	}

	fmt.Fprintln(w, "ok")
}

func notify(res chan<- int) {
	time.Sleep(5 * time.Millisecond)
	res <- 42
}

func lockPair(first, second *sync.Mutex, done chan<- struct{}) {
	first.Lock()
	time.Sleep(time.Millisecond)
	second.Lock()
	first.Unlock()
	second.Unlock()
	done <- struct{}{}
}

func handleUpstream(w http.ResponseWriter, r *http.Request) {
	time.Sleep(20 * time.Millisecond)
	io.WriteString(w, "ok\n")
}

func timeout(w http.ResponseWriter) {
	http.Error(w, "timeout", http.StatusServiceUnavailable)
}
