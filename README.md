# ruvo

**ruvo — see where your goroutines are stuck.**

*ruvo = routine + view*

When a Go service starts eating memory or slowing down because of goroutines,
`pprof` gives you a wall of text. ruvo turns it into a live table: where
goroutines wait, how many, for how long, which groups keep growing, and which
ones have leaked for good — on a running service, without changing its code.

> v0.1 shows the list view. The flow view that draws who waits for whom is
> coming in v0.2.

## Install

```bash
go install github.com/TwelfthKyomu/ruvo/cmd/ruvo@latest
```

## Watch any service — no code changes

Point ruvo at a service that already exposes `net/http/pprof`:

```bash
ruvo http://localhost:6060
```

and open <http://localhost:7070/>. `-listen` serves the page on another
address. The target may carry a path in front of `/debug/pprof` and a login:
`ruvo https://user:password@host/internal` — the password is never printed.

## Or build it into your service

```go
import "github.com/TwelfthKyomu/ruvo"

mux.Handle("/debug/ruvo/", ruvo.Handler())
```

The page is at `/debug/ruvo/`, the JSON at `/debug/ruvo/api/snapshot`, and a
live stream of snapshots (server-sent events) at `/debug/ruvo/api/events`. In
the JSON, durations are integer nanoseconds. The core module uses only the
standard library.

Mount it only on a listener you control, like `net/http/pprof`: the page shows
every goroutine stack and can trigger full dumps.

## What you see

| Column | Meaning |
|---|---|
| Where they wait | the first frame of your code where a group of goroutines is blocked, and the function they were started with |
| Goroutines | how many are there now |
| Waiting | the longest any of them has been waiting (from the last full dump; `—` if the group was not in it) |
| Growth/min | how fast the group grows over the last two minutes — a steady `+N` is a leak in progress |
| Leaked | goroutines that can never wake up, found by the `goroutineleak` profile (press **Find leaks**) |
| State | `chan receive`, `select`, `IO wait`, `sync.Mutex.Lock`, … |

Click a row to see its stack and where the goroutines were created.

## How it stays cheap

ruvo must not slow down what it watches:

- It reads goroutines only while someone has the page open, and stops ten
  minutes after the last viewer leaves.
- The frequent read is the `debug=1` profile, which the runtime collects
  without stopping the world for long. ruvo waits at least 200× the time the
  last read took (2 s to 1 min between reads).
- The full `debug=2` dump stops the world while it walks every stack, so ruvo
  takes it rarely: at least 1000× its cost apart (1 to 30 min), which keeps the
  stopped time under 0.1%. **Full dump** takes one now. On first open the page
  takes one only if the target has up to 10,000 goroutines.
- `goroutineleak` runs an extra GC cycle, so ruvo never runs it on its own —
  only when you press **Find leaks**.
- A full dump or a leak search runs at most once per 10 s, and everything that
  asks for one at the same time shares that one run. POSTs from other sites are
  refused.

## Overhead

Reading the current process, Apple M1 Pro, Go 1.27.3
(`go test -bench BenchmarkSample`):

| Goroutines | `debug=1` read | full dump: world stopped | full dump: total |
|---|---|---|---|
| 1,000 | 1.1 ms | 2.6 ms | 3.6 ms |
| 10,000 | 9.9 ms | 11 ms | 25 ms |
| 100,000 | 83 ms | 103 ms | 241 ms |

The `debug=1` read keeps the world stopped only briefly, whatever the number of
goroutines. A full dump of your own process stops the world once; parsing runs
after the pause. A remote target serves the dump with its own
`net/http/pprof`, which may walk the stacks a few times while it grows its
buffer.

## Try it

[`examples/leaky`](examples/leaky) is a small service you can break on purpose:

```bash
go run ./examples/leaky &
ruvo http://localhost:6061
curl 'localhost:6061/break?mode=leak'
```

## Modules

| Module | What it is | Dependencies |
|---|---|---|
| `github.com/TwelfthKyomu/ruvo` | collector, JSON/SSE API, list view | standard library only |
| `github.com/TwelfthKyomu/ruvo/cmd/ruvo` | the `ruvo` command | `ruvo` |
| `github.com/TwelfthKyomu/ruvo/examples/leaky` | a service with switchable breakages | standard library only |
| `github.com/TwelfthKyomu/ruvo/flow` | the flow view — coming in v0.2 | `ruvo` |

Building ruvo needs Go 1.27+. The target can be older; **Find leaks** needs a
target built with Go 1.27+ (or Go 1.26 with `GOEXPERIMENT=goroutineleakprofile`).

## License

MIT
