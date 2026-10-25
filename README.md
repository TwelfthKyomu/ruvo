# ruvo

**ruvo — see where your goroutines are stuck.**

*ruvo = routine + view*

When a Go service starts eating memory or slowing down because of goroutines,
`pprof` gives you a wall of text. ruvo turns it into a live table and a graph:
where goroutines wait, how many, for how long, who wakes them, which groups
keep growing, and which ones have leaked for good — on a running service,
without changing its code.

## Install

```bash
go install github.com/TwelfthKyomu/ruvo/cmd/ruvo@latest
```

## Watch any service — no code changes

Point ruvo at a service that already exposes `net/http/pprof`:

```bash
ruvo http://localhost:6060
```

ruvo prints `ruvo: open http://localhost:7070/flow/`. That is the flow page,
which draws who wakes whom; the list of groups is at <http://localhost:7070/>.
`-listen` serves the pages on another address. The target may carry a path in
front of `/debug/pprof` and a login:
`ruvo https://user:password@host/internal` — the password is never printed.

## Or build it into your service

List only, standard library only:

```go
import "github.com/TwelfthKyomu/ruvo"

mux.Handle("/debug/ruvo/", ruvo.Handler())
```

With the flow page, from the `flow` module:

```go
import (
	"github.com/TwelfthKyomu/ruvo"
	"github.com/TwelfthKyomu/ruvo/flow"
)

mux.Handle("/debug/ruvo/", ruvo.Handler(flow.View()))
```

The list is at `/debug/ruvo/` and the flow page at `/debug/ruvo/flow/`
(`/debug/ruvo/flow` redirects there). The `flow` module needs
`golang.org/x/exp` to read execution traces; the core module does not.

Mount it only on a listener you control, like `net/http/pprof`: the page shows
every goroutine stack and can trigger full dumps and traces.

## Who wakes whom

The flow page draws the same groups as the list, plus the handoffs between
them. Press **Record trace**: ruvo records an execution trace of the target for
5 seconds and turns the wakeups in it into arrows.

- **Node** — a group of goroutines that wait at the same place and were started
  by the same function, with how many there are, their state and how long they
  have waited. A red node is growing or has leaked goroutines. Click a node for
  its stack, where its goroutines were created, who wakes them and whom they
  wake.
- **Arrow** A → B — goroutines of A woke goroutines of B during the recording,
  so B was waiting for A. The label reads `N/s · ≈W waiting`: A woke B about N
  times per second, and on average about W goroutines of B were waiting for it.
- **Thickness** — the share of the recording that B's goroutines spent waiting
  for A, not how often A woke them. A frequent but instant handoff stays thin;
  a rare handoff that keeps many goroutines waiting is thick.
- **`jam`** — the group where goroutines spend the most time waiting to be
  woken (at least one goroutine's worth over the recording). Groups with your
  code on the stack come first; a group that waits only inside the standard
  library is marked only when no other group qualifies. Its heaviest incoming
  arrow is red: that is the group it waits for.
- **`network`, `timer`** — a badge on a node shows what share of its waiting
  ended with a wakeup from the network or a timer instead of a goroutine
  (shown from 20%). They are not nodes: no goroutine is to blame.
- **spawn** — a checkbox. When on, dashed arrows show which group started
  which, from `created by` in the last full dump. Off by default.

The graph shows up to 40 groups: those at the ends of arrows first, then red
ones, then the biggest; the rest are in the list.

**Record trace** records 5 seconds by default, at most once per 10 seconds;
presses at the same time share one recording. It is the only thing that
traces the target — ruvo never records on its own. While it records, a
CPU-bound target loses up to about 4% of its throughput (measured on a service
doing ~40,000 requests per second on one core; a service that mostly waits
loses nothing measurable). During the recording ruvo takes one full dump to
learn where goroutines that fell asleep before it were waiting; it skips that
dump if a full dump was attempted in the previous 10 s. A 5 s trace is about
6–7 MB at 7,000 requests per second and about 32 MB at 40,000; ruvo reads about
40 MB/s of it on an Apple M1 Pro and refuses a trace larger than 80 MB. With the
CLI the trace is parsed in the `ruvo` process. Embedded, everything happens in
yours: the trace is buffered in memory, up to 80 MB (briefly 2–3 times that
while the buffer grows), plus a copy of the stacks from the full dump and the
parsed trace — only while a recording runs.

Arrows come from the last recording and stay until the next one or until the
target restarts. The target must serve `/debug/pprof/trace` (`net/http/pprof`
does), and its server's write timeout must be longer than the recording.

## What you see in the list

| Column | Meaning |
|---|---|
| Where they wait | the first frame of your code where a group of goroutines is blocked, and the function they were started with |
| Goroutines | how many are there now |
| Waiting | the longest any of them has been waiting (from the last full dump; `—` if the group was not in it) |
| Growth/min | how fast the group grows over the last two minutes — a steady `+N` is a leak in progress |
| Leaked | goroutines that can never wake up, found by the `goroutineleak` profile (press **Find leaks**) |
| State | `chan receive`, `select`, `IO wait`, `sync.Mutex.Lock`, … |

Click a row to see its stack and where the goroutines were created.

## API

Under the mount path (`/debug/ruvo/`; the flow page serves the same API under
`/debug/ruvo/flow/`). In the JSON, durations are integer nanoseconds.

| Request | What it does |
|---|---|
| `GET api/snapshot` | the current snapshot |
| `GET api/events` | a stream of snapshots (server-sent events, event `snapshot`) |
| `POST api/full` | take a full dump now |
| `POST api/leaks` | run the `goroutineleak` profile |
| `POST api/trace?seconds=N` | record a trace for N seconds (1 to 30, default 5); answers 404 unless a view that reads traces, like `flow.View()`, is mounted |

The POSTs return the new snapshot and refuse requests from other sites. A
snapshot has:

| Field | Meaning |
|---|---|
| `at`, `target`, `goroutines`, `interval` | when it was taken, what it watches, how many goroutines there are, how long until the next one |
| `fullAt`, `fullCount`, `fullCut` | when the last full dump was taken, how many goroutines it holds, and whether it was cut at 64 MB (then `fullCount` is less than `goroutines`) |
| `leaksAt` | when the `goroutineleak` profile last ran |
| `traceAt`, `traceFor` | when the last trace ended and how long it ran; `edges` of kind `unblock` come from it |
| `error` | set when the last read of the target failed; the data is the last good one |
| `groups` | `id`, `waitSite`, `entry`, `createdBy`, `state`, `count`, `maxWait`, `inFullDump`, `growthPerMin`, `leaked`, `stack`, `std` (no code of yours on the stack) |
| `edges` | `from`, `to`, `kind` (`unblock` or `spawn`), `count`, and for `unblock` also `wait` (total time the woken goroutines waited) and `waiters`; `from` is a group id, or `network` or `timer` |

## How it stays cheap

ruvo must not slow down what it watches:

- It reads goroutines only while someone has the page open, and stops ten
  minutes after the last viewer leaves.
- The frequent read is the `debug=1` profile, which the runtime collects
  without stopping the world for long. After a read that took `cost`, ruvo
  waits `clamp(200 × cost, 2 s, 1 min)`.
- The full `debug=2` dump stops the world while it walks every stack, so ruvo
  takes it rarely: after a dump that took `cost`, it waits
  `clamp(1000 × cost, 1 min, 30 min)`. The time the world is stopped is at most
  `cost`, so its share stays at or under 0.1% only while a full dump costs
  1.8 s or less. A slower dump hits the 30-minute cap, and the share is then
  `cost / 30 min`. **Full dump** takes one now. On first open the page takes
  one at once, unless the target has more than 10,000 goroutines: then the
  first one comes on schedule, a minute later.
- A full dump of the current process is read into at most 64 MB. On a target
  with huge stacks it is cut: the snapshot has `fullCut` set and `fullCount`
  goroutines, and the pages show "N of M". State, waiting time and wake sites
  then cover only those goroutines.
- `goroutineleak` runs an extra GC cycle, so ruvo never runs it on its own —
  only when you press **Find leaks**.
- A full dump, a leak search or a trace runs at most once per 10 s from the
  last attempt, and everything that asks for one at the same time shares that
  one run. POSTs from other sites are refused.

## Overhead

Reading the current process, Apple M1 Pro, Go 1.27.3
(`go test -bench BenchmarkSample`):

| Goroutines | `debug=1` read | full dump: world stopped | full dump: total |
|---|---|---|---|
| 1,000 | 1.2 ms | 2.7 ms | 3 ms |
| 10,000 | 7.4 ms | 11 ms | 21 ms |
| 100,000 | 77 ms | 95 ms | 215 ms |

The `debug=1` read keeps the world stopped only briefly, whatever the number of
goroutines. A full dump of your own process stops the world once; parsing runs
after the pause. A remote target serves the dump with its own
`net/http/pprof`, which may walk the stacks a few times while it grows its
buffer.

Resolving the arrows costs about 0.2 ms per snapshot at 1,000 groups (0.4 ms
with `spawn` arrows). Recording a trace has its own cost, described under
[Who wakes whom](#who-wakes-whom).

## Try it

[`examples/leaky`](examples/leaky) is a small service you can break on purpose.
Start it in one terminal and send it load from another — a separate process, so
the load's own goroutines do not show up in the graph:

```bash
go run ./examples/leaky -break slow-loader
```

```bash
go run ./examples/leaky -load localhost:6061 -clients 32
```

Then watch it:

```bash
ruvo http://localhost:6061
```

Open <http://localhost:7070/flow/> and press **Record trace**: the group the
others wait for stands at the start of the heaviest arrows, and `jam` marks
where the waiting piles up. Switch the breakage while it runs and record again
to watch the arrows change:

```bash
curl 'localhost:6061/break?mode=slow-workers'
```

`mode=leak` fills the list with a growing group; press **Find leaks** to see
it marked as leaked.

## Modules

| Module | What it is | Dependencies |
|---|---|---|
| `github.com/TwelfthKyomu/ruvo` | collector, JSON/SSE API, list view | standard library only |
| `github.com/TwelfthKyomu/ruvo/flow` | the flow view and the trace reader | `ruvo`, `golang.org/x/exp` |
| `github.com/TwelfthKyomu/ruvo/cmd/ruvo` | the `ruvo` command | `ruvo`, `flow` |
| `github.com/TwelfthKyomu/ruvo/examples/leaky` | a service with switchable breakages | standard library only |

Building ruvo needs Go 1.27+. The target can be older; **Find leaks** needs a
target built with Go 1.27+ (or Go 1.26 with `GOEXPERIMENT=goroutineleakprofile`).

## License

MIT
