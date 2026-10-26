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

## Keep the numbers in Prometheus

`GET api/metrics` serves the snapshot in the Prometheus text format: whether
the target answers, how many goroutines and groups it has, and for the 200
largest groups their size, growth per minute and, after **Find leaks**, the
leaked count, labelled by `id`, `site` and `entry`; plus when the last full
dump, leak search and trace ran.

```yaml
scrape_configs:
  - job_name: ruvo
    metrics_path: /api/metrics          # /debug/ruvo/api/metrics when embedded
    static_configs:
      - targets: ["localhost:7070"]
```

Each scrape counts as someone watching: ruvo keeps reading the target, on the
same budget as with the page open.

## Save it and show a colleague

**Export** on either page saves a zip file: what the page shows, plus the raw
data behind it, so the diagnosis can be checked without the service.

| File | What it is |
|---|---|
| `snapshot.json` | the snapshot the page showed |
| `goroutines.txt` | the last goroutine profile, `debug=1` |
| `goroutines-full.txt` | the last full dump, `debug=2` |
| `goroutineleak.txt` | the last `goroutineleak` profile |
| `trace.out` | the last execution trace — `go tool trace trace.out` opens it |
| `trace-goroutines-full.txt` | the full dump taken while that trace ran, if it is not the last full dump |
| `session.json`, `README.txt` | when each file was read, the target, the versions of ruvo and Go |

Only files ruvo has are included. Open the session again, without the
service:

```bash
ruvo -open ruvo-localhost-6060-20261026-150405.zip
```

The pages show the saved snapshot as it was, with a note that it comes from a
file; Full dump, Find leaks and Record trace are off. In your own code,
`ruvo.OpenSession` serves a session file the same way.

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
- **`not woken N`** — N goroutines of the group waited through the whole
  recording: nothing woke them, so no arrow leads to them. A deadlock or a leak
  looks like this, and so does an idle pool.
- **Dotted arrow** — the group an end of the arrow was recorded for is gone,
  and ruvo matched it to a current group by its wait site (only when the two
  stacks share the start function) or by its start function alone. A match that
  fits more than one group is dropped rather than guessed.
- **spawn** — a checkbox. When on, dashed arrows show which group started
  which, from `created by` in the last full dump. Off by default.

The graph shows up to 40 groups: those at the ends of arrows first, then red
ones, then the biggest; the rest are in the list.

Above the graph, a line says it in words: which group leaks or grows, which
group is the jam and what it waits for, which of your groups slept through the
whole trace. The list page has the same line without the arrows.

**Record trace** records 5 seconds by default, at most once per 10 seconds;
presses at the same time share one recording. It is the only thing that
traces the target — ruvo never records on its own. While it records, a
CPU-bound target loses up to about 4% of its throughput (measured on a service
doing ~40,000 requests per second on one core; a service that mostly waits
loses nothing measurable). During the recording ruvo takes one full dump to
learn where goroutines that fell asleep before it were waiting; it skips that
dump if a full dump was attempted in the previous 10 s. The trace itself shows
when the dump stopped the world, and its stacks are used only for wakeups after
that moment: a goroutine woken earlier may have waited somewhere else by the
time of the dump. Wakeups whose waiting site stays unknown get no arrow; the
page shows how many (`trace: … · N unknown`). A 5 s trace is about
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

Click a row to see its stack, where the goroutines were created and, if your
code sets [pprof labels](https://pkg.go.dev/runtime/pprof#Do), how the group
splits by them: `route=/work tenant=acme ×31` says which requests left those
goroutines there. ruvo reads the labels from the same `debug=1` profile, at no
extra cost to the target; the flow page shows them in the node panel.

## API

Under the mount path (`/debug/ruvo/`; the flow page serves the same API under
`/debug/ruvo/flow/`). In the JSON, durations are integer nanoseconds.

| Request | What it does |
|---|---|
| `GET api/snapshot` | the current snapshot |
| `GET api/events` | a stream of snapshots (server-sent events, event `snapshot`) |
| `GET api/events?lean=1` | the same stream, lighter: after a `snapshot`, events `update` carry snapshots whose groups have `stack: null` as long as every group was in the last `snapshot`; take the stacks from it |
| `POST api/full` | take a full dump now |
| `POST api/leaks` | run the `goroutineleak` profile |
| `POST api/trace?seconds=N` | record a trace for N seconds (1 to 30, default 5); answers 404 unless a view that reads traces, like `flow.View()`, is mounted |
| `GET api/export` | the session as a zip file (see [Save it](#save-it-and-show-a-colleague)) |
| `GET api/metrics` | the snapshot as Prometheus metrics (see [Prometheus](#keep-the-numbers-in-prometheus)) |

The POSTs return the new snapshot and refuse requests from other sites. A
snapshot has:

| Field | Meaning |
|---|---|
| `seq` | orders snapshots: a later one has a larger `seq` (it follows `at` in microseconds and never goes back) |
| `at`, `target`, `goroutines`, `interval` | when it was taken, what it watches, how many goroutines there are, how long until the next one |
| `fullAt`, `fullCount`, `fullCut` | when the last full dump was taken, how many goroutines it holds, and whether it was cut at 64 MB (then `fullCount` is less than `goroutines`) |
| `leaksAt` | when the `goroutineleak` profile last ran |
| `traceAt`, `traceFor` | when the last trace ended and how long it ran; `edges` of kind `unblock` come from it |
| `traceUnknown` | how many wakeups of that trace have no edge because it is not known where the woken goroutines waited |
| `error` | set when the last read of the target failed; the data is the last good one |
| `offline` | the snapshot comes from a session file: nothing reads the target, and the POSTs answer 409 |
| `groups` | `id`, `waitSite`, `entry`, `createdBy`, `state`, `count`, `maxWait`, `inFullDump`, `growthPerMin`, `leaked`, `unwoken` (waited through the whole last trace), `labels` (up to 10 pprof label sets with their counts), `stack`, `std` (no code of yours on the stack) |
| `edges` | `from`, `to`, `kind` (`unblock` or `spawn`), `count`, `approx` (an end was matched to a current group by wait site or start function), and for `unblock` also `wait` (total time the woken goroutines waited) and `waiters`; `from` is a group id, or `network` or `timer` |

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
- For **Export**, ruvo keeps the text of what it last read — the `debug=1`
  profile, the full dump (about 250–350 bytes per goroutine), the leak profile —
  and the last trace, only while it is running; it drops them when it stops.
  Writing the zip takes about 15 ms for a dump of 100,000 goroutines.
- A full dump, a leak search or a trace runs at most once per 10 s from the
  last attempt, and everything that asks for one at the same time shares that
  one run. POSTs from other sites are refused.

## Overhead

Reading the current process, Apple M1 Pro, Go 1.27.3
(`go test -bench BenchmarkSample`):

| Goroutines | `debug=1` read | full dump: world stopped | full dump: total | full dump: allocated |
|---|---|---|---|---|
| 1,000 | 0.8 ms | 2.7 ms | 3.1 ms | 1.3 MB |
| 10,000 | 7.7 ms | 12 ms | 16 ms | 6.2 MB |
| 100,000 | 83 ms | 104 ms | 151 ms | 59 MB |

The `debug=1` read keeps the world stopped only briefly, whatever the number of
goroutines; almost all of its cost is `runtime/pprof` formatting the profile.
A full dump of your own process stops the world once; parsing runs after the
pause, at about 750 MB/s, and keeps one copy of each distinct stack. Most of
what a full dump allocates is the dump text itself. A remote target serves the dump with its own
`net/http/pprof`, which may walk the stacks a few times while it grows its
buffer.

Publishing a snapshot of 1,000 groups with 20-frame stacks — parsing, groups,
growth and arrows — takes about 5 ms (`go test -bench BenchmarkPublish`); a
`debug=1` profile that has not changed since the last read is not parsed
again. The pages get stacks only when a group first appears: their usual event
is 258 KB instead of 1.8 MB and takes 0.5 ms to encode instead of 2 ms
(`BenchmarkEncode`), once per snapshot however many pages are open. Recording a trace has its own cost, described under
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
