# leaky

A small service you can break on purpose to see what ruvo shows.

Requests go through a typical worker pool:

```
handler ──► requests ──► loader ×1 ──► jobs ──► worker ×8 ──► reply
```

## Run

From the repository root:

```bash
go run ./examples/leaky
```

Flags: `-addr` (default `localhost:6061`), `-workers` (default `8`),
`-break` (initial mode, default `none`).

## Load

The flow page draws arrows only for goroutines that were woken while **Record
trace** was running, so an idle leaky shows none. Send it load from a second
terminal:

```bash
go run ./examples/leaky -load localhost:6061 -clients 32
```

With `-load <addr>`, leaky does not serve: `-clients` (default `32`) clients
call `/work` at that address in a loop until Ctrl+C, each waiting up to
2 seconds for an answer and pausing 100 ms after an error or a non-200 answer.
It runs as a separate process so its own goroutines stay out of what ruvo
shows.

## Endpoints

| Path | What it does |
|---|---|
| `/work` | one request through the pool |
| `/break` | prints the current mode; `/break?mode=<mode>` switches it |
| `/upstream` | a slow dependency (answers after 20 ms), used by `net-wait` |
| `/debug/pprof/` | `net/http/pprof`, including `goroutineleak` and `trace` |

## Modes

| Mode | What breaks | Who waits for whom |
|---|---|---|
| `none` | nothing | — |
| `slow-loader` | the loader takes 20 ms per job | workers wait for the loader, handlers queue up behind it |
| `slow-workers` | each job takes 20 ms | the loader waits for workers to take the next job |
| `net-wait` | the loader calls `/upstream` for every job | the loader waits on the network |
| `leak` | the handler gives up after 1 ms and leaves `notify` behind | `notify` waits forever on a channel nobody reads |
| `deadlock` | two goroutines lock two mutexes in opposite order | each waits for the other, forever |

```bash
curl 'localhost:6061/break?mode=slow-loader'
```
