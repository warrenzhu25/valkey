# 01 — Server Lifecycle & the Event Loop

Everything in Valkey hangs off one event loop. Understand this note and the rest of
the codebase has a place to attach to.

## Startup

`main()` is at `src/server.c:7511`. It is declared `__attribute__((weak))` (`server.c:7510`),
which lets test harnesses link their own `main` over it — that's how the unit tests in
`src/unit/` run engine internals without booting a whole server.

The sequence that matters:

1. `initServerConfig()` (`src/server.c:2308`) — populate the global `server` struct with
   defaults, before any config file is read.
2. Config file / command-line parsing.
3. `initServer()` (`src/server.c:2924`) — create the event loop, listening sockets,
   databases, and register the cron timer and accept handlers.
4. Load data from disk (AOF or RDB).
5. `aeMain()` (`src/ae.c:540`) — enter the loop and never return.

## The loop

`aeMain` (`src/ae.c:540`) is literally a `while (!eventLoop->stop)` around `aeProcessEvents`
(`src/ae.c:411`). One iteration:

```
beforeSleep()        <- server.c:1854   flush output, do deferred work
   |
poll/epoll/kqueue    <- blocks until an fd is ready or a timer expires
   |
afterSleep()         <- server.c:2056
   |
fire file events     <- readable/writable handlers (accept, read query, write reply)
fire time events     <- serverCron
```

`ae.c` is a thin portability shim; the real polling lives in `ae_epoll.c` (Linux),
`ae_kqueue.c` (macOS/BSD), `ae_evport.c`, `ae_select.c`. Which one compiles in is
decided at the bottom of `ae.c` by `#ifdef`. You will rarely need to read them.

Handlers are registered with `aeCreateFileEvent` (`src/ae.c:185`). `beforeSleep` is
installed via `aeSetBeforeSleepProc` (`src/ae.c:551`), called from `initServer`.

One subtlety in `aeProcessEvents`: the loop can be told **not to block** on the next
poll via `aeSetDontWait` (called at the end of `beforeSleep`, `server.c:2040`). This
matters when a connection type buffers data above the socket layer — TLS is the classic
case: bytes can be sitting in OpenSSL's buffer with the underlying fd showing *not*
readable, so if the loop blocked in `epoll_wait` it would deadlock. `connTypeHasPendingData`
(`server.c:1894`) detects that and forces a zero timeout.

## `beforeSleep` — the most important function you've never heard of

`src/server.c:1854`. It runs **every single loop iteration, right before blocking**.
This is where Valkey does all the work it deferred while executing commands, because
doing it inline would have been wrong or slow.

**The ordering is not arbitrary — read the comments.** Almost every step in `beforeSleep`
has a `must be done before X` comment justifying its position, and those constraints *are*
the design. The load-bearing ones, in execution order:

| Step | Line | Ordering constraint (from the code comments) |
|------|------|----------------------------------------------|
| `trySendPollJobToIOThreads` | 1858 | offload the poll itself to an I/O thread when there's pending I/O |
| `processIOThreadsResponses` | 1886 | collect finished reads "ASAP after event loop" |
| `connTypeProcessPendingData` (TLS) | 1890 | "must be done before flushAppendOnlyFile" |
| `clusterBeforeSleep` | 1900 | may flip cluster ok↔fail; must run before serving unblocked clients |
| `blockedBeforeSleep` | 1905 | before AOF flush, since unblocked clients may write, relevant to `appendfsync=always` |
| fast expire cycle | 1916 | see below — **primary only** |
| `sendGetackToReplicas` (for `WAIT`) | 1933 | after unblocking, before sleeping |
| `flushAppendOnlyFile(0)` | 1962 | "before handleClientsWithPendingWrites, in case of appendfsync=always" |
| `handleClientsWithPendingWrites` | 1982 | the main path replies actually reach sockets (note 03) |
| `freeClientsInAsyncFreeQueue` | 1999 | deferred client teardown (note 03) |
| `evictClients` | 2006 | disconnect clients over the output-buffer limit |

Two details worth internalizing:

- **The fast expire cycle only runs on a primary:**
  `if (server.active_expire_enabled && !server.import_mode && iAmPrimary())`
  (`server.c:1915`). This is the code-level enforcement of note 05's rule that *replicas
  never expire keys on their own* — they wait for the primary's `DEL`.

- **The re-entrancy subset.** When a long-running command (a slow Lua script, a module
  call) pumps the loop via `processEventsWhileBlocked`, `beforeSleep` takes an early
  branch (`ProcessingEventsWhileBlocked`, `server.c:1868`) that runs only a *vital
  subset*: collect I/O responses, flush AOF, flush client writes, free async clients.
  Everything else (expiry, cluster, eviction) is skipped so the nested pump stays cheap.
  If you ever wonder why some `beforeSleep` work seems to *not* happen during a busy
  script, this is why.

**The module GIL boundary.** The last thing `beforeSleep` does is `moduleReleaseGIL()`
(`server.c:2047`), and there's a shouting comment forbidding anything below it. While the
main thread sleeps in `epoll_wait`, module background threads are allowed to touch the
dataset; `afterSleep` (`server.c:2056`) re-acquires the GIL the instant the loop wakes.
So the module concurrency window is *exactly* the poll.

When you're wondering *"where does this actually get sent?"* — the answer is very
often `beforeSleep`. Commands typically append to a buffer; `beforeSleep` drains it.

### It also measures itself

`beforeSleep` brackets its phases with `getMonotonicUs()` and feeds `durationAddSample`
for `EL_DURATION_TYPE_AOF`, `_EL` (whole event loop), and `_CRON` (`server.c:1966`,
2015, 2027). These are the buckets `INFO` exposes and the exact hook the
[main-thread CPU-distribution proposal](proposal-mainthread-cpu-distribution.md) builds on
to answer "is this server execution-bound or I/O-bound?" without a profiler.

## `serverCron` — the background heartbeat

`src/server.c:1537`, registered as a time event, runs at `hz` frequency (default 10/sec,
configurable; it also scales with client count). It is the janitor.

**How it does different work at different rates:** the `run_with_period(ms) { ... }` macro
(you'll see it all over `serverCron`) gates a block so it only runs every `ms`
milliseconds regardless of `hz`. So instantaneous-metric sampling runs every 100 ms
(`server.c:1552`), the "N keys in M slots" debug log every 5000 ms (`server.c:1597`),
etc. One timer, many cadences.

What it drives:

- Active expiry **slow** cycle (`activeExpireCycle`, via `databasesCron`, note 05)
- Eviction if over `maxmemory`
- **Triggering background saves.** The RDB save-param check is right here
  (`server.c:1642`): for each `save <seconds> <changes>` rule, if `server.dirty >=
  changes` *and* enough seconds have elapsed *and* the last bgsave didn't just fail, it
  calls `rdbSaveBackground` (note 06). The AOF auto-rewrite growth check (`server.c:1661`)
  is a few lines below.
- Reaping finished child processes (`checkChildrenDone`, `server.c:1638`)
- Graceful shutdown on `SIGTERM`/`SIGINT` — the signal handler just sets
  `server.shutdown_asap`; the actual `prepareForShutdown` runs here (`server.c:1580`),
  off the signal context, which is why shutdown can flush AOF and save cleanly.
- Client timeouts, resizing/rehashing hash tables (`databasesCron`, `src/server.c:1304`)
- `replicationCron` and `clusterCron` (notes 07 and 08)

**Key mental model:** `serverCron` does *periodic, amortized* work on a timer.
`beforeSleep` does *pending, must-happen-now* work every iteration. Both are on the
main thread; neither may block for long, so every expensive job here is incremental
(expire a few keys, rehash a few buckets, then yield).

## Where the threads are

Valkey is **not** a multi-threaded database in the way people usually mean. Command
execution is single-threaded on the main thread, always. Other threads exist only to
take work *off* that thread:

- **I/O threads** (`io_threads.c`) — socket reads, writes, protocol parsing, object
  freeing. Never execute commands. See `design-docs/io-threads.md`.
- **BIO / background threads** (`bio.c`) — slow syscalls: `close()`, `fsync()`, freeing
  big objects, and (in newer code) receiving an RDB to disk during replication.
- **Forked children** — RDB save, AOF rewrite. Separate *processes*, not threads;
  they get a copy-on-write snapshot of memory for free.

So the concurrency story is: one thread owns all the data; everything else is I/O or
a fork. That is why you'll see remarkably few locks in the keyspace code.

## Exercise

Start the server with `--loglevel debug` and watch the 5-second `serverCron` heartbeat
logs (`server.c:1607`, 1620) tick by with no traffic — that's the janitor running on an
idle server. Then in one shell run `valkey-benchmark -t set -n 1000000` and in another
`valkey-cli --latency`; the latency you see is almost entirely the `beforeSleep` +
command-execution portion of each loop iteration. Now set `appendonly yes appendfsync
always` and re-run: the added latency is `flushAppendOnlyFile` moving from a background
thread onto the main-thread path at `server.c:1962`. You just felt the ordering table
above.

## Read next

Note 02, which traces a single command through this loop.
