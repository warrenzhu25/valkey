# 01 — Server Lifecycle & the Event Loop

Everything in Valkey hangs off one event loop. Understand this note and the rest of
the codebase has a place to attach to.

## Startup

`main()` is at `src/server.c:7511`. It is declared `__attribute__((weak))`, which lets
test harnesses link their own `main` over it.

The sequence that matters:

1. `initServerConfig()` (`src/server.c:2308`) — populate the global `server` struct with
   defaults, before any config file is read.
2. Config file / command-line parsing.
3. `initServer()` (`src/server.c:2924`) — create the event loop, listening sockets,
   databases, and register the cron timer and accept handlers.
4. Load data from disk (AOF or RDB).
5. `aeMain()` (`src/ae.c:540`) — enter the loop and never return.

## The loop

`aeMain` (`src/ae.c:540`) is a `while (!stop)` around `aeProcessEvents`
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

## `beforeSleep` — the most important function you've never heard of

`src/server.c:1854`. It runs **every single loop iteration, right before blocking**.
This is where Valkey does all the work it deferred while executing commands, because
doing it inline would have been wrong or slow. Roughly:

- Flush pending writes to clients (this is the main path by which replies actually
  reach sockets).
- Hand read/write jobs to I/O threads and collect their results
  (`IOThreadsBeforeSleep`, `src/io_threads.c:113`).
- Fast-path expiry of keys.
- Flush the AOF buffer to disk (`flushAppendOnlyFile`).
- Propagate accumulated writes to replicas and the AOF.
- Handle clients unblocked by the commands just executed.

When you're wondering *"where does this actually get sent?"* — the answer is very
often `beforeSleep`. Commands typically append to a buffer; `beforeSleep` drains it.

## `serverCron` — the background heartbeat

`src/server.c:1537`, registered as a time event, runs at `hz` frequency (default 10/sec,
configurable; it also scales with client count). It is the janitor:

- Active expiry cycle (`activeExpireCycle`, see note 05)
- Eviction if over `maxmemory`
- Trigger background saves (RDB/AOF rewrite) when thresholds are met
- Reap finished child processes
- Client timeouts, resizing hash tables, incremental rehashing (`databasesCron`,
  `src/server.c:1304`)
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

## Read next

Note 02, which traces a single command through this loop.
