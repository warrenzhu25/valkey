# 01 — Server Lifecycle & the Event Loop

Almost every database you've used answers requests by handing each connection to a thread
and letting the operating system juggle them. Valkey does the opposite: **one thread
handles every client, one request at a time, in a single loop that never stops turning.**
That sounds like it should be slow, and the fact that it isn't — that one thread serves
hundreds of thousands of operations a second — is the first thing to understand, because
every other design decision in the codebase follows from it. No command ever waits on a
lock, because there is only one thread touching the data. Nothing blocks, because a blocked
thread would freeze every client at once. This chapter is that loop: how the server boots
into it, what one turn of it does, and the two functions (`beforeSleep` and `serverCron`)
where all the real work is scheduled.

## Startup

`main()` lives at `server.c:7511`, and the first surprising thing about it is the
declaration: `__attribute__((weak)) int main(...)` (`server.c:7511`). A *weak* symbol can be
overridden at link time, which is exactly how the unit tests in `src/unit/` link their own
`main` over the server's and exercise engine internals — a hash table, an encoding — without
booting a whole server. Keep that in mind the first time you wonder how `src/unit/` tests
run "inside" the code.

The boot sequence that matters is short:

1. **`initServerConfig()`** (`server.c:2308`) — fill the one global `struct valkeyServer
   server` with compiled-in defaults. This happens *before* any config file is read, so the
   config parser has a fully-formed baseline to overwrite.
2. **Config parsing** — the file and command-line arguments layer on top of those defaults.
3. **`initServer()`** (`server.c:2924`) — the real construction: create the event loop,
   open listening sockets, allocate the databases, and register two kinds of callbacks — the
   `serverCron` timer and the socket *accept* handlers. After this call the machine exists
   but isn't turning.
4. **Load data from disk** — replay the AOF, or load the RDB, so the keyspace is warm before
   the first client connects.
5. **`aeMain()`** (`ae.c:540`) — enter the loop and never return until shutdown.

## The loop itself

`aeMain` is anticlimactically simple (`ae.c:540`):

```c
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop, AE_ALL_EVENTS | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
    }
}
```

All the substance is in one turn of `aeProcessEvents` (`ae.c:411`). Read in order, a single
iteration does this:

```
beforeSleep()          server.c:1854   flush replies, fsync AOF, deferred work
   │
compute poll timeout   ae.c:440        = time until the next serverCron firing
   │
aeApiPoll()            ae.c:449        epoll_wait / kqueue — SLEEP here until an fd
   │                                   is ready or the timeout expires
afterSleep()           server.c:2056   re-acquire module GIL, per-wake bookkeeping
   │
fire file events       ae.c:460        readable then writable handlers, per ready fd
   │
processTimeEvents()    ae.c:513        run serverCron if its timer is due
```

Three things about this are worth pinning down, because they explain behavior you'll
otherwise find mysterious.

**`beforeSleep` runs *inside* the poll call, right before sleeping** (`ae.c:426`) — it is
not a separate loop stage. That is deliberate: the last thing the thread does before going
to sleep is drain everything it owes (send buffered replies, fsync the AOF), so no work sits
waiting while the thread is blocked in `epoll_wait`.

**The poll timeout is the time until the next timer.** When there's nothing to do, the loop
doesn't spin — it computes `usUntilEarliestTimer` (`ae.c:440`), the microseconds until
`serverCron` is next due, and passes that as the `epoll_wait` timeout. So an idle server
with `hz 10` wakes up ten times a second exactly, does its janitorial tick, and sleeps
again. A zero timeout (busy-poll) happens only when `beforeSleep` sets the *don't-wait* flag
via `aeSetDontWait` (`ae.c:126`) — the classic case is TLS, where decrypted bytes can sit in
OpenSSL's buffer while the underlying fd reports *not readable*; blocking in `epoll_wait`
then would deadlock, so `connTypeHasPendingData` (`server.c:1894`) forces a zero-timeout poll
to come straight back and process them.

**Readable fires before writable — usually.** For each ready fd, `aeProcessEvents` normally
runs the read handler first, then the write handler (`ae.c:485`), so a query read at the top
of the iteration can have its reply written at the bottom of the *same* iteration. The
exception is `AE_BARRIER` (`ae.c:477`): a handler can ask for the order to be *inverted* —
write before read — which is how "fsync the AOF in `beforeSleep` before we reply to the
client" is enforced. The reply must not leave the box until the data it acknowledges is
durable; the barrier guarantees that ordering.

`ae.c` itself is a thin portability shim. The actual polling lives in a backend chosen at
compile time by `#ifdef` at the bottom of `ae.c`: `ae_epoll.c` on Linux, `ae_kqueue.c` on
macOS/BSD, with `ae_evport.c` and `ae_select.c` as fallbacks. You will almost never need to
read them — the abstraction (`aeApiPoll` returns the list of ready fds) is all that matters
upward.

## `beforeSleep` — the most important function you've never heard of

`server.c:1854`. It runs **every iteration, right before the poll**, and it is where Valkey
does all the work it *deferred* while executing commands — because doing that work inline,
mid-command, would have been either wrong (replying before the AOF is durable) or slow
(fsyncing once per command instead of once per batch).

The ordering of its steps **is the design.** Nearly every step carries a `must be done
before X` comment that pins its position, and those constraints encode real correctness
requirements. The load-bearing ones, in execution order:

| Step | Line | Ordering constraint (from the code) |
|------|------|-------------------------------------|
| `trySendPollJobToIOThreads` | 1858 | offload the poll itself to an I/O thread when I/O is pending |
| `processIOThreadsResponses` | 1886 | collect finished reads ASAP after the loop wakes |
| `connTypeProcessPendingData` (TLS) | 1890 | must precede `flushAppendOnlyFile` |
| `clusterBeforeSleep` | 1900 | may flip cluster ok↔fail; must run before serving unblocked clients |
| `blockedBeforeSleep` | 1905 | before AOF flush, since unblocked clients may write (`appendfsync=always`) |
| fast expire cycle | 1915 | **primary only** — see below |
| `sendGetackToReplicas` (`WAIT`) | 1933 | after unblocking, before sleeping |
| `flushAppendOnlyFile(0)` | 1962 | before writes reach clients, for `appendfsync=always` |
| `handleClientsWithPendingWrites` | 1982 | the main path: buffered replies reach sockets (chapter 03) |
| `freeClientsInAsyncFreeQueue` | 1999 | deferred client teardown (chapter 03) |
| `evictClients` | 2006 | disconnect clients over the output-buffer limit |

Two subtleties reward internalizing:

- **The fast expire cycle only runs on a primary.** The guard is
  `if (server.active_expire_enabled && !server.import_mode && iAmPrimary())`
  (`server.c:1915`). This *is* the code-level enforcement of chapter 05's rule that a replica
  never expires a key on its own initiative — it waits for the primary's `DEL` to arrive over
  the replication stream. The rule you read about in the expiry chapter is one `iAmPrimary()`
  check here.

- **The re-entrant subset.** When a long command pumps the loop mid-execution — a slow Lua
  script, a module call, via `processEventsWhileBlocked` — `beforeSleep` takes an early
  branch (`if (ProcessingEventsWhileBlocked)`, `server.c:1868`) that runs only a *vital few*
  steps: collect I/O responses, flush the AOF, flush client writes, free async clients.
  Everything else — expiry, cluster, eviction — is skipped so the nested pump stays cheap.
  If you ever notice some `beforeSleep` work mysteriously *not* happening during a busy
  script, this branch is why.

**The module GIL boundary.** The very last thing `beforeSleep` does is release the module
Global Interpreter Lock — `if (moduleCount()) moduleReleaseGIL()` (`server.c:2047`) — under a
shouting comment forbidding any code below it. While the main thread sleeps in `epoll_wait`,
module background threads are permitted to touch the dataset; `afterSleep` (`server.c:2056`)
re-acquires the GIL the instant the loop wakes. So the window in which a module's own thread
may safely mutate data is *exactly the poll* — not a microsecond more.

### It measures itself

`beforeSleep` brackets its phases with `getMonotonicUs()` and feeds `durationAddSample` for
three buckets: `EL_DURATION_TYPE_AOF`, `_EL` (the whole event loop), and `_CRON`
(`server.c:1966`, 2015, 2027). Those are the numbers `INFO` reports under the event-loop
metrics, and they answer a question operators actually care about — is this server spending
its time executing commands, or waiting on I/O? — without attaching a profiler.

## `serverCron` — the background heartbeat

`server.c:1537`, registered as the loop's time event, fires at `hz` (default 10 times a
second, and it scales up with client count). If `beforeSleep` is "must happen now,"
`serverCron` is "housekeeping on a schedule."

How does one 10-Hz timer run jobs that need wildly different cadences? The `run_with_period(ms)
{ ... }` macro, which you'll see all over `serverCron`, gates a block to run at most once
every `ms` milliseconds regardless of `hz`. Instantaneous metrics sample every 100 ms, the
"N keys in M slots" debug line logs every 5000 ms, and so on — one timer, many rates.

What `serverCron` drives:

- The active-expiry **slow** cycle (`activeExpireCycle`, via `databasesCron`, chapter 05).
- Eviction, when memory is over `maxmemory`.
- **Triggering background saves.** The `save <seconds> <changes>` check is right here
  (`server.c:1642`): for each rule, if `server.dirty >= changes` *and* enough seconds have
  elapsed *and* the last bgsave didn't just fail, it calls `rdbSaveBackground` (chapter 06).
  The AOF auto-rewrite growth check sits a few lines below (`server.c:1661`).
- Reaping finished fork children (`checkChildrenDone`, `server.c:1638`).
- Graceful shutdown: the `SIGTERM`/`SIGINT` handler only sets `server.shutdown_asap`; the
  real `prepareForShutdown` runs *here* (`server.c:1580`), off the signal context, which is
  why shutdown can safely flush the AOF and save.
- Client timeouts, and incremental hash-table resizing/rehashing (`databasesCron`,
  `server.c:1304`).
- `replicationCron` and `clusterCron` (chapters 07 and 08).

**The mental model:** `serverCron` does *periodic, amortized* work on a timer; `beforeSleep`
does *pending, must-happen-now* work every iteration. Both run on the main thread, and
neither may block, so every expensive job either subsystem schedules is **incremental** —
expire a few keys, rehash a few buckets, trim a little backlog, then yield back to the loop.
That is the same "bounded work per operation" instinct you meet in every chapter.

## Where the threads are

Valkey is **not** multi-threaded in the sense people usually mean. Command execution is
single-threaded, on the main thread, always. The other threads exist only to lift work
*off* it:

- **I/O threads** (`io_threads.c`) — socket reads, socket writes, RESP parsing, and object
  freeing. They never execute a command; they hand parsed arguments to the main thread and
  take finished replies back. (`design-docs/io-threads.md`.)
- **BIO / background threads** (`bio.c`) — slow syscalls that must not stall the loop:
  `close()`, `fsync()`, freeing very large objects, and receiving a replication RDB to disk.
- **Forked children** — RDB save and AOF rewrite are separate *processes*, not threads. Fork
  gives them a copy-on-write snapshot of memory for free (chapter 06).

So the whole concurrency story is: one thread owns the data; everything else is I/O or a
fork. That is why the keyspace code has almost no locks — there is nothing to lock against.
Chapter 09 takes this apart in full: how the I/O worker pool, the BIO threads, and the fork
children divide the work without ever letting a second thread touch the keyspace.

## Worked example — narrate one turn of the loop

Picture a server with `hz 10`, `appendonly yes`, `appendfsync everysec`, one connected
client. Watch two consecutive iterations.

**Iteration A — idle.** No client has sent anything.

```
beforeSleep():   nothing buffered → the steps run but find no work
poll timeout  =  usUntilEarliestTimer() ≈ 87 ms   (next serverCron tick)
aeApiPoll():     sleeps ~87 ms, wakes on TIMEOUT (0 fds ready)
afterSleep():    re-acquire GIL, cheap bookkeeping
file events:     none (numevents == 0)
processTimeEvents(): serverCron is due → run janitor:
                   sample metrics, maybe expire a few keys, check save params
```

Total main-thread CPU spent: a few microseconds of `serverCron`. The other ~87 ms was spent
asleep in the kernel. This is what "idle" costs — ten cheap wakeups a second.

**Iteration B — a `SET k v` arrives.** Between polls, the client's bytes land in the socket.

```
beforeSleep():   still nothing buffered from before → quick
poll timeout  =  time to next serverCron tick
aeApiPoll():     wakes IMMEDIATELY — the client fd is READABLE (returns 1 fd)
afterSleep():    re-acquire GIL
file events:     fd is readable → fire readQueryFromClient (chapter 02):
                   read bytes → parse RESP → argv = ["SET","k","v"]
                   → processCommand → call → setCommand
                   → the value is written; server.dirty++
                   → addReply("+OK") APPENDS "+OK\r\n" to the client's buffer
                 (the reply is NOT on the wire yet — see the payoff below)
processTimeEvents(): serverCron may or may not be due this turn
```

The reply is now sitting in the client's output buffer, and the AOF has a pending `SET` to
persist. **Neither has been sent.** They go out at the *top of the next iteration*, in
`beforeSleep`:

```
--- iteration C ---
beforeSleep():
   flushAppendOnlyFile(0)          server.c:1962  → the SET is written to the AOF buffer
                                                    (fsync itself is handed to a BIO thread
                                                     under appendfsync=everysec)
   handleClientsWithPendingWrites  server.c:1982  → write(2) "+OK\r\n" to the socket
poll ...
```

The single most clarifying observation in the whole architecture is hiding in that
sequence: **the command handler did not send the reply.** `setCommand` only appended `+OK`
to a buffer. The event loop flushed it, one iteration later, *after* the AOF write — so a
client can never receive an acknowledgment for a write that isn't yet on its way to disk.
The read-then-write ordering within an iteration (and the `AE_BARRIER` inversion) is what
lets a low-latency reply still ride out in the same turn when durability doesn't force a
wait. Internalize this deferral and the rest of the codebase — buffered replies (chapter
03), batched propagation (chapter 02), incremental everything — stops looking like a
collection of tricks and starts looking like one idea applied everywhere.

You can *feel* this ordering, too. Start the server with `--loglevel debug` and watch the
5-second `serverCron` heartbeat logs (`server.c:1607`, 1620) tick by on an idle server —
that's iteration A repeating. Then run `valkey-benchmark -t set -n 1000000` and, in another
shell, `valkey-cli --latency`: the latency you measure is essentially the
command-execution-plus-`beforeSleep` slice of each iteration B/C. Now switch to `appendfsync
always` and re-run — the added latency is `flushAppendOnlyFile` moving its fsync from a BIO
thread onto the main-thread path at `server.c:1962`, so every reply now waits on disk. You
just moved one row of the `beforeSleep` table and watched the tail latency respond.

## Read next

Chapter 02, which zooms into iteration B above and traces a single command all the way from
the socket read to the buffered reply — the middle of the loop, in full detail.
