# 09 — Threading & the I/O Model

Every chapter so far has leaned on one sentence: *the main thread owns all the data, and it
executes commands one at a time.* That's what lets the keyspace code run without a single lock
(chapter 04), what makes replication deterministic (chapter 02), and what forces every
expensive job to be incremental (chapters 01, 05). But a modern server has many CPU cores, and
a strictly single-threaded process would leave most of them idle. So Valkey threads the
*edges* — reading sockets, writing sockets, parsing protocol, `fsync`, freeing memory — while
keeping the *center*, data mutation, on one thread. This chapter is how that's done: the I/O
worker pool, the background (BIO) threads, and the fork children, and the rule that keeps them
from ever corrupting each other.

The one thing to fix in your head before anything else: **Valkey's I/O threading is not the
classic "fan the reads out to N threads and block until they're done" model.** It's an
asynchronous, queue-based system where the main thread hands off jobs, keeps working, and
collects results later. Understanding that distinction is most of the chapter.

## The one rule

**Thread 0 — the main thread — is the only thread that mutates server state.** Every other
thread does pure *transport* (bytes on/off a socket) or *memory* work (allocate, free) on data
its job explicitly carries. Workers must never touch the keyspace, the client list, or any
shared structure outside the one object handed to them. Correctness leans on this so heavily
that the code asserts `inMainThread()` (`io_threads.c:55`) all over the places that mutate
state.

This is why "is Valkey single-threaded or multi-threaded?" has an annoying answer: **both.**
Command execution is single-threaded and always will be; I/O is parallel. The data structures
need no locks because the only thread that changes them is thread 0.

There are three distinct kinds of "other thread," and they're easy to confuse:

| Kind | File | Work | Threads? |
|------|------|------|----------|
| **I/O worker pool** | `io_threads.c` | socket read/write, RESP parse, accept, poll, deferred obj free | Yes — pool of `pthread`s, scaled dynamically |
| **BIO / background** | `bio.c` | slow *blocking* syscalls: `close(2)`, AOF `fsync`, lazy-free of huge objects, RDB-to-disk | Yes — a small fixed set (5) |
| **Fork children** | `rdb.c`, `aof.c` | RDB save, AOF rewrite | No — separate **processes** (chapter 06) |

Command execution lives on none of them. Keep the three separate as we go.

## I/O threads

By default `io-threads` is **1** (`config.c:3457`), meaning no workers exist and the main
thread does its own socket I/O inline. Raising it spawns workers `1..N` (thread 0 stays the
main thread). They connect to the main thread not through a shared lock but through **three
lock-free queues** in `src/queues.c`:

```
                 io_shared_inbox   (SPMC: main → ANY worker)
   Main thread  ───────────────────────────────────────────►  Workers 1..N
   (thread 0)    io_private_inbox[i] (SPSC: main → worker i)
                 io_shared_outbox  (MPSC: ANY worker → main)
                ◄───────────────────────────────────────────
```

| Queue | Kind | Direction | Purpose |
|-------|------|-----------|---------|
| `io_shared_inbox` | single-producer, **multi**-consumer | main → any worker | default request channel; any free worker claims the next job |
| `io_private_inbox[i]` | single-producer, single-consumer | main → worker `i` | for jobs that must run on a *specific* worker |
| `io_shared_outbox` | **multi**-producer, single-consumer | any worker → main | the single results channel |

Why a *private* inbox per worker as well as the shared one? Two reasons (`io-threads.md`
"Queue Topology"): freeing an argv batch (`FREE_ARGV`) must happen on the same thread that
built it, because the per-thread allocator would otherwise contend; and at high thread counts,
`POLL` jobs are pinned to one worker to avoid every worker fighting over the shared queue's
head. A worker always drains its private SPSC queue first, then checks the shared SPMC queue
(`IOThreadMain`, `io_threads.c:282`).

### Tagged pointers and job kinds

Jobs are passed as **tagged pointers**: the low 3 bits hold the job type, the rest is the data
pointer. This works because `zmalloc` returns 8-byte-aligned pointers, so the low 3 bits are
always zero and free to reuse (`tagJob`/`untagJob`, `io_threads.c:38`). Three bits cap the
scheme at 8 job kinds. The kinds (`io_threads.h`):

```
Request (main → worker):  READ_CLIENT | WRITE_CLIENT | FREE_ARGV | FREE_OBJ | POLL | ACCEPT
Result  (worker → main):  READ_CLIENT | WRITE_CLIENT
```

The worker loop is a `switch` over the tag: a `READ_CLIENT` job calls
`ioThreadReadQueryFromClient` (`networking.c:6611`) — `recv()` plus RESP parse into `argv`; a
`WRITE_CLIENT` job calls `ioThreadWriteToClient` (`networking.c:6662`) — `write()` the client's
already-filled reply buffer; `FREE_OBJ` just `decrRefCount`s a big object off-thread. Notice
what's absent: there is **no** `EXECUTE` job. Workers never run a command.

### Offload is best-effort — the eligibility gate

Every offload goes through a `try*ToIOThreads` helper that first checks whether the handoff is
even safe, and returns `C_ERR` if not — in which case the main thread just does the work
inline. Nothing depends on a worker for correctness. Read `trySendReadToIOThreads`
(`io_threads.c:501`) and you can see the gate directly:

```c
if (server.active_io_threads_num <= 1) return C_ERR;   // no workers awake → do it myself
if (c->io_read_state != CLIENT_IDLE) return C_OK;      // already in flight on a worker
if (getClientType(c) == CLIENT_TYPE_REPLICA) return C_ERR;  // replica read traffic is tiny
if (c->flag.blocked || c->flag.unblocked) return C_ERR;     // blocked clients stay on main
if (c->flag.close_asap) return C_ERR;                       // dying client stays on main
...
c->io_read_state = CLIENT_PENDING_IO;                  // mark in-flight, then enqueue
spmcEnqueue(&io_shared_inbox, tagJob(c, JOB_REQ_READ_CLIENT));
```

That per-client `io_read_state` / `io_write_state` (values `CLIENT_IDLE`,
`CLIENT_PENDING_IO`, `CLIENT_COMPLETED_IO`, `server.h:1107`) is the little state machine that
keeps a client from being handed to two workers at once, and lets the main thread know when a
worker is done with it.

### The lifecycle of one offloaded read

```
[MAIN]  trySendReadToIOThreads(c)            io_threads.c:501
          gate passes → mark c PENDING_IO
          spmcEnqueue(READ_CLIENT), io_jobs_submitted++
   │
[QUEUE] io_shared_inbox
   │
[WORKER] IOThreadMain                        io_threads.c:282
          untag → READ_CLIENT
          ioThreadReadQueryFromClient(c)     networking.c:6611  (recv + parse argv)
          atomic io_jobs_finished++
          post READ_CLIENT result → io_shared_outbox
   │
[QUEUE] io_shared_outbox
   │
[MAIN]  processIOThreadsResponses()          io_threads.c:859   (called from beforeSleep)
          dequeue batch → for each parsed client:
            processCommand → call            (execute — on thread 0, serialized, safe)
```

The parallelism is entirely in the `recv`/parse; the decision and the mutation are serialized
on the main thread in `processIOThreadsResponses`. Writes are the mirror image: once a reply
buffer is filled, `trySendWriteToIOThreads` (`io_threads.c:537`) hands it to a worker to
`write()` out.

### Dynamic scaling — the genuinely new part

Workers are not simply always-on. `IOThreadsBeforeSleep` / `IOThreadsAfterSleep`
(`io_threads.c:113`, `149`) implement an *ignition* policy: workers stay parked — blocked on a
per-thread mutex — until the main thread's own active time crosses a threshold; then they
ignite, scale up while the shared inbox is backing up, and scale back down after a cooldown of
idleness. The main thread parks a worker by *holding* its mutex and wakes it by *releasing*
it. `io-threads-always-active` disables the policy and keeps all configured workers awake. The
practical upshot: a lightly loaded server configured with `io-threads 8` does **not** burn
eight cores — most workers sleep until load actually demands them.

`CONFIG SET io-threads` is live-reconfigurable via `updateIOThreads` (`io_threads.c:429`),
which first **drains** all in-flight jobs (`drainIOThreadsQueue` — spin until
`io_jobs_submitted == io_jobs_finished`) so nothing is lost across the resize. When the main
thread must wait for a *single* client's I/O to settle — e.g. before freeing it —
`waitForClientIO` (`io_threads.c:95`) spins on that client's `io_*_state` instead of the global
counter.

## BIO threads — for the slow syscalls

Some operations block for *milliseconds*, too long even to hand to an I/O worker, because
they're not transport — they're the kernel doing something slow. `bio.c` runs a small fixed set
of **5 background workers** (`bio_workers`, `bio.c:91`), each pinned to a category, with jobs
routed by a static table (`bio_job_to_worker`, `bio.c:76`):

| Job (`bio.h`) | Worker | Why it can't run on the main thread |
|---------------|--------|-------------------------------------|
| `BIO_CLOSE_FILE` (`:50`) | `bio_close_file` | `close(2)` can block when it triggers an unlink/delete |
| `BIO_AOF_FSYNC` (`:51`) | `bio_aof` | the `fsync` under `appendfsync everysec` (chapter 06) |
| `BIO_CLOSE_AOF` (`:53`) | `bio_aof` | deferred close of rotated AOF files |
| `BIO_LAZY_FREE` (`:52`) | `bio_lazy_free` | freeing a huge object/collection off-thread (`UNLINK`, `FLUSHALL ASYNC`) |
| `BIO_RDB_SAVE` | `bio_rdb_save` | receiving a replication RDB to disk (chapter 07) |

A caller submits with helpers like `bioCreateFsyncJob` (`bio.c:227`), `bioCreateLazyFreeJob`
(`bio.c:193`), or `bioCreateCloseJob` (`bio.c:208`), which enqueue via `bioSubmitJob`
(`bio.c:186`); each worker's loop (`bioProcessBackgroundJobs`, `bio.c:248`) processes its queue
oldest-first. BIO is largely fire-and-forget — historically there was no completion callback at
all. This is precisely why `appendfsync everysec` doesn't stall the event loop: the `fsync`
becomes a `BIO_AOF_FSYNC` job and the main thread moves on. (Under `appendfsync always`, by
contrast, the flush is synchronous on the main thread — chapter 01's ordering table — which is
why it's slower.)

## Fork children are not threads

The third category is worth restating because people lump it in: `BGSAVE` and `BGREWRITEAOF`
spawn separate **processes** via `fork()`, not threads (chapter 06). The child gets a
copy-on-write snapshot of memory and shares nothing mutable with the parent, so there's no
locking question at all — just the COW page cost. When you see "Valkey uses multiple
processes," this is what's meant, and it's a completely different mechanism from the two thread
pools above.

## Worked example — one `SET` under load with `io-threads 4`

A busy server, `io-threads 4` with two workers currently ignited, `appendonly yes appendfsync
everysec`. A client sends `SET k v`. Follow the request across every thread it touches.

```
IO WORKER (thread 2)              MAIN THREAD (thread 0)                 BIO THREAD
 fd readable → job claimed
 ioThreadReadQueryFromClient
   recv "SET k v", parse
   argv=["SET","k","v"]
   post result → outbox
                          ─────►  processIOThreadsResponses (io_threads.c:859)
                                    processCommand → call → setCommand
                                    writes k=v into the keyspace   ← ONLY thread 0
                                    server.dirty++
                                    addReply "+OK" → c->buf
                                    feedAppendOnlyFile → aof_buf
                                                          ── fsync job ──►  bio_aof:
                                                                            fsync(aof fd)
                                    trySendWriteToIOThreads(c)
 ioThreadWriteToClient  ◄────────  (hands c's reply buffer to a worker)
   write "+OK" to socket
```

Trace the responsibilities: **thread 2** did the `recv` and the parse; **thread 0** made the
sole decision to mutate the keyspace and buffered the reply; a **BIO thread** took the `fsync`
so the loop didn't wait on disk; and a worker did the final `write`. Four threads cooperated on
one request, yet exactly one of them touched the data — and it touched it alone, so no lock was
needed anywhere. Now imagine ten thousand such requests a second across four workers: the
`recv`/parse/`write`/`fsync` work spreads across cores, while the `setCommand` calls queue up
and run one after another on thread 0. That serialization is not a bottleneck to apologize for;
it's the reason the whole thing is correct without locks.

## Try it yourself

Watch the workers ignite and the offload happen. Start with `valkey-server --io-threads 4`,
then in one shell flood it: `valkey-benchmark -t set,get -n 2000000 -c 50`. In another, watch
the threads appear and do work:

```
valkey-cli info threads          # active_io_threads, and per-thread activity
ps -T -p $(pgrep -n valkey-server) | grep -E 'io_thd|bio_'   # the actual OS threads
```

You'll see `io_thd_1..3` and the `bio_*` threads as real OS threads, and `active_io_threads`
rise under load and fall when the benchmark stops — the ignition policy from above. Now set
`io-threads 1` (`valkey-cli config set io-threads 1`) and re-run the benchmark: throughput on
a multi-core box drops, because every `recv`/parse/`write` is back on thread 0 competing with
command execution. Finally, `config set appendfsync always` and watch latency rise — that's the
`fsync` moving off the `bio_aof` thread onto the main-thread path.

## Read next

You've now seen the whole machine: the loop (chapter 01), the command path (02), clients (03),
the keyspace (04), expiry and eviction (05), persistence (06), replication (07), the cluster
(08), and — here — how work is spread across threads and processes without ever giving up the
single-threaded ownership that makes all of it correct. From here, the appendix (chapter 10) on
Dragonfly's forkless snapshot model, and the proposals, explore what changing these
foundations would take.
