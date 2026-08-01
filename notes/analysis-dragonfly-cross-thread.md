# How Dragonfly handles cross-thread command execution (and what it means for slot-per-thread)

*Analysis date: 2026-07-31. Source read locally at `~/github/dragonfly/src`
(commit as checked out). File:line references are to that tree.*

## Why this comparison

Valkey's `conn-el` branch is retrofitting a shared-nothing, slot-per-thread execution
model onto the single-threaded `aeEventLoop`. In doing so we hit a recurring tax on every
**REMOTE** command (one whose key is owned by a different shard than the connection):

- a cross-thread hand-off (argv move, mpsc inbox, self-pipe `write()` wake),
- a heavyweight continuation (`blockClient` / `unblockClient` + reprocessing),
- a global `clients_index_mutex` on every completion,
- a reply copy on the way back.

Measured result on uniform-random keys (macOS, indicative): `shard-threads 4` stays
**below** single-thread on every workload — ~0.84–0.86x at P1, ~0.51–0.64x at P16 — because
~(N-1)/N of commands take the REMOTE path and the per-command hand-off dominates.

Dragonfly is the same shared-nothing premise done as a ground-up design, and it scales
across cores. This doc records how it does the cross-thread part, mapped point-by-point to
what we built, and the strategic conclusions.

## Dragonfly's model in one paragraph

Keys are hashed to shards, one shard-thread owns each slice exclusively
(`Shard(key, shard_set->size())`, `server/transaction.cc:265`) — same as our slot→shard map.
The difference is the **runtime**: helio **fibers** plus an **io_uring proactor per core**,
and a unified **transaction framework** that bridges the connection thread and the data
threads. A command runs as:

```cpp
// Transaction::Execute, server/transaction.cc:934
ScheduleInternal();      // acquire ordering/locks across the involved shards
DispatchHop();           // arm + post callbacks to the owning shard thread(s)
run_barrier_.Wait();     // suspend the *connection fiber* until shards signal done
```

## Point-by-point vs. what we built

### 1. The continuation — fibers instead of `blockClient`/`unblockClient`

`run_barrier_` is a `util::fb2::EmbeddedBlockingCounter` (`server/transaction.h:620`) — a
**fiber** primitive. `run_barrier_.Wait()` suspends the *connection's fiber*; the thread's
proactor immediately runs other connection fibers. Each shard decrements the barrier when its
callback finishes. There is **no id lookup, no global mutex, no reprocessing queue** on
completion. The connection literally is a fiber: `util::fb2::Fiber async_fb_`
(`facade/dragonfly_connection.h:586`).

> Our entire `#3` (pin the client to drop the `clients_index_mutex` — the +82% pipelined win)
> plus the `blockClient` / `unblockClient` machinery is the retrofit-tax that a fiber `.Wait()`
> erases outright.

### 2. The local fast path — `CanRunInlined()`

```cpp
// server/transaction.cc, Transaction::CanRunInlined()
if (unique_shard_cnt_ != 1 || unique_shard_id_ != ss->thread_index())
  return false;
...
// DispatchHop(): if CanRunInlined()
EngineShard::tlocal()->PollExecution("exec_cb", this);   // inline, no hop
```

If the single shard a command needs is the coordinator's own thread, the callback runs
**inline — zero hand-off.** Same idea as our LOCAL path, but with no message and no queue.
(It also guards against nesting: no inlining if another tx is already running on the shard, or
if a blocking controller has suspended txs.)

### 3. The cross-thread hop — cheap by construction

```cpp
// server/engine_shard_set.h:57
template <typename F> auto Add(ShardId sid, F&& f) {
  return shards_[sid]->GetFiberQueue()->Add(std::forward<F>(f));
}
```

`shard_set->Add(sid, cb)` posts `cb` to shard `sid`'s **lock-free FiberQueue**, drained by that
shard's io_uring proactor. This is our mpsc-inbox + self-pipe-`write()` wake — but the
notification rides io_uring rather than an epoll/kqueue self-pipe, so far fewer syscalls. Our
wake-coalescing was chasing the same goal the hard way.

### 4. Pipelining — `MultiCommandSquasher` (the big one)

From `server/multi_command_squasher.h`:

> "squashing multiple consecutive single-shard commands into one hop whenever it's possible,
> thus parallelizing command execution and **greatly decreasing the dispatch overhead**"
> (`max_squash_size = 32`).

Under a pipeline, Dragonfly groups a connection's queued single-shard commands **by shard** and
dispatches each group as **one hop** that runs the whole batch on the shard thread; the groups
run in parallel across shards. So N pipelined commands become ~(#distinct shards) hops.

> This is precisely why Dragonfly's pipelined multi-core throughput scales and ours does not.
> Our P16 `st=4` = 0.51x of `st=1` because **every** pipelined command is its own independent
> hop + continuation. Squashing amortizes both.

### 4b. Allocation — mimalloc per-thread heaps, not object pools

Dragonfly overrides global `new`/`delete` with **mimalloc** (`#include <mimalloc-new-delete.h>`,
`server/dfly_main.cc:21`) and tunes it (`mi_option_set_default(mi_option_purge_delay, 0)`).
Every allocation hits a **per-thread heap**, so the alloc/free hot path -- including the
cross-thread free in a hop (coordinator allocs, owner frees) -- takes no global lock. It does
**not** heavily pool per-command objects: `new Transaction{cid}` per command
(`server/main_service.cc:861`), freed after. The fast allocator *is* the strategy.

Empirically confirmed on our side: profiling a libc build showed ~40% of worker time in
malloc/free + `_os_unfair_lock` (macOS libmalloc's zone lock). Rebuilding Valkey with jemalloc
(its Linux default, per-thread tcache) removed it -- pipelined st=4 throughput ~doubled (GET
P16 +87%, SET P16 +62%). So the "allocation churn" is a per-thread-allocator problem already
solved by config; no object pool needed. See [[perf-baseline-slot-per-thread]].

### 5. Locality — Dragonfly does **not** migrate connections

There is no connection-migration path. The connection stays on its accept thread; commands hop
to the data. Dragonfly bet entirely on **making the hop cheap** (fiber wait + inline-local +
squashing + io_uring) rather than **avoiding the hop** (our `#2` idea).

## Summary table

| Concern | Dragonfly | Our Valkey retrofit |
|---|---|---|
| Runtime | helio fibers + io_uring proactor per core (built for it) | single-threaded `aeEventLoop` + epoll/kqueue (retrofit) |
| Connection wait | fiber suspend (`run_barrier_.Wait()`); thread serves other conns | `blockClient`/`unblockClient` + reprocessing queue |
| Completion signal | barrier decrement (no lookup, no lock) | was global mutex + rax lookup; now pinned-pointer (`#3`) |
| Cross-thread post | lock-free FiberQueue + io_uring notify | mpsc inbox + self-pipe `write()` (wake-coalesced) |
| Local fast path | `CanRunInlined()` — inline, zero hop | LOCAL: `call()` directly (have it) |
| Pipelining | `MultiCommandSquasher` — one hop per shard per batch | one message per command (no batching of work) |
| Locality | not addressed via migration — hop made cheap | debated migration (`#2`); concluded not worth it |
| Reply path | reply-capture per squashed command | reply-by-reference (blocks moved, `#1`, partial) |

## Strategic conclusions for slot-per-thread

1. **Drop connection migration (`#2`).** Dragonfly scaling well *without* it is strong evidence
   it's the wrong lever — complexity the winning design deliberately avoided. Make the hop cheap
   instead of avoiding it.

2. **Highest-value missing piece: pipeline squashing.** It directly attacks our worst case (P16,
   where we sit at 0.51x). Group a connection's same-owner REMOTE commands into a single batched
   hop that executes K commands on the owner in one shot. Implementable on the existing REMOTE
   machinery (a job carrying K commands per owner) **without** adopting fibers, and a far bigger
   win than migration.

3. **The continuation wants to get lighter, toward fibers.** `blockClient`/`unblockClient` per
   REMOTE command is exactly what `run_barrier_.Wait()` doesn't pay. Short of a fiber runtime (a
   huge change), a dedicated lightweight "awaiting-shard-result" client state — skipping bstate
   alloc, the timeout table, and the reprocess path — captures part of it.

4. **Meta-point / reframing Stage-0.** Dragonfly's cross-shard cost is *intrinsically* cheap
   because helio (fibers + io_uring) was built for it; we're retrofitting onto a single-threaded
   event loop. The self-pipe wakes, block/unblock, and per-command mutex are all symptoms of that.
   This suggests the original Stage-0 fork (slot-per-thread vs. an io_uring/fiber backend) is not
   an either/or: **an io_uring + fiber substrate may be a prerequisite for slot-per-thread to pay
   off**, not a competing alternative.

## Where this leaves the Valkey session

Delivered this session (all pushed to `origin/conn-el`): wake coalescing, argv move, clientsCron
UAF + barrier back-to-back hardening, mpsc acquire (TSAN de-noise), keyspace barrier gaps +
exclusion-race fix, LRU/time-cache atomics, reply-by-reference, and the pinned-pointer completion
(the +82% pipelined win). These shrink the hop; none of them changes the fact that on uniform
workloads slot-per-thread is still a bit behind a single thread.

Next lever, per the analysis above: **prototype squashing**, not migration.
