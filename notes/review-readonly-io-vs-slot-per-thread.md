# Review — read-only commands on I/O threads (the fork/join branch) vs. slot-per-thread

**Status: design review.** Written against the `issue-2022` branch in this repo — three
commits on top of `ca4927ec4`:

| Commit | |
|---|---|
| `f9dad117b` | Make `current_client` and `executing_client` thread-local (16 files) |
| `7e80180bd` | Split `call()` into prologue / invoke / epilogue |
| `6902ffec5` | Execute read-only commands on I/O threads (+ tests) |

The question this note answers: **is per-batch fork/join offload of read-only command procs
the right way to use more than one core, or is it a dead end?**

Short answer: the two refactors are good and should survive. The offload mechanism is
structurally capped in a way no amount of tuning fixes, and its isolation boundary is in the
wrong place. The alternative is written up as
[proposal-readonly-io-execution.md](proposal-readonly-io-execution.md).

All `file:line` anchors verified against the branch at review time.

---

## 1. What the branch does

`processClientsCommandsBatch()` (`src/memory_prefetch.c`) collects a group of clients whose
next command is read-only into a **run**, dispatches them across I/O threads by
`slot % active_io_threads_num`, executes its own share, spins until every dispatched command
finishes, then completes the clients serially on the main thread in arrival order.

Two claimed safety properties:

1. work is assigned by slot, so no two threads touch the same hashtable;
2. the main thread does nothing between dispatch and join, so cron, eviction, defrag and
   incremental rehashing cannot overlap the run.

Property 2 is not a shortcut — it is load-bearing, and it is also the ceiling. See §4.

Property 1 is genuinely required: a lookup mutates the table (`hashtableFind`,
`src/hashtable.c:1606` → `findBucket` → `rehashStepOnReadIfNeeded`, `:914`; the warning is
documented at `:1712`). Any path that lets a slot run on two threads is corruption, not a
benign race.

## 2. Correctness findings

Reported in full separately; summarized here because the *pattern* matters more than the
individual bugs.

| # | Finding | Where |
|---|---|---|
| 1 | Read-only commands get propagated to AOF and replicas. The completion loop runs `callEpilogue()` and then resumes that client's pipeline before completing the *next* client, so a neighbour's pipelined `SET` inflates `server.dirty` and the next `GET`'s epilogue sees a non-zero delta → `alsoPropagate(...)` | `src/io_threads.c:1013`, `src/server.c:4028`, `:4111` |
| 2 | A nested run (via `processEventsWhileBlocked`) reuses and may `zrealloc` the global `io_run` array while the outer completion loop is still indexing it → double-completion or use-after-free | `src/io_threads.c:1095-1101` |
| 3 | `serverAssert(commandCanRunOnIOThread(c, c->cmd))` can fire: `moduleCallCommandFilters()` (`src/server.c:4390`) re-runs `prepareCommand()` (`src/module.c:11597-11600`) *after* the eligibility pre-check, so a filter that rewrites `GET` → a write command panics the server | `src/server.c:4775` |
| 4 | When an owner's queue is full the command falls back to the main thread — but if a same-slot command was already dispatched, two threads are now in one hashtable, breaking property 1 | `src/io_threads.c:1068` |
| 5 | `addReply` → output-buffer limit → `freeClientAsync()` mutates the global `server.clients_to_close` list from an I/O thread, unsynchronized | `src/networking.c:701` → `:6281` → `:2287` |

**The pattern.** Findings 1, 3 and 5 are all the same mistake: the design shares the *real
client object* across threads and then tries to enumerate everything a command proc might
touch and defer it. That list is open-ended. `deferredStats` covers keyspace hit/miss and
error stats; it does not cover the output-buffer-limit close path, and there are more where
that came from — cluster slot stats, commandlog large-reply tracking, `robj` LRU/LFU writes.
Every future change to `addReply()` is a chance to silently break it.

This is an argument about the **boundary**, not about care. A boundary of "argv in, RESP
bytes out" against a socket-less executor client makes the auditable surface two pointers.
A boundary of "here is the real client, please avoid touching anything global" makes it the
whole server.

## 3. The handback is forced, and by what

The deeper objection: even with every bug fixed, a command must return to the main thread to
finish. That decomposes into four independent forces with very different fix costs.

**(a) Global bookkeeping — real, but the *cheap* one, fixed expensively here.** Counters
don't need a handback; they need to stop being global. Per-thread stats summed at `INFO`
time is the standard answer and has in-tree precedent (`used_active_time_io_thread[]`,
`src/io_threads.c`). `deferredStats` instead accumulates per client and replays *in arrival
order* on the main thread — strictly more work than the inline version it replaced, buying
an ordering of stat updates that no client can observe.

**(b) Propagation — not actually a force.** For a read-only command this is a no-op that
should not run. `callEpilogue` runs it anyway, which is the entire mechanism of finding 1.

**(c) Connection and event-loop ownership — the one that actually forces it.**
`server.clients_pending_write` is a single global list because there is a single
`aeEventLoop`. An I/O thread can fill `c->buf` (it does), but it cannot queue the client for
writing, so the client must come back. Same for `commandProcessed`, `resetClient`,
`updateClientMemUsageAndBucket`, and resuming the pipeline. Remove (a) and (b) entirely and
every command still round-trips, because that is where the fd lives.

**(d) The main thread's own background work — why it can't even be fire-and-forget.** Cron,
eviction, defrag and incremental rehashing touch arbitrary slots. That is what "the main
thread does nothing between dispatch and join" means, and why the spin at
`src/io_threads.c:1087` is load-bearing rather than lazy. The main thread doesn't merely
*receive* the handback — it **idles** until the last dispatched command lands.

The branch attacks (a) by the costliest available method, ignores (b) and gets a bug from
it, and *works around* (c) and (d) with a barrier.

## 4. Why it cannot use multiple cores — from the constants

`prefetch-batch-max-size` defaults to **16**, hard maximum 128 (`src/config.c:3407`). That
bounds a run.

So one run is: ≤16 clients split by `slot % active_io_threads_num`. With 8 I/O threads that
is ~2 commands per thread, and the ~1/n whose slots map to tid 0 never leave the main
thread. Then a full barrier — the main thread hard-spins (`src/io_threads.c:1087`) until the
last one lands. Then the completion loop runs **serially** on the main thread: epilogue,
`applyDeferredStats`, `commandProcessed`, `updateClientMemUsageAndBucket`, write-queueing,
`processInputBuffer`, per client.

Two consequences, neither of which needs a profiler:

**The phases never overlap.** Main spins → I/O threads execute → I/O threads idle → main
completes serially. At every instant exactly one group is doing useful work. That is not
using multiple cores; it is passing a baton between them and paying fork/join fences at each
handoff. For ~2 `GET` procs per thread, the barrier costs more than the work it
parallelizes.

**Raising the batch size does not change the ratio.** The parallel section is O(run size)
and so is the serial completion loop; both grow together. The asymptote is roughly
`proc / (proc + epilogue + completion)` regardless of batch size — Amdahl *inside each run*,
with the serial fraction structurally fixed. No configuration escapes it.

It degrades further under realistic traffic: any non-qualifying command in the batch
triggers `ioThreadRunJoin()` mid-loop (`src/memory_prefetch.c:261`), fragmenting runs. A run
of size 1 dispatches one command to one thread and spins for it — pure loss.

A design whose stated purpose is core utilization, and whose main thread's steady state is
`while (completed < dispatched);`, has answered the question about itself.

## 5. Comparison

Against slot-per-thread **Phase 4** specifically
([proposal-slot-per-thread-phase4.md](proposal-slot-per-thread-phase4.md)) — same goal, same
slot partition, same read-only scope.

| | fork/join branch | Phase 4 |
|---|---|---|
| Isolation boundary | shares the real `client`; enumerate-and-defer every global it might touch | argv in, RESP bytes out, against a socket-less executor |
| Auditable surface | open-ended | `commandNeedsBarrier()` + argv lifetime + disconnect guard |
| Parallelism window | ≤16 commands, then a barrier | continuous |
| Main thread during parallel work | hard spin | serves its own clients |
| Main-thread work per command | prologue + epilogue + `commandProcessed` + mem-bucket + write-queue | zero on the LOCAL path |
| Slot exclusivity | violated by the full-queue fallback (finding 4) | exclusive by construction |
| Expired key on read | reported missing, not deleted — a real semantic divergence | owner owns the dict, deletes normally |
| Keyspace-miss notifications | feature silently disabled when `Km` is configured | emitted by the owner |
| Tail latency | bounded by the slowest command in each run — one big `LRANGE` stalls the whole server | one slow command occupies one shard |
| Cost when it loses | none (falls back) | REMOTE hop tax; parent §11 admits it is slower at low core count |
| Implementation cost | one ~900-line PR | `conn->el`, per-shard `aeEventLoop`, `SO_REUSEPORT`, eventfd wake, eventually connection migration |

**Where the proposal is weaker than the branch, and should be fixed.** Phase 4 §3.3 says
`call(x, CMD_CALL_FULL & ~CMD_CALL_PROPAGATE)` and treats that as sufficient. It is not:
`call()` also bumps `server.stat_numcommands` (`src/server.c:4112`), `cmd->calls` and
`cmd->microseconds` (`:4043-4044`), the commandlog, the latency monitor, and keyspace
hit/miss — the same globals the branch had to solve. The branch's prologue/invoke/epilogue
split and `deferredStats` are the most reusable artifacts on it, and Phase 4 needs an answer
here. [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md) §6 is that
answer, as a full inventory rather than a partial one.

## 6. Verdict

**Phase 4's model is better.** The fork/join branch is, structurally, slot-per-thread with
the wrong isolation boundary — it shares an object graph where the proposal ships bytes —
and pays for it with an unbounded audit surface and a barrier that caps the win below
anything worth the concurrency risk.

But the two are not the same size of bet, and "do Phase 4 instead" is not actionable this
quarter. Recommendation:

1. **Land `f9dad117b` and `7e80180bd` independently.** Thread-local `current_client` and the
   `call()` split are prerequisites for *any* off-main-thread execution and are defensible
   refactors on their own merits. Note that the design in
   [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md) does **not** need
   the `call()` split — that a command's two halves must be separated is itself a symptom of
   the fork/join boundary — but it costs nothing to have.
2. **Do not ship `6902ffec5` as-is.** Not primarily because of the five bugs, which are
   fixable, but because a disappointing benchmark from it would read as "threading reads
   doesn't pay" when the real conclusion is "this boundary doesn't pay."
3. **Rebuild it on the boundary in
   [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md).** Persistent slot
   ownership instead of per-run dispatch, executor clients instead of the shared client,
   `BLOCKED_IO_EXEC` instead of a barrier, per-owner counters instead of deferred replay,
   barriers at cron rate (10/s) instead of batch rate (thousands/s). Every piece of it is a
   prerequisite of Phase 4 — the same road, one exit earlier.
4. **Scope the claim to large-value reads.** The gain is proportional to proc cost. For a
   20-byte `GET` the proc is a small fraction of per-command work; for `GETRANGE` or a large
   `HGET` it dominates. Benchmark there first; if it cannot be shown there it will not appear
   elsewhere. The branch's own `io_threaded_cmds_executed` / `io_threaded_cmd_runs` counters
   give average run size directly, which is the fastest way to see §4 empirically.

## 7. See also

- [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md) — the alternative
  design this review argues for.
- [proposal-slot-per-thread.md](proposal-slot-per-thread.md) — §5b (event-loop
  de-globalization) is the load-bearing piece both designs are measured against.
- [proposal-slot-per-thread-phase4.md](proposal-slot-per-thread-phase4.md) — the
  `BLOCKED_SHARD` continuation, restricted in the alternative design.
- [09-threading-and-io-model.md](09-threading-and-io-model.md) — how the I/O thread pool
  works today.
