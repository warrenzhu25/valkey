# Proposal — Read-only command execution on I/O threads

**Status: pre-issue draft.** A design for executing read-only commands on threads other than
the main thread, in cluster mode, without per-thread event loops.

This document has two halves. §1 is a review of the **fork/join attempt** that already
exists as the `issue-2022` branch in this repo — what it does, why it is structurally capped,
and what its bugs say about where the thread boundary belongs. §2 onward is the design that
review argues for.

It is narrower than [proposal-slot-per-thread.md](proposal-slot-per-thread.md) and stricter
than the branch. It exists because there is a real gap between "offload the command proc
inside a batch" (cheap, structurally capped) and "give every thread its own `aeEventLoop`"
(uncapped, multi-quarter). This design occupies that gap, and every piece of it is reused
verbatim by the full slot-per-thread design later. Nothing here is throwaway.

All `file:line` anchors verified against this checkout.

---

# Part I — Why the fork/join approach is capped

## 1.1 What the branch does

The `issue-2022` branch is three commits on top of `ca4927ec4`:

| Commit | |
|---|---|
| `f9dad117b` | Make `current_client` and `executing_client` thread-local (16 files) |
| `7e80180bd` | Split `call()` into prologue / invoke / epilogue |
| `6902ffec5` | Execute read-only commands on I/O threads (+ tests) |

`processClientsCommandsBatch()` (`src/memory_prefetch.c`) collects a group of clients whose
next command is read-only into a **run**, dispatches them across I/O threads by
`slot % active_io_threads_num`, executes its own share, spins until every dispatched command
finishes, then completes the clients serially on the main thread in arrival order.

Two claimed safety properties:

1. work is assigned by slot, so no two threads touch the same hashtable;
2. the main thread does nothing between dispatch and join, so cron, eviction, defrag and
   incremental rehashing cannot overlap the run.

Property 1 is genuinely required, and is required by *any* design in this space: a lookup
mutates the table. `hashtableFind()` (`src/hashtable.c:1606`) → `findBucket()` →
`rehashStepOnReadIfNeeded()` (`src/hashtable.c:914`), and `src/hashtable.c:1712` documents
that "hashtableFind() may cause incremental rehashing to move entries in memory." Two
threads reading one slot is corruption, not a benign race. Any path that lets a slot run on
two threads "just this once" — a full queue, a fallback — is wrong.

Property 2 is not a shortcut. It is load-bearing, and it is also the ceiling.

## 1.2 The phases never overlap

`prefetch-batch-max-size` defaults to **16**, hard maximum 128 (`src/config.c:3407`). That
constant bounds a run.

So one run is ≤16 clients split by `slot % active_io_threads_num`. With 8 I/O threads that
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

## 1.3 What actually forces the handback

Even with every bug fixed, a command must return to the main thread to finish. That
decomposes into four independent forces with very different fix costs, and it is worth
separating them because three of the four are cheaper than they look.

**(a) Global bookkeeping — real, but the cheap one, fixed expensively.** Counters don't need
a handback; they need to stop being global. Per-thread stats summed at `INFO` time is the
standard answer and has in-tree precedent (`used_active_time_io_thread[]`,
`src/io_threads.c`). The branch's `deferredStats` instead accumulates per client and replays
*in arrival order* on the main thread — strictly more work than the inline version it
replaced, buying an ordering of stat updates that no client can observe.

**(b) Propagation — not actually a force.** For a read-only command this is a no-op that
should not run at all. `callEpilogue` runs it anyway, which is the entire mechanism of
finding 1 below.

**(c) Connection and event-loop ownership — the one that genuinely forces it.**
`server.clients_pending_write` is a single global list because there is a single
`aeEventLoop`. An I/O thread can fill `c->buf` — it does — but it cannot queue the client for
writing, so the client must come back. Same for `commandProcessed`, `resetClient`,
`updateClientMemUsageAndBucket`, and resuming the pipeline. Remove (a) and (b) entirely and
every command still round-trips, because that is where the fd lives.

**(d) The main thread's own background work — why it can't even be fire-and-forget.** Cron,
eviction, defrag and incremental rehashing touch arbitrary slots. That is what "the main
thread does nothing between dispatch and join" means, and why the spin is load-bearing
rather than lazy. The main thread doesn't merely *receive* the handback — it **idles** until
the last dispatched command lands.

The branch attacks (a) by the costliest available method, ignores (b) and gets a bug from
it, and *works around* (c) and (d) with a barrier. Part II attacks (a) with sharded
counters, deletes (b), accepts (c) as the design's stated ceiling, and dismantles (d) — which
is the move that lets owner threads run continuously.

## 1.4 The boundary mistake

Five correctness findings from reviewing `6902ffec5`. They are listed for the *pattern*, not
the individual bugs — all are fixable, and if the branch is repaired or dropped this table
goes stale while §1.2 and §1.3 do not.

| # | Finding | Where |
|---|---|---|
| 1 | Read-only commands get propagated to AOF and replicas. The completion loop runs `callEpilogue()` and then resumes that client's pipeline before completing the *next* client, so a neighbour's pipelined `SET` inflates `server.dirty` and the next `GET`'s epilogue sees a non-zero delta → `alsoPropagate(...)` | `src/io_threads.c:1013`, `src/server.c:4028`, `:4111` |
| 2 | A nested run (via `processEventsWhileBlocked`) reuses and may `zrealloc` the global `io_run` array while the outer completion loop is still indexing it → double-completion or use-after-free | `src/io_threads.c:1095-1101` |
| 3 | `serverAssert(commandCanRunOnIOThread(c, c->cmd))` can fire: `moduleCallCommandFilters()` (`src/server.c:4390`) re-runs `prepareCommand()` (`src/module.c:11597-11600`) *after* the eligibility pre-check, so a filter that rewrites `GET` → a write command panics the server | `src/server.c:4775` |
| 4 | When an owner's queue is full the command falls back to the main thread — but if a same-slot command was already dispatched, two threads are now in one hashtable, breaking property 1 | `src/io_threads.c:1068` |
| 5 | `addReply` → output-buffer limit → `freeClientAsync()` mutates the global `server.clients_to_close` list from an I/O thread, unsynchronized | `src/networking.c:701` → `:6281` → `:2287` |

**The pattern.** Findings 1, 3 and 5 are the same mistake: the design shares the *real client
object* across threads and then tries to enumerate everything a command proc might touch and
defer it. That list is open-ended. `deferredStats` covers keyspace hit/miss and error stats;
it does not cover the output-buffer-limit close path, and there are more where that came
from — cluster slot stats, commandlog large-reply tracking, `robj` LRU/LFU writes. Every
future change to `addReply()` is a chance to silently break it.

This is an argument about the **boundary**, not about care. A boundary of "argv in, RESP
bytes out" against a socket-less executor client makes the auditable surface two pointers. A
boundary of "here is the real client, please avoid touching anything global" makes it the
whole server. §2.2 picks the former.

## 1.5 What to keep

- **Keep `f9dad117b` and `7e80180bd`.** Thread-local `current_client` and the `call()` split
  are prerequisites for *any* off-main-thread execution and are defensible refactors on
  their own merits. (The design below does not strictly need the `call()` split — that a
  command's two halves must be separated is itself a symptom of the fork/join boundary — but
  it costs nothing to have.)
- **Do not ship `6902ffec5` as-is.** Not primarily because of the five findings, which are
  fixable, but because a disappointing benchmark from it would read as "threading reads
  doesn't pay" when the real conclusion is "this boundary doesn't pay."
- **Its `io_threaded_cmds_executed` / `io_threaded_cmd_runs` counters give average run size
  directly**, which is the fastest way to see §1.2 empirically before touching anything.

---

# Part II — The design

## 2.1 The claim

**Owner threads must run continuously, not in bursts.** The motivation for moving work off
the main thread is to use more than one core, and §1.2 is what happens when a design
dispatches, idles at a barrier, then completes serially. So the central constraint is:

> Between two consecutive barriers, an owner thread executes commands back-to-back without
> ever synchronizing with the main thread, and the main thread keeps parsing and serving
> other clients while it does.

Everything below follows from taking that seriously. The two things that otherwise force a
barrier are §1.3's (d) — background work touching arbitrary slots — and (a) — global mutable
bookkeeping. §2.6 and §2.5 dispose of both.

## 2.2 Scope

**In scope.** Single-slot, read-only commands, in cluster mode, on plain connected clients,
executed on an owner thread. Everything else runs on the main thread exactly as today.

**Out of scope.** Writes and the replication journal (slot-per-thread Phase 5); per-thread
event loops and connection migration (parent §5b, Phase 4a); standalone mode; modules;
scripts.

**Explicitly a non-goal:** making the main thread stop being a serialization point. It still
parses every command and still owns every socket — §1.3(c) is accepted, not solved. This
design raises the ceiling from *"how many commands can one thread parse and execute"* to
*"how many commands can one thread parse and relay."* A real but bounded win. Anyone who
wants more is describing per-thread event loops. §2.10 does not hide this.

## 2.3 Ownership model

```text
        slot 0 .......................... 16383      (kvstore: one hashtable per slot)
        +-----+-----+-----+-----+ ... +-----+
        |  0  |  1  |  2  |  3  |     |16383|
        +--+--+--+--+--+--+--+--+     +--+--+
           |     |     |     |           |
        slot_to_owner[16384]  (uint8_t, read-mostly, changed only under barrier)
           |     |     |     |           |
    +------v-+ +-v-----v-+ +-v-----------v--+
    | main   | | owner 1 | |    owner N-1   |
    | (id 0) | | thread  | |    thread      |
    +--------+ +---------+ +----------------+
```

- `slot_to_owner[16384]` is a plain `uint8_t[]` (16 KB). Owner `0` is the main thread.
- Ownership is **persistent**, not per-run. A slot has the same owner from one barrier to the
  next. This is the single most important difference from the branch: because ownership is
  stable, an owner can execute a queue of commands back-to-back with no handshake per
  command.
- Ownership is exclusive for **all** access, including background work (§2.6).
- Assigned contiguously at startup, rebalanceable under a barrier: moving a slot is moving an
  index, not data.

## 2.4 The isolation boundary — data, not object graphs

**An owner thread never touches the requesting client.** It executes against a per-owner
**executor client**: a long-lived socket-less `client` created once per owner thread, the
same pattern the AOF loader uses (`createAOFClient`, `src/aof.c:1490`, used at `:1560`).

```text
  MAIN THREAD (owns sockets)                 OWNER THREAD (owns the slot's data)
  ──────────────────────────                 ──────────────────────────────────
  parse; slot known                          [running continuously]
  owner = slot_to_owner[slot] != 0
  blockClient(c, BLOCKED_IO_EXEC)   ─ job{argv, id, resp, dbid, slot} ─┐
  return to event loop, serve others                                   ▼
        ...                                    x = self->executor
        ...                                    x->argv = job->argv (borrowed, read-only)
        ...                                    x->resp = job->resp
        ...                                    call(x, no propagation)
        ...                                    bytes = detachReply(x)   /* flat RESP sds */
        ...                                    side  = collected effects (§2.5)
  beforeSleep: drain results        ◀─ result{id, bytes, side, duration} ─┘
    validate client still live (§2.8)
    addReplyProto(c, bytes)
    apply side effects (§2.5)
    unblockClient(c, 1)  ── resume, parse next pipelined command
```

What crosses the boundary is **argv in, RESP bytes out**. No shared `client`, no shared reply
buffer, no shared list. The set of things an owner can corrupt is the set of things it is
handed, and that is two pointers.

### 2.4.1 Detaching the reply — the one real subtlety

Valkey reply buffers are not always plain bytes. `_addReplyPayloadToList`
(`src/networking.c:660-702`) may write **encoded** chunks: a `payloadHeader`
(`src/networking.c:116`) followed by either a plain reply or a `BULK_STR_REF`
(`src/networking.c:109`) — a *pointer to an `robj`* rather than a copy of its bytes.

Handing a `BULK_STR_REF` across threads is unsafe: the referenced `robj` lives in the owner's
slot and `robj` refcounts are not atomic. The main thread would be reading, and eventually
`decrRefCount`-ing, an object another thread may free.

**Decision: force plain, self-contained replies on the executor.** Disable encoding and
deferred replies on executor clients (`isDeferredReplyEnabled`, `src/networking.c:261`,
already special-cases `c->flag.fake`), so `detachReply()` is a concatenation of
`x->buf[0..bufpos]` plus each `x->reply` node into one `sds`, with no pointers inside.

**Honest cost:** this reintroduces a copy for large bulk values — precisely the workload where
offloading pays off most (§2.10). Two escapes exist if measurement demands them, both
deferred: (a) transfer `robj` ownership explicitly, with the main thread posting a
`decrRefCount` job back to the owner; (b) make `BULK_STR_REF` refcounts atomic. Neither
belongs in v1.

The executor's RESP version must be pinned per job (`x->resp = c->resp`), or a RESP3 client
gets RESP2 map and double encodings.

## 2.5 Dispatch, suspend, resume

### 2.5.1 Eligibility — a positive allowlist

A new command flag `CMD_IO_SAFE`, **default off**, opted into per command in
`src/commands/*.json`. v1 set: trivially safe single-key reads — `GET`, `STRLEN`, `EXISTS`,
`TYPE`, `TTL`, `HGET`, `HLEN`, `LLEN`, `LINDEX`, `SCARD`, `SISMEMBER`, `ZSCORE`, `ZCARD`,
`GETRANGE`.

This is deliberately the opposite of the branch's negative filter (*"readonly and not module
and not blocking and not…"*). A negative filter makes every newly added command eligible by
default and silently absorbs any command whose flags don't fully describe it. A positive
allowlist makes the safe set explicit and reviewable, and makes the default for anything new
"runs on the main thread as today."

A command dispatches to an owner only if **all** hold:

| Condition | Why |
|---|---|
| `cmd->flags & CMD_IO_SAFE` | the allowlist above |
| `server.cluster_enabled` | dict-per-slot only exists in cluster mode |
| `server.io_owner_threads > 1` | otherwise there is nowhere to send it |
| `c->slot >= 0` | exactly one slot; keyless and cross-slot are ineligible |
| `slot_to_owner[c->slot] != 0` | main-owned slots run inline, zero cost |
| `c->conn != NULL`, not fake/module/script/primary/replica | executor has no socket path for these |
| `!c->flag.multi`, `!c->flag.blocked`, `!c->flag.monitor` | reply path or state lives on main |
| `!c->flag.tracking` | v1 only; see §2.5 table |
| `!c->slot_migration_job` | slot ownership is in flux |
| `!server.loading` | keyspace is being mutated wholesale |
| not over `maxmemory` | eviction needs a global view; §2.6.3 |

**The check must be re-evaluated, not asserted.** It is computed at parse time from
`c->parsed_cmd` and `c->slot`, but `moduleCallCommandFilters()` (`src/server.c:4390`) can
rewrite argv and re-run `prepareCommand()` (`src/module.c:11597-11600`) afterwards, changing
both. The dispatch point must re-check and *fall back to inline execution*, never assert —
this is finding 3 above, and it is a live crash on the branch.

### 2.5.2 Suspend — reuse the blocking framework

A cross-thread hop is not new control flow; it is a new **blocking type**. Valkey already
supports "start a command, discover it cannot complete synchronously, suspend without
resetting the client or propagating, resume later." That is `BLPOP`.

Add `BLOCKED_IO_EXEC` to `blocking_type` before `BLOCKED_NUM` (`src/server.h:349`).
`server.blocked_clients_by_type[]` (`src/server.h:2214`) is sized by it, so `INFO` counts it
for free.

```c
int ioExecDispatch(client *c, int owner) {
    ioExecJob *job = zmalloc(sizeof(*job));
    job->client_id = c->id;        /* not the pointer — §2.8 */
    job->argv = c->argv;           /* borrowed; safe because c stays blocked */
    job->argc = c->argc;
    job->slot = c->slot;
    job->dbid = c->db->id;
    job->resp = c->resp;

    blockClient(c, BLOCKED_IO_EXEC);   /* src/blocked.c:106 */

    if (!spscEnqueue(&owners[owner].inbox, tagJob(job, IO_REQ_EXEC), true))
        return ioExecOverflow(c, owner, job);   /* §2.8.3 — never run it here */
    return C_OK;
}
```

Three properties come free from `blockClient`:

- **No premature reset or propagation.** `commandProcessed()` returns early at
  `src/networking.c:3887` when `c->flag.blocked` is set — argv and argc are preserved, the
  client is not reset, the replication offset is not advanced. Exactly what a suspended
  command needs, and the reason `job->argv` may borrow rather than copy.
- **Per-client ordering, at no cost.** While blocked, the query buffer accumulates but is not
  parsed into new commands. Command *k+1* cannot be dispatched before *k* resumes — the same
  mechanism that stops `BLPOP` then `GET` from reordering on one connection.
- **A resume path already exists.** `unblockClient(c, 1)` (`src/blocked.c:217`) queues the
  client on `server.unblocked_clients`; `processUnblockedClients()` (`src/blocked.c:158`)
  drains it in `beforeSleep`.

### 2.5.3 Resume — finalize, do not re-execute

The one place the `BLPOP` analogy needs care. `BLPOP` re-runs its command on wake; an
I/O-executed read must **not** — the reply is already produced.

The `BLOCKED_IO_EXEC` arm of `unblockClient` clears `c->flag.pending_command` before
queueing, so the re-drive through `processPendingCommandAndInputBuffer`
(`src/networking.c:3962`) skips re-dispatch and proceeds to parse the **next** pipelined
command from the held query buffer. This mirrors how `BLOCKED_WAIT` finalizes without redo.

Order of operations on the main thread per completed client:

1. validate the client is still live (§2.8.1);
2. `addReplyProto(c, bytes, len)` — the normal path, so output-buffer limits, encoding and
   `clients_pending_write` all behave exactly as for an inline command;
3. apply the side effects the owner collected, in completion order;
4. `c->duration += result->duration`;
5. `unblockClient(c, 1)`.

## 2.5.4 Global state — the complete inventory

This is the section the design lives or dies on, and §1.4 is why. Any item missed here is a
silent data race. The rule throughout: **shard the counter, or hand back the effect — never
mutate a global from an owner.**

| Global | Today | Under this design |
|---|---|---|
| `server.stat_numcommands` (`src/server.c:4112`) | main thread `++` | per-owner counter; summed in `INFO` |
| `cmd->calls`, `cmd->microseconds` (`src/server.c:4043-4044`) | shared command table | per-owner `commandStats[]` indexed by command id (~240 × 32 B ≈ 8 KB/owner); summed in `INFO commandstats` |
| `cmd->failed_calls`, `rejected_calls` | shared | same |
| `server.stat_keyspace_hits` / `_misses` | main thread `++` | per-owner; summed |
| `server.stat_total_error_replies` + errors rax | main thread | per-owner counter + per-owner rax; merged in `INFO errorstats` |
| commandlog (slow / large-reply) | main thread ring | per-owner ring; merged at `COMMANDLOG GET` |
| latency monitor | main thread | per-owner samples; merged at `LATENCY` |
| `server.dirty` | main thread | reads must not change it — assert unchanged on the executor |
| propagation (`alsoPropagate`) | main thread | **not run at all**: call the executor without `CMD_CALL_PROPAGATE`. A read-only command has nothing to propagate, and computing a `server.dirty` delta across a fork/join is exactly how the branch leaks `GET` into the AOF (finding 1) |
| `server.clients_pending_write` | main thread list | untouched by owners; the main thread queues the client in step 2 of §2.5.3 via the ordinary `addReply` path |
| `server.clients_to_close` (output-buffer limit) | main thread list | unreachable from an owner: the executor is `flag.fake` and `closeClientOnOutputBufferLimitReached` returns 0 for fake clients at `src/networking.c:6266`. The limit is applied when the main thread appends. Structurally fixes finding 5 |
| keyspace-miss notification | published inline | owner records `(key, event)` in the side-effect list; main thread publishes at completion. Removes any need to disable the feature when `notify-keyspace-events` includes `Km` |
| lazy expire (`DEL` + propagate) | main thread | owner **may** delete — it owns the dict — and hands the `DEL` back for the main thread to propagate |
| client-side-caching tracking table | main thread | v1: tracking clients ineligible. v2: owner returns the key list, main thread calls `trackingRememberKeys` at completion |
| LRU / LFU counters on `robj` | inline | owner-only, exclusive slot access — safe by construction |
| cluster slot stats (per-slot array) | inline | owner-only, disjoint by slot — safe by construction |
| `server.cmd_time_snapshot` | one per execution unit | per-owner snapshot, refreshed from `server.mstime` at the top of each executed command |

Two are worth dwelling on.

**Per-owner counters instead of deferred replay.** The alternative — accumulate per command
and replay in arrival order — preserves an interleaving of stat updates no client can
observe, and in exchange makes every command round-trip through the main thread for
bookkeeping alone. Sharded counters summed at `INFO` time are cheaper and have in-tree
precedent. `INFO` becomes O(owners × commands) instead of O(1), irrelevant at `INFO`
frequencies.

**Lazy expire becomes correct again.** Because the owner has exclusive access, it can
actually delete an expired key rather than reporting it missing and leaving it — which is
what the branch must do, and a real semantic divergence. Only the `DEL`'s *propagation* needs
the main thread, and that rides the side-effect list.

## 2.6 What forces a barrier, and how often

A **barrier** quiesces every owner: broadcast ENTER, each owner finishes its current command
and parks, main thread proceeds with a global view, broadcast LEAVE. The design is viable
only if barriers are rare. Three sources:

### 2.6.1 Slot-iterating background work → **skip, don't barrier**

Active expiry (`activeExpireCycle`) and incremental rehashing (`kvstoreIncrementallyRehash`
in `databasesCron`) walk slots. Both already iterate per-slot, so both take an ownership
check: the main thread processes only slots where `slot_to_owner[slot] == 0`, and each owner
runs the same cycle over its own slots between commands, in its own loop.

No barrier, and it parallelizes expiry as a side effect. **This is why per-shard expiry is a
prerequisite here rather than the "Phase 6" it is in the parent proposal** — without it, the
main thread's fast expire cycle in `beforeSleep` would need a barrier on every event loop
iteration, which is §1.2 with extra steps.

### 2.6.2 Genuinely global work → **barrier, at bounded frequency**

Defrag, `fork()` for RDB/AOF, `FLUSHALL`, `SWAPDB`, `DEBUG`, `CLUSTER SETSLOT`, slot
migration, ownership rebalance, and `SCAN`/`KEYS`/`DBSIZE` if not made owner-aware.

Frequency: cron runs at `server.hz` (default 10/s), saves are rare, `FLUSHALL` is rare. Ten
barriers per second, each costing one command's latency per owner, is nothing. Contrast with
a per-batch fork/join at thousands per second — **that is the entire architectural difference
between Part I and Part II.**

`SCAN` deserves better than a barrier and can have it: the `kvstore` cursor already encodes
the table index, so a `SCAN` that fans out to owners and merges is close to free.

### 2.6.3 Eviction → **disable offload under memory pressure**

`performEvictions` (`src/evict.c`) needs global sampling against a global `maxmemory` and runs
on the *hot path* whenever the limit is approached. Barriering there would be constant
exactly when the server is most loaded.

v1 answer: if `maxmemory` is set and used memory is within a margin of it, stop dispatching
(§2.5.1) and let in-flight work drain. The server degrades to today's behavior under memory
pressure, which is a defensible place to degrade to. Per-owner eviction with slack is a later
step needing its own design.

## 2.7 Why the owner threads stay busy

With §2.6 in place, an owner's loop is:

```c
for (;;) {
    drain inbox: execute each job back-to-back, post results     /* no sync with main */
    run this owner's share of active expire / rehash              /* §2.6.1 */
    if (barrier requested) { park; wait for LEAVE; }              /* §2.6.2 — rare */
    else if (inbox empty) { brief spin, then futex/condvar wait; }
}
```

There is no point in that loop where an owner waits for the main thread to reach a particular
place. That is the whole design. Result delivery is fire-and-forget onto an SPSC completion
queue plus a coalesced wake (only when the queue transitions empty → non-empty).

The main thread, symmetrically, never waits for an owner. It dispatches and returns to the
event loop. Its per-command cost for a dispatched command is parse, eligibility check,
`blockClient`, enqueue — then later — validate, `addReplyProto`, side effects,
`unblockClient`, `resetClient`. Strictly less than executing the command, and it overlaps
with owners executing.

## 2.8 Lifetimes, failure, backpressure

### 2.8.1 Client disconnect mid-flight

The client may be freed (connection reset, `CLIENT KILL`, timeout) while a job is in flight.
The job carries `client_id` (a monotonic `uint64`), not just a pointer; on completion the main
thread looks the client up by id and drops the result if it is gone or recycled. Freeing a
`BLOCKED_IO_EXEC` client must go through normal blocked-client teardown so no block leaks —
`unblockClient` is already called from the client-free path for other block types.

The borrowed `argv` is the hazard: freeing the client frees argv while an owner may be reading
it. Rule: **a `BLOCKED_IO_EXEC` client is not freed synchronously.** Mark it `close_asap` and
let `freeClientsInAsyncFreeQueue` collect it after the in-flight result has been drained or
dropped. A per-client in-flight counter makes this a one-line check.

### 2.8.2 Errors and panics on an owner

Errors are ordinary RESP bytes and need no handling beyond per-owner error stats (§2.5.4).
`serverPanic`/`serverAssert` on an owner is a process abort as anywhere else, but the crash
log must record which owner and which command, so `logStackTrace` needs to know it may run
off the main thread.

### 2.8.3 Owner inbox full

**The one thing the main thread must not do is execute the command itself** — that is finding
4. If a same-slot command was already dispatched, the main thread and the owner would be in
one hashtable concurrently.

Correct options in order of preference: (a) park the job on a per-owner overflow list drained
next `beforeSleep`, leaving the client blocked; (b) unblock with a transient `-TRYAGAIN`.
Start with (a). Size the SPSC inbox generously (the existing `IO_SPSC_QUEUE_SIZE` is 4096,
`src/io_threads.c:13`) so this is a genuine edge case.

### 2.8.4 A slow command blocks its owner's queue

An owner executes serially, so one `GETRANGE` over a 512 MB string delays every command for
slots that owner holds. This is head-of-line blocking the main thread does not have today
(where a slow command delays everyone, but there is only one queue). A genuine tail-latency
change, must be measured, and the main argument for keeping the v1 allowlist to commands with
bounded output.

## 2.9 Configuration, observability, files

- `io-owner-threads N` — default **1**, meaning no slot is owned by anyone but the main thread
  and the feature is inert. Must be a provable runtime no-op at the default. Reuse the
  io-threads worker pool rather than spawning a second set of threads.
- `INFO stats` gains `io_exec_commands`, `io_exec_overflow` (§2.8.3), `io_exec_dropped`
  (§2.8.1), `io_exec_barriers` and `io_exec_barrier_usec` (§2.6 — the number to watch; if
  barriers are frequent the design is not working).
- `INFO clients` shows `BLOCKED_IO_EXEC` in `blocked_clients_by_type` for free.
- `DEBUG SLOT-OWNER <slot>` for tests.

| File | Change |
|---|---|
| `src/io_exec.{c,h}` | **New.** `slot_to_owner[]`, owner loop, executor clients, dispatch, completion drain, detach/reattach, barrier. |
| `src/server.h` | `BLOCKED_IO_EXEC` before `BLOCKED_NUM` (`:349`); `CMD_IO_SAFE`; per-owner stats struct. |
| `src/server.c` | `processCommand` tail: eligibility re-check (§2.5.1) → `ioExecDispatch` or `call()`. `beforeSleep`: drain completions. `serverCron`/`databasesCron`: ownership skip (§2.6.1) + barrier points (§2.6.2). |
| `src/blocked.c` | `BLOCKED_IO_EXEC` arm of `unblockClient` (`:217`) — finalize without re-execution; teardown discards in-flight results. |
| `src/networking.c` | Executor client creation; `detachReply()`; force plain replies on executors (§2.4.1). `commandProcessed` (`:3879`) and `processPendingCommandAndInputBuffer` (`:3962`) need **no change** — they already do the right thing for blocked clients. |
| `src/expire.c`, `src/kvstore.c` | Ownership-aware slot iteration (§2.6.1). |
| `src/evict.c` | Memory-pressure gate (§2.6.3). |
| `src/commands/*.json` | `IO_SAFE` on the v1 allowlist; regenerate `commands.def`. |
| `src/unit/test_io_exec.cpp` | **New.** Eligibility matrix, detach/reattach round-trip. |
| `tests/unit/cluster/io-exec.tcl` | **New.** End-to-end. |

Notably absent: `call()` does not need splitting into prologue/invoke/epilogue. The whole
command runs on one thread start to finish. That the fork/join design needs such a split is
itself a symptom of its boundary being in the wrong place.

## 2.10 Comparison and honest limits

Against slot-per-thread **Phase 4**
([proposal-slot-per-thread.md §19](proposal-slot-per-thread.md#19-phase-4-part-2-dispatch-and-the-remote-continuation))
and against Part I:

| | fork/join branch | this design | Phase 4 |
|---|---|---|---|
| Isolation boundary | shares the real `client` | argv in, RESP bytes out | argv in, RESP bytes out |
| Parallelism window | ≤16 commands, then a barrier | continuous | continuous |
| Main thread during parallel work | hard spin | serves its own clients | serves its own clients |
| Barrier frequency | per batch (thousands/s) | per cron tick (~10/s) | per escalated command |
| Main-thread work per command | prologue + epilogue + `commandProcessed` + mem-bucket + write-queue | parse + relay | zero on the LOCAL path |
| Slot exclusivity | violated by the full-queue fallback | exclusive | exclusive |
| Expired key on read | reported missing, not deleted | deleted normally | deleted normally |
| Implementation cost | one ~900-line PR | new module + per-owner cron + allowlist | + `conn->el`, per-shard `aeEventLoop`, `SO_REUSEPORT`, eventfd wake, migration |

**Where the Phase 4 proposal is weaker than the branch, and should be fixed.** Phase 4 §3.3
says `call(x, CMD_CALL_FULL & ~CMD_CALL_PROPAGATE)` and treats that as sufficient. It is not:
`call()` also bumps `server.stat_numcommands`, `cmd->calls`/`microseconds`, the commandlog,
the latency monitor and keyspace hit/miss — the same globals the branch had to solve. §2.5.4
is the answer, as a full inventory rather than a partial one.

**Limits.**

- **The main thread still parses and relays everything.** Expect a low single-digit multiple,
  not linear scaling.
- **The gain is proportional to proc cost.** For a `GET` of a 20-byte value the proc is a
  small fraction of per-command work and dispatch + completion overhead may exceed it. For
  `GETRANGE`, large `HGET`, big `LINDEX`, the proc dominates. **This is a large-value read
  accelerator**, and the v1 allowlist and benchmark plan should both be built around that.
- **The forced reply copy (§2.4.1) cuts against exactly that workload.** The one measurement
  that must happen before writing much code: for a 4 KB bulk reply, does executing the proc on
  another thread beat the extra `memcpy` on the main thread? If not, the design needs `robj`
  ownership transfer before it is worth building.
- **Head-of-line blocking per owner** (§2.8.4) is a new tail-latency risk.
- **Hot slots** cap throughput at one owner. Rebalancing helps a hot *slot*; nothing here
  helps a hot *key*.
- **Cluster mode only.** Standalone has one hashtable per db; virtual slots would be needed
  first (parent §4 / Phase 3).

## 2.11 What would make me abandon this

- Barrier frequency (`io_exec_barriers`) cannot be held to cron rate, meaning §2.6.1's
  skip-don't-barrier approach does not cover the real background work.
- The §2.10 large-value measurement shows the reply copy eats the win, *and* `robj` ownership
  transfer turns out to need atomic refcounts throughout.
- The per-owner counter merge (§2.5.4) proves not to be equivalent for `INFO commandstats` /
  `errorstats` in a way users would notice.

If instead it works, none of it is wasted: `slot_to_owner[]`, the executor client, the RESP
detach/reattach, `BLOCKED_IO_EXEC`, the barrier, and per-owner background work are all
prerequisites of the full slot-per-thread design. This is Phase 4 minus `conn->el` — the same
road, one exit earlier.

## 3. Code anchors

| Thing | Where |
|---|---|
| Lookups mutate the table | `src/hashtable.c:1606` (`hashtableFind`), `:914` (`rehashStepOnReadIfNeeded` in `findBucket`), `:1712` (the documented warning) |
| Batch size that caps the fork/join approach | `prefetch-batch-max-size`, default 16, max 128 — `src/config.c:3407` |
| The branch's run + join | `src/io_threads.c:1045` (`ioThreadRunJoin`), `:1068` (dispatch), `:1087` (spin), `:1013` (completion); collection at `src/memory_prefetch.c:243-264` |
| Slot computed at parse time | `prepareCommand` `src/server.c:4341`; `unprepareCommand` `:4357` |
| Command filters rewrite after parse | `src/server.c:4390` (`moduleCallCommandFilters`), `src/module.c:11597-11600` |
| Blocked-client return contract | `src/networking.c:3879` (`commandProcessed`), early-out `:3887` |
| Block / resume framework | `src/blocked.c:106` (`blockClient`), `:158` (`processUnblockedClients`), `:217` (`unblockClient`); re-drive `src/networking.c:3962` |
| Blocking type enum | `src/server.h:347-349`; counted by `blocked_clients_by_type` `src/server.h:2214` |
| Socket-less executor precedent | `src/aof.c:1490` (`createAOFClient`), used `:1560` |
| Reply encoding / `BULK_STR_REF` | `src/networking.c:107-127` (`payloadType`, `payloadHeader`), `:545` (`upsertPayloadHeader`) |
| Fake clients skip deferred replies | `src/networking.c:261` |
| Output-buffer limit is fake-safe | `src/networking.c:6266` (early return), `:2283` (`freeClientAsync`) |
| Per-command stat globals | `src/server.c:4043-4044` (`calls`, `microseconds`), `:4112` (`stat_numcommands`) |
| Propagation from the epilogue | `src/server.c:4028` (dirty delta), `:4111` (`propagate_flags`) |
| Queue transport | `src/queues.h` (SPSC/SPMC/MPSC); tagged jobs `src/io_threads.c:38`; `IO_SPSC_QUEUE_SIZE` `src/io_threads.c:13` |
| Per-thread counter precedent | `used_active_time_io_thread[]`, `src/io_threads.c` |

## 4. See also

- [proposal-slot-per-thread.md](proposal-slot-per-thread.md) — the full design this is a
  subset of; §5b is the event-loop work deliberately excluded here.
- [proposal-slot-per-thread.md §19](proposal-slot-per-thread.md#19-phase-4-part-2-dispatch-and-the-remote-continuation)
  — the `BLOCKED_SHARD` continuation, which §2.5.2 is a restriction of.
- [proposal-stage0-measurement.md](proposal-stage0-measurement.md) — the measurement that
  gates all of this.
- [09-threading-and-io-model.md](09-threading-and-io-model.md) — how the I/O thread pool works
  today.
