# Proposal — Read-only command execution on I/O threads

**Status: pre-issue draft.** A design for executing read-only commands on threads other
than the main thread, in cluster mode, without per-thread event loops.

This is a narrower, cheaper design than
[proposal-slot-per-thread.md](proposal-slot-per-thread.md), and a stricter one than the
fork/join approach reviewed in
[review-readonly-io-vs-slot-per-thread.md](review-readonly-io-vs-slot-per-thread.md).
It exists because there is a real gap between "offload the command proc inside a batch"
(cheap, structurally capped) and "give every thread its own `aeEventLoop`" (uncapped,
multi-quarter). This design occupies that gap and — importantly — every piece of it is
reused verbatim by the full slot-per-thread design later. Nothing here is throwaway.

All `file:line` anchors verified against this checkout.

---

## 1. The claim

**Owner threads must run continuously, not in bursts.** The motivation for moving work off
the main thread is to use more than one core. A design where the main thread dispatches,
then idles at a barrier, then serially completes, uses one core at a time in three phases —
it passes a baton rather than adding a core. So the central design constraint is:

> Between two consecutive barriers, an owner thread executes commands back-to-back without
> ever synchronizing with the main thread, and the main thread keeps parsing and serving
> other clients while it does.

Everything below follows from taking that constraint seriously. The two things that
otherwise force a barrier are (a) the main thread's own background work touching arbitrary
slots and (b) global mutable bookkeeping. §7 and §8 dispose of both.

## 2. Scope

**In scope.** Single-slot, read-only commands, in cluster mode, on plain connected clients.
Executed on an owner thread. Everything else runs on the main thread exactly as today.

**Out of scope.** Writes and the replication journal (that is slot-per-thread Phase 5);
per-thread event loops and connection migration (§5b of the parent, Phase 4a); standalone
mode; modules; scripts.

**Explicitly a non-goal:** making the main thread stop being a serialization point. It still
parses every command and still owns every socket. This design raises the ceiling from *"how
many commands can one thread parse and execute"* to *"how many commands can one thread parse
and relay"* — a real but bounded win. Anyone who wants more must do the event-loop work.
That honesty is the point of §12.

## 3. Ownership model

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
- Ownership is **persistent**, not per-run. A slot has the same owner from one barrier to
  the next. This is the single most important difference from a per-batch dispatch scheme:
  because ownership is stable, an owner can execute a queue of commands back-to-back with no
  handshake per command.
- Ownership is exclusive for **all** access, including background work. §7.
- Assigned contiguously at startup (`owner = slot * (N-1) / 16384 + 1`, or round-robin —
  measure). Rebalanceable under a barrier: moving a slot is moving an index, not data.

**Why exclusivity is not optional.** A lookup mutates the hashtable: `hashtableFind()`
(`src/hashtable.c:1606`) → `findBucket()` → `rehashStepOnReadIfNeeded()`
(`src/hashtable.c:914`), and `src/hashtable.c:1712` documents that "hashtableFind() may
cause incremental rehashing to move entries in memory." Two threads reading the same slot
is a corruption, not a benign race. Any design that lets a slot's command run on a
different thread "just this once" (a full queue, a fallback path) is wrong.

## 4. The isolation boundary — data, not object graphs

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
        ...                                    call(x, CMD_CALL_SLOW | no propagation)
        ...                                    bytes = detachReply(x)   /* flat RESP sds */
        ...                                    side = collected effects (§6)
  beforeSleep: drain results        ◀─ result{id, bytes, side, duration} ─┘
    validate client still live (§9)
    addReplyProto(c, bytes)
    apply side effects (§6)
    unblockClient(c, 1)  ── resume, parse next pipelined command
```

What crosses the thread boundary is **argv in, RESP bytes out**. No shared `client`, no
shared reply buffer, no shared list. This is the property that makes the design auditable:
the set of things an owner can corrupt is the set of things it is handed, and that is two
pointers.

### 4.1 Detaching the reply — the one real subtlety

Valkey reply buffers are not always plain bytes. `_addReplyPayloadToList`
(`src/networking.c` around `:660-702`) may write **encoded** chunks: a `payloadHeader`
(`src/networking.c:116`) followed by either a plain reply or a `BULK_STR_REF`
(`src/networking.c:109`) — a *pointer to an `robj`* rather than a copy of its bytes.

Handing a `BULK_STR_REF` across threads is unsafe: the referenced `robj` lives in the
owner's slot, and `robj` refcounts are not atomic. The main thread would be reading (and
eventually `decrRefCount`-ing) an object another thread may free.

**Decision: force plain, self-contained replies on the executor.** Disable encoding and
deferred replies on executor clients (`isDeferredReplyEnabled`, `src/networking.c:261`
already special-cases `c->flag.fake`), so `detachReply()` is a concatenation of
`x->buf[0..bufpos]` plus each `x->reply` node into one `sds`, with no pointers inside.

**Honest cost:** this reintroduces a copy for large bulk values — precisely the workload
where offloading pays off most (§12). Two escapes exist if measurement demands them, both
deferred: (a) transfer `robj` ownership explicitly, with the main thread posting a
`decrRefCount` job back to the owner; (b) make `BULK_STR_REF` refcounts atomic. Neither
should be attempted in v1.

The executor's RESP version must be pinned per job (`x->resp = c->resp`), or a RESP3 client
gets RESP2 map/double encodings.

## 5. Dispatch and continuation

### 5.1 Eligibility — a positive allowlist

A new command flag `CMD_IO_SAFE`, **default off**, opted into per command in
`src/commands/*.json`. v1 set: trivially safe single-key reads — `GET`, `STRLEN`, `EXISTS`,
`TYPE`, `TTL`, `HGET`, `HLEN`, `LLEN`, `LINDEX`, `SCARD`, `SISMEMBER`, `ZSCORE`, `ZCARD`,
`GETRANGE`.

This is deliberately the opposite of a negative filter (*"readonly and not module and not
blocking and not…"*). A negative filter makes every newly added command eligible by default
and silently absorbs any command whose flags don't describe it fully. A positive allowlist
makes the safe set an explicit, reviewable list, and makes the default for anything new
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
| `!c->flag.tracking` | v1 only; see §6 |
| `!c->slot_migration_job` | slot ownership is in flux |
| `!server.loading` | keyspace is being mutated wholesale |
| not over `maxmemory` | eviction needs a global view; see §7.3 |

**The check must be re-evaluated, not asserted.** It is computed at parse time from
`c->parsed_cmd` and `c->slot`, but `moduleCallCommandFilters()` (`src/server.c:4390`) can
rewrite argv and re-run `prepareCommand()` (`src/module.c:11597-11600`), changing both the
command and the slot afterwards. The dispatch point must therefore re-check and *fall back
to inline execution*, never assert. (This exact assumption is a live crash in the branch
reviewed in the companion note.)

### 5.2 Suspend — reuse the blocking framework

A cross-thread hop is not new control flow; it is a new **blocking type**. Valkey already
supports "start a command, discover it cannot complete synchronously, suspend without
resetting the client or propagating, resume later." That is `BLPOP`.

Add `BLOCKED_IO_EXEC` to `blocking_type` before `BLOCKED_NUM` (`src/server.h:349`).
`server.blocked_clients_by_type[]` (`src/server.h:2214`) is sized by it, so it is counted in
`INFO` for free.

```c
int ioExecDispatch(client *c, int owner) {
    ioExecJob *job = zmalloc(sizeof(*job));
    job->client_id = c->id;        /* not the pointer — see §9 */
    job->argv = c->argv;           /* borrowed; safe because c stays blocked */
    job->argc = c->argc;
    job->slot = c->slot;
    job->dbid = c->db->id;
    job->resp = c->resp;

    blockClient(c, BLOCKED_IO_EXEC);   /* src/blocked.c:106 */

    if (!spscEnqueue(&owners[owner].inbox, tagJob(job, IO_REQ_EXEC), true))
        return ioExecOverflow(c, owner, job);   /* §9.3 — never run it here */
    return C_OK;
}
```

Three properties come free from `blockClient`:

- **No premature reset or propagation.** `commandProcessed()` returns early at
  `src/networking.c:3887` when `c->flag.blocked` is set — argv and argc are preserved, the
  client is not reset, the replication offset is not advanced. Exactly what a suspended
  command needs, and the reason `job->argv` may borrow rather than copy.
- **Per-client ordering, at no cost.** While blocked, the query buffer accumulates but is
  not parsed into new commands. Command *k+1* cannot be dispatched before *k* resumes. The
  same mechanism that stops `BLPOP` then `GET` from reordering on one connection.
- **A resume path already exists.** `unblockClient(c, 1)` (`src/blocked.c:217`) queues the
  client on `server.unblocked_clients`; `processUnblockedClients()` (`src/blocked.c:158`)
  drains it in `beforeSleep`.

### 5.3 Resume — finalize, do not re-execute

This is the one place the `BLPOP` analogy needs care. `BLPOP` re-runs its command on wake.
An I/O-executed read must **not**: the reply is already produced.

The `BLOCKED_IO_EXEC` arm of `unblockClient` therefore clears `c->flag.pending_command`
before queueing, so the re-drive through `processPendingCommandAndInputBuffer`
(`src/networking.c:3962`) skips re-dispatch and proceeds to parse the **next** pipelined
command from the held query buffer. This mirrors how `BLOCKED_WAIT` finalizes without redo.

Order of operations on the main thread when a result arrives, per client:

1. validate the client is still live (§9.1);
2. `addReplyProto(c, bytes, len)` — normal path, so output-buffer limits, encoding and
   `clients_pending_write` all behave exactly as for an inline command;
3. apply the side effects the owner collected (§6), in completion order;
4. `c->duration += result->duration`;
5. `unblockClient(c, 1)`.

## 6. Global state — the complete inventory

This is the section the design lives or dies on. A command proc touches more than its
keyspace, and any item missed here is a silent data race. The rule applied throughout:
**shard the counter, or hand back the effect — never mutate a global from an owner.**

| Global | Today | Under this design |
|---|---|---|
| `server.stat_numcommands` (`src/server.c:4112`) | main thread `++` | per-owner counter; summed in `INFO` |
| `cmd->calls`, `cmd->microseconds` (`src/server.c:4043-4044`) | shared command table | per-owner `struct commandStats[]` indexed by command id (~240 × 32 B ≈ 8 KB/owner); summed in `INFO commandstats` |
| `cmd->failed_calls`, `rejected_calls` | shared | same |
| `server.stat_keyspace_hits` / `_misses` | main thread `++` | per-owner; summed |
| `server.stat_total_error_replies` + errors rax | main thread | per-owner counter + per-owner rax; merged in `INFO errorstats` |
| commandlog (slow / large-reply) | main thread ring | per-owner ring; merged at `COMMANDLOG GET` |
| latency monitor | main thread | per-owner samples; merged at `LATENCY` |
| `server.dirty` | main thread | reads must not change it — assert unchanged on the executor |
| propagation (`alsoPropagate`) | main thread | **not run at all**: call the executor without `CMD_CALL_PROPAGATE`. A read-only command has nothing to propagate, and computing a `server.dirty` delta across a fork/join is how the reviewed branch leaks `GET` into the AOF |
| `server.clients_pending_write` | main thread list | untouched by owners; the main thread queues the client in step 2 of §5.3 via the ordinary `addReply` path |
| `server.clients_to_close` (output-buffer limit) | main thread list | unreachable from an owner: the executor is `flag.fake`, and `closeClientOnOutputBufferLimitReached` returns 0 for fake clients at `src/networking.c:6266`. The limit is applied when the main thread appends |
| keyspace-miss notification | published inline | owner records `(key, event)` in the job's side-effect list; main thread publishes at completion. Removes any need to disable the feature when `notify-keyspace-events` includes `Km` |
| lazy expire (`DEL` + propagate) | main thread | owner **may** delete — it owns the dict — and hands the `DEL` back as a side effect for the main thread to propagate |
| client-side-caching tracking table | main thread | v1: tracking clients are ineligible (§5.1). v2: owner returns the key list, main thread calls `trackingRememberKeys` at completion |
| LRU / LFU counters on `robj` | inline | owner-only, and the owner has exclusive access to the slot — safe by construction |
| cluster slot stats (`c->slot` per-slot array) | inline | owner-only, disjoint by slot — safe by construction |
| `server.cmd_time_snapshot` | one per execution unit | per-owner snapshot, refreshed from `server.mstime` at the top of each executed command |

Two of these are worth dwelling on.

**Per-owner counters instead of deferred replay.** The alternative — accumulate effects per
command and replay them on the main thread in arrival order — preserves the exact
interleaving of stat updates, which no client can observe, and in exchange makes every
command round-trip through the main thread for bookkeeping alone. Sharded counters summed
at `INFO` time are cheaper, are the standard answer, and have in-tree precedent
(`used_active_time_io_thread[]` in `src/io_threads.c`). `INFO` becomes O(owners × commands)
instead of O(1), which is irrelevant at `INFO` frequencies.

**Lazy expire becomes correct again.** Because the owner has exclusive access to the slot,
it can actually delete an expired key rather than reporting it as missing and leaving it.
Only the `DEL`'s *propagation* needs the main thread, and that rides the side-effect list.
A design that must return "expired but still present" is accepting a real semantic
divergence to work around an ownership model that isn't exclusive enough.

## 7. What forces a barrier, and how often

A **barrier** quiesces every owner: broadcast ENTER, each owner finishes its current command
and parks, main thread proceeds with a global view, broadcast LEAVE. The design is viable
only if barriers are rare. Three sources:

### 7.1 Slot-iterating background work → **skip, don't barrier**

Active expiry (`activeExpireCycle`) and incremental rehashing (`kvstoreIncrementallyRehash`
in `databasesCron`) walk slots. Both already iterate per-slot, so both take an ownership
check: the main thread processes only slots where `slot_to_owner[slot] == 0`, and each owner
runs the same cycle over its own slots between commands, in its own loop.

No barrier, and it parallelizes expiry as a side effect. This is why per-shard expiry is a
**prerequisite** here rather than the "Phase 6" it is in the parent proposal — without it,
the main thread's fast expire cycle in `beforeSleep` would need a barrier on every event
loop iteration, which is the reviewed branch's problem with extra steps.

### 7.2 Genuinely global work → **barrier, at bounded frequency**

Defrag, `fork()` for RDB/AOF, `FLUSHALL`, `SWAPDB`, `DEBUG`, `CLUSTER SETSLOT`, slot
migration, ownership rebalance, and `SCAN`/`KEYS`/`DBSIZE` if not made owner-aware.

Frequency: cron runs at `server.hz` (default 10/s), saves are rare, `FLUSHALL` is rare. Ten
barriers per second, each costing one command's latency per owner, is nothing. Contrast with
a per-batch fork/join at thousands per second — that is the entire architectural difference
between this design and the one reviewed in the companion note.

`SCAN` deserves better than a barrier and can have it: the `kvstore` cursor already encodes
the table index, so a `SCAN` that fans out to owners and merges is close to free. Worth
doing rather than escalating.

### 7.3 Eviction → **disable offload under memory pressure**

`performEvictions` (`src/evict.c`) needs global sampling against a global `maxmemory` and
runs on the *hot path* whenever the limit is approached. Barriering there would be constant
exactly when the server is most loaded.

v1 answer: if `maxmemory` is set and used memory is within some margin of it, stop
dispatching (the eligibility check in §5.1) and let the barrier drain naturally. The server
degrades to today's behavior under memory pressure, which is a defensible place to degrade
to. Per-owner eviction with slack is a later step and needs its own design.

## 8. Why the owner threads stay busy

With §7 in place, an owner's loop is:

```c
for (;;) {
    drain inbox: execute each job back-to-back, post results     /* no sync with main */
    run this owner's share of active expire / rehash              /* §7.1 */
    if (barrier requested) { park; wait for LEAVE; }              /* §7.2 — rare */
    else if (inbox empty) { brief spin, then futex/condvar wait; }
}
```

There is no point in that loop where an owner waits for the main thread to reach a
particular place. That is the whole design. Result delivery is fire-and-forget onto an SPSC
completion queue plus a coalesced wake (only when the queue transitions empty → non-empty).

The main thread, symmetrically, never waits for an owner. It dispatches and returns to the
event loop. Its per-command cost for a dispatched command is: parse, eligibility check,
`blockClient`, enqueue — then later — validate, `addReplyProto`, side effects,
`unblockClient`, `resetClient`. That is strictly less than executing the command, and it
overlaps with owners executing.

## 9. Lifetimes, failure, and backpressure

### 9.1 Client disconnect mid-flight

The client may be freed (connection reset, `CLIENT KILL`, timeout) while a job is in flight.
The job carries `client_id` (a monotonic `uint64`), not just a pointer. On completion the
main thread looks the client up by id and drops the result if it is gone or recycled.
Freeing a `BLOCKED_IO_EXEC` client must go through the normal blocked-client teardown so no
block leaks; `unblockClient` is already called from the client-free path for other block
types.

The job's borrowed `argv` is the hazard: freeing the client frees argv while an owner may be
reading it. Rule: **a `BLOCKED_IO_EXEC` client is not freed synchronously.** Mark it
`close_asap` and let `freeClientsInAsyncFreeQueue` collect it after the in-flight job's
result has been drained (or dropped). A per-client in-flight counter makes this a one-line
check.

### 9.2 A command that errors or panics on an owner

Errors are ordinary RESP bytes and need no special handling beyond the per-owner error stats
(§6). `serverPanic`/`serverAssert` on an owner thread is a process abort as it is anywhere
else; the crash log must record which owner and which command, so `logStackTrace` needs to
know it may run off the main thread.

### 9.3 Owner inbox full

**The one thing the main thread must not do is execute the command itself.** That breaks
slot exclusivity: if another client's command for the same slot was already dispatched, the
main thread and the owner would be in the same hashtable concurrently — §3.

Correct options, in order of preference: (a) park the job on a per-owner overflow list
drained on the next `beforeSleep`, leaving the client blocked; (b) unblock the client with a
transient `-TRYAGAIN`. Start with (a); (b) is a fallback if the overflow list turns out to
grow unboundedly. Size the SPSC inbox generously (the existing `IO_SPSC_QUEUE_SIZE` is 4096)
so this is a genuine edge case rather than a routine path.

### 9.4 A slow command blocks its owner's queue

An owner executes its inbox serially, so one `GETRANGE` over a 512 MB string delays every
other command for slots that owner holds. This is a head-of-line blocking property the main
thread does not have today (where a slow command delays *everyone*, but there is only one
queue). It is a genuine tail-latency change, must be measured, and is the main argument for
keeping the v1 allowlist to commands with bounded output.

## 10. Configuration and observability

- `io-owner-threads N` — default **1**, meaning "no slot is owned by anyone but the main
  thread" and the entire feature is inert. Must be a provable runtime no-op at the default.
  Reuse the io-threads worker pool rather than spawning a second set of threads.
- `INFO stats` gains: `io_exec_commands` (dispatched), `io_exec_overflow` (§9.3),
  `io_exec_dropped` (§9.1), `io_exec_barriers` and `io_exec_barrier_usec` (§7 — the number
  to watch; if barriers are frequent the design is not working).
- `INFO clients` shows `BLOCKED_IO_EXEC` in `blocked_clients_by_type` for free.
- `DEBUG SLOT-OWNER <slot>` for tests.

## 11. Files touched

| File | Change |
|---|---|
| `src/io_exec.{c,h}` | **New.** `slot_to_owner[]`, owner loop, executor clients, dispatch, completion drain, detach/reattach, barrier. |
| `src/server.h` | `BLOCKED_IO_EXEC` before `BLOCKED_NUM` (`:349`); `CMD_IO_SAFE`; per-owner stats struct. |
| `src/server.c` | `processCommand` tail: eligibility re-check (§5.1) → `ioExecDispatch` or `call()`. `beforeSleep`: drain completions. `serverCron`/`databasesCron`: ownership skip (§7.1) + barrier points (§7.2). |
| `src/blocked.c` | `BLOCKED_IO_EXEC` arm of `unblockClient` (`:217`) — finalize without re-execution; teardown discards in-flight results. |
| `src/networking.c` | Executor client creation; `detachReply()`; force plain replies on executors (§4.1). `commandProcessed` (`:3879`) and `processPendingCommandAndInputBuffer` (`:3962`) need **no change** — they already do the right thing for blocked clients. |
| `src/expire.c`, `src/kvstore.c` | Ownership-aware slot iteration (§7.1). |
| `src/evict.c` | Memory-pressure gate (§7.3). |
| `src/commands/*.json` | `IO_SAFE` on the v1 allowlist; regenerate `commands.def`. |
| `src/unit/test_io_exec.cpp` | **New.** Eligibility matrix, detach/reattach round-trip. |
| `tests/unit/cluster/io-exec.tcl` | **New.** End-to-end. |

Notably absent: `call()` does not need splitting into prologue/invoke/epilogue. The whole
command runs on one thread, start to finish. That the fork/join design needs such a split is
a symptom of its boundary being in the wrong place.

## 12. Honest limits

- **The main thread still parses and relays everything.** The ceiling is one thread's parse
  + relay rate. Expect a low single-digit multiple, not linear scaling. Anyone promising
  more is describing per-thread event loops, not this.
- **The gain is proportional to proc cost.** For a `GET` of a 20-byte value the proc is a
  small fraction of per-command work and the dispatch + completion overhead may exceed it.
  For `GETRANGE`, large `HGET`, big `LINDEX`, the proc dominates and this wins. **This is a
  large-value read accelerator.** The v1 allowlist and the benchmark plan should both be
  built around that, and if it cannot be demonstrated there, it will not appear elsewhere.
- **The forced reply copy (§4.1) cuts against exactly that workload.** The one measurement
  that must happen before writing much code: for a 4 KB bulk reply, does executing the proc
  on another thread beat the extra `memcpy` on the main thread? If not, the design needs the
  `robj` ownership transfer before it is worth building.
- **Head-of-line blocking per owner** (§9.4) is a new tail-latency risk.
- **Hot slots** cap throughput at one owner. Slot rebalancing under barrier helps a hot
  *slot*; nothing here helps a hot *key*.
- **Cluster mode only.** Standalone has one hashtable per db; virtual slots would be needed
  first (parent §4 / Phase 3).

## 13. What would make me abandon this

- Barrier frequency (`io_exec_barriers`) cannot be held to cron rate, meaning §7.1's
  skip-don't-barrier approach does not cover the real background work.
- The §12 large-value measurement shows the reply copy eats the win, *and* `robj` ownership
  transfer turns out to need atomic refcounts throughout.
- The per-owner counter merge (§6) proves not to be equivalent for `INFO commandstats` /
  `errorstats` in a way users would notice.

If instead it works, none of it is wasted: `slot_to_owner[]`, the executor client, the RESP
detach/reattach, `BLOCKED_IO_EXEC`, the barrier, and per-owner background work are all
prerequisites of the full slot-per-thread design. This is Phase 4 minus `conn->el` — the
same road, one exit earlier.

## 14. Code anchors

| Thing | Where |
|---|---|
| Per-slot kvstore | `src/kvstore.c` (`kvstoreCreate`); `slot_count_bits` `src/server.c` |
| Lookups mutate the table | `src/hashtable.c:1606` (`hashtableFind`), `:914` (`rehashStepOnReadIfNeeded` in `findBucket`), `:1712` (the documented warning) |
| Slot computed at parse time | `prepareCommand` `src/server.c:4341`; `unprepareCommand` `:4357` |
| Command filters can rewrite after parse | `src/server.c:4390` (`moduleCallCommandFilters`), `src/module.c:11597-11600` (re-`prepareCommand`) |
| Blocked-client return contract | `src/networking.c:3879` (`commandProcessed`), early-out at `:3887` |
| Block / resume framework | `src/blocked.c:106` (`blockClient`), `:158` (`processUnblockedClients`), `:217` (`unblockClient`); re-drive `src/networking.c:3962` |
| Blocking type enum | `src/server.h:347-349`; counted by `blocked_clients_by_type` `src/server.h:2214` |
| Socket-less executor precedent | `src/aof.c:1490` (`createAOFClient`), used `:1560` |
| Reply encoding / `BULK_STR_REF` | `src/networking.c:107-127` (`payloadType`, `payloadHeader`), `:545` (`upsertPayloadHeader`) |
| Fake clients skip deferred replies | `src/networking.c:261` |
| Output-buffer limit is fake-safe | `src/networking.c:6266` (early return), `:2283` (`freeClientAsync`) |
| Per-command stat globals | `src/server.c:4043-4044` (`calls`, `microseconds`), `:4112` (`stat_numcommands`) |
| Queue transport | `src/queues.h` (SPSC/SPMC/MPSC); tagged-pointer jobs `src/io_threads.c:38`; `IO_SPSC_QUEUE_SIZE` `src/io_threads.c:13` |
| Per-thread counter precedent | `used_active_time_io_thread[]`, `src/io_threads.c` |
| Batch size that caps the fork/join alternative | `prefetch-batch-max-size`, default 16, max 128 — `src/config.c:3407` |

---

## 15. See also

- [review-readonly-io-vs-slot-per-thread.md](review-readonly-io-vs-slot-per-thread.md) —
  why the per-batch fork/join alternative is structurally capped, with the bug inventory
  that motivated §4's boundary and §6's completeness requirement.
- [proposal-slot-per-thread.md](proposal-slot-per-thread.md) — the full design this is a
  subset of; §5b is the event-loop work deliberately excluded here.
- [proposal-slot-per-thread-phase4.md](proposal-slot-per-thread-phase4.md) — the
  LOCAL/REMOTE/BARRIER branch and `BLOCKED_SHARD`, which §5.2 here is a restriction of.
- [proposal-stage0-measurement.md](proposal-stage0-measurement.md) — the measurement that
  gates all of this.
