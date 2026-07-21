# Proposal — Slot-per-thread Phase 4: the dispatch branch point + the REMOTE continuation

**Status: pre-issue draft.** This is the concrete, line-anchored design of the one hot
integration point of slot-per-thread: the **LOCAL / REMOTE / BARRIER branch** at command
dispatch, and the **continuation mechanism** that lets a command started on one thread finish
on another and reply correctly. It is the code half of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §5 / §5a and the concrete form of
that doc's §14.3, refined against the real return-value and blocking contracts in this
checkout.

It assumes Phase 2 ([proposal-slot-per-thread-phase2.md](proposal-slot-per-thread-phase2.md):
`slot_to_shard[]` + `shard-threads`) has landed, and that Model B's per-thread event loops
(§5b — `conn->el`, per-shard `aeEventLoop`, `SO_REUSEPORT` accept) exist or land alongside.
The event-loop de-globalization is **not** re-derived here; this note is strictly the dispatch
decision and the cross-thread result path. Per parent §10 step 4, the executable scope is
**single-key reads**: LOCAL and REMOTE carry reads; everything else takes the barrier. Writes
+ journal + sequencer are Phase 5.

All `file:line` anchors were checked against this checkout.

---

## 1. The one claim this note rests on

**A REMOTE hop is not a new control-flow mechanism. It is a new blocking type.** Valkey
already has, and relies on every day, the exact primitive a cross-thread hop needs: *start
executing a command, discover it cannot complete synchronously, suspend it without resetting
the client or propagating anything, and resume it later when a result arrives.* That is
`BLPOP`. The blocking framework already guarantees the three properties the continuation
needs:

- **No premature reset / propagation.** `commandProcessed` returns early when
  `c->flag.blocked` is set (`src/networking.c:3887`) — argv/argc are preserved, the client is
  not reset, replication offset is not advanced. Exactly what a suspended REMOTE command
  needs.
- **Input is held, not processed.** While `CLIENT_BLOCKED` is set the query buffer is
  accumulated but not parsed into new commands (`blockClient`, `src/blocked.c:104`). This is
  what preserves **per-client ordering for free**: command *k+1* is not dispatched until *k*
  resumes and unblocks — the same reason `BLPOP` then `GET` on one connection can't reorder.
- **A resume path exists.** `unblockClient(c, queue_for_reprocessing=1)` (`src/blocked.c:217`)
  queues the client on `server.unblocked_clients`; `processUnblockedClients`
  (`src/blocked.c:158`) drains it in `beforeSleep` and re-drives the held command via
  `processPendingCommandAndInputBuffer` (`src/networking.c:3962`, gated on
  `c->flag.pending_command`).

So Phase 4 adds one enum value — `BLOCKED_SHARD` — and reuses the machinery, rather than
inventing the ad-hoc `awaiting_shard` flag sketched in parent §14.3. The genuinely new work is
narrow and nameable: (a) the owner must run the command **without a socket** and hand back
**reply bytes**, and (b) the unblock signal arrives **cross-thread**, so it must wake the
coordinator's event loop instead of running inline like `handleClientsBlockedOnKeys`.

## 2. Verified anchors (this checkout)

- **Dispatch tail.** `processCommand` (`src/server.c:4315`) returns `C_OK`/`C_ERR` and today
  ends by calling `call(c, ...)` (`src/server.c:3875`). Its caller
  `processCommandAndResetClient` (`src/networking.c:3932`) runs `commandProcessed(c)` only when
  `processCommand` returned `C_OK` (`:3936-3937`).
- **The blocked-client early-out.** `commandProcessed` (`src/networking.c:3879`) bails at
  `:3887` (`if (c->flag.blocked) return;`) — no reset, no reploff advance, no propagation.
- **Block / unblock / resume.** `blockClient(c, btype)` (`src/blocked.c:106`);
  `unblockClient(c, queue_for_reprocessing)` (`src/blocked.c:217`); `processUnblockedClients`
  (`src/blocked.c:158`) in `beforeSleep`; re-drive via `processPendingCommandAndInputBuffer`
  (`src/networking.c:3962`) which consumes `c->flag.pending_command` (`:3967-3968`).
- **Blocking enum.** `blocking_type` (`src/server.h:340-351`): `BLOCKED_POSTPONE` at `:347`,
  `BLOCKED_SHUTDOWN` at `:348`, terminated by `BLOCKED_NUM` at `:349`. A new `BLOCKED_SHARD`
  goes before `BLOCKED_NUM`. `server.blocked_clients_by_type[btype]` is sized by `BLOCKED_NUM`.
- **Client flags bitfield.** `struct ClientFlags` (`src/server.h:1112`): `blocked : 1`
  (`:1117`), `unblocked : 1` (`:1120`), `pending_command : 1` (`:1146`). No new flag needed —
  `BLOCKED_SHARD` rides `blocked`/`pending_command`.
- **Block state.** `c->bstate` (`src/server.h:1329`), `blockingState.btype`
  (`src/server.h:960`), lazily inited by `initClientBlockingState` (called from `blockClient`).
- **Reply buffers (single-owner, home thread only).** `c->buf[]`/`c->bufpos`
  (`src/server.h:1289-1290,1340`), `c->reply` list (`:1334`), `c->reply_bytes` (`:1338`).
  Append helpers: `_addReplyToBufferOrList` (`src/networking.c:719`), `_addReplyProtoToList`
  (`src/networking.c:700`), `addReplyProto` (`src/networking.c:824`).
- **Fake-client precedent (execute with no socket).** `createClient(NULL)` (`src/aof.c:1491`);
  the AOF loader runs full commands through a `fakeClient` with `argv`/`argc` set directly and
  `call()`-equivalent execution (`src/aof.c:1560-1640`). This is the template for a per-shard
  executor client.
- **Queue transport.** `src/queues.h`: `spscQueue` (`:112`), `spscEnqueue(q, data, commit)`
  (`:136`), `spscDequeueBatch` (`:140`), `spscIsFull`/`spscIsEmpty` (`:132,142`); `mpscQueue`
  (`:45`) for the many-coordinators→one-owner direction if needed. Tagged-pointer job trick:
  `src/io_threads.c:38`.
- **Parent structures already defined.** `shard`, `slot_to_shard[]`, `shardExecJob`
  (parent §14.2); `shardDispatch`/`shardBarrierRun` sketch (parent §14.3/§14.5). This note
  refines the dispatch/continuation half; it does not redefine the shard table.

## 3. Design

The REMOTE round trip at a glance — a command whose slot is owned by another thread. Nothing
executes across the boundary: the coordinator hands over *data* (argv) and gets back *data*
(reply bytes); neither thread touches the other's client, socket, or reply buffer.

```text
  COORDINATOR thread (owns the socket)              OWNER thread (owns the slot's data)
  ────────────────────────────────────              ───────────────────────────────────
  1. shardDispatch: owner != me            (§3.1)
       blockClient(c, BLOCKED_SHARD)        (§3.2)   ── client suspended, thread keeps working
       build shardExecJob{argv,handle,coord}
       spscEnqueue(owner->inbox, job) ───────────┐
       return to event loop, serve other clients │
                                                  ▼
                                          2. shardBeforeSleep drains inbox     (§3.3)
                                               call() on socket-less executor client
                                               shardDetachReply → flat RESP `sds` bytes
                                          ┌── spscEnqueue(coord->results, res)
                                          │    shardWakeLoop(coord_shard) ── 1 byte to eventfd
                                          │        (§3.5: wakes coord's aeApiPoll)
                                          ▼
  4. shardBeforeSleep drains results       (§3.4)
       shardLookupLiveClient(handle)   ◀── §4: drop if client died mid-hop
       _addReplyProtoToList(c, bytes)
       unblockClient(c, 1) ── resume: finalize, parse NEXT pipelined command (no re-exec)
```

The LOCAL case skips all of this (`call()` inline, §3.1); the BARRIER case parks every shard
and runs on the coordinator (parent §14.5). Only REMOTE pays the two-hop cost above.

### 3.1 The branch point — `shardDispatch(client *c)`

The branch replaces the direct `call()` at the tail of `processCommand` (`src/server.c:4315`).
`c->slot` is already computed (`clusterSlotByCommand` inside `prepareCommandGeneric`, parent
§2), and all gate checks (auth, cluster redirect, OOM, etc.) have already passed, so dispatch
sees a validated, slot-tagged command.

```c
/* src/shard.c — tail of processCommand, after all gate checks. Returns like
 * processCommand: C_OK if handled (incl. suspended), C_ERR if the client died. */
int shardDispatch(client *c) {
    if (server.shard_threads == 1)                 /* Phase-2 identity: provable no-op */
        return call(c, CMD_CALL_FULL), C_OK;

    int slot = c->slot;
    if (slot < 0 || commandNeedsBarrier(c))        /* keyless/global/multi-slot/MULTI/Lua/module */
        return shardBarrierRun(c);                  /* parent §14.5 */

    int owner = slot_to_shard[slot];
    if (owner == myShardId())                       /* LOCAL: the fast path, zero hops */
        return call(c, CMD_CALL_FULL), C_OK;

    return shardRemoteBegin(c, owner);              /* REMOTE: suspend + hop (§3.2) */
}
```

`commandNeedsBarrier(c)` is a predicate over `c->cmd->flags` + argv: true unless the command
is flagged `CMD_SHARD_SAFE` **and** resolves to a single slot. Default the flag **off**; in
Phase 4 only trivially-safe single-key **reads** (`GET`, `STRLEN`, `HGET`, `LLEN`, …) get it.
This keeps the correctness surface tiny — anything not explicitly proven safe takes the barrier
and runs exactly as today.

### 3.2 REMOTE begin — suspend as `BLOCKED_SHARD`, then hop

```c
int shardRemoteBegin(client *c, int owner) {
    shardExecJob *job = zmalloc(sizeof(*job));
    job->coordinator = c;
    job->handle      = c->id;              /* uint64 client id, for disconnect validation (§4) */
    job->coord_shard = myShardId();
    job->argv = c->argv; job->argc = c->argc;   /* borrowed; lifetime rule in §4 */
    job->slot = c->slot;

    /* Suspend using the existing blocking framework — NOT a bespoke flag. */
    blockClient(c, BLOCKED_SHARD);         /* src/blocked.c:106; querybuf now held, not parsed */
    c->flag.pending_command = 1;           /* so processPendingCommandAndInputBuffer re-drives on resume */

    if (spscIsFull(&server_shards[owner].inbox)) {
        /* Backpressure: owner inbox full. Do NOT spin the coordinator. Park the job on a
         * per-owner overflow list drained by shardBeforeSleep; or, simplest for Phase 4,
         * unblock with a transient -TRYAGAIN. Measure before choosing (§6). */
        return shardRemoteAbort(c, owner, job);
    }
    spscEnqueue(&server_shards[owner].inbox, tagJob(job, SHARD_REQ_EXEC), /*commit=*/true);
    return C_OK;                            /* no reply yet; commandProcessed() sees blocked, bails */
}
```

Because `blockClient` set `c->flag.blocked`, the return path is already correct with **zero new
plumbing**: `processCommandAndResetClient` calls `commandProcessed`, which returns at
`src/networking.c:3887` without resetting the client or advancing offsets. The client sits
blocked; its socket stays registered on the coordinator's loop; its half-built reply buffer is
untouched.

### 3.3 Owner side — execute with no socket, capture reply bytes

The owner thread drains its inbox in `shardBeforeSleep` (parent §5b). For a `SHARD_REQ_EXEC`
job it runs the command against a **per-shard executor client** — one long-lived
`createClient(NULL)` per shard, the AOF-loader pattern (`src/aof.c:1491,1560`) — never against
the coordinator's client (which lives on another thread and owns a socket this thread must not
touch):

```c
void shardExecOne(shard *self, shardExecJob *job) {
    client *x = self->executor;            /* per-shard, socket-less, reply goes to c->buf/c->reply */
    x->argv = job->argv; x->argc = job->argc; x->slot = job->slot;
    x->db   = server.db + job->dbid;

    call(x, CMD_CALL_FULL & ~CMD_CALL_PROPAGATE);   /* Phase 4 = reads; no journal/propagation yet */

    /* Detach the reply as a flat byte blob. x->buf (bufpos) + x->reply list -> one sds. */
    sds bytes = shardDetachReply(x);       /* concat x->buf[0..bufpos] + each reply node */
    resetClient(x);                        /* frees argv-independent per-command state on the executor */

    shardResult *res = zmalloc(sizeof(*res));
    res->handle = job->handle; res->coordinator = job->coordinator; res->bytes = bytes;
    spscEnqueue(&server_shards[job->coord_shard].results, tagRes(res, SHARD_RES_DONE), true);
    shardWakeLoop(job->coord_shard);       /* cross-thread wake of the coordinator's el (§3.5) */
}
```

The owner produces **RESP bytes**, not a formatted-for-this-socket reply — the coordinator owns
formatting/encoding decisions (RESP2/3, push frames). In practice `call()` on the executor
already emits RESP into `x->buf`/`x->reply`; `shardDetachReply` just concatenates those into one
`sds` and clears them. The executor's protocol version must be pinned to the coordinator
client's (`x->resp = c->resp`) so map/set/double encodings match what the real client
negotiated — set it per job.

### 3.4 Coordinator side — deliver the continuation and resume

The coordinator's `shardBeforeSleep` drains its `results` queue:

```c
void shardDrainResults(shard *self) {
    void *items[64]; size_t n;
    while ((n = spscDequeueBatch(&self->results, items, 64))) {
        for (size_t i = 0; i < n; i++) {
            shardResult *res = untagRes(items[i]);
            client *c = shardLookupLiveClient(res->coordinator, res->handle);  /* §4 disconnect guard */
            if (c) {
                _addReplyProtoToList(c, c->reply, res->bytes, sdslen(res->bytes)); /* src/networking.c:700 */
                c->reply_bytes += sdslen(res->bytes);
                putClientInPendingWriteQueue(c);      /* flush on this shard's beforeSleep */
                unblockClient(c, /*queue_for_reprocessing=*/1);  /* src/blocked.c:217 */
            }
            sdsfree(res->bytes); zfree(res);
        }
    }
}
```

`unblockClient(..., 1)` puts `c` on `server.unblocked_clients` (per-shard under Model B);
`processUnblockedClients` (`src/blocked.c:158`) then calls
`processPendingCommandAndInputBuffer` (`src/networking.c:3962`). Here is the subtle part worth
stating explicitly: **the resume must NOT re-execute the command.** The reply is already
appended and this was a read (nothing to redo). So `BLOCKED_SHARD` clears `pending_command`
and lets the normal post-unblock flow finalize stats (`updateStatsOnUnblock`,
`src/blocked.c:129`) and reset the client — it re-enters `processPendingCommandAndInputBuffer`
only to *finish* the command's bookkeeping and then continue parsing the **next** pipelined
command from the held querybuf. Concretely: set `pending_command = 0` in the `BLOCKED_SHARD`
arm of `unblockClient` (mirroring how `BLOCKED_WAIT` finalizes without redo), append reply
before unblocking, and the re-drive resumes at the *next* command. This is the one place the
"reuse BLPOP" analogy needs care: BLPOP re-runs its command on wake; REMOTE does not.

### 3.5 The cross-thread wake — the only piece with no in-tree analog

`handleClientsBlockedOnKeys` wakes blocked clients **inline on the same thread**. A REMOTE
result is produced on the owner thread while the coordinator sits in `aeApiPoll`. So each shard
loop needs a **wake fd** (an `eventfd`/self-pipe registered on its own `el` with a no-op read
handler); `shardWakeLoop(coord_shard)` writes one byte to force `aeApiPoll` to return, after
which `shardBeforeSleep` → `shardDrainResults` runs. This is the standard "async loop wake"
pattern; it is new to Valkey's client path but trivial and well-trodden. Coalesce writes (only
wake if the results queue transitioned empty→non-empty) to avoid a syscall per result.

## 4. Correctness details the sketch must pin down

- **argv lifetime across the hop.** `job->argv` borrows the coordinator client's argv. The
  coordinator is `BLOCKED_SHARD`, so `commandProcessed` did **not** reset it
  (`src/networking.c:3887`) — argv stays valid for the whole hop. Rule: the owner **reads**
  argv, never frees it; the coordinator frees argv only in `resetClient` after the result is
  delivered. The `robj`s are shared read-only across threads for the hop's duration; this is
  safe only because they are not mutated (reads) — a write path (Phase 5) must either deep-copy
  argv into the job or guarantee the coordinator won't touch them, and that is a Phase-5
  decision, not smuggled in here.
- **Client disconnect mid-hop.** The coordinator may free the client (connection reset,
  `CLIENT KILL`, timeout) while a job is in flight. The job carries `handle = c->id` (a
  monotonic id, `src/server.h`), not just the pointer. On result delivery,
  `shardLookupLiveClient` validates the pointer is still live **and** the id matches; a freed
  or recycled client drops the result. Symmetrically, freeing a `BLOCKED_SHARD` client must
  mark it so a late result is discarded — reuse the blocked-client teardown path
  (`unblockClient` is already called from client-free for other block types).
- **A REMOTE command that itself blocks on the owner (Phase 5+).** `BLPOP` on a remote slot
  would block *on the owner shard*. In Phase 4 (reads only) this cannot arise. When it does, the
  owner blocks its executor on keys as usual and posts the result only when the owner-side
  block resolves; the coordinator client stays `BLOCKED_SHARD` throughout. Flagged here so the
  Phase-4 `CMD_SHARD_SAFE` set explicitly excludes blocking commands.
- **Per-client causal order (read-your-writes).** Writes take the barrier (§3.1), which
  quiesces all shards and runs on the coordinator synchronously before the client's next
  command is parsed; a subsequent REMOTE read therefore observes the write. Within a client,
  `BLOCKED_SHARD` holds the querybuf so a read is never dispatched ahead of an earlier command.
- **No reply reordering under pipelining.** Same mechanism: the coordinator processes one
  client's commands strictly in order, and each REMOTE command fully resumes (reply appended)
  before the next is parsed. Reply bytes are appended to `c->reply` in dispatch order.

## 5. Files touched

- `src/shard.c` / `src/shard.h` — **extend** (parent §14.2 defines the module): `shardDispatch`
  branch, `shardRemoteBegin`, `shardExecOne`, `shardDetachReply`, `shardDrainResults`,
  `shardWakeLoop`, the per-shard executor client + wake fd, `commandNeedsBarrier`, `myShardId`.
- `src/server.h` — add `BLOCKED_SHARD` to `blocking_type` before `BLOCKED_NUM`
  (`:349`); add `CMD_SHARD_SAFE` command flag. No new `ClientFlags` bit (reuse
  `blocked`/`pending_command`).
- `src/blocked.c` — `unblockClient` (`:217`) gains a `BLOCKED_SHARD` arm that finalizes
  **without re-execution** (clear `pending_command`, run `updateStatsOnUnblock`); teardown on
  client-free discards in-flight results.
- `src/server.c` — `processCommand` (`:4315`) tail calls `shardDispatch(c)` instead of `call()`.
- `src/networking.c` — no change to `commandProcessed`/`processCommandAndResetClient`; they
  already do the right thing for blocked clients (the point of §1). Reply append reuses
  `_addReplyProtoToList` (`:700`).
- `src/commands/*.json` — add `SHARD_SAFE` to the Phase-4 read set; regenerate `commands.def`.
- `src/unit/test_shard_dispatch.cpp` — **new**: branch-selection + detach/reattach unit tests.
- `tests/unit/shard-remote.tcl` — **new**: end-to-end LOCAL/REMOTE read correctness.

**Not touched in Phase 4:** the journal/sequencer (Phase 5), propagation, `call()` internals,
connection migration (§5b Change 4 / Phase 4a).

## 6. Test plan

**Unit (`src/unit/test_shard_dispatch.cpp`)** — no threads; drive the pure pieces:

1. `BranchSelectsLocalWhenOwnerIsSelf` — stub `slot_to_shard`/`myShardId`; a single-slot
   `SHARD_SAFE` read on an owned slot picks LOCAL (no job enqueued).
2. `BranchSelectsRemoteForForeignSlot` — foreign slot enqueues exactly one `SHARD_REQ_EXEC`
   and blocks the client as `BLOCKED_SHARD`.
3. `BranchSelectsBarrierForNonSafeOrMultiSlot` — non-`SHARD_SAFE` cmd, keyless cmd, and
   multi-slot argv all route to `shardBarrierRun`.
4. `DetachReattachRoundTrips` — a reply built into an executor client's `buf`+`reply` list,
   detached to `sds`, reattached to a second client, yields byte-identical output for RESP2
   and RESP3 (verifies the encoding pin in §3.3).

**Integration (`tests/unit/shard-remote.tcl`)** — real server, `shard-threads > 1`:

5. LOCAL/REMOTE transparency — with keys spread so some land on foreign shards, `GET`/`MGET`
   (single-slot in cluster) return identical results to `shard-threads 1`.
6. Pipeline ordering — a pipeline mixing LOCAL and REMOTE reads returns replies in request
   order.
7. Disconnect mid-hop — kill a client with an in-flight REMOTE read (inject latency on the
   owner); server does not crash, no leaked block, `blocked_clients` returns to 0.
8. Read-your-writes — `SET` (barrier) then `GET` (REMOTE) on the same connection observes the
   write.
9. No-op at default — `shard-threads 1`: the whole existing suite passes unchanged (dispatch
   is the identity branch).

**Backpressure/perf (`src/valkey-benchmark`):** saturate one owner from many coordinators;
confirm the §3.2 full-inbox path degrades gracefully (no coordinator spin), and publish the
REMOTE per-command tax vs LOCAL (the break-even the parent §11 demands).

## 7. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardDispatch*'`.
2. `./runtest --single unit/shard-remote`.
3. Regression: `./runtest` with default config — zero behavior change (identity branch).
4. Manually: `shard-threads 4`, cluster mode; `DEBUG SLOT-SHARD` (Phase 2) to find a foreign
   slot; `GET` a key on it; confirm the reply is correct and `INFO clients` shows the client
   transiently `BLOCKED_SHARD` under load.

## 8. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §5 (execution
  paths), §5a (connection mechanics / continuation), §5b (per-thread loop — prerequisite),
  §14.3 (the dispatch sketch this note refines), §14.5 (barrier).
- Phase 2 (prerequisite): [proposal-slot-per-thread-phase2.md](proposal-slot-per-thread-phase2.md).
- Code: `src/server.c:4315` (`processCommand`), `src/networking.c:3879,3932,3962`
  (blocked-client return contract), `src/blocked.c:106,158,217` (block/resume framework),
  `src/server.h:340` (`blocking_type`), `src/aof.c:1491` (socket-less executor precedent),
  `src/queues.h` (transport).
