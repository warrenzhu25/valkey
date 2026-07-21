# Proposal — Slot-per-thread Phase 4a: connection migration

**Status: pre-issue draft.** This is the concrete, line-anchored design of **Phase 4a** of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — moving a live client connection
from one shard thread to another, so that a client whose traffic targets a foreign slot stops
paying a REMOTE hop on every command.

Parent §5b Change 4 calls this "the genuinely fiddly part" and insists it is "scope, not
polish." Parent §5a explains why: **without migration, a badly-placed client pays a REMOTE hop on
every command, forever**, and static placement cannot fix a client whose hot slot lives on
another thread.

Prerequisites: [Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md) (per-thread event
loops, `conn->el`, the adopt path) and [Phase 4 part 2](proposal-slot-per-thread-phase4.md)
(LOCAL/REMOTE dispatch — without it there is no REMOTE ratio to measure and nothing to optimize).

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Connection migration — move a live connection to the shard its traffic targets, so arbitrary
> clients go LOCAL. Independently gated because live migration (fd re-arm across loops, partial
> buffers, blocking state) is the fiddliest single piece." (parent §10.4a)

**In scope:** the transfer protocol for a live `client` + `connection` between shard loops; the
safety predicate that decides when a client is transferable; the policy that decides when and
where to move it; the observability to tell whether it is working.

**Out of scope:** placement at accept (part 1 §4.3 — round-robin); slot rebalancing between
shards (parent §9 — that moves *data*, this moves *clients*, and the two should not be
conflated); writes and the journal (Phase 5).

**The honest framing.** Migration is an optimization whose absence is a correctness-neutral
performance cliff. It should therefore be built so that **failing to migrate is always safe**:
every decision point can decline, and the client simply keeps paying REMOTE hops. A migration
system that must succeed is far harder than one that may give up, and there is no reason to
build the harder one.

## 2. Verified anchors (this checkout)

- **What a client owns, and therefore what must move.** `struct client` (`src/server.h`):
  `connection *conn` (`:1300`), `sds querybuf` (`:1302`) — partially-parsed input,
  `char *buf` + `list *reply` (`:1332`, `:1334`) — unflushed output,
  `listNode clients_pending_write_node` (`:1339`) — embedded node, linked into a *list owned by a
  shard*, `blockingState *bstate` (`:1329`, lazily created), `multiState *mstate` (`:1328`),
  `hashtable *pubsub_channels` (`:1203`), `rax *client_tracking_prefixes` (`:1210`),
  `int slot` (`:1378`), `volatile uint8_t io_read_state` / `io_write_state` (`:1356-1357`),
  `_Atomic(size_t) io_tracked_reply_len` (`:1373`).
- **The embedded pending-write node is the sharp edge.** `clients_pending_write_node` is a
  `listNode` *inside* the client struct (`src/server.h:1339`), linked with
  `listLinkNodeHead(server.clients_pending_write, &c->clients_pending_write_node)`
  (`src/networking.c:422`) and unlinked at `src/networking.c:2045-2046`. It is not a pointer that
  can be reassigned — the node lives in the client and is threaded into whichever shard's list.
  Migration must unlink it on the source thread and relink on the destination thread, never
  leaving it linked into a list another thread is walking.
- **Registries the client is a member of.** `server.clients` + `c->client_list_node`
  (`src/networking.c:212,216`), `server.clients_index` rax keyed by client id (`:218`, looked up
  at `:2426`), plus the per-shard lists part 1 §4.5 introduces
  (`clients_pending_write`, `unblocked_clients`, `clients_to_close` — `src/networking.c:422`,
  `:2055-2057`, `:2250-2251`).
- **The precedent for "this client is busy, skip it."** `clientsCron` already refuses to touch a
  client with I/O in flight: `if (c->io_read_state != CLIENT_IDLE || c->io_write_state !=
  CLIENT_IDLE) continue;` (`src/server.c:1236`). The migration safety predicate is the same idea,
  with more conditions.
- **Blocking framework** (used to define "quiescent"): `blockClient` (`src/blocked.c:106`),
  `unblockClient` (`src/blocked.c:217`), `processUnblockedClients` (`src/blocked.c:158`),
  `blocking_type` enum (`src/server.h:340-351`), and part 2's `BLOCKED_SHARD`.
- **The fd registration calls that must run on the owning thread.** `aeCreateFileEvent` /
  `aeDeleteFileEvent` via `connSetReadHandler` (`src/connection.h:282`), `connSetWriteHandler`
  (`:275`), `connSetWriteHandlerWithBarrier` (`:291`), `connHasReadHandler` (`:389`) — all of
  which reach `src/socket.c:239-254` (and the TLS mirror `src/tls.c:1201-1208,1250-1260`), where
  part 1 §4.2 replaced `server.el` with `conn->el`.
- **Transport for the control messages.** `spscQueue` (`src/queues.h:112`), `spscEnqueue`
  (`:136`), `spscDequeueBatch` (`:140`).

## 3. Design

### 3.1 The protocol

The invariant that makes this safe is one sentence: **at every instant, exactly one thread may
touch a given `client`, and that thread is the one whose loop the fd is registered on.** The
protocol is therefore a hand-off with a gap in the middle where the fd is registered *nowhere*
and the client belongs to the message in flight.

```text
   SOURCE shard A (owns the client now)            DEST shard B
   ────────────────────────────────────            ───────────────────────────────
   1. policy proposes move (§3.3)
   2. safety predicate says yes (§3.2)
   3. DETACH — all on A's thread:
        connSetReadHandler(conn, NULL)   ─ fd unregistered from A->el
        connSetWriteHandler(conn, NULL)
        unlink clients_pending_write_node from A's list
        remove from A's clients / unblocked / close lists
        conn->el = NULL          ← "registered nowhere" window opens
   4. spscEnqueue(B->inbox, SHARD_REQ_ADOPT_LIVE{client*}) ──────┐
      shardWakeLoop(B)                                            │
      (A never touches this client again)                        ▼
                                                    5. ADOPT — all on B's thread:
                                                         conn->el = B->el
                                                         link into B's clients list
                                                         relink pending-write node if
                                                           the client had unflushed output
                                                         connSetReadHandler(conn, readQueryFromClient)
                                                         re-arm write handler if needed
                                                       ← window closes; B owns it
```

**During the window (step 4) the socket is not polled by anyone.** That is safe: incoming bytes
sit in the kernel receive buffer, and outgoing bytes sit in `c->buf`/`c->reply`. It is *not* safe
for the window to be unbounded, so:

- The adopt message must never be dropped. If B's inbox is full, **abort the migration**: A
  re-arms the handlers and keeps the client. Aborting is always allowed (§1), and this is the
  first place to use it.
- B must process adoptions with priority over ordinary work in `shardBeforeSleep`, so the window
  is one loop iteration.
- A watchdog counts clients in the window; a non-zero count that persists across cron cycles is a
  bug, and should log loudly rather than silently stall a connection.

**Nothing is copied.** The `client` and `connection` structs move by pointer; only *ownership*
changes. This is what makes the operation cheap and, more importantly, what keeps `c->querybuf`,
`c->buf`, `c->reply`, `mstate`, `bstate`, and the tracking rax trivially intact — they are the
same memory, reachable from exactly one thread at a time. The only things that genuinely need
work are the **list memberships** (§2: the embedded `clients_pending_write_node`) and the **fd
registration**.

### 3.2 The safety predicate — when a client may move

Refuse unless *all* of these hold. Each one is a condition under which some other thread or some
in-flight operation still has a claim on the client:

| Condition | Why |
|---|---|
| `c->io_read_state == CLIENT_IDLE && c->io_write_state == CLIENT_IDLE` | An I/O worker may be mid-read/write on this client's buffers. Same guard `clientsCron` uses (`src/server.c:1236`). |
| Not `c->flag.blocked` — in particular not `BLOCKED_SHARD` | A REMOTE result is in flight carrying this client's pointer (part 2 §4). Moving it would deliver the reply on the wrong thread. Also excludes `BLPOP`/`WAIT`/`XREAD` waiters, whose wake-up state is registered with the *source* shard. |
| No in-flight command: not `c->flag.pending_command`, `c->argc == 0` | Migrating mid-command means two threads reason about one execution. |
| Not inside `MULTI` (`c->flag.multi` / `c->mstate` non-empty) | The queued commands and the `WATCH` set are consistent with the source shard's view. Deferring until `EXEC` completes costs nothing. |
| Not a replica, not the primary link, not a monitor, not a module-blocked client | These connections are shard 0's by construction (part 1 §4.2 rule 1) and must never move. |
| No pending TLS handshake / `connTypeHasPendingData()` for this conn | Transport-level state that assumes a loop. |
| Not currently being freed (`c->flag.close_asap` / on a close list) | Racing a teardown. |
| Cooldown since its last migration has elapsed | §3.3. |

The predicate is evaluated **on the source thread**, which owns the client, so it reads only
thread-local state — no locking, and no TOCTOU window, because nothing else can change these
fields.

**Clients that subscribe.** Pub/Sub is global and routed through the coordinator (parent §8), so
a subscriber may migrate — but its `pubsub_channels` (`src/server.h:1203`) participates in the
global channel registry (`server.pubsub_channels`, `src/server.h:2252`). Until that registry is
made shard-aware, treat "has any subscription" as a refusal condition. It is a small population
and a large correctness surface.

### 3.3 The policy — when and where to move

The mechanism is the hard part; the policy should be as dumb as it can be while still working.

**Measurement.** Each client keeps a tiny per-shard histogram of the shards its recent commands
targeted: `uint16_t shard_hits[N]` decayed periodically (or an exponentially-weighted top-1
estimate if N is large). Updated in dispatch (part 2 §3.1), which already computes `owner`, so it
costs one increment on a cache line the thread already owns.

**Trigger.** In `shardBeforeSleep` (or on a slower per-shard cron tick, which is cheaper and
plenty responsive), for each client: if `shard_hits[best] / total > threshold` (start at 0.8),
`best != myShardId()`, and the sample count exceeds a floor (say 100 commands, so a handful of
early commands cannot move a connection), propose migration to `best`.

**Damping — the part that will actually bite.** Without it, a client whose traffic alternates
between two shards will ping-pong, and migration is not free (a loop-iteration stall plus two
`epoll` mutations plus cache-cold buffers on the destination).

- Per-client **cooldown**: no migration within N seconds of the last one; back off multiplicatively
  on repeated moves of the same client.
- Per-shard **rate cap**: at most K migrations per shard per second, so a topology shift cannot
  turn into a migration storm.
- **Hysteresis**: require the dominant shard to stay dominant across two consecutive evaluation
  windows.
- **Global kill switch**: `shard-connection-migration no` disables the whole mechanism, which is
  how an operator recovers from pathological behavior without a restart, and how a benchmark
  isolates its effect.

**A client with no dominant shard should not move at all.** A client spreading uniformly over
all slots cannot be made LOCAL; migrating it just adds cost. The threshold handles this, but say
it explicitly, because the tempting "move it to the least-loaded shard" heuristic is load
balancing, not locality, and this mechanism should do exactly one thing.

### 3.4 Interaction with slot rebalancing

Parent §9 allows `slot_to_shard[]` to be re-partitioned under a barrier — moving *data* ownership
between threads. That instantly invalidates every client's affinity measurement, and could
trigger a stampede of migrations.

Rule: **on any `slot_to_shard[]` change, reset all affinity histograms and start the cooldown
clock**. Rebalancing is rare and already takes a barrier, so this is a cheap addition at exactly
the right place. Without it, the two mechanisms will fight each other, and that failure mode is
very hard to diagnose from the outside.

### 3.5 Observability

Migration is a background behavior that silently determines whether the whole design delivers.
It must be visible or it cannot be tuned:

- `INFO`: `shard_migrations_total`, `shard_migrations_refused` broken down by predicate reason,
  `shard_migrations_aborted` (inbox full), current in-window count.
- Per-shard: client count, and the aggregate **LOCAL hit ratio** — the single number that says
  whether placement is working. Publish it per shard and overall.
- `CLIENT INFO` / `CLIENT LIST`: the client's home shard, its dominant target shard, and its
  migration count. This is what turns "my p99 is bad" into "this client is pinned to the wrong
  shard and keeps getting refused because it holds a subscription."
- A `DEBUG SHARD-MIGRATE <client-id> <shard>` for tests to force a migration deterministically —
  without it, §5's tests depend on policy timing, which makes them flaky.

## 4. Files touched

- `src/shard.{c,h}` — `shardMigrateConnection(client *c, int dst)`, the `SHARD_REQ_ADOPT_LIVE`
  message and its handler, the safety predicate, the policy evaluator, per-shard rate limiting,
  counters.
- `src/server.h` — per-client affinity state (`shard_hits[]`, last-migration timestamp, migration
  count) on `struct client`; keep it small and cold-packed, since it is per-connection memory.
- `src/networking.c` — factor client registry link/unlink into `clientAttachToShard(c, shard)` /
  `clientDetachFromShard(c)` covering `client_list_node` (`:212-216`),
  `clients_pending_write_node` (`:422`, `:2045-2046`), `unblocked_clients` (`:2055-2057`), and
  `clients_to_close` (`:2250-2251`). Part 1 already needs this factoring for accept-time adoption;
  4a reuses it for both sides of the hand-off.
- `src/blocked.c` — the predicate consults `c->flag.blocked` / `bstate->btype`; no behavior
  change.
- `src/config.c` — `shard-connection-migration` (bool, default **yes** once proven; ship it
  **no** and flip after the benchmark), `shard-migration-threshold`,
  `shard-migration-cooldown-ms`, `shard-migration-max-per-sec`.
- `src/debug.c` — `DEBUG SHARD-MIGRATE <client-id> <shard>`.
- `tests/unit/shard-migration.tcl` — **new**: §5.
- `src/unit/test_shard_policy.cpp` — **new**: the policy as a pure function over a histogram.

## 5. Test plan

**Unit (`src/unit/test_shard_policy.cpp`)** — no threads:

1. `NoDominantShardDoesNotMigrate` — uniform histogram proposes nothing.
2. `DominantForeignShardProposesMove` — 90% on shard 3 proposes shard 3.
3. `CooldownAndHysteresisSuppressPingPong` — alternating dominance across windows produces at
   most one move, then nothing until cooldown expires.
4. `RateCapHolds` — 10k eligible clients on one shard yield at most K proposals per second.

**Integration (`tests/unit/shard-migration.tcl`)**, using `DEBUG SHARD-MIGRATE` for determinism:

5. **Correctness across a move.** A client with a deep pipeline in flight and a partially-received
   command in `querybuf` is migrated; every reply arrives, in order, with no duplication and no
   loss. Run it with the migration forced at many different points via a fuzzing loop, because
   the interesting bugs live at boundaries the test author did not think of.
6. **Unflushed replies survive.** Force a migration while `c->buf`/`c->reply` hold unflushed data
   (client not reading); after the move, the destination flushes it correctly and in order.
7. **The predicate is honored.** A blocked (`BLPOP`) client, a `BLOCKED_SHARD` client, a client
   inside `MULTI`, a subscriber, and a replica each refuse migration; the refusal reason is
   counted in `INFO`.
8. **Abort path.** Fill the destination inbox; the migration aborts, the client keeps working on
   the source shard, `shard_migrations_aborted` increments, and nothing is left half-detached.
9. **Disconnect during the window.** Kill the client between detach and adopt; no crash, no leak,
   no double free, `connected_clients` returns to baseline.
10. **Rebalance interaction.** Change `slot_to_shard[]` under load; histograms reset, no
    migration storm, LOCAL ratio recovers.
11. **Policy end to end.** Start 100 clients with round-robin placement and slot-affine traffic;
    within a bounded time the LOCAL hit ratio exceeds 90% and migrations stop. **This is the
    phase's headline test** — it is the claim parent §5a makes about Dragonfly, tested.
12. **Kill switch.** With migration disabled, behavior is identical to Phase 4 and the LOCAL ratio
    stays at its round-robin baseline.

## 6. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardPolicy*'`.
2. `./runtest --single unit/shard-migration`.
3. Full suite at `shard-threads 4` with migration **on**, and again with it **off** — identical
   results.
4. **TSan** with migration on and an aggressive policy (threshold 0.5, no cooldown) under load.
   This configuration exists to break things; it is the strongest signal available on whether the
   hand-off is really exclusive.
5. ASan run of §5.9 (disconnect-in-window) in a loop.
6. Benchmark: round-robin placement, migration off vs on. Report LOCAL hit ratio, throughput, and
   p99 latency, plus the transient cost of a migration burst at the start of the run. **The gap
   between off and on is the entire justification for this phase** — if it is small, say so, and
   consider whether smarter accept-time placement plus client-side hints would have been enough.

## 7. Honest risks

- **The window is a real stall.** A migrating connection is unpolled for a loop iteration. Under a
  latency SLO this shows up as a p99 blip correlated with migration bursts. The rate cap bounds
  it; the benchmark must report it rather than average it away.
- **The predicate will be wrong before it is right.** Every refusal condition in §3.2 is a claim
  that no other thread holds a reference. New features add new references — a future
  cross-shard blocking type, a new module API, a tracking mechanism — and each is a chance to
  migrate a client someone else is holding. Mitigation: make the predicate one function with a
  comment naming *why* each condition exists, and add an assertion on the destination that the
  client's every list membership is empty on arrival.
- **Policy is where this gets tuned forever.** Thresholds, decay, and cooldown are workload
  dependent. Ship conservative defaults and the kill switch; resist adding adaptivity until real
  traffic asks for it.
- **It may not be worth it.** If accept-time placement plus cluster-aware clients (which already
  know which node owns a slot, parent §5) get most of the LOCAL ratio, this phase's complexity
  buys little. §6.6 is designed to answer that honestly, and "we measured it and skipped 4a" is a
  legitimate outcome — it is why parent §10 gates it separately.
- **Subscribers and blocked clients are permanently excluded** under §3.2 as written. For a
  workload dominated by blocking commands or pub/sub, migration does approximately nothing.
  Name that population before promising the feature.

## 8. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §5a (why migration is
  scope, not polish), §5b Change 4, §9 (slot rebalancing — the other thing that moves), §10.4a.
- Prerequisites: [Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md) (the loops and the
  adopt path this reuses), [Phase 4 part 2](proposal-slot-per-thread-phase4.md) (`BLOCKED_SHARD`,
  the REMOTE hop this exists to eliminate).
- Background: [03-clients-and-networking.md](03-clients-and-networking.md) (client struct and
  reply buffers).
- Code: `src/server.h:1203,1210,1300,1302,1328,1329,1332,1334,1339,1356,1378`,
  `src/networking.c:212,422,2045,2055,2250,2426`, `src/server.c:1236` (the busy-client guard
  precedent), `src/connection.h:275,282,291`, `src/blocked.c:106,158,217`, `src/queues.h:112`.
