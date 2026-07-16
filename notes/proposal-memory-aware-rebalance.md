# Proposal — Per-slot memory tracking & memory-aware rebalancing

**Status: pre-issue draft.** Companion to
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md), but unlike the
other proposals in this folder **this one does not depend on the threading rearchitecture**
([slot-per-thread](proposal-slot-per-thread.md) / [VLL](proposal-vll-transactions.md)). It
extends a feature Valkey **already shipped** — `CLUSTER SLOT-STATS` — and improves a tool
Valkey **already ships** — `valkey-cli --cluster rebalance`. That makes it the most
immediately actionable idea in this series: no new threads, no new hash table, no
replication redesign.

Like [09-dragonfly-snapshot-model.md](09-dragonfly-snapshot-model.md), Part A studies
**Dragonfly** at "vendor says so" confidence; Part B maps onto Valkey with verified
`file:line` anchors.

---

# Part A — How Dragonfly does it

## A1. Two problems, one dependency

Cluster operators hit the same wall in every KV store: **slots are not equal**. Redis and
Valkey rebalance clusters by **slot count** — move slots until every node owns roughly the
same *number* of slots — which silently assumes every slot holds roughly the same amount of
data. Real keyspaces are skewed: a handful of slots can hold most of the bytes (a big hash
tag, a hot user-id prefix). Balance the slot *count* and you can still end up with one node
at 90% memory and another at 30%.

Fixing that needs two things, and the second depends on the first:

1. **Know how many bytes each slot holds** — cheaply, continuously, without scanning.
2. **Rebalance to equalize bytes**, not slot count — which requires (1) to pick *which*
   slots to move.

Dragonfly does both. The interesting engineering is (1); (2) is a policy change that (1)
unlocks.

## A2. The tracking mechanism — incremental deltas, never a scan

Dragonfly keeps per-slot statistics (`SlotStats`) that include `memory_bytes` alongside
key count and read/write counters. The load-bearing design decision:

> **Memory is accounted incrementally at the mutation site, never by scanning a slot.**

When a key is added, its byte size is **added** to its slot's counter. When removed,
**subtracted**. When a value is modified in place, the counter is adjusted by the **delta**
(new size − old size). The per-slot number is always up to date and is never recomputed
from scratch — reading it is O(1), maintaining it is O(1) per write.

This only works if "the byte size of this object" is cheap to obtain at the mutation site.
It is, in Dragonfly, because of the allocator/representation:

- **`CompactObj` knows its own allocation footprint cheaply.** Dragonfly's value
  representation is designed so that `MallocUsed()` is a near-constant-time query, not a
  deep recursive walk. Small values are inlined; larger ones carry their own size.
- Dragonfly uses **mimalloc** and can query actual allocated block sizes cheaply, so the
  "bytes" it charges reflect real allocator usage, not a hand-rolled estimate.

So each write already computes (or trivially knows) the size delta it caused, and charging
that delta to a slot counter is a couple of integer ops. **The expensive operation — a deep
size traversal of a nested structure — never happens on the write path.** That is the whole
trick.

## A3. Memory-aware rebalancing / migration

With `memory_bytes` per slot available:

- **Balance by bytes.** The rebalancer computes each node's total memory and moves slots so
  that *memory* is equalized across nodes (optionally weighted), instead of equalizing slot
  count.
- **Pick slots intelligently.** To move ~X bytes off an overfull node, choose the specific
  slots whose sizes sum closest to X — a small bin-packing choice that moves the fewest
  slots for the needed byte transfer, rather than moving an arbitrary count.
- **Estimate migration cost up front.** Bytes-per-slot is also the size of the data that
  has to cross the wire, so the planner can predict migration time and avoid needlessly
  shipping a giant slot.

## A4. Confidence

The *existence* of per-slot memory accounting, its incremental (mutation-site) nature, and
memory-aware balancing are from Dragonfly's docs/behavior. Exact struct/field names
(`SlotStats.memory_bytes`), and whether the charged bytes come from mimalloc block sizes vs.
an internal estimate, are **(approx)** — verify against `src/server/cluster/` and
`src/core/compact_object.*` before relying on specifics.

---

# Part B — How it maps onto Valkey

## B1. Valkey is 80% of the way there already

Valkey shipped `CLUSTER SLOT-STATS` (`src/cluster_slot_stats.c`). It maintains **per-slot
counters, updated by a per-command hook, behind an opt-in flag** — which is *exactly* the
skeleton per-slot memory tracking needs. Everything except the memory metric itself is
already built and tested:

| Piece needed | Already in Valkey | Anchor |
|---|---|---|
| Per-slot stat array | `slotStat slot_stats[CLUSTER_SLOTS]` | `src/cluster_legacy.h:497` |
| Per-slot stat struct (add a field here) | `slotStat { cpu_usec; network_bytes_in/out }` | `src/cluster_legacy.h:426` |
| Per-command accounting hook | `clusterSlotStatsAddCpuDuration(c, c->duration)` after `call()` | `src/server.c:4047`, fn `src/cluster_slot_stats.c:222` |
| Opt-in flag (free when off) | `cluster-slot-stats-enabled`, default `0` | `src/config.c:3389`, `src/server.h:2299` |
| Reply plumbing / sort-by-metric | `getSlotStat` / `collectAndSortSlotStats` | `src/cluster_slot_stats.c:51,80` |
| Per-slot **key count** (already O(1)) | `kvstoreHashtableSize(db->keys, slot)` | `src/cluster.c:831` |

The `slotStat` struct at `src/cluster_legacy.h:426` has `cpu_usec`, `network_bytes_in`, and
`network_bytes_out` — **and no memory field.** Adding `uint64_t memory_bytes;` there, a
`MEMORY_BYTES` case in `getSlotStat` (`src/cluster_slot_stats.c:51`), and a new metric in
the reply is the *entire* surface-area of the API change. The hard part is not the API; it
is filling that counter cheaply (§B3).

## B2. Why key count was easy and memory is not

Valkey already exposes per-slot **key count** for free because the `kvstore` is physically
partitioned per slot — `countKeysInSlot()` is just `kvstoreHashtableSize()`
(`src/cluster.c:831`), an O(1) read of a table's element count. No accounting needed; the
data structure *is* the counter.

Memory has no such free structure. Valkey's only "how big is this key" primitive is
`objectComputeSize()` (`src/object.c:1225`) — a **deep, recursive walk** that even *samples*
large aggregates because visiting every element is too slow (it's what `MEMORY USAGE` calls,
`src/object.c:1821`). Calling that on every write is a non-starter. So the Dragonfly
mechanism (§A2) has to be rebuilt on a representation that was **not** designed to make
`MallocUsed()` cheap. That mismatch is the crux of this proposal.

## B3. The tracking design for Valkey — cheap deltas, honest approximation

The key realization: **rebalancing needs a good relative measure, not an exact byte count.**
`MEMORY USAGE`'s exactness (deep walk + sampling) is overkill for "which node is fatter."
So charge each slot a **cheap, top-level allocation estimate**, updated by delta, and accept
approximation on nested structures.

**Cheap size sources Valkey already has:**
- `zmalloc_size(ptr)` — the allocator's actual block size for one allocation, O(1)
  (`src/object.c` uses it throughout `objectComputeSize`, e.g. `:1227`).
- `sdsAllocSize(s)` — a string's allocation, O(1).
- Most aggregate encodings already track their own footprint (listpack byte length in its
  header; quicklist maintains node sizes; hashtable/dict know entry counts).

**Where to hook — the mutation sites in `db.c`:**

| Event | Hook | Charge to `c->slot` |
|---|---|---|
| Key created | `dbAdd` `src/db.c:228` | `+ estimatedSize(key,val)` |
| Value replaced | `dbReplaceValue` `src/db.c:398` / `dbSetValue` `src/db.c:319` | `+ new − old` |
| Key deleted / expired | `dbDelete` `src/db.c:554` | `− estimatedSize(key,val)` |

**The gap this leaves — in-place value mutation.** `APPEND`, `HSET` on an existing hash,
`LPUSH`, `SADD`, `ZADD`, `SETRANGE` change a value's size **without** going through
`dbAdd`/`dbReplaceValue`. Those are the writes that actually grow a slot. Two options,
neither free:

- **(a) Type-command deltas.** Have the type commands report their size delta the way they
  already report keyspace events / dirty counts. Most precise, but touches every write
  command — real work.
- **(b) Recompute-on-touch, sampled.** On write, cheaply re-estimate the *top-level*
  allocation of the touched value (`zmalloc_size` of the object + its main allocation, not a
  deep walk) and apply the delta. O(1), misses deep nested growth, but tracks the dominant
  case (the value's primary allocation) well enough for balancing.

I'd ship **(b) first** (cheap, one hook, approximate) and only move to **(a)** for specific
hot commands if measurement shows the approximation misleads the rebalancer. This mirrors
Valkey's own choice to *sample* in `objectComputeSize` rather than always be exact.

**Two correctness traps specific to Valkey:**
- **Shared objects.** Small integers are shared and refcounted (`makeObjectShared`,
  `OBJ_SHARED_INTEGERS`, `src/object.c:161,441`). A shared int belongs to no single slot;
  charging its bytes to every slot that references it double-counts. Estimator rule: objects
  with `refcount == OBJ_SHARED_REFCOUNT` (or `> 1`) contribute **zero**.
- **Logical bytes ≠ RSS.** The counter sums *logical allocation* (zmalloc sizes). Because of
  jemalloc fragmentation, moving "X logical bytes" of keys off a node does **not** free X
  bytes of RSS. So this balances *logical* footprint; RSS follows only approximately. That's
  still dramatically better than slot count, but the doc/metric must say "logical bytes,"
  not promise RSS parity. (This is also why `MEMORY USAGE` and `used_memory` already differ
  from RSS today — consistent with existing Valkey behavior.)

All of this stays behind `cluster-slot-stats-enabled`, so a user who doesn't want the
per-write tax pays **nothing** — same contract as the existing cpu/network metrics.

## B4. Memory-aware rebalancing in `valkey-cli`

Today `clusterManagerCommandRebalance` (`src/valkey-cli.c:7936`) balances by slot count:

```c
int expected = (int)(((float)CLUSTER_MANAGER_SLOTS / total_weight) * n->weight);
n->balance = n->slots_count - expected;          /* src/valkey-cli.c:8005 */
```

`balance` is a **slot** surplus/deficit; the mover shuffles slot *counts* until balanced.
The memory-aware version changes the currency from slots to bytes:

1. Query per-slot memory via the extended `CLUSTER SLOT-STATS ... ORDERBY memory-bytes`
   (already sortable — `collectAndSortSlotStats`, `src/cluster_slot_stats.c:80`), and sum
   per node to get `node_memory`.
2. `expected_bytes = (total_memory / total_weight) * weight`;
   `balance = node_memory − expected_bytes`.
3. When moving slots off an overfull node, **select the slots whose byte sizes best fill the
   deficit** of the target node (greedy/bin-pack over the per-slot sizes), instead of moving
   an arbitrary slot count.

This is additive: keep slot-count balancing as the default and add
`--cluster-use-memory` (or reuse `--weight` semantics with a memory basis) so the behavior
change is opt-in and the existing tool is untouched for anyone who doesn't ask.

## B5. What this buys

- **Real balance.** Nodes end up near-equal on *bytes*, so no node OOMs while a peer sits
  half-empty — the actual operational failure that slot-count balancing causes.
- **Fewer, smarter moves.** Byte-aware slot selection moves less data to achieve balance,
  and lets the planner *estimate* migration time (bytes ÷ bandwidth) up front.
- **A visible per-slot memory metric** — useful on its own for capacity planning and finding
  hot-tag hotspots, independent of rebalancing, and it composes with **atomic slot
  migration** (`design-docs/atomic-slot-migration.md`) to prioritize/measure migrations.

## B6. Honest risks

- **The in-place-mutation gap (§B3) is the whole ballgame.** If option (b)'s top-level
  estimate diverges badly from true size for the workloads that matter (deeply nested
  hashes/sorted sets that grow element-by-element), the rebalancer optimizes a lie. This
  must be validated against `MEMORY USAGE` ground truth on real data before trusting it.
- **Per-write overhead, even if small.** A size estimate + counter update on every write is
  not zero. It's gated off by default, but when on it taxes the hot path; the tax must be
  measured, not assumed negligible.
- **Logical vs RSS (§B3).** Managing expectations here is a documentation problem that will
  generate "I rebalanced and RSS didn't move" reports if not stated loudly.
- **Memory balance can fight access balance.** The fattest slot and the hottest slot are
  often different slots; optimizing bytes can worsen CPU/network skew (which SLOT-STATS also
  measures). The rebalancer should expose the objective, not silently pick one. Balancing
  multiple objectives at once is genuinely harder and out of scope for a first cut.
- **Cross-DB.** Slots span all DBs (`countKeysInSlot` sums over `server.db[i]`,
  `src/cluster.c:835`); the memory counter must too, or it under-counts multi-DB setups.

## B7. Phasing

1. **Add `memory_bytes` to `slotStat` + the estimator behind `cluster-slot-stats-enabled`**,
   using option (b) (cheap top-level delta) at the `db.c` mutation hooks. Expose via
   `CLUSTER SLOT-STATS`. **This is independently shippable and independently useful** — an
   observability win with no rebalancer changes.
2. **Validate the estimate** against `MEMORY USAGE` on representative datasets; decide
   per-command whether option (a) precise deltas are needed anywhere.
3. **Add memory-aware `--cluster rebalance`** in `valkey-cli`, opt-in, reusing the sortable
   SLOT-STATS output.
4. *(Optional)* Feed per-slot bytes into atomic-slot-migration planning for cost estimates.

Stopping after step 1 still leaves Valkey strictly better off (a new, genuinely useful
metric). Nothing here is all-or-nothing.

## B8. What would make me abandon this

- **The cheap estimate can't be made trustworthy.** If option (b) is too inaccurate and
  option (a) means invasively instrumenting every write command for marginal rebalancer
  benefit, the cost/benefit tips toward "just run `MEMORY USAGE` sampling offline in the
  ops tooling" instead of tracking online.
- **The per-write overhead is material even when the feature is the point.** If turning it on
  measurably dents throughput, it becomes a niche diagnostic, not a standing metric.
- **Real clusters aren't slot-skewed enough to matter.** If measurement shows production
  keyspaces are close to uniform per slot, slot-count balancing was already fine and this is
  solving a non-problem. (I doubt this — hash-tag skew is a well-known pain — but it's the
  falsifiable claim the whole proposal rests on, so it's written down.)

## B9. Code anchors

| Thing | Where |
|---|---|
| Per-slot stat struct (add `memory_bytes`) | `src/cluster_legacy.h:426`; array `:497` |
| Metric switch (add `MEMORY_BYTES`) | `getSlotStat` `src/cluster_slot_stats.c:51` |
| Per-command accounting hook (model for memory) | `src/server.c:4047`; `clusterSlotStatsAddCpuDuration` `src/cluster_slot_stats.c:222` |
| Opt-in flag | `cluster-slot-stats-enabled` `src/config.c:3389`, `src/server.h:2299` |
| Sortable stats output | `collectAndSortSlotStats` `src/cluster_slot_stats.c:80` |
| Mutation hooks for the estimator | `dbAdd` `src/db.c:228`, `dbReplaceValue` `src/db.c:398`, `dbSetValue` `src/db.c:319`, `dbDelete` `src/db.c:554` |
| Cheap size primitives | `zmalloc_size`/`sdsAllocSize` in `objectComputeSize` `src/object.c:1225` |
| Deep size (what NOT to call on writes) | `objectComputeSize` `src/object.c:1225`, `MEMORY USAGE` `src/object.c:1821` |
| Shared-object trap | `makeObjectShared` `src/object.c:161`, `OBJ_SHARED_INTEGERS` `src/object.c:441` |
| Per-slot key count (the "free" precedent) | `countKeysInSlot` `src/cluster.c:831,835` |
| Rebalance currency to change | `clusterManagerCommandRebalance` `src/valkey-cli.c:7936`, balance calc `:8005` |

## B10. Sources (Dragonfly — vendor, not Valkey code)

- Dragonfly cluster / `SlotStats` docs and source (`src/server/cluster/`), `CompactObj`
  memory accounting (`src/core/compact_object.*`) — verify field names per §A4.
- Companion notes: [proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md)
  (the ranked plan this fits into) and
  [design-docs/atomic-slot-migration.md](../design-docs/atomic-slot-migration.md) (the
  migration machinery per-slot bytes would inform).
</content>
