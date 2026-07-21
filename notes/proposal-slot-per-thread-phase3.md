# Proposal — Slot-per-thread Phase 3: virtual slots for standalone

**Status: pre-issue draft, ready to implement.** This is the concrete, line-anchored design of
**Phase 3** of [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — making standalone
mode use the same per-slot `kvstore` partition cluster mode already uses, so that one routing
path serves both modes.

It assumes [Phase 2](proposal-slot-per-thread-phase2.md) (`slot_to_shard[]` + `shard-threads`)
has landed. It is still **pre-threading**: nothing dispatches by shard until Phase 4. But unlike
Phase 2, this phase is *not* a no-op — it changes a real data structure on the hot path, and it
carries an independent win that justifies it even if the threading program stops here.

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Virtual slots for standalone. Unifies the routing path; independently fixes standalone's
> rehash spike. Watch `SCAN` cursor semantics and `RANDOMKEY`." (parent §10.3)

Today `createDatabase` (`src/server.c:2893`) builds each database's three kvstores with
`slot_count_bits = 0` in standalone and `CLUSTER_SLOT_MASK_BITS` (14 → 16384 tables) only when
`server.cluster_enabled` (`src/server.c:2894-2899`). Phase 3 gives standalone the partitioned
kvstore too, hashing keys to **virtual slots** — the same `keyHashSlot` value, used purely as a
table index and a routing key, with **no cluster semantics attached**.

**In scope:** the kvstore fan-out for standalone, the key→index seam, `SCAN`/`RANDOMKEY`
behavior under a multi-table kvstore in standalone, and the memory-overhead decision.

**Out of scope:** threading and dispatch (Phase 4); the journal (Phase 5); any user-visible
cluster behavior in standalone — **no `-CROSSSLOT`, no `-MOVED`, no slot ownership, no
`CLUSTER` command changes**. A standalone server after this phase must be behaviorally
indistinguishable from one before it, except in memory and rehash latency.

## 2. Why this phase pays for itself

Two reasons, and the second one is the honest justification if the threading program stalls:

1. **One routing path.** Phase 4's dispatch reads `c->slot`. Today that field is populated only
   in cluster mode — `clusterSlotByCommand()` inside `prepareCommandGeneric`
   (`src/server.c:4276`), reset to -1 by `unprepareCommand` (`src/server.c:4304`). Without Phase
   3, standalone would need either a second routing mechanism or a permanent "standalone always
   takes the barrier" carve-out, which means **standalone gets no scaling at all**. Parent §11
   already warns standalone is the weaker case; this is the phase that stops it being the
   *hopeless* case.

2. **It fixes standalone's giant-rehash problem outright.** A standalone database is one
   `hashtable` per kvstore. Growing past a power of two rehashes *the whole table*: at 100M keys
   that is a multi-hundred-megabyte allocation and an incremental rehash that drags on every
   command touching the table. Cluster mode does not have this problem, because 16384 tables
   means each rehash is ~1/16384 the size — and `kvstore` already tracks rehashing per table
   (`rehashing` list and `resize_cursor` in `struct _kvstore`, `src/kvstore.c:60`). Phase 3 hands
   standalone that property for free. **This is shippable value with no threads involved**, and
   it is the thing to point at if someone asks what Phase 3 buys before Phase 4 exists.

## 3. Verified anchors (this checkout)

- **The branch to change.** `createDatabase` (`src/server.c:2893`):
  ```c
  int slot_count_bits = 0;
  int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND;
  if (server.cluster_enabled) {
      flags |= KVSTORE_FREE_EMPTY_HASHTABLES;
      slot_count_bits = CLUSTER_SLOT_MASK_BITS;
  }
  ```
  followed by three kvstores built from it: `db->keys`, `db->expires`,
  `db->keys_with_volatile_items` (`src/server.c:2902-2904`).
- **Databases are created lazily.** `createDatabaseIfNeeded` (`src/server.c:2917`) allocates on
  first use; `initServer` eagerly creates only db 0 (`src/server.c:3012`). This bounds the
  memory cost of §5 to databases actually used.
- **The key→index seam.** `getKVStoreIndexForKey` (`src/db.c:233`):
  `return server.cluster_enabled ? getKeySlot(key) : 0;` (`src/db.c:234`). One line, one place.
- **`getKeySlot` asserts cluster mode** (`src/db.c:240-241`: `serverAssert(server.cluster_enabled)`)
  and caches via `server.current_client->slot` when `flag.executing_command` is set and the
  client is not one we must obey (`src/db.c:252-257`), falling back to `keyHashSlot`
  (`src/cluster.c:58`). The `mustObeyClient` backfill at `src/db.c:260-262` exists because
  `getNodeByQuery()` never runs for primary/AOF-sourced commands.
- **`CLUSTER_SLOTS` = 16384** = `1 << CLUSTER_SLOT_MASK_BITS` (`src/cluster.h:10`);
  `MAX_HASHTABLES_BITS` = 16 (`src/kvstore.c:56`), so 14 is comfortably legal.
- **kvstore allocation cost.** In `kvstoreCreate` (`src/kvstore.c:294`): `kvs->hashtables =
  zcalloc(sizeof(hashtable *) * kvs->num_hashtables)` and `kvs->hashtable_size_index =
  kvs->num_hashtables > 1 ? zcalloc(sizeof(unsigned long long) * (kvs->num_hashtables + 1)) :
  NULL`. The tables themselves are lazy under `KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND`
  (`src/kvstore.c:314`).
- **The `SCAN` cursor encodes the table index.** `hashtableCursorToKvstoreCursor`
  (`src/kvstore.c:141`): `return (ht_cursor << kvs->num_hashtables_bits) | didx;` and the inverse
  at `src/kvstore.c:146-147`. **Changing `num_hashtables_bits` changes the cursor encoding.**
- **`kvstoreScan` already supports a table range.** `src/kvstore.c:476`, documented at
  `:472-475`: `first_idx == -1 && last_idx == -1` scans all; `>= 0` scans `[first_idx,
  last_idx]`. Cluster `SCAN` already uses it (`src/db.c:1355`), while the whole-store scan path
  passes `-1, -1` (`src/db.c:2338`).
- **`RANDOMKEY`** → `dbRandomKey` (`src/db.c:442`), reached from `randomkeyCommand`
  (`src/db.c:973`). Fair sampling across tables already exists and is used by eviction:
  `kvstoreGetFairRandomHashtableIndex` (`src/evict.c:116`), plus
  `kvstoreHashtableSampleEntries` (`src/kvstore.h:74`) and `kvstoreHashtableRandomEntry`
  (`src/kvstore.h:72`).
- **RDB carries no slot info on the save side in this checkout.** `RDB_OPCODE_SLOT_INFO` is
  defined at `src/rdb.h:146` ("Foreign slot info, safe to ignore") and appears only on the load
  path (`src/rdb.c:3274`). So the index is recomputed from the key at load time and **no RDB
  format change is implied by this phase**.
- **Dead declaration worth deleting while here:** `int calculateKeySlot(sds key);`
  (`src/server.h:3553`) has no definition anywhere in `src/`.

## 4. Design

### 4.1 The change itself

```text
   standalone TODAY                      standalone AFTER Phase 3
   ────────────────                      ────────────────────────
   db->keys ─► [ one hashtable ]         db->keys ─► [ 0 ][ 1 ][ 2 ] ... [ 16383 ]
               all keys, one rehash                   ▲                     (lazy: allocated
               of the whole table                     │                      on first key)
                                          index = keyHashSlot(key) & 16383
                                          — a *virtual* slot: a table index and a
                                            routing key, with no cluster meaning
```

Two edits carry it:

```c
/* src/server.c:2893 createDatabase — slots always, cluster or not. */
int slot_count_bits = CLUSTER_SLOT_MASK_BITS;
int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES;
```

```c
/* src/db.c:233 getKVStoreIndexForKey — one path for both modes. */
int getKVStoreIndexForKey(sds key) {
    return getKeySlot(key);
}
```

and `getKeySlot` (`src/db.c:240`) drops its `serverAssert(server.cluster_enabled)`. Everything
else in that function — the `current_client->slot` cache, the `mustObeyClient` backfill —
becomes *more* valuable in standalone, not less, because it is now on every keyed command.

### 4.2 The one thing to be strict about: virtual ≠ cluster

`c->slot` is currently populated only by `clusterSlotByCommand()` (`src/server.c:4276`), which
is a cluster-mode routing function that also produces `-CROSSSLOT` and drives redirects. Phase 3
must **not** start calling it in standalone. Instead:

- `getKeySlot` computes and caches the virtual slot on the key-access path, exactly as it does
  today in cluster mode. That is enough to populate `c->slot` for Phase 4's dispatch, because
  dispatch runs after the command's keys are known.
- Multi-key commands spanning slots stay **legal** in standalone. They will take the escalation
  barrier in Phase 4 (parent §6, §11) — a performance property, never an error. Nothing in Phase
  3 may introduce a rejection path.
- `CLUSTER KEYSLOT` (`src/cluster.c:876`) and every other `CLUSTER` subcommand keep their current
  cluster-only behavior. The virtual slot is an internal index that happens to use the same hash.

Write these three as tests (§7), because "standalone silently started rejecting `MGET a b`"
is the exact regression this phase risks.

### 4.3 `SCAN`: the cursor encoding changes — and why that is acceptable

Because the cursor is `(ht_cursor << num_hashtables_bits) | didx` (`src/kvstore.c:141`), moving
standalone from 0 bits to 14 bits **changes the numeric cursors a standalone server hands out**.
The consequences, in order of how much they matter:

- **The `SCAN` guarantee is unaffected.** The contract is that elements present for the whole
  iteration are returned at least once, and no error is returned for a stale cursor. That is a
  property of the per-table reverse-binary iteration plus the table walk, both of which cluster
  mode already relies on with 16384 tables. Standalone inherits a mode that is already in
  production.
- **A cursor is opaque and single-run.** Valkey never promised a cursor survives a restart, a
  version change, or a failover. A cursor minted before the upgrade and replayed after it is
  already outside the contract. It should still not *crash*: a large `didx` field is simply a
  table index that exists, so the scan resumes at some table — degraded position, defined
  behavior. Verify this explicitly (§7.6) rather than assuming it.
- **Cursor values become large immediately.** A first-page cursor in standalone today is a small
  integer; after this it carries 14 low bits of table index. Clients storing cursors as
  something narrower than 64 bits, or logging/diffing them, will see the change. This is worth a
  release-note line, and it is the one user-visible artifact of the phase.
- **`SCAN` cost with `COUNT` on a sparse standalone db.** With 16384 mostly-empty tables, a scan
  step must skip empty tables. `kvstoreScan` handles this by jumping to the next **non-empty**
  table index (`kvstoreGetNextNonEmptyHashtableIndex`, used at `src/kvstore.c:513`), so the walk
  is proportional to non-empty tables, not to 16384. Confirm with a benchmark on a 10-key db
  (§8.5) — a full `SCAN` of a nearly-empty standalone database is the pathological case and the
  one most likely to show up as "SCAN got slower after upgrade".

### 4.4 `RANDOMKEY` and sampling fairness

`dbRandomKey` (`src/db.c:442`) must now pick a table before picking an entry, and picking
uniformly among *tables* would badly bias toward keys in sparse tables. The correct primitive
already exists and is already used by eviction: `kvstoreGetFairRandomHashtableIndex`
(`src/evict.c:116`), which weights by key count via the kvstore's cumulative-count binary
indexed tree (`hashtable_size_index`, `src/kvstore.c:60`). If `dbRandomKey` does not already
route through it in cluster mode, this phase makes it do so for both modes; either way, add a
statistical test (§7.5) rather than trusting inspection — sampling bias is invisible until it
distorts eviction quality.

The same argument covers `evictionPoolPopulate` (`src/evict.c:113`) and the `expires` scan
(`src/expire.c:341`): both already operate on a multi-table kvstore in cluster mode, so they are
correct by construction here. They get *slower per sample* in standalone (a table lookup before
the entry lookup) — measure it (§8.5), because eviction quality under `maxmemory` is the most
sensitive consumer of sampling cost.

### 4.5 Memory overhead — the real decision in this phase

Per kvstore, independent of key count:

| Allocation | Size at 16384 tables |
|---|---|
| `hashtables` pointer array (`src/kvstore.c` in `kvstoreCreate`) | 16384 × 8 B = **128 KB** |
| `hashtable_size_index` BIT (`num_hashtables + 1` entries) | 16385 × 8 B = **~128 KB** |
| **per kvstore** | **~256 KB** |
| **per database** (`keys` + `expires` + `keys_with_volatile_items`) | **~768 KB** |

Only databases actually created pay it (`createDatabaseIfNeeded`, `src/server.c:2917`; only db 0
at boot, `src/server.c:3012`). So:

- A default standalone server touching db 0 only: **~0.75 MB**. Irrelevant.
- A server using all 16 databases: **~12 MB**. Irrelevant on a normal box; *not* irrelevant to
  someone packing hundreds of tiny Valkey instances onto one host, which is a real deployment
  pattern.

**Recommendation: take the full 14 bits and the 768 KB/db, and measure empty-instance RSS as a
gate (§8.4).** Cluster mode already pays exactly this and nobody has filed a bug about it.
Adding a knob to a data-structure decision this central invites a matrix of configurations that
every later phase must then support.

**The fallback if RSS is judged unacceptable** — write it down now so it is not re-invented
later: decouple **routing granularity** from **table granularity**. Keep 16384 virtual slots for
routing (`keyHashSlot & 16383`, free), but give the kvstore `2^b` tables with `b < 14` and map
`table = slot >> (14 - b)`. Then a shard's slot range must be **table-aligned**, or two shards
would share a hashtable and exclusive ownership — the entire premise of the design — would
break. Phase 2's partition (`slot * N / 16384`, contiguous) makes alignment achievable by
rounding shard boundaries to multiples of `16384 >> b`, but it is a constraint every later phase
inherits. That is the cost of the knob, and it is why the recommendation is to avoid it.

### 4.6 `KVSTORE_FREE_EMPTY_HASHTABLES` in standalone

Cluster mode sets this flag (`src/server.c:2897`) so emptied slots release their table
(`freeHashtableIfNeeded`, guarded at `src/kvstore.c:216`). Standalone should set it too:
without it, a workload that fills and empties keys leaves 16384 allocated tables behind
permanently. The cost is alloc/free churn on a table that repeatedly empties — a real but
already-shipped tradeoff, since cluster mode lives with it. Keep the flags identical between
modes so there is one behavior to reason about.

## 5. Interactions to check, and why each is probably fine

| Area | Verdict |
|---|---|
| **RDB save/load** | No format change. Save iterates the kvstore; load recomputes the index from the key via `getKVStoreIndexForKey`. `RDB_OPCODE_SLOT_INFO` is load-side only (`src/rdb.h:146`, `src/rdb.c:3274`). An RDB written before the change loads fine after it. |
| **AOF load** | Same path as any client write; index recomputed per key. |
| **Replication** | Byte stream is commands, not tables. Unaffected. |
| **`DBSIZE`** | `kvstoreSize` is an O(1) maintained counter (`key_count`, `src/kvstore.c:60`). Unaffected. |
| **`KEYS`** | Full iteration; now walks non-empty tables. Already how cluster mode behaves. |
| **`FLUSHALL`/`FLUSHDB`** | `kvstoreEmpty` already loops all tables (`src/kvstore.c:319`). |
| **Defrag** | Operates over the kvstore's tables; cluster mode already exercises the multi-table path. Re-run the defrag tests (§8.3) — this is the least-covered consumer. |
| **`db->blocking_keys` / `watched_keys` / `ready_keys`** | Plain dicts (`src/server.c:2907-2910`), not slot-partitioned. Untouched by this phase; they become per-shard in Phase 4/5. |
| **`OBJECT FREQ` / LFU / LRU** | Per-object; unaffected. |
| **Modules keyspace iteration** | Uses the same kvstore APIs cluster mode already feeds. |

## 6. Files touched

- `src/server.c` — `createDatabase` (`:2893-2899`): always 14 slot bits, identical flags in both
  modes.
- `src/db.c` — `getKVStoreIndexForKey` (`:233`) always returns the slot; `getKeySlot` (`:240`)
  drops the cluster-mode assert and keeps its caching/backfill logic.
- `src/db.c` — `dbRandomKey` (`:442`): route through the fair random table index.
- `src/server.h` — delete the dead `calculateKeySlot` declaration (`:3553`).
- `tests/unit/standalone-virtual-slots.tcl` — **new**: the §7 integration tests.
- `src/unit/test_kvstore.cpp` — extend with the cursor-encoding and fair-sampling cases (§7.5,
  §7.6) if not already covered.
- `valkey.conf` / release notes — one line noting that standalone `SCAN` cursor **values** change
  (the guarantee does not).

**Not touched:** `cluster.c` (no new caller of `clusterSlotByCommand`), `replication.c`,
`rdb.c`, the command table, dispatch.

## 7. Test plan

**Behavior preservation — the point of the phase (`tests/unit/standalone-virtual-slots.tcl`):**

1. `MGET a b c` / `DEL a b c` / `MSET` across keys hashing to different slots all succeed in
   standalone. **No `-CROSSSLOT` may ever appear.**
2. `CLUSTER KEYSLOT` and the rest of `CLUSTER` behave exactly as before in standalone.
3. `SCAN` full-iteration completeness: insert 100k keys, iterate to cursor 0, assert every key
   seen exactly once; repeat with concurrent writes asserting only the weak guarantee (present
   throughout ⇒ returned).
4. `SCAN` with `MATCH`, `COUNT`, and `TYPE` unchanged; `HSCAN`/`SSCAN`/`ZSCAN` (which scan an
   object, not the kvstore) untouched.
5. `RANDOMKEY` distribution: with keys skewed heavily into few slots, a large sample must follow
   the key distribution, not the table distribution. (Chi-squared or a coarse bucket assertion —
   this is the fairness test §4.4 demands.)
6. Cursor robustness: feed `SCAN` a hand-crafted large cursor; assert no crash, no error, and
   eventual termination.
7. `DBSIZE`, `KEYS *`, `FLUSHDB`, `SWAPDB`, `MOVE`, `COPY` across databases all unchanged.
8. RDB round trip: save with the old build, load with the new one, and the reverse; keyspace
   identical both ways.
9. Expiry still works end to end (`expires` kvstore is now partitioned too): set TTLs across many
   slots, confirm active and lazy expiry both fire.

**Rehash win — the independent value (§2.2):**

10. Load a large keyspace in standalone and record max single-command latency during growth,
    before vs after. The spike from the whole-table rehash should disappear. This is the number
    worth publishing even if the threading program stops.

## 8. Verification

1. `make -C src` and a CMake build.
2. `./runtest --single unit/standalone-virtual-slots`.
3. Full suite in **both** modes: `./runtest` (standalone) and `./runtest-cluster`, plus the
   defrag and expire suites explicitly — they are the heaviest kvstore consumers.
4. **Empty-instance RSS**: start a default server, `INFO memory`; compare `used_memory` and RSS
   before vs after, and again with all 16 databases touched. Publish both numbers; §4.5's
   recommendation stands or falls on them.
5. **Benchmarks** on standalone, before vs after: `SET`/`GET` throughput (the extra `keyHashSlot`
   per key access is the cost), `SCAN` over a 10-key database (the sparse-table pathological
   case), `RANDOMKEY` throughput, and eviction rate under `maxmemory` with `allkeys-lru`.
6. Latency-during-growth run from §7.10.

## 9. Honest risks

- **A per-key hash appears on the standalone hot path.** `keyHashSlot` (CRC16 over the key, or
  the hashtag substring) now runs for standalone key access where it never did. The
  `current_client->slot` cache (`src/db.c:252-257`) absorbs most of it for single-key commands,
  but multi-key commands hash every key. Measure it (§8.5); if it shows, the mitigation is
  caching per argv entry, not abandoning the phase.
- **Sparse-database `SCAN` regression.** 16384 tables holding 10 keys is a worse shape than 1
  table holding 10 keys. The non-empty-table skip makes it survivable; measure it anyway.
- **RSS on small instances.** §4.5. The fallback exists but adds an alignment constraint to every
  later phase.
- **This phase is not reversible in place.** Once cursors and table layout change, rolling back
  means another cursor-encoding change. Land it early in a release cycle, not late.

## 10. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §4 (ownership model,
  the virtual-slots paragraph), §10.3 (phasing), §11 (standalone's weaker case).
- Prerequisite: [Phase 2](proposal-slot-per-thread-phase2.md) — `slot_to_shard[]` +
  `shard-threads`.
- Consumer: [Phase 4](proposal-slot-per-thread-phase4.md) — dispatch reads `c->slot`, which this
  phase makes meaningful in standalone.
- Background: [04-keyspace-and-data-model.md](04-keyspace-and-data-model.md) (kvstore and
  hashtable), [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) (the other,
  orthogonal answer to the rehash-spike problem).
- Code: `src/server.c:2893` (`createDatabase`), `src/db.c:233,240,442`, `src/kvstore.c:56,141,
  294,476`, `src/evict.c:113,116`, `src/cluster.h:10`, `src/rdb.h:146`.
