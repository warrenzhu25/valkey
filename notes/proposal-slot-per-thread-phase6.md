# Proposal — Slot-per-thread Phase 6: per-shard expiry and eviction

**Status: pre-issue draft.** This is the concrete, line-anchored design of **Phase 6** of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — moving the two background
keyspace sweepers, active expiry and eviction, off the single thread and onto the shards that own
the data.

Prerequisites: [Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md) (per-shard loops
and timers), [Phase 4 part 2](proposal-slot-per-thread-phase4.md) (dispatch), and
[Phase 5](proposal-slot-per-thread-phase5.md) (the journal — both sweepers propagate writes, so
they cannot run on a shard before there is a journal to put those writes in).

**This phase is really two phases with very different risk profiles**, and the recommendation is
to land them separately:

- **6a — per-shard expiry.** A clean win. The scan primitive it needs (`kvstoreScan` over a
  hashtable *range*) already exists and is already used by cluster `SCAN`. Slot ownership makes
  it embarrassingly parallel with no shared state.
- **6b — per-shard eviction.** Genuinely hard, and gated on measurement. Eviction is a *global*
  decision (a memory limit for the whole process) implemented by *local* actions, and every
  simple partition of it either overshoots the limit or evicts the wrong keys. The honest first
  answer is to leave eviction on the barrier and only build 6b if the barrier is measured to hurt.

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Per-shard expiry and eviction." (parent §10.6)
>
> "Expiry: per-shard cycles over owned slots. Naturally parallel; the lazy-expire path is already
> local to the key's shard. Eviction: per-shard eviction against a global `maxmemory` atomic with
> per-shard slack, to avoid a shared counter on the hot path." (parent §8)

**In scope:** the active-expire cycle scoped to owned slots; per-shard cron scheduling and time
budgets; the eviction analysis, its per-shard design, and the decision of when to build it.

**Out of scope:** lazy expiry (already correct — §3.1); `evictClients` (output-buffer eviction of
*clients*, not keys — that stays on shard 0, part 1 §4.4); the field-expiry (`FIELDS`/hash-TTL)
job is in scope structurally but gets the same treatment as `KEYS`, so it is not discussed
separately.

## 2. Verified anchors (this checkout)

**Expiry:**

- `activeExpireCycle(int type)` (`src/expire.c:459`) is the entry point, called from
  `databasesCron` (`src/server.c:1304`, slow cycle at `:1312`) and from `beforeSleep` for the fast
  cycle (`src/server.c:1916`).
- The work happens in `activeExpireCycleJob(jobType, cycleType, timelimit_us)`
  (`src/expire.c:199`). It iterates `CRON_DBS_PER_CALL` databases per call, keeping **file-static
  cross-call state**: `static expireState _expire_state[ACTIVE_EXPIRY_TYPE_COUNT]`
  (`src/expire.c:210`) holding `current_db` and `timelimit_exit`.
- Per-database scan position is `db->expiry[jobType].cursor` (field at `src/server.h:909`, reset
  by `resetDbExpiryState`, declared `src/server.h:3792`), advanced by
  `kvstoreScan(kvs, cursor, -1, -1, scan_cb, expireShouldSkipTableForSamplingCb, &data)`
  (`src/expire.c:341`) — note the **`-1, -1`**: scan the whole store.
- The kvstore being scanned is `db->expires` for `KEYS` and `db->keys_with_volatile_items` for
  `FIELDS` (`src/expire.c:273-289`).
- Budget constants: `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` 20, `ACTIVE_EXPIRE_CYCLE_FAST_DURATION`
  1000 µs, `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` 25%, `ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE` 10%
  (`src/expire.c:122-125`), scaled by `activeExpireEffort()` (`src/expire.c:195`).
- Stats consulted to decide whether to run a fast cycle at all:
  `server.stat_expired_keys_stale_perc` and `server.stat_expired_keys_with_vola_stale_perc`
  (`src/expire.c:215-218`).
- Per-key expiry action: `activeExpireCycleTryExpire(db, val, now, didx)` (`src/expire.c:66`),
  called from the scan callback at `src/expire.c:150`.
- **The enabling primitive:** `kvstoreScan` already supports a bounded table range —
  `first_idx`/`last_idx`, documented at `src/kvstore.c:472-475` and implemented at `:476` with
  fast-forward (`didx < first_idx`), early return past `last_idx`, and range-exhausted checks at
  `:511-520`. Cluster `SCAN` already uses it (`src/db.c:1355`).
- **Lazy expiry is already slot-local:** `expireIfNeededWithDictIndex(db, key, val, flags,
  dict_index)` (`src/db.c:2203`) takes the table index the lookup already computed;
  `expireIfNeeded` (`src/db.c:2263`) derives it at `:2265-2266`.

**Eviction:**

- `performEvictions()` (`src/evict.c:404`) — the whole mechanism, called before command execution.
- `getMaxmemoryState(...)` (`src/evict.c:265`) reads `zmalloc_used_memory()` (`:270`, `:310`) —
  a **process-global** figure.
- The eviction loop `while (mem_freed < (long long)mem_tofree)` (`src/evict.c:437`) samples
  **every database** (`for (i = 0; i < server.dbnum; i++)`, `:452`) via
  `evictionPoolPopulate(db, kvs, pool)` (`:473`; defined `:113`), which picks a table with
  `kvstoreGetFairRandomHashtableIndex(samplekvs)` (`:116`) and samples it with
  `kvstoreHashtableSampleEntries` (`:120`).
- The pool itself is a file-static single instance: `static struct evictionPoolEntry
  *EvictionPoolLRU` (`src/evict.c:64`, allocated at `:102`, used at `:449`).
- Freed memory is measured by `zmalloc_used_memory()` deltas around the delete
  (`src/evict.c:561`, `:567`), and progress is re-checked with `getMaxmemoryState(NULL, NULL,
  NULL, NULL)` (`:593`, `:622`).
- Time budget: `evictionTimeLimitUs()` (`src/evict.c:363`, used at `:427`).
- Random-key fallback for the non-LRU policies: `kvstoreHashtableRandomEntry(kvs, slot, &entry)`
  (`src/evict.c:537`).
- `zmalloc_used_memory()` sums **per-thread** counters (`src/zmalloc.c:98-116`), so it is already
  safe to call from any thread — it is the *decision*, not the read, that is unsafe to parallelize.

## 3. Phase 6a — per-shard expiry

### 3.1 What is already correct

**Lazy expiry needs no work.** `expireIfNeededWithDictIndex` (`src/db.c:2203`) runs on whichever
thread looked the key up, and after Phase 4 that thread is the slot's owner by construction. The
`DEL`/`UNLINK` it propagates lands in that shard's op array and journal (Phase 5 §4.2). This is
the payoff of slot ownership showing up as an absence of work.

### 3.2 The change: scope the scan to owned slots

Because Phase 2 partitions slots into **contiguous** ranges (`slot * N / CLUSTER_SLOTS`), each
shard owns exactly one interval `[first_slot, last_slot]` — which is precisely the shape
`kvstoreScan`'s range mode takes.

```c
/* Per shard, per expiry job type. Replaces db->expiry[jobType].cursor. */
typedef struct shardExpiryState {
    unsigned long cursor;        /* kvstore cursor, confined to this shard's range */
    unsigned int  current_db;    /* was the file-static _expire_state[].current_db */
    bool          timelimit_exit;
    double        stale_perc;    /* was server.stat_expired_keys_*_stale_perc */
} shardExpiryState;

/* The scan, per iteration — the only substantive line that changes. */
cursor = kvstoreScan(kvs, cursor, shard->first_slot, shard->last_slot,
                     scan_cb, expireShouldSkipTableForSamplingCb, &data);
```

`kvstoreScan` handles the boundaries itself: it fast-forwards a cursor below `first_idx`, returns
0 when the range is exhausted (`src/kvstore.c:511-520`), and the shard then restarts at
`first_slot`. **No new kvstore API is needed for expiry** — which is the single biggest reason
6a is cheap and 6b is not.

Three pieces of state move from globals/db onto the shard:

| Today | Anchor | Becomes |
|---|---|---|
| `static expireState _expire_state[]` | `src/expire.c:210` | `shard->expiry[jobType]` |
| `db->expiry[jobType].cursor` | `src/server.h:909` | `shard->expiry[jobType].cursor` — one cursor per (shard, db, jobType). Keep it indexed by db, since the cycle still round-robins databases. |
| `server.stat_expired_keys_stale_perc`, `…_with_vola_stale_perc` | `src/expire.c:215-218` | Per-shard; summed (weighted by sampled keys) for `INFO`. |

### 3.3 Scheduling and the time budget

Each shard runs its own cycles on its own loop:

- **Fast cycle** in `shardBeforeSleep` (mirroring `src/server.c:1916`), bounded by
  `ACTIVE_EXPIRE_CYCLE_FAST_DURATION` (`src/expire.c:123`).
- **Slow cycle** on a per-shard timer, mirroring `databasesCron` (`src/server.c:1304-1312`).

**The budget subtlety that must not be got wrong.** `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` is 25%
(`src/expire.c:124`) — of *one thread's* time. If each of N shards spends 25% of its own loop
expiring, total expiry CPU is 0.25 × N cores, which is the correct generalization: each shard
owns 1/N of the keys and spends the same *fraction* of its own capacity on them. **Do not** divide
the percentage by N — that would leave total expiry throughput flat while the keyspace and the
insert rate scale, and stale keys would accumulate. State this explicitly in the code comment,
because "keep the total the same" is the intuitive and wrong instinct.

The adaptive parts (`ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE`, the fast-cycle skip when the previous
cycle did not hit its time limit, `src/expire.c:225-229`) become per-shard decisions on per-shard
statistics — which is strictly better than today, since a hot shard can expire aggressively while
a cold one stays idle. Today one global decision serves the whole keyspace.

### 3.4 Propagation

Expired-key deletions propagate `DEL`/`UNLINK`. They go through the owning shard's op array and
journal exactly like a client write (Phase 5 §3.2-3.3), taking a commit id and appearing in the
sequenced stream. Two details:

- The unit is one expire action (or the batch the current code treats as a unit); it must not be
  merged with an unrelated client command's unit.
- `propagatePendingCommands` avoids the `MULTI`/`EXEC` wrapper for
  `CMD_TOUCHES_ARBITRARY_KEYS` commands (`src/server.c:3759-3763`) and active expire is
  explicitly cited in that comment as the analogous case. Preserve that: expire records are
  standalone, not transactions.
- Replicas must not expire keys themselves; that gate is unchanged and lives above this layer.

### 3.5 Why 6a is worth doing on its own

On a large keyspace with many volatile keys, active expiry is a measurable slice of the single
thread's time, and it competes directly with command execution. Sharding it removes that
competition *and* scales the sweep rate with the keyspace. It has no cross-shard coordination, no
new API, no global counter, and its correctness surface is one scan-range argument and some state
relocation. If Phase 6 ships only 6a, that is a good outcome.

## 4. Phase 6b — per-shard eviction, and why it is hard

### 4.1 The structural problem

Eviction is a **global** decision — "the process is over `maxmemory`" — carried out by **local**
actions. Every part of `performEvictions` (`src/evict.c:404`) assumes one actor:

- **The target is global.** `getMaxmemoryState` (`src/evict.c:265`) reads
  `zmalloc_used_memory()` for the whole process. N shards each independently concluding "we are
  4 GB over" and each freeing 4 GB overshoots by 4×(N-1) GB — evicting most of the keyspace to
  fix a small overage. This is a *data-loss-shaped* bug, not a performance bug.
- **The sampling is global.** `evictionPoolPopulate` (`src/evict.c:113`) picks a random table
  across the entire kvstore (`kvstoreGetFairRandomHashtableIndex`, `:116`). A shard must sample
  only its own slots, or it will try to evict a key it does not own. **There is no range-limited
  fair-random API today** — `kvstoreGetFairRandomHashtableIndex` takes no bounds — so 6b needs a
  new kvstore primitive, e.g. `kvstoreGetFairRandomHashtableIndexInRange(kvs, first, last)`
  weighted by the cumulative-count BIT over that sub-range. That is a real but contained addition.
- **The pool is a singleton.** `static struct evictionPoolEntry *EvictionPoolLRU`
  (`src/evict.c:64`) must become per-shard. Easy.
- **Progress measurement is a global delta.** `mem_freed` accumulates
  `zmalloc_used_memory()` differences around each delete (`src/evict.c:561`, `:567`). With N
  shards allocating and freeing concurrently, those deltas measure *the process*, not *this
  eviction* — the loop's termination condition becomes noise-driven. This is the part that most
  resists a clean fix.
- **Eviction quality degrades under partition.** Approximated LRU/LFU works by sampling the
  *whole* keyspace for the globally-coldest keys. Per-shard sampling finds each shard's coldest
  keys, which is a different and worse answer when shards are unevenly cold. The user-visible
  effect is a lower cache hit rate at the same `maxmemory` — a silent quality regression that no
  functional test catches.

### 4.2 The recommendation: barrier first, measure, then decide

**Ship Phase 6 with eviction still on the barrier** (the Phase 5 §4.6 decision: over `maxmemory`
⇒ escalate). Then measure, on a `maxmemory`-constrained workload at `shard-threads 4/8`:

1. What fraction of wall time is spent inside eviction barriers?
2. What is the p99 latency impact of a barrier during steady-state eviction?
3. How often does the workload actually cross the limit — continuously (a cache at capacity, the
   common case) or rarely (a database with headroom)?

If eviction barriers are a small fraction, **stop** — build nothing, and document that
`maxmemory`-bound workloads scale less. If they dominate (likely for a cache running permanently
at its limit, which is a very common deployment), build 6b with the design below.

### 4.3 If 6b is built: credit-based eviction

The design that avoids both overshoot and a shared counter on the hot path:

```text
   shard 0 cron (the only thread that decides "how much"):
     getMaxmemoryState() ─► mem_tofree                       (one global read, once per round)
     split into per-shard credits, weighted by each shard's
     tracked memory, not equally  ────────────────┐
                                                  ▼
   each shard, in its own loop:  evict from its own slots until its credit is consumed
                                 (per-shard pool, range-limited sampling, per-shard deltas)
                                 report actual freed bytes back ──┐
                                                                  ▼
   shard 0 next round: recompute from the real global figure, re-issue credits
```

- **One decider.** Only shard 0 reads the global state and hands out budgets, so overshoot is
  bounded by one round's credits rather than N× the deficit.
- **Weighted credits, not equal shares.** An equal split evicts from shards that hold little,
  which is both ineffective and destructive. Weight by per-shard memory — which needs per-shard
  memory tracking, and that is exactly what
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md) designs for slots.
  **6b should depend on that work rather than inventing a second accounting.**
- **Local progress measurement.** A shard measures its own freed bytes from its own per-thread
  `zmalloc` counter (`src/zmalloc.c:98-116` — the counters are already per-thread, so a shard can
  read *its own* slot exactly), sidestepping §4.1's noisy global delta. This is a genuinely nice
  fit and the strongest argument that 6b is buildable.
- **Quality mitigation.** Keep a small cross-shard "coldest candidates" summary that shard 0
  refreshes each round from per-shard pools, so credits skew toward shards holding globally-cold
  keys. Measure hit-rate against the single-threaded baseline at equal `maxmemory` (§6.7); if the
  regression is material, credits alone are not enough and the honest answer is to keep the
  barrier.
- **Writes still need a synchronous check.** A command that would exceed `maxmemory` must not
  proceed on the strength of a stale per-shard view. Keep a cheap fast-path check against a
  relaxed global atomic with per-shard slack (parent §8's phrasing), and escalate to the barrier
  when the slack is exhausted — so the *hard* limit is always enforced by one thread, and the
  concurrent path only handles the steady-state.

## 5. Files touched

**6a:**

- `src/expire.c` — `activeExpireCycleJob` (`:199`) takes a shard; `_expire_state` (`:210`) moves
  to the shard; the scan (`:341`) passes `shard->first_slot, shard->last_slot`; stale-percentage
  stats become per-shard (`:215-218`).
- `src/server.h` — `db->expiry[].cursor` (`:909`) relocates to per-shard state; `INFO`
  aggregation for the expiry stats.
- `src/server.c` — `databasesCron` (`:1304`) and the `beforeSleep` fast cycle (`:1916`) become
  per-shard invocations on shard timers/`shardBeforeSleep`.
- `src/shard.{c,h}` — `shardExpiryState`, the per-shard cron tick.
- `tests/unit/shard-expire.tcl` — **new**.

**6b (only if §4.2 says so):**

- `src/kvstore.{c,h}` — `kvstoreGetFairRandomHashtableIndexInRange(kvs, first, last)`, weighted
  by the BIT over the sub-range.
- `src/evict.c` — per-shard `EvictionPoolLRU` (`:64`); `evictionPoolPopulate` (`:113`) takes a
  range; `performEvictions` (`:404`) splits into a shard-0 "decide" half and a per-shard "evict my
  credit" half; local `zmalloc` accounting replaces the global deltas (`:561`, `:567`).
- `src/shard.{c,h}` — credit issue/consume/report.
- `src/server.h` / `src/config.c` — per-shard memory tracking hooks (shared with
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md)).
- `tests/unit/shard-eviction.tcl` — **new**.

## 6. Test plan

**6a:**

1. **Coverage.** Every volatile key eventually expires, whatever shard owns it, at
   `shard-threads` 1/2/4/8. No slot is ever skipped — the range-boundary bug this phase most
   risks. Seed keys deliberately at slot 0, at each shard boundary, and at slot 16383.
2. **No double expiry.** Two shards must never both expire the same key (they cannot, by
   ownership — assert it with a counter and a test rather than trusting it).
3. **Replication.** Expired-key `DEL`s reach the replica in sequenced order; primary and replica
   digests match after a mass-expiry event (Phase 5 §6).
4. **Stale-key ratio under load.** With a high insert rate of volatile keys, the ratio of expired
   but not-yet-collected keys does not degrade as `shard-threads` grows — the property §3.3's
   budget rule exists to protect.
5. **`shard-threads 1` is unchanged.** Existing expire suite passes bit-identically.
6. **Cursor persistence across a rebalance.** Change `slot_to_shard[]` under barrier mid-cycle;
   no key is skipped and no shard scans a slot it no longer owns.

**6b:**

7. **`maxmemory` is respected without overshoot.** Drive the server past the limit at
   `shard-threads 8`; peak `used_memory` overshoot stays within a stated bound (the one-round
   credit), and the server converges back under the limit.
8. **Eviction quality.** Same workload, same `maxmemory`, measure hit rate at `shard-threads` 1
   vs 8. Publish the delta. **This is 6b's gate** — a large regression means the barrier was the
   better answer.
9. **No cross-shard eviction.** A shard never deletes a key outside its slot range (assert in a
   debug build).
10. **Policy coverage.** All `maxmemory-policy` values, including `volatile-ttl` and the random
    policies (which use a different code path, `src/evict.c:537`).
11. **`maxmemory 0`** (unlimited) never engages any of this.

## 7. Verification

1. `make -C src`; `./runtest --single unit/shard-expire` (and `unit/shard-eviction` for 6b).
2. Full suite plus `runtest-cluster` at `shard-threads 1` and `4`; the existing `unit/expire` and
   `unit/maxmemory` suites are the ones that matter most here.
3. **TSan** at `shard-threads 4` with a heavy volatile-key workload — expiry runs concurrently
   with commands on every shard, which is the most concurrent thing in the server after this
   phase.
4. Long soak with primary/replica `DEBUG DIGEST` comparison across mass-expiry events.
5. Benchmarks: expiry sweep rate vs `shard-threads`; stale-key ratio at a fixed insert rate;
   command-latency impact of the slow cycle at each shard count. For 6b: §6.7 and §6.8.

## 8. Honest risks

- **Range-boundary bugs skip keys silently.** An off-by-one in `first_slot`/`last_slot` leaves a
  slot never actively expired; its keys still expire lazily on access, so nothing fails — memory
  just grows and nobody knows why. §6.1's boundary seeding is the defense, and it deserves to be
  a permanent test, not a one-off check.
- **Expiry CPU multiplies by N.** §3.3's budget rule is correct, but it means an 8-shard server
  can spend 2 cores' worth of time expiring. That is the right behavior for a keyspace 8× larger;
  it will still surprise someone reading `top`. Make it visible in `INFO`.
- **Eviction quality is a silent regression.** Nothing errors when per-shard sampling picks worse
  victims; the hit rate just drops. Only §6.8 catches it, and only if it is actually run.
- **Overshoot is data loss.** The single most dangerous outcome in this phase is N shards each
  evicting the full deficit. The credit design exists specifically to make that impossible;
  if 6b is built any other way, this is the failure to fear.
- **6b's dependency on per-shard memory tracking** means it should not start before
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md) lands, or it will grow
  a second, incompatible accounting.

## 9. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §8 (the table this
  phase implements two rows of), §10.6.
- Prerequisites: [Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md) (per-shard cron),
  [Phase 5](proposal-slot-per-thread-phase5.md) (the journal both sweepers write into; §4.6 is the
  barrier decision this phase revisits).
- Dependency for 6b: [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md)
  (per-slot memory tracking).
- Background: [05-expiration-and-eviction.md](05-expiration-and-eviction.md).
- Code: `src/expire.c:66,122-125,199,210,341,459`, `src/server.c:1304,1312,1916`,
  `src/server.h:909`, `src/db.c:2203,2263`, `src/kvstore.c:472-476,511-520`,
  `src/evict.c:64,102,113,116,120,265,363,404,437,473,537,561,567`, `src/zmalloc.c:98-116`.
