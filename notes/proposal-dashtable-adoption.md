# Proposal — Adopting Dashtable in Valkey

**Status: pre-issue draft.** Companion to
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md) (which lists
this under **D2/D3** and marks a full swap a *non-goal*, §3), to
[proposal-slot-per-thread.md](proposal-slot-per-thread.md), and to the mechanics
reference [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md). This is the
detailed design for *how* one would actually bring Dashtable into Valkey. It is a design,
not a commitment to build.

All `file:line` anchors were checked against this checkout; verify before relying on them.

---

## 1. What "use Dashtable" has to mean here

The naive reading — "replace `src/hashtable.c` with a port of Dragonfly's Dash" — is the
wrong project, and the code says why. Valkey's `hashtable.c` is **not** the chained
`dict` Dash was designed to beat. It is *already* a cache-conscious, bucketized table:

- 64-byte (one cache line) buckets, **7 entries per bucket**, with a `presence` bitmask
  and a **1-byte hash tag per slot** — i.e. Dash's fingerprints — for SIMD-friendly
  negative lookups (`src/hashtable.c:295-297`, `ENTRIES_PER_BUCKET` at `:164`).
- ~91% fill factor by construction (`BUCKET_FACTOR`/`BUCKET_DIVISOR`, `:192`).
- Embedded entries: for the main keyspace the stored entry *is* the value object with the
  key embedded (`kvstoreHashtableAdd(db->keys, …, val)`, `src/db.c:222`) — no separate
  `dictEntry` allocation.

Dash's lookup-cost and memory-overhead advantage over a chained dict is therefore
**already captured**. Porting Dash to beat `hashtable.c` on lookups would re-implement a
recently-optimized structure to arrive roughly where we started, while re-solving
everything `hashtable.c` already integrates: the SCAN cursor contract, defrag,
embedded-key layout, random sampling, incremental find, and `kvstore` wiring. That is all
downside.

So this design defines the goal as adopting the **two properties Dash has that Valkey's
table genuinely lacks**:

| # | Dash property | Valkey today | What it unlocks |
|---|---|---|---|
| **P1** | **Version stamps + a serialize-before-mutate hook** | No version field anywhere; mutations have no snapshot hook | **Fork-less snapshotting (D3)** — no `fork()`, no 2× RSS, no COW page storms |
| **P2** | **Incremental resize one segment at a time** | Two-table incremental rehash (`tables[2]`, `rehash_idx`, `src/hashtable.c:308-309`) | Smoother resize: no transient second full table, tighter tail latency |

**P1 is the prize. P2 is a refinement.** The rest of this document is mostly about P1,
because that is where the value and the risk both live.

## 2. The decision: extend `hashtable.c`, don't port Dash

Two candidate shapes:

- **Design A — Port Dash as an alternative `kvstore` backend** (`dashtable.c`), selectable,
  living behind the same call sites `kvstore` uses.
- **Design B — Extend `hashtable.c`** in place to gain P1 (and later P2), taking Dash's
  *ideas*, not its code.

**Recommendation: Design B.** Rationale:

1. **The lookup engine is already good** (§1). A port's main deliverable would duplicate
   solved work.
2. **The load-bearing integrations are in `hashtable.c`/`kvstore.c`, not in Dash.** SCAN's
   reverse-binary cursor (`hashtableScan`, `src/hashtable.h:162`; kvstore cursor encoding
   `src/kvstore.c:141-148`), defrag (`hashtableScanDefrag`; `src/defrag.c` kvstore
   helper), the safe-iterator save path (`src/rdb.c:1417-1421`), random sampling, and
   incremental find are all wired to the current table. Design A must reproduce every one
   of them or regress a user-visible contract.
3. **P1 does not actually require Dash's segmented topology.** Version stamps + a mutation
   hook can be added to the current bucket table directly (§4). Segmented resize (P2) is
   separable and optional (§6).

Design A only becomes attractive in the **slot-per-thread endgame** (proposal §4.4), where
each shard *exclusively owns* its table and you might want native per-segment versioning
as the storage substrate rather than retrofitting it. Even then, Design B first is the
lower-risk path to the same snapshot capability. §8 keeps Design A on the table as the
explicit alternative.

## 3. Non-goals

- **Not** replacing the `hashtable.c` bucket/lookup engine (§1, and Dragonfly proposal §3).
- **Not** growing the 64-byte bucket. It is exactly one cache line and fully packed
  (`presence` + `hashes[7]` + `entries[7]`). A version field inside the bucket spills to a
  second cache line on the hottest read path — **forbidden**. This constraint is the whole
  reason §4 uses a side array.
- **Not** changing the SCAN guarantee, RESP, replication offset contract, or the module
  ABI.
- **Not** fibers, io_uring, or the transaction framework — those are other proposals.

## 4. Design of P1 — version stamps + fork-less snapshot

### 4.1 Where the version lives

Because the bucket cannot grow (§3), versions live in a **side array allocated only while a
snapshot is in flight**:

- Per hashtable, keep a **snapshot epoch counter** in the table's existing per-table
  metadata region (`getMetadataSize`/`hashtableMetadata`, `src/hashtable.h:78-84,123` —
  `kvstore` already uses this for the rehashing list node, `src/kvstore.c:97-101`).
- At snapshot start, lazily allocate a **per-bucket `uint32` version array**, sized to the
  table's bucket count. Steady-state cost when no snapshot is running: **zero**. Cost
  during a snapshot: ~4 bytes / 7 entries ≈ **0.6 B/key**, freed when the snapshot ends.
- Granularity is **per bucket**, not per entry — matching Dash, and the only affordable
  option given §3.

### 4.2 The cut and the two actors

Mirrors [10 §2](10-dragonfly-snapshot-model.md) exactly (`<=` comparison, conservative
variant). On a shard/table:

- **Start:** `cut = epoch++`. Allocate the version array, zero-initialized (0 ≤ cut, so
  every bucket starts eligible). Flip a per-table `snapshot_active` flag.
- **Serializer** walks buckets in index order:
  ```
  if (ver[b] <= cut) { ver[b] = cut + 1; serialize_bucket(b); }
  ```
- **Mutation hook** (see §4.3), before any insert/overwrite/delete touches bucket `b`:
  ```
  if (snapshot_active && ver[b] <= cut) {
      serialize_bucket(b);      // conservative: pre-image first
      ver[b] = cut + 1;
  }
  ```

Whoever reaches a bucket first serializes it and lifts it above the cut; the other side's
`<=` test then skips it. Result: every entry present at the cut is serialized exactly once,
in its at-cut state.

### 4.3 Wiring the hook into the mutation API

The mutation surface is small and centralized — this is what makes P1 tractable. Every
keyspace write lands in one of:

- `hashtableAdd` / `hashtableAddOrFind` (`src/hashtable.h:148-149`)
- `hashtableInsertAtPosition` (`:151`) — the two-phase insert `db.c` uses (`src/db.c:286`)
- `hashtableDelete` / `hashtableTwoPhasePopDelete` (`:153,155`) — used at `src/db.c:494`
- overwrite via `hashtableFindRef` + in-place value replace

Each computes the target bucket already. The hook is a single branch on `snapshot_active`
(predictably false in steady state, ~free) that, when set, runs the §4.2 pre-image check
against that bucket. **No new call sites in `db.c`** — the hook lives inside the hashtable
mutators, so every caller (keyspace, and anything else on a snapshotting table) is covered
uniformly.

### 4.4 The hard part: structural change during a snapshot

The version array is indexed by bucket. If the table **rehashes or resizes mid-snapshot,
buckets move and the index breaks.** This is the single hardest detail (flagged in
Dragonfly proposal §4.1). Options, in order of preference:

1. **Pause structural change for the snapshot's duration.** `hashtable.c` already supports
   pausing (`hashtablePauseAutoShrink`, `pause_rehash`, and the `HASHTABLE_RESIZE_*`
   policy — `RESIZE_FORBID` is exactly what the fork *child* uses today, `src/hashtable.c`
   resize-policy block ~`:127-151`). A forkless snapshot is the single process playing both
   roles, so the analog is: **hold rehash/resize on this table while its snapshot runs.**
   New inserts still land in the current table, take versions `> cut`, and are correctly
   skipped; deletes/overwrites hit the hook. Splits are merely deferred.
   - Cost: a needed expansion is postponed until the snapshot finishes. Bounded by snapshot
     duration; acceptable for a per-shard table. This is the recommended v1.
2. **Cover both tables.** If a rehash is already in progress when the snapshot starts,
   allocate version arrays for *both* `tables[0]` and `tables[1]` and let the serializer
   walk both. More complex; only needed if pausing an in-progress rehash is unacceptable.

v1 ships option 1. Option 2 is a follow-up if pause-induced resize latency shows up in
practice.

### 4.5 Bucket-granularity output

Serialize at **whole-bucket granularity** (as Dash does, [10 §3](10-dragonfly-snapshot-model.md)):
the serializer and the hook both emit a full bucket's live entries, never a partial
bucket, so a concurrent mutation cannot interleave a half-serialized bucket.

### 4.6 What produces the RDB, and replication

> The full producer/lifecycle/diskless-fan-out design that consumes this primitive is
> [proposal-forkless-rdb.md](proposal-forkless-rdb.md). This section is just the hand-off
> point.


Today the fork child drives `rdbSaveRio` over a safe `kvstore` iterator
(`src/rdb.c:1417-1421`), streaming through the `rio` abstraction that already feeds both
disk and replica sockets (`rdbSaveToReplicasSockets`, note 06). Fork-less changes **who
produces the bytes**, not the format or the RDB-plus-backlog replication contract:

- A snapshot producer (a job on the owning thread, or the main thread cooperatively
  time-sliced like incremental rehash) walks buckets and feeds the same `rio`.
- The **RDB byte format is unchanged.** A fork-less RDB is byte-compatible; loaders,
  version tags, and tooling are untouched.
- For replication full sync, the snapshot base + the command backlog accumulated since the
  cut is exactly today's contract. **Cross-shard consistency, when multiple owning threads
  each snapshot independently, comes from the replication journal, not the snapshot** — see
  [10 §5](10-dragonfly-snapshot-model.md) and the commit-id sequencer in
  [slot-per-thread §"replication"](proposal-slot-per-thread.md). In single-threaded mode
  today there is one table walk at a time, so this reduces to the current guarantee.

### 4.7 Conservative vs. relaxed

Expose the [10 §4](10-dragonfly-snapshot-model.md) knob:

- **Conservative** (pre-image) for **replication/RDB** — true point-in-time as of the cut,
  composes with the backlog.
- **Relaxed** (serialize new value in place, no pre-image) is available for uses that only
  need a complete-but-not-instant dump, at lower write-amplification. Default is
  conservative.

## 5. Integration surfaces that must not regress

| Surface | Where | Design impact |
|---|---|---|
| **SCAN** | `hashtableScan` + kvstore cursor (`src/kvstore.c:141-148`) | **Untouched.** The snapshot uses its own bucket walk + version array, not the SCAN cursor. SCAN semantics unchanged. |
| **Defrag** | `hashtableScanDefrag`, `src/defrag.c` kvstore helper | Defrag relocates entries → moves them between buckets → must be treated like a mutation. Simplest: **defrag participates in the pause (§4.4)** or routes through the same hook. Must be verified. |
| **Fork saves during transition** | `serverFork`, `src/rdb.c` | Fork-less is **opt-in per save type**; the fork path stays as fallback until fork-less is proven. Both can coexist. |
| **Memory accounting** | `trackMemUsage` callback (`src/hashtable.h:75-77`) | Version-array alloc/free reported through the existing `trackMemUsage` hook so `INFO`/`MEMORY` stay accurate. |
| **Child-only resize policy** | `HASHTABLE_RESIZE_FORBID` | Reused conceptually for the snapshot pause (§4.4); no new mechanism. |
| **Module ABI** | `hashtable.h` | Additive only (new snapshot entry points). No signature changes to existing API. |

## 6. Design of P2 — segmented resize (optional, later)

Only pursue after P1 ships and if resize tail-latency is shown to matter (measure first —
Dragonfly proposal §"Stage 0"). Two-table incremental rehash already amortizes and is
COW-aware; the win is removing the transient second full table.

Sketch: replace the single logical table with an extendible **directory of fixed-size
segments**; on a full segment, split *that* segment and (rarely) double the directory —
never allocate a second copy of the whole table. This is the larger, riskier change and
interacts with SCAN's cursor math and the version array's indexing, so it is deliberately
**decoupled** from P1 and gated on its own measurement. If P2 is not justified, P1 stands
alone and delivers fork-less snapshotting on the existing rehashing table.

## 7. Staged plan

| Stage | Deliverable | Gate |
|---|---|---|
| **0** | Measure: fork stall (ms) and peak COW RSS on a large, write-heavy instance; snapshot-walk cost estimate. | Numbers justify P1. |
| **1** | Per-table epoch + lazily-allocated version side array; `trackMemUsage` wired. No behavior change yet. | Unit tests; zero steady-state overhead confirmed. |
| **2** | Mutation hook (§4.3) behind `snapshot_active`; snapshot pause of rehash/defrag (§4.4). | Hook correctness tests; steady-state perf unchanged (hook branch is free). |
| **3** | Fork-less RDB producer feeding existing `rio`; byte-compat RDB. Opt-in flag; fork path retained. | RDB round-trip byte-identical to fork path on fuzz corpora. |
| **4** | Fork-less full-sync (replication), conservative variant + backlog. | Replica converges bit-for-bit vs fork-based full sync. |
| **5** *(optional)* | P2 segmented resize. | Separate measurement shows resize tail latency matters. |

## 8. Alternatives considered

- **Design A — port Dash as a `dashtable.c` backend.** Rejected for now (§2): duplicates a
  solved lookup engine and forces re-implementation of SCAN/defrag/embedded-keys/sampling.
  Revisit only in the slot-per-thread world if per-shard ownership makes native segment
  versioning cleaner than the side array.
- **Per-entry versions inside the object.** Rejected: no spare room in the packed
  bucket/embedded value without cache-line spill (§3); per-bucket is the right granularity
  anyway.
- **Keep fork, optimize COW only.** Legitimate and cheaper, but leaves the fork stall and
  2× RSS ceiling that motivate operators' over-provisioning — the actual pain P1 removes.

## 9. Risks

1. **Rehash/defrag during snapshot (§4.4)** — the primary correctness risk. Mitigation:
   pause-during-snapshot v1; both-tables coverage as fallback.
2. **Snapshot-walk latency on the serving thread.** Unlike the fork child, the walk shares
   the CPU with request serving. Mitigation: cooperative time-slicing like incremental
   rehash; per-shard walks in the threaded world.
3. **Write amplification under conservative mode** — a write-heavy burst during a snapshot
   pushes many pre-images. This is inherent to fork-less CoW-at-record-level; bounded by
   dataset churn during the window, and still avoids the fork's page-level 2× ceiling.
4. **Two save paths during transition** raise test surface. Mitigation: byte-for-byte
   equivalence gates (Stages 3–4) before making fork-less the default.

## 10. Open questions

- Who runs the producer in **single-threaded** mode — a cooperative main-thread job, or a
  dedicated I/O-thread task (`io_threads.c`)? Affects latency isolation.
- Should the version array be **per-`kvstore`** (all slots) or **per-hashtable** (per
  slot)? Per-hashtable composes better with slot-per-thread and per-slot snapshots.
- Interaction with **AOF rewrite**, which also forks today (`src/aof.c`). Same mechanism
  should apply, but the base-file format path needs its own equivalence gate.

## 11. Sources / references

- Mechanics and exact rules: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md)
- Ranking and Valkey-vs-Dragonfly gap: [proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md) §2, §3, §4.1
- Threaded endgame and journal ordering: [proposal-slot-per-thread.md](proposal-slot-per-thread.md)
- Code: `src/hashtable.c`, `src/hashtable.h`, `src/kvstore.c`, `src/db.c`, `src/rdb.c`, `src/defrag.c`
