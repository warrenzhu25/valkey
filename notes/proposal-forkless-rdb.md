# Proposal — Fork-less RDB in Valkey (primitive + producer, end to end)

**Status: pre-issue draft.** The complete design for producing an RDB — for `BGSAVE`,
automatic saves, and replication full sync — **without `fork()`**, in two layers that used to
be two documents:

- **Part I — the primitive (P1):** per-bucket version stamps + a serialize-before-mutate hook
  in `src/hashtable.c`, ready-to-code and testable on its own. (Was
  `proposal-hashtable-snapshot-versioning.md`.)
- **Part III — the producer:** the cooperative snapshot walk, save lifecycle, and diskless
  fan-out that consume the primitive and replace the fork/child/reap machinery.

They are merged here because **the interface between them had defects** that only surface when
the two are read together — a producer that walks entries while the primitive versions buckets,
a "pre-image is a memcpy" claim that is actually a synchronous serialize, and two correctness
gaps (overwrites and defrag) that the primitive defers and the producer assumes away.
**Part II is the new material: the seam contract and the six defects that must be closed.**

The conceptual parent — *why* P1, the Design-A-vs-B decision, and the optional P2 segmented
resize — stays in [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §4/§6.
Mechanics reference: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md). Fork
baseline: [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md). Threaded endgame:
[proposal-slot-per-thread.md](proposal-slot-per-thread.md).

All `file:line` anchors were checked against this checkout.

---

## 1. Goal and scope

**Produce a byte-identical RDB stream without forking**, for the three consumers that funnel
through the same serializer today:

- **`BGSAVE` / automatic saves** → disk (`rdbSaveBackground`, `src/rdb.c:1673`).
- **Diskless replication full sync** → replica sockets (`rdbSaveToReplicasSockets`,
  `src/rdb.c:3756`).
- (Unchanged) **`SAVE`** — already blocking in the main process (`rdbSave`, `src/rdb.c:1634`);
  not a fork case, left as-is.

**In scope:** the primitive (Part I), the producer and its cooperative scheduling, the save
lifecycle that replaces the child process, diskless fan-out, and the config/observability
surface.

**Out of scope:** the RDB *byte format*, deliberately unchanged (§16). The optional P2
segmented resize (Dashtable-adoption §6). AOF-base-file production, which also forks but needs
its own equivalence gate (§20).

## 2. What fork buys today, and the machinery to replace

The fork trick (note 06): the child gets a COW snapshot of memory for free — a consistent
point-in-time view, no locks, no pause — then walks the keyspace via a safe `kvstore` iterator
(`src/rdb.c:1417-1421`) and writes it out while the parent serves traffic.

The costs that motivate this proposal:

- **Fork stall** — a page-table copy, tens to hundreds of ms of hard stall mid-event-loop on a
  large instance.
- **COW amplification** — a write-heavy instance approaches **2× RSS** during a save, driving 2×
  memory over-provisioning and conservative `maxmemory`. Tracked as `current_cow_size` /
  `rdb_last_cow_size` (`src/server.c:6413,6425`).
- **OOM-kill risk** at fork time — a genuine production hazard.

The machinery that implements the fork model — **all of which fork-less must replace or
retire**:

| Piece | Location | Fork-less replacement |
|---|---|---|
| Fork the child | `serverFork(CHILD_TYPE_RDB)` at `src/rdb.c:1682`, `:3836` | No fork; start a **producer job** (§14) |
| Child does the walk | `rdbSaveRio` / `rdbSaveRioWithEOFMark` (`src/rdb.c:1481,1534`) | Same record writers, driven **cooperatively** (§15) |
| Reap child, dispatch | `checkChildrenDone` → `waitpid(WNOHANG)` (`src/server.c:1426,1430`), `backgroundSaveDoneHandler` (`src/rdb.c:3719`) | **In-process completion** callback (§17) |
| Abort a save | `killRDBChild` (`src/rdb.c:3740`) | Cancel the producer job (§17) |
| "Is a save running" | `hasActiveChildProcess()` / `child_type == CHILD_TYPE_RDB` (`src/server.c:877,897`) | A `rdbSaveInProgress()` predicate not tied to a PID (§17, §24) |
| COW/progress reporting | child info pipe → `current_cow_size` etc. | New progress + pre-image metrics (§19) |

The fork model needs no cut protocol because COW *is* the cut. Fork-less has no COW, so it must
build the cut in software — that is Part I.

---

# Part I — The snapshot primitive (P1)

Ready-to-code, self-contained in `src/hashtable.c`/`.h`, testable with no RDB code. Implements
the Dash mechanism ([10 §2](10-dragonfly-snapshot-model.md)): a per-table cut, a lazily
allocated per-bucket version array, a mutation hook on every keyspace write, and a
snapshot-duration freeze on structural change.

**Zero behavior change when no snapshot is active** is a hard requirement: the only steady-state
cost is one `if (unlikely(ht->snapshot_active))` branch per mutation.

## 3. Scope of the primitive

**In scope:** per-table epoch/cut, the version array, the mutation hook wired into every
keyspace-mutating entry point in `hashtable.c`, and the structural-change freeze. The hook fires
through a caller-supplied callback; at this layer there is no real consumer, so a unit-test
suite plays that role.

**Out of scope at this layer, but mandatory before the producer is correct (Part II):** the
`dbSetValue` overwrite path (`src/db.c:319-370`, §12 S5) and defrag's cross-bucket moves (§12
S6). The versioning layer *defers* these; the producer *depends* on them, so Part II reclassifies
them as blocking.

## 4. The cut protocol

Mirrors [10 §2](10-dragonfly-snapshot-model.md) (`<=` comparison, conservative variant). On a
table:

- **Start:** `cut = epoch++`. Allocate the version array, zero-initialized (`0 <= cut`, so every
  bucket starts eligible). Set `snapshot_active`.
- **Serializer walk**, buckets in index order:
  ```
  if (ver[b] <= cut) { ver[b] = cut + 1; serialize_bucket(b); }
  ```
- **Mutation hook**, before any insert/overwrite/delete touches bucket `b`:
  ```
  if (snapshot_active && ver[b] <= cut) {
      serialize_bucket(b);      // conservative: pre-image first
      ver[b] = cut + 1;
  }
  ```

Whoever reaches a bucket first serializes it and lifts it above the cut; the other side's `<=`
test then skips it. Every entry present at the cut is serialized exactly once, in its at-cut
state. Serialization is **whole-bucket** ([10 §3](10-dragonfly-snapshot-model.md)): both actors
emit a bucket's full live entry set, never a partial bucket, so a concurrent mutation cannot
interleave a half-serialized bucket. **This bucket granularity is load-bearing for the seam —
see §12 S1.**

### 4.1 Two gaps closed vs. the conceptual design

1. **`pause_rehash` alone does not freeze structural change.** It stops an *already-started*
   rehash from stepping (`rehashStepOn{Read,Write}IfNeeded`, `hashtable.c:726,736`) but does
   **not** stop a *new* one from starting: `hashtableExpandIfNeeded`/`ShrinkIfNeeded` call
   `resize()` (`hashtable.c:746`) whenever `!hashtableIsRehashing(ht)`, which allocates
   `tables[1]` and sets `rehash_idx = 0`. Enough inserts during a snapshot would silently start a
   rehash mid-snapshot, invalidating the "only `tables[0]` matters" invariant. **Fix:** a
   `snapshot_active` guard *inside* `resize()` (§7) — the single choke point for
   expand/shrink/rightsize.
2. **Bucket chaining.** The `chained:1` bit (`hashtable.c:293-298`) lets a bucket overflow into
   64-byte child buckets (`getChildBucket`, `:238-256`). A "bucket" for versioning must mean the
   whole chain rooted at a top-level slot, or overflow entries go unversioned. **Fix:** key the
   version array on the *top-level* bucket index only (§5) — a mutation anywhere in a chain hits
   the same version slot.

## 5. New fields on `struct hashtable` (`hashtable.c:306-317`)

Inserted between `iter *safe_iterators;` and the flexible-array `metadata[]` (which must stay
last). `hashtableCreate` `zmalloc`s the struct **without** zeroing (`hashtable.c:1263`), so every
field is explicitly initialized (§9).

```c
struct hashtable {
    hashtableType *type;
    ssize_t rehash_idx;
    bucket *tables[2];
    size_t used[2];
    int8_t bucket_exp[2];
    int16_t pause_rehash;
    int16_t pause_auto_shrink;
    size_t child_buckets[2];
    iter *safe_iterators;
    /* --- Snapshot (P1) --- */
    bool snapshot_active;         /* True while a snapshot is in progress on this table. */
    uint64_t snapshot_epoch;      /* Monotonic; ++ on every successful Start. Never reset. */
    uint64_t snapshot_cut;        /* = snapshot_epoch captured at the start of the current snapshot. */
    uint64_t *snapshot_versions;  /* NULL unless active. One entry per top-level bucket in tables[0]. */
    hashtableSnapshotCaptureCallback snapshot_on_capture; /* NULL unless active. */
    void *snapshot_privdata;
    void *metadata[];
};
```

`snapshot_versions` is indexed by top-level bucket index in `tables[0]` only: a snapshot can only
start when `rehash_idx == -1` (§8) and the `resize()` guard keeps it there for the whole snapshot,
so `tables[0]` is provably the only live table and `bucket_exp[0]` is stable. `table_index` is
therefore always `0` at every hook call site while active. Element type is `uint64_t` (matching
the epoch), ~8 B/bucket only while a snapshot is in flight, freed after.

## 6. Public API (`hashtable.h`)

```c
typedef enum {
    HASHTABLE_SNAPSHOT_CAPTURE_MUTATE, /* A write is about to touch this bucket for the first time since the cut. */
    HASHTABLE_SNAPSHOT_CAPTURE_WALK,   /* hashtableSnapshotWalkVisit() reached this bucket first. */
} hashtableSnapshotCaptureReason;

/* Invoked once per top-level bucket the first time it is captured. 'table_index' is always 0.
 * 'bucket_index' covers the whole chain rooted there. The callback receives an INDEX, not a
 * bucket pointer or an entry — see §12 S2/S3 for what the producer must build on top of this. */
typedef void (*hashtableSnapshotCaptureCallback)(hashtable *ht, int table_index, size_t bucket_index,
                                                  hashtableSnapshotCaptureReason reason, void *privdata);

bool     hashtableSnapshotStart(hashtable *ht, hashtableSnapshotCaptureCallback on_capture, void *privdata);
void     hashtableSnapshotEnd(hashtable *ht);
bool     hashtableSnapshotIsActive(hashtable *ht);
uint64_t hashtableSnapshotGetCut(hashtable *ht);
bool     hashtableSnapshotWalkVisit(hashtable *ht, size_t bucket_index);
```

`hashtableSnapshotWalkVisit` is the **driver-ready walk hook**: it lets the serializer race the
mutation hook for a given bucket using the same "first touch, `<=` cut, wins" rule. This is the
walk primitive the producer is *supposed* to use — §12 S1 is that the producer as originally
drafted did not.

## 7. Mutation hook + the `resize()` freeze

```c
/* Called just before a write touches the top-level bucket 'hash' maps to in tables[0]. */
static inline void snapshotMutationHook(hashtable *ht, uint64_t hash) {
    if (likely(!ht->snapshot_active)) return;
    size_t bucket_index = hash & expToMask(ht->bucket_exp[0]);
    assert(bucket_index < numBuckets(ht->bucket_exp[0]));
    if (ht->snapshot_versions[bucket_index] <= ht->snapshot_cut) {
        ht->snapshot_versions[bucket_index] = ht->snapshot_cut + 1;
        if (ht->snapshot_on_capture)
            ht->snapshot_on_capture(ht, 0, bucket_index, HASHTABLE_SNAPSHOT_CAPTURE_MUTATE, ht->snapshot_privdata);
    }
}
```

In `resize()` (`hashtable.c:746`), immediately after the existing `HASHTABLE_RESIZE_FORBID`
check (`:771-774`):

```c
    if (ht->snapshot_active) {
        /* Structural change frozen for the snapshot's duration so top-level bucket indices —
         * and the version array — stay valid. Resumes automatically when the snapshot ends. */
        return false;
    }
```

This single choke point covers every expand/shrink/rightsize path — closing gap #1 of §4.1.
**It does not cover defrag**, which relocates entries outside `resize()` — see §12 S6.

## 8. Lifecycle functions

```c
bool hashtableSnapshotStart(hashtable *ht, hashtableSnapshotCaptureCallback on_capture, void *privdata) {
    if (ht->snapshot_active) return false;
    if (hashtableIsRehashing(ht)) return false;      /* reject; caller retries after rehash finishes */
    if (ht->bucket_exp[0] < 0) return false;         /* never sized — avoids deadlocking the first insert's resize */

    size_t n = numBuckets(ht->bucket_exp[0]);
    uint64_t *versions = zcalloc_num(n, sizeof(uint64_t));   /* overflow-checked, src/zmalloc.c:311 */
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, sizeof(uint64_t) * n);

    ht->snapshot_versions = versions;
    ht->snapshot_cut = ht->snapshot_epoch;
    ht->snapshot_epoch++;
    ht->snapshot_on_capture = on_capture;
    ht->snapshot_privdata = privdata;
    ht->snapshot_active = true;

    hashtablePauseRehashing(ht); /* also freezes delete-time chain compaction (fillBucketHole gated by
                                  * hashtableIsRehashingPaused at :1738/:1868) — keeps chain layout stable. */
    return true;
}

void hashtableSnapshotEnd(hashtable *ht) {
    if (!ht->snapshot_active) return;
    size_t n = numBuckets(ht->bucket_exp[0]);
    zfree(ht->snapshot_versions);
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, -(ssize_t)(sizeof(uint64_t) * n));
    ht->snapshot_versions = NULL;
    ht->snapshot_on_capture = NULL;
    ht->snapshot_privdata = NULL;
    ht->snapshot_active = false;
    hashtableResumeRehashing(ht);
}

bool     hashtableSnapshotIsActive(hashtable *ht) { return ht->snapshot_active; }
uint64_t hashtableSnapshotGetCut(hashtable *ht)   { assert(ht->snapshot_active); return ht->snapshot_cut; }

bool hashtableSnapshotWalkVisit(hashtable *ht, size_t bucket_index) {
    assert(ht->snapshot_active);
    assert(bucket_index < numBuckets(ht->bucket_exp[0]));
    if (ht->snapshot_versions[bucket_index] <= ht->snapshot_cut) {
        ht->snapshot_versions[bucket_index] = ht->snapshot_cut + 1;
        if (ht->snapshot_on_capture)
            ht->snapshot_on_capture(ht, 0, bucket_index, HASHTABLE_SNAPSHOT_CAPTURE_WALK, ht->snapshot_privdata);
        return true;
    }
    return false;
}
```

`hashtablePauseRehashing`/`ResumeRehashing` stay `static` and are called directly from these new
exported symbols in the same translation unit. **Rehash-in-progress decision:** reject and let
the caller retry (simplest safe v1). Finishing the rehash synchronously or covering both tables
are follow-ups if this proves too restrictive.

## 9. `hashtableCreate` / `hashtableEmpty` / `hashtableMemUsage`

- **`hashtableCreate`** (`:1260-1276`): initialize all six new fields (`snapshot_active = false`,
  counters `0`, pointers `NULL`) beside the existing `safe_iterators = NULL;`.
- **`hashtableEmpty`** (`:1280-1281`) — first statement in the body, since emptying invalidates
  bucket indices and `hashtableRelease` calls `hashtableEmpty` internally (`:1328`):
  ```c
      if (ht->snapshot_active) hashtableSnapshotEnd(ht);
  ```
- **`hashtableMemUsage`** (`:1380-1385`): add the version array while active:
  ```c
      size_t snapshot_size = ht->snapshot_active ? numBuckets(ht->bucket_exp[0]) * sizeof(*ht->snapshot_versions) : 0;
      return sizeof(hashtable) + metasize + sizeof(bucket) * num_buckets + snapshot_size;
  ```

## 10. The four mutator call sites

Every keyspace write in `hashtable.c` lands in one of these. `hashtableReplaceReallocatedEntry`
(`:1766-1791`, a content-preserving pointer swap) is deliberately **not** hooked — but note it is
*not* the defrag path that matters (§12 S6).

- **`insert()`** (`:1092-1103`, behind `hashtableAdd`/`AddOrFind`) — `hash` in scope; hook after
  `findBucketForInsert` (`:1098`), before `b->entries[pos] = entry` (`:1099`).
- **`hashtableInsertAtPosition`** (`:1713-1723`) — opaque position carries no hash; recompute from
  the entry key, gated behind `unlikely(ht->snapshot_active)` so steady state pays nothing:
  ```c
      if (unlikely(ht->snapshot_active)) snapshotMutationHook(ht, hashKey(ht, entryGetKey(ht, entry)));
  ```
- **`hashtablePop`** (`:1728-1748`, behind `hashtableDelete`) — hook after `findBucket` succeeds
  (`:1734`), before `b->presence &= ~(1 << pos)` (`:1736`).
- **`hashtableTwoPhasePopDelete`** (`:1853-1876`) — entry still present, recover its key; hook
  before the presence-bit clear (`:1862`). The `hashtableResumeRehashing` here undoes the
  *pair-local* pause from `TwoPhasePopFindRef`; `pause_rehash` is a plain counter, so it composes
  with the snapshot-level pause without conflict.

The correctness property the whole design rests on: **the hook fires strictly before the
bucket-slot mutation** at every site. Re-read all four after editing.

## 11. Primitive test plan (`src/unit/test_hashtable_snapshot.cpp` — new; auto-discovered)

Fixtures: a `snapEntry{long key; long value;}` with the usual wiring; a constant-hash
`hashtableType` to force 100% collisions for the chaining test; a `CaptureLog` vector and a
`testOnCapture` callback. Fixture asserts `mem_usage == 0` in `TearDown`.

1. `SnapshotInactiveHasNoEffectOnNormalOps` — no snapshot; add/find/delete loop; no
   snapshot-attributable allocation ever.
2. `SnapshotStartRejectsUnsizedTable` — `bucket_exp[0] == -1` → `Start` returns false.
3. `SnapshotStartRejectsWhileRehashing` — populate past the resize threshold → `Start` false.
4. `SnapshotStartEndIdempotencySafety` — double `Start` → second false; double `End` safe.
5. `MutationHookCapturesExactlyOnceInPreCutState` — delete K (bucket B): callback once, `MUTATE`,
   and K still visible from inside the callback (hook fires before the write); delete a second key
   in B → no second callback (`<=` semantics).
6. `WalkerWinsRaceAgainstLaterWriter` — `WalkVisit(B)` before any mutation → true, `WALK`; then a
   delete in B → hook does not fire again.
7. `WriterWinsRaceAgainstLaterWalker` — mutate first (`MUTATE`), then `WalkVisit(B)` → false, no
   callback. Together with #6, catches a `<` vs `<=` mistake.
8. `ChainedBucketSharesOneVersionSlot` — constant-hash type, force chaining, delete a head entry
   and a child entry → exactly one callback, `bucket_index == 0`.
9. `SnapshotStartEndTracksMemoryUsage` — `mem_usage` up by exactly
   `numBuckets(bucket_exp[0]) * sizeof(uint64_t)` on Start, back to baseline on End.
10-13. Insert-via-add / insert-via-two-phase / delete / two-phase-delete each fire the hook
    strictly before the entry becomes visible / is removed.
14. `SnapshotFreezesResizeUntilEnd` — inserts that would resize don't; explicit
    `hashtableExpand`/`TryExpand` return false while active; resize resumes after End.
15. `EmptyDuringActiveSnapshotEndsItSafely` — `Empty`/`Release` without `End` → inactive, no leak.
16. `SnapshotEpochIsMonotonicAcrossGenerations` — `cut2 > cut1` across non-overlapping snapshots.

Gate: `make test-unit UNIT_TEST_PATTERN='*Snapshot*'`, then full `make test-unit`. Test 1 must run
thousands of ops so "the hook branch is free" is substantiated.

---

# Part II — The seam: the primitive/producer contract

This is the section that did not exist while the two designs were separate documents, and it is
where they disagree. The producer (Part III) consumes the primitive (Part I). Read together, six
mismatches surface. **S1, S2, S5, S6 are blocking — the producer is incorrect or uncompilable
against the primitive as written until they are closed.** S3, S4 are lesser but real.

## 12. The interface, and six defects

**What the primitive gives:** a per-table cut; a `snapshot_active` flag; a callback fired **once
per top-level bucket index** the first time that bucket is captured (by a write, `MUTATE`, or by
`hashtableSnapshotWalkVisit`, `WALK`); and `hashtableSnapshotGetCut`. The callback is a **pure
notification** — it serializes nothing and hands over an *index*, not entries.

**What the producer needs:** to serialize every live entry of every bucket present at the cut,
exactly once, in at-cut state, as RDB bytes, cooperatively across event-loop ticks.

The gap between those two sentences is the seam.

### S1 — Walk-mechanism mismatch (blocking)

The producer as originally drafted (§22) walks with a long-lived **`kvstoreIterator`**
(`kvstoreIteratorNext`, entry-by-entry, §15). The primitive is built around a **bucket-indexed
walk** (`hashtableSnapshotWalkVisit`, bucket-by-bucket) that updates the version array so the
mutation hook knows the walker already passed a bucket.

**These do not compose.** A `kvstoreIterator` walk never calls `hashtableSnapshotWalkVisit`, so it
never lifts a bucket above the cut on the walk side. Then a write to an already-walked bucket sees
`ver[b] <= cut`, fires the hook, and serializes the bucket *again* → **the same keys appear twice
in the RDB**. Conversely, if the walk serializes an entry the hook has already captured, same
duplication. The version-array cut protocol only works if **both** actors go through the `<=`/lift
dance on the **same coordinate** (top-level bucket index).

**Resolution:** the producer must walk **bucket indices**, calling `hashtableSnapshotWalkVisit(ht,
b)` for `b` in `0 .. numBuckets(bucket_exp[0])-1`, and serialize bucket `b` only when it returns
true. The entry-granularity `kvstoreIterator` in §15/§22 is replaced by this bucket walk. This is
the single largest correction the merge forces on the producer.

### S2 — No per-bucket entry accessor (blocking)

Both the walk (S1) and the hook need to *serialize a bucket given its index*: enumerate the live
entries in top-level bucket `b` **and its overflow chain**, and feed each to
`rdbSaveKeyValuePairFromEntry`. **Neither document provides an API for this.** The primitive's
callback yields a `bucket_index`; there is no `hashtable.c` function that turns that index into an
entry list.

**Resolution:** add one primitive-layer accessor, e.g.

```c
/* Visit every live entry in the top-level bucket `bucket_index` and its overflow chain,
 * in tables[0]. Safe to call only while a snapshot is active (table is frozen, §7). */
void hashtableSnapshotVisitBucketEntries(hashtable *ht, size_t bucket_index,
                                         void (*fn)(void *entry, void *privdata), void *privdata);
```

It is a few lines over `getChildBucket` + the `presence` bitmask, but it belongs in Part I and its
absence is why the producer's `snapshotStep` (§22) cannot be written as drafted. It also needs a
matching kvstore-level shim so the producer addresses `(dbid, slot, bucket_index)`.

### S3 — Hook signature (minor, mechanical)

The producer draft (§22) declares `void snapshotPreimageHook(hashtable *ht, bucket *b)`. The
primitive's callback is `(hashtable *ht, int table_index, size_t bucket_index,
hashtableSnapshotCaptureReason reason, void *privdata)`. There is no `bucket *` and there is a
`reason`. **Resolution:** the producer registers a callback of the primitive's type; inside it,
resolve entries via S2's accessor. The `bucket *b` form in §22 is wrong and is corrected there.

### S4 — "Pre-image is a memcpy" is false; it is a synchronous serialize (latency defect)

The producer claims (§15) the mutation hook's inline cost is "a memcpy, not an I/O," staging raw
bytes for the producer to serialize later. **This is unsafe and therefore wrong.** A bucket's
entries are *pointers* to `robj` values living elsewhere. The mutation that triggered the hook —
an overwrite or a delete — may **free the old `robj`** the instant the hook returns. Copying the
64-byte bucket copies dangling pointers.

So conservative capture must **fully RDB-serialize the bucket's entries synchronously, inside the
hook, before the write proceeds** — `rdbSaveKeyValuePairFromEntry` for each entry, which allocates
and may LZF-compress. The hook is therefore **un-budgeted serialization on the serving thread**
(§15 already says the hook is not time-sliced — but under-counts its cost). Worst case: a single
`SET` overwriting one key in a bucket that also holds a 1 MB value pays the cost of serializing
that 1 MB value inline before the `SET` completes.

**Resolution / mitigations, none free:**
- Surface it honestly in the latency model (§25) and metric it (`rdb_forkless_preimage_bytes`,
  §19).
- Cap: refuse fork-less save (fall back to fork, or defer) for tables holding values above a
  size threshold, or serialize oversized values via the relaxed variant.
- The pre-image *can* still be staged as bytes — but they must be the **serialized RDB bytes**,
  produced in the hook, not a raw bucket copy. Staging then defers only the `rioWrite`, not the
  encoding.

### S5 — The overwrite path bypasses the hook (blocking correctness)

`dbSetValue` (`src/db.c:319-370`) — the overwrite path for `SET` on an existing key — does **not**
go through any of the four hooked mutators. It uses read-only `hashtableFindRef` and then mutates
in place / raw-writes the value pointer. The primitive (§3) explicitly defers this "until the RDB
producer needs overwrite coverage."

**The producer needs it now.** Without it, an overwrite during a snapshot never fires the hook, so
the pre-image is never captured, so the walk later serializes the **new** value → the RDB is
**not point-in-time**. This directly falsifies the producer's §16 claim of "a true point-in-time
image as of the cut." Eviction is fine (it is a delete, hooked); plain overwrites are the hole.

**Resolution (mandatory, Part I follow-up):** either add a hooked overwrite entry point
(`hashtableOverwriteAtRef`-style) and route `dbSetValue` through it, or add an explicit
`snapshotMutationHook` call at the `dbSetValue` site. Must land **before** the Stage-2 consistency
gate (§26), and the gate's write mix must include overwrites of existing keys or it will not catch
this.

### S6 — Defrag is not frozen (blocking correctness)

The §7 `resize()` guard freezes expand/shrink/rightsize. **It does not freeze active defrag.**
Defrag relocates entries between buckets via `defrag.c`'s callbacks and
`hashtableReplaceReallocatedEntry` — outside `resize()` and outside the four mutators. During a
snapshot, moving an unserialized entry from an unwalked bucket into an already-walked one **loses**
it from the RDB; moving a serialized entry into an unwalked bucket **duplicates** it. The primitive
defers this (§3); Dashtable-adoption §5 flags it "must be verified."

**Resolution (mandatory):** defrag must participate in the freeze while a snapshot is active —
either suspend active-defrag on a table with `snapshot_active` (simplest; bounded by save
duration, consistent with the resize freeze), or route defrag relocations through the mutation
hook so a moved entry is captured before it moves. v1: suspend. Add it to the §24
`hasActiveChildProcess` audit's sibling — a "structural-mutation-during-snapshot" audit.

### Seam summary

| # | Defect | Severity | Lands in |
|---|---|---|---|
| S1 | Producer walks entries; primitive versions buckets → double/lost keys | **Blocking** | Producer §22 rewrite to bucket walk |
| S2 | No "entries of bucket i" accessor | **Blocking** | New primitive API (§12 S2) |
| S3 | Hook signature `bucket *` vs `(index, reason)` | Minor | Producer §22 |
| S4 | Hook cost is serialize, not memcpy | Latency | Honest §25 + cap |
| S5 | `dbSetValue` overwrite bypasses hook → not point-in-time | **Blocking** | Primitive follow-up + gate |
| S6 | Defrag moves entries unfrozen → lost/dup keys | **Blocking** | Freeze defrag during snapshot |

None of these is visible reading either document alone. All are cheap to state, and S1/S2 change
the producer's core loop — which is exactly why the merge was worth doing before writing code.

---

# Part III — The producer

Consumes Part I (as corrected by Part II). Replaces the fork/child/reap machinery of §2.

## 13. Architecture — the producer replaces the child

A **snapshot producer** runs **in the serving process**:

- It holds a P1 cut on the target table(s) and walks **bucket indices** (§12 S1) in order, feeding
  each captured bucket's live entries to the existing record writer over a `rio` (§16).
- Concurrent writes hit the P1 mutation hook, which serializes a bucket's pre-image into the same
  stream before the write applies (conservative, §12 S4).
- Structural change (rehash/resize, §7; **and defrag**, §12 S6) is paused for the save's duration,
  so bucket indices stay valid.

Single-threaded: **one producer at a time** (matching today's single child). In the
slot-per-thread world, each owning thread runs a producer over its own slots, and cross-shard
consistency comes from the replication journal, not the walk ([10 §5](10-dragonfly-snapshot-model.md);
slot-per-thread §replication).

## 14. Cooperative, time-sliced execution

The fork child had its own CPU; the producer shares the serving thread, so it **must not run to
completion in one shot** — it yields:

- Drive it from the event loop, like incremental rehash's microsecond budget
  (`hashtableRehashMicroseconds`, `src/hashtable.h:135`) and the cron save-check
  (`src/server.c:1732`). Each `beforeSleep`/cron tick, serialize for a bounded budget (a few
  hundred µs), persist a **resume cursor** (`(dbid, slot, bucket_index)` — the coordinates the
  kvstore cursor already encodes, `src/kvstore.c:141-148`), and return to serving.
- The mutation hook is **not** budgeted — it completes its capture inline (§12 S4). Its cost is a
  full bucket serialize, not a memcpy; the budget bounds the *walk*, not the hook.

The central latency trade: fork-less removes the fork stall but spreads serialization CPU across
many ticks, plus imposes un-budgeted per-write capture cost during the save. Total wall-clock save
time rises — acceptable and tunable; the per-write spikes are the sharp edge (§25).

## 15. Byte production and consistency

`rdbSaveRio` (`src/rdb.c:1481`) writes through the `rio` abstraction that feeds a file, a socket,
or a buffer identically — why the same code serves disk saves *and* replica streaming
(`rdbSaveRioWithEOFMark`, `:1534`). Fork-less changes **who calls the record writer and how it
yields**, not the writer:

- Disk → `rio` over a temp RDB file, `rename(2)` on completion (as
  `backgroundSaveDoneHandlerDisk`, `:3667`).
- Diskless → `rio` fanning out to N replica sockets (§18).

The producer replaces the *driver*, not the *record writer*. Instead of one uninterrupted
`kvstoreIterator` pass, it walks **buckets** under the cut (§12 S1) with the §14 resume cursor.

**Format unchanged (§16 requirement).** Version tags, opcodes, and per-type encodings are
untouched; a fork-less RDB is byte-compatible with a forked one — same loaders, tooling, replicas.
This is the basis of the equivalence gate (§25).

**Consistency.** With S5/S6 closed, conservative mode gives a true point-in-time image as of the
cut. For replication, writes after the cut accumulate in the replica output buffer / backlog and
are sent after the RDB — **exactly today's RDB-plus-backlog contract**; the PSYNC/offset contract
is unchanged.

## 16. (Format is unchanged — see §15.)

The RDB byte format is a hard invariant, called out separately because it is the equivalence
gate's whole premise: fuzz-corpus and real-dump saves under fork vs. fork-less must be
byte-identical (or load-equal by `DEBUG DIGEST` if aux timestamps differ). §25.

## 17. Save lifecycle without a child

Fork-less replaces fork→reap→dispatch with an in-process state machine:

```
IDLE → STARTING → RUNNING → { FINALIZING → DONE | FAILED | ABORTED } → IDLE
```

- **STARTING:** take the P1 cut (`hashtableSnapshotStart` per owned table), pause structural
  change **and defrag** (§12 S6), open the target (temp file or replica sockets), init the resume
  cursor.
- **RUNNING:** each tick, walk buckets within the budget (§14); the hook drains pre-images inline;
  advance the cursor.
- **FINALIZING (disk):** flush/fsync temp file, `rename(2)`, update
  `stat_rdb_saves`/`lastbgsave_status`, run the same post-save bookkeeping as
  `backgroundSaveDoneHandlerDisk`.
- **FINALIZING (socket):** write the EOF mark, transition replicas `WAIT_BGSAVE_END` → online
  (mirrors `backgroundSaveDoneHandlerSocket`, `:3694`).
- **FAILED / ABORTED:** `hashtableSnapshotEnd` per table (frees the version arrays, resumes
  structural change + defrag), discard the temp file / drop replicas. `ABORTED` replaces
  `killRDBChild` (`:3740`) — teardown, no signal.

**`hasActiveChildProcess()` decoupling.** Many sites gate on `child_type == CHILD_TYPE_RDB`
(`:897`). Fork-less introduces `rdbSaveInProgress()`. Some of those gates exist *because of fork*
(e.g. "don't rehash while a child holds COW pages") and are **no longer needed** fork-less — each
must be reviewed (§24).

## 18. Diskless replication fan-out — the sharp edge

Today one child streams to all attaching replicas; a slow replica just back-pressures the child —
harmless to serving (`startBgsaveForReplication`, `src/replication.c:1019`; attach cases
`:1246-1288`). Fork-less breaks that isolation: the producer *is* the serving thread, so **a slow
replica must never block serving.**

- Replica sockets are **non-blocking**; the producer writes what it can and, on `EAGAIN`, buffers
  into that replica's output buffer and yields — never spins or blocks.
- Forward progress is gated by the **slowest replica's buffer high-water mark**, not a blocking
  write. A replica exceeding `client-output-buffer-limit` is **dropped** (same policy as today);
  the producer continues.
- Multiple attaching replicas share **one cut and one walk**; each gets the same bytes via the
  fan-out `rio`. Late attachers within the same cut join (CASE 1, `:1246`); those arriving after
  the walk passed their coverage wait for the next save (CASE 3, `:1288`).
- Disk-vs-socket target selection (`:1033`) is unchanged.

The single biggest new risk fork introduced-did-not-have; §25 treats it as such.

## 19. Config and observability

**Config:**
- `rdb-forkless {no|yes|replication-only}` — opt-in, default `no` (§26). Enables it for saves, for
  replication, or both.
- `rdb-forkless-slice-us <n>` — the per-tick **walk** budget (§14). (Does not bound the hook, §12
  S4.)

**INFO remapping** (`src/server.c:6413-6425`):
- `rdb_bgsave_in_progress` stays "a background save is running" — true when the producer is
  RUNNING, so existing tooling keeps working.
- `current_cow_size` / `rdb_last_cow_size` → **~0** fork-less (no COW). Keep the fields; add:
  - `rdb_save_progress_pct` — buckets walked / total.
  - `rdb_forkless_preimage_bytes` — bytes serialized by the mutation hook this save. **The
    fork-less analog of COW pressure and the §12 S4 latency signal — the number operators watch.**
- `rdb_last_bgsave_status` / `stat_rdb_saves` unchanged.

## 20. Interactions

- **maxmemory / eviction during a save.** Evictions are ordinary deletes on the serving thread and
  hit the mutation hook, so evicted keys are captured at their at-cut value before removal —
  correct by construction, and cleaner than fork (the child was oblivious to parent evictions).
- **AOF rewrite** also forks (`src/aof.c`, gated `src/server.c:1631,1661`). The same P1 primitive
  applies; the AOF base-file production is a follow-up with its own equivalence gate. Not designed
  here.
- **One heavy op at a time.** A fork-less RDB and a fork-less AOF rewrite over the same tables is
  possible but should be **serialized** initially (one producer), matching today's single-child
  reality.
- **Shutdown during a save.** `SHUTDOWN` waits for or aborts the producer instead of the child; a
  `SHUTDOWN SAVE` can finalize synchronously via the blocking `rdbSave` path.

## 21. AOF — snapshot-frequency as a durability knob

Dragonfly is snapshot-only; it is tempting to read "cheap snapshots replace AOF." They don't, and
being precise about why says which half Valkey should follow. (Terminology trap: "relaxed" in
[10 §4](10-dragonfly-snapshot-model.md) is a *within-snapshot* consistency mode — serialize the
new value in place, no pre-image — **not** a durability scheme. For persistence use
**conservative**; relaxed's "as-of-finish" is not a clean crash-recovery instant.)

| | Cost per unit of durability | Data-loss window on crash |
|---|---|---|
| **AOF** | **O(writes)** — each write logged once | fsync interval: **≤1 s** (`everysec`) or **0** (`always`) |
| **Snapshot-only** | **O(dataset)** — re-serialize everything, each time | the **snapshot interval** |

To match `everysec` with snapshots you'd re-serialize the whole dataset every second — fine for a
small instance, absurd for a large one. So snapshot-only is a **simplification with a durability
downgrade**, defensible for a replication-HA + backup-DR posture, not a strict win.

**Valkey already owns the stronger half.** The multi-part AOF (chapter 06) is *a snapshot base
plus an incremental tail*: `aof-use-rdb-preamble` defaults yes (`src/config.c:3364`), so the AOF
base **is** an RDB snapshot and the incrementals are the command log since it, tied by a manifest.
That is strictly more capable than snapshot-only.

**So what fork-less snapshotting changes is not "drop AOF" — it makes the snapshot cheap enough to
move the durability knob:**
- **Snapshot-only becomes a real *option*** — once a snapshot costs no fork stall and no 2× COW,
  snapshotting every few seconds is affordable for users who tolerate an interval-sized loss
  window. Offer it as an explicit mode, **never** as a replacement.
- **AOF's replay tail can shrink toward zero** — a cheap fork-less base lets you rewrite the base
  far more often, keeping the command tail (replay time, loss surface) tiny. The manifest already
  supports sliding along this continuum; fork-less RDB is the enabler, no new format.

Bottom line: follow Dragonfly's cheap-snapshot half wholeheartedly; follow its drop-AOF half only
as an opt-in mode.

## 22. Producer data structures and functions (seam-corrected)

The single hardest problem: today's walk (`rdbSaveRio` → `rdbSaveDb` → `kvstoreIteratorNext`,
`src/rdb.c:1417-1421`) runs to completion in one synchronous call. Fork-less makes it **resumable
across ticks** while the tables keep serving — **and, per §12 S1, bucket-indexed, not
entry-indexed.**

### 22.1 The resumable-walk decision

| Option | How | Verdict |
|---|---|---|
| **A. Bucket-index walk** *(required by S1)* | Each tick, for `b` from the resume cursor: `if (hashtableSnapshotWalkVisit(ht, b)) serialize bucket b via the S2 accessor`. Cursor is `(dbid, slot, bucket_index)`. | **Use this.** It is the only walk that participates in the version-array cut protocol, so walk and hook never double-capture. |
| **B. Long-lived `kvstoreIterator`** *(original draft)* | Keep one iterator alive, `kvstoreIteratorNext` per tick. | **Rejected — §12 S1.** Entry-granularity; does not lift buckets on the walk side, so it duplicates or loses keys against the hook. |

The original draft recommended B; the seam analysis overturns it. B's apparent simplicity is the
bug.

### 22.2 Structures (`src/rdb_forkless.h`)

```c
typedef enum { SNAP_IDLE, SNAP_STARTING, SNAP_RUNNING,
               SNAP_FINALIZING, SNAP_DONE, SNAP_FAILED, SNAP_ABORTED } snapState;
typedef enum { SNAP_TARGET_DISK, SNAP_TARGET_SOCKETS } snapTarget;

typedef struct snapshotProducer {
    snapState        state;
    snapTarget       target;
    rio              rdb;              /* the SAME rio (§15): file or fan-out */
    int              req, rdbver;
    rdbSaveInfo     *rsi;

    /* Resume state: bucket-indexed walk (§22.1 A). */
    int              cur_db;
    int              cur_slot;         /* kvstore hashtable index within the db */
    size_t           cur_bucket;       /* next top-level bucket to WalkVisit in the current table */
    int              phase;            /* header / dbs / footer — mirrors rdbSaveRio's stages */

    /* Pre-image staging (§12 S4): the hook serializes into here; the producer drains it. */
    unsigned char   *preimage_buf; size_t preimage_len, preimage_cap;

    char            *tmpfile; int fd;  /* disk target */
    list            *replicas;         /* socket target: clients sharing this cut */

    unsigned long long buckets_total, buckets_done, preimage_bytes;   /* metrics (§19) */
    monotime         started;
} snapshotProducer;

extern snapshotProducer *server_snapshot;   /* NULL when IDLE; one at a time */
```

### 22.3 Functions (`src/rdb_forkless.c`)

```c
/* Replaces rdbSaveBackground's fork branch. Takes the cut on every owned table
 * (hashtableSnapshotStart), registers the capture callback, pauses defrag, returns
 * immediately; the walk happens over subsequent ticks. */
int  snapshotStart(int req, snapTarget target, char *filename_or_null,
                   rdbSaveInfo *rsi, list *replicas_or_null);

/* Driven from beforeSleep. Walks buckets for up to slice_us via hashtableSnapshotWalkVisit,
 * drains the pre-image buffer first, advances the cursor. RUNNING->FINALIZING when done. */
int  snapshotStep(snapshotProducer *p, int slice_us);

/* FINALIZING half (§17). Disk: fsync+rename+bookkeeping (mirrors :3667). Socket: EOF mark +
 * replica online (mirrors :3694). Calls hashtableSnapshotEnd per table; resumes defrag. */
void snapshotFinalize(snapshotProducer *p);

/* Replaces killRDBChild (:3740): hashtableSnapshotEnd per table, discard temp file / drop
 * replicas, resume defrag. No signal. */
void snapshotAbort(snapshotProducer *p);

/* The P1 capture callback (primitive signature, §12 S3), registered on every owned table.
 * Called INLINE before a write applies (NOT budgeted, §12 S4). Serializes the bucket's live
 * entries — via the S2 accessor — into p->preimage_buf as RDB bytes (NOT a raw copy). */
void snapshotOnCapture(hashtable *ht, int table_index, size_t bucket_index,
                       hashtableSnapshotCaptureReason reason, void *privdata);

static inline int rdbSaveInProgress(void) {
    return server_snapshot && server_snapshot->state == SNAP_RUNNING;
}
```

### 22.4 `snapshotStep` — bucket walk, seam-correct

```c
int snapshotStep(snapshotProducer *p, int slice_us) {
    monotime deadline = getMonotonicUs() + slice_us;

    /* Drain pending pre-images first — they are the at-cut truth (§12 S4). Already RDB bytes. */
    if (p->preimage_len && rioWrite(&p->rdb, p->preimage_buf, p->preimage_len) == 0) goto werr;
    p->preimage_len = 0;

    switch (p->phase) {
    case PHASE_HEADER:                 /* magic + aux + modules-aux + functions (:1488-1496) */
        if (writeHeader(p) < 0) goto werr;
        p->phase = PHASE_DBS; p->cur_db = 0; p->cur_slot = 0; p->cur_bucket = 0;
        /* fallthrough */
    case PHASE_DBS:
        for (; p->cur_db < server.dbnum; p->cur_db++, p->cur_slot = 0) {
            kvstore *ks = server.db[p->cur_db].keys;
            for (; p->cur_slot < kvstoreNumHashtables(ks); p->cur_slot++, p->cur_bucket = 0) {
                hashtable *ht = kvstoreGetHashtable(ks, p->cur_slot);
                if (!ht) continue;
                if (p->cur_bucket == 0 && emitSelectAndResize(p, ht) < 0) goto werr;   /* SELECT/RESIZEDB opcodes */
                size_t nb = hashtableSnapshotBucketCount(ht);                          /* = numBuckets(bucket_exp[0]) */
                for (; p->cur_bucket < nb; p->cur_bucket++) {
                    if (getMonotonicUs() >= deadline) return C_OK;                     /* YIELD: resume here */
                    if (hashtableSnapshotWalkVisit(ht, p->cur_bucket))                 /* §12 S1: lift on walk side */
                        if (serializeBucket(p, ht, p->cur_bucket) < 0) goto werr;      /* §12 S2 accessor */
                    p->buckets_done++;
                }
            }
        }
        p->phase = PHASE_FOOTER;
        /* fallthrough */
    case PHASE_FOOTER:                 /* modules-aux-after + EOF opcode + CRC64 (:1508-1517) */
        if (writeFooter(p) < 0) goto werr;
        p->state = SNAP_FINALIZING;
    }
    return C_OK;
werr:
    p->state = SNAP_FAILED;
    return C_ERR;
}
```

`writeHeader`/`emitSelectAndResize`/`writeFooter` are lifted line-for-line from
`rdbSaveRio`/`rdbSaveDb`; the only new logic is the deadline check and the bucket cursor.
`serializeBucket` and `snapshotOnCapture` share one helper — "serialize every live entry of
bucket `b` via the S2 accessor" — so the walk and the hook emit identical bytes for a bucket.

## 23. Integration points, file by file

| File | Change |
|---|---|
| `src/hashtable.{c,h}` | **Part I** in full: 6 struct fields, `snapshotMutationHook`, the `resize()` guard, the 5 lifecycle functions, the 4 hook call sites, create/empty/memusage. **Plus the §12 S2 accessor** `hashtableSnapshotVisitBucketEntries`. |
| `src/db.c` | **§12 S5:** route `dbSetValue`'s overwrite (`:319-370`) through a hooked path, or add an explicit `snapshotMutationHook` call there. |
| `src/defrag.c` | **§12 S6:** suspend active-defrag on a table while `snapshot_active`, or route relocations through the hook. |
| `src/rdb_forkless.{c,h}` | **New.** §22. |
| `src/rdb.c` | `rdbSaveBackground` (`:1673`): when `server.rdb_forkless` on and target eligible, call `snapshotStart()` instead of `serverFork(CHILD_TYPE_RDB)` (`:1682`). Refactor `writeHeader`/`emitSelectAndResize`/`writeFooter` out of `rdbSaveRio`/`rdbSaveDb` so fork and fork-less share them (byte-identical for the forked path). `rdbSaveToReplicasSockets` (`:3756`) grows a fork-less branch. |
| `src/server.c` | `beforeSleep` (`:1854`): `if (rdbSaveInProgress()) snapshotStep(server_snapshot, server.rdb_forkless_slice_us);` plus a FINALIZING check → `snapshotFinalize`. `checkChildrenDone` (`:1426`) unchanged for AOF/module children; RDB completion no longer flows through it. Audit `hasActiveChildProcess()`/`CHILD_TYPE_RDB` (`:877,:897`) — §24. |
| `src/replication.c` | `startBgsaveForReplication` (`:1019`) → `snapshotStart(..., SNAP_TARGET_SOCKETS, replicas)` when fork-less; attach-to-in-flight (`:1246`) shares `p->replicas` and one cut. |
| `src/config.c` | `createEnumConfig("rdb-forkless", …)` (`no`/`yes`/`replication-only`, default `no`), mirroring `repl-diskless-load` (`:3435`); `createIntConfig("rdb-forkless-slice-us", NULL, MODIFIABLE_CONFIG, 50, 5000, server.rdb_forkless_slice_us, 300, INTEGER_CONFIG, NULL, NULL)`. |
| `src/server.c` INFO (`:6413-6425`) | `rdb_bgsave_in_progress` also true when `rdbSaveInProgress()`; add `rdb_save_progress_pct`, `rdb_forkless_preimage_bytes`; `current_cow_size` ~0. |

## 24. The `hasActiveChildProcess()` audit

Grep `hasActiveChildProcess\|CHILD_TYPE_RDB`; classify each site:

- **Keep, retarget to `rdbSaveInProgress()`** — genuine mutual exclusion: "don't start a second
  heavy op," "don't start AOF rewrite while a save runs" (§20), shutdown coordination.
- **Drop** — the gate exists *only* because fork COW made it unsafe, e.g. the dict resize/rehash
  guard at `src/server.c:855` (`server.dict_resizing` / `in_fork_child`). Fork-less has no COW to
  protect and P1 pauses structural change explicitly, so this is a fork artifact. **Dropping the
  wrong one is risk #6 (§25)** — one site at a time, with a test.

A sibling audit is new to the merge: every **structural mutation during a snapshot** — resize
(covered, §7), defrag (§12 S6), and the `dbSetValue` overwrite (§12 S5) — must be either frozen or
hooked. Enumerate them the same way.

## 25. Equivalence and latency harness

- **Byte-equality (Stage 1 gate).** `tests/integration/rdb-forkless.tcl`: for a corpus
  (fuzz-generated + real dumps), produce an RDB with `rdb-forkless no` and `yes` under identical,
  quiesced state; assert byte-identical (or load-equal via `DEBUG RELOAD` + `DEBUG DIGEST` if aux
  timestamps differ). Mechanizes §16.
- **Consistency under writes (Stage 2 gate).** Continuous writes during a fork-less save; assert
  the loaded RDB equals a `DEBUG DIGEST` captured at the cut. **The write mix must include
  overwrites of existing keys and run with defrag enabled**, or it will not exercise §12 S5/S6 —
  the two gaps most likely to pass a naïve test and corrupt real data.
- **Latency (Stage 2 gate).** `valkey-benchmark` write load during a save; assert p99 within
  target. **Two bounds to check, not one:** the *walk* never exceeds `rdb-forkless-slice-us` by
  more than one `rdbSaveKeyValuePair` (deadline checked between buckets — a single huge value is
  the walk's worst case); and the *hook* (§12 S4) capture cost, which is **un-budgeted** — measure
  the worst-case single-write latency when overwriting into a bucket holding a large value, and
  publish it as the fork-less latency ceiling.
- **Slow-replica isolation (Stage 3 gate).** A replica reading at 1 MB/s must not raise primary
  p99; the producer keeps serving others and drops the slow one at the COB limit rather than
  stalling.

---

# Part IV — Rollout, risks, open questions

## 26. Unified build order

Fork-less must earn `default on`, coexisting with fork throughout. This merges the primitive's and
producer's staged plans into one seam-aware order:

| Stage | Deliverable | Gate |
|---|---|---|
| **0** | Stage 0 numbers: fork stall (ms), peak COW RSS on a large write-heavy instance, snapshot-walk cost estimate. | Numbers justify the build ([proposal-stage0-measurement.md](proposal-stage0-measurement.md) §5.D). |
| **1** | **Primitive (Part I):** version array + hook + `resize()` freeze + the §12 S2 bucket-entry accessor. | The §11 unit suite; zero steady-state overhead confirmed. |
| **1b** | **Close the blocking seam gaps: S5** (`dbSetValue` hook) and **S6** (freeze defrag). | Unit + targeted tests: an overwrite and a defrag relocation during an active snapshot both fire capture exactly once. **This gate did not exist in either source doc and is the merge's main addition.** |
| **2** | Refactor `writeHeader`/`emitSelectAndResize`/`writeFooter` out of `rdbSaveRio`/`rdbSaveDb`; fork path stays byte-identical. | Pure refactor; forked RDB unchanged on fuzz corpora. |
| **3** | Fork-less **disk** `BGSAVE` (§22, bucket walk) behind `rdb-forkless=yes`; fork path default. | §25 byte-equality **and** consistency-under-writes-with-overwrites-and-defrag. |
| **4** | Cooperative budget tuning; latency gate incl. the un-budgeted hook ceiling (§25). | p99 within target; save completes in bounded wall-clock; hook ceiling published. |
| **5** | Fork-less **diskless replication** full sync + fan-out + slow-replica handling (§18). | Replica converges bit-for-bit; a stalled replica does not raise serving latency. |
| **6** | Flip default to fork-less where supported; keep fork as fallback one release. | Soak in production-like load; no regression in save success rate. |

Stage **1b** is the reason the merge matters: on the separate docs it fell through the crack
between "the primitive defers this" and "the producer assumes this."

## 27. Risks

1. **Un-budgeted per-write capture cost (§12 S4).** The sharpest new edge, and under-stated in the
   source docs. A write into an un-captured bucket pays a full inline bucket serialize; a large
   value makes it a large synchronous stall on one command. Mitigation: value-size cap / relaxed
   fallback for oversized values; metric it (`rdb_forkless_preimage_bytes`); publish the ceiling.
2. **Serving latency from in-process walk (§14).** The budget bounds per-tick walk impact; per-shard
   producers in the threaded world; measure p99, not mean.
3. **Slow-replica back-pressure onto serving (§18).** New failure mode. Strictly non-blocking
   fan-out, COB-limit drops, never block the producer.
4. **Lost/duplicated keys from an unfrozen structural mutation (§12 S1/S6).** The correctness core.
   Bucket-indexed walk (S1) + defrag freeze (S6) + overwrite hook (S5) together close it; the
   consistency gate must exercise all three.
5. **Structural-change pause stalls a needed resize (§7).** A long save defers growth. Bounded save
   duration; both-tables coverage as a follow-up.
6. **Silent removal of a fork-motivated gate (§24).** Dropping a `hasActiveChildProcess` gate that
   guards more than COW is a subtle bug. Review each site explicitly; keep genuine mutual-exclusion
   gates.
7. **Two save paths during transition.** Larger test surface. Byte-equality gates before any
   default flip.

## 28. Open questions

- **Who runs the producer in single-threaded mode** — a `beforeSleep` job, or a dedicated
  I/O-thread task (`src/io_threads.c`) to isolate serialization CPU from command execution?
- **Pre-image staging cap and policy** when a write burst outruns the drain rate — block the
  writer briefly, or spill? (Interacts with §12 S4: the staged bytes are already-serialized RDB
  bytes, so the cap is on encoded size.)
- **Interaction with `WAIT`/`WAITAOF`** and the exact offset assigned at the cut, so post-cut
  acknowledgements remain correct.
- **Should `rdb-forkless-slice-us` be adaptive** (widen when idle, shrink under load)?
- **Overwrite-hook shape (§12 S5)** — a new `hashtableOverwriteAtRef` API vs. an explicit hook call
  at the `dbSetValue` site. The former is cleaner but widens the `hashtable.h` ABI.
- **Per-`kvstore` vs per-hashtable version array** — per-hashtable (per-slot) composes better with
  slot-per-thread and per-slot snapshots (Dashtable-adoption §10).

## 29. References

- Conceptual parent (why P1, Design A/B, optional P2):
  [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §4, §6.
- Exact version/cut rules: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md).
- Fork baseline being replaced: [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md).
- The gating measurement: [proposal-stage0-measurement.md](proposal-stage0-measurement.md) §5.D.
- Threaded endgame and journal ordering:
  [proposal-slot-per-thread.md](proposal-slot-per-thread.md).
- Code: `src/hashtable.{c,h}` (primitive, mutators, `zcalloc_num` at `src/zmalloc.c:311`),
  `src/db.c:319` (`dbSetValue`, S5), `src/defrag.c` (S6), `src/rdb.c` (save + done handlers),
  `src/replication.c` (full sync), `src/server.c` (child lifecycle, `beforeSleep`, INFO),
  `src/kvstore.c` (walk + cursor).
