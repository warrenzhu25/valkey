# Proposal — Hashtable snapshot versioning primitive (P1)

**Status: pre-issue draft, ready to implement.** This is the concrete, line-anchored
implementation of **P1** from
[proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §4 — the version-stamp +
serialize-before-mutate hook, modeled on Dragonfly's Dash mechanism
([10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) §2). It is the first of
several stages toward fork-less RDB snapshotting
([proposal-forkless-rdb.md](proposal-forkless-rdb.md)), and is scoped to be useful and
testable entirely on its own.

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

**In scope:** a self-contained primitive in `src/hashtable.c`/`src/hashtable.h` —
per-table epoch/cut, a lazily-allocated per-bucket version array, a mutation hook wired
into every keyspace-mutating entry point in `hashtable.c`, and a snapshot-duration freeze
on structural change (rehash/resize). The hook fires through a caller-supplied callback;
this stage has no real consumer, so a unit-test suite plays that role.

**Out of scope:** RDB, fork, replication, `rio`, the resumable serializer walk itself
(`snapshotStart`/`Step`/`Finalize` from proposal-forkless-rdb.md §13), and the
`dbSetValue` overwrite-path gap in `src/db.c` (§7 below) — all deferred to whenever the
real RDB producer is built. **Zero behavior change** when no snapshot is active is a hard
requirement: the only steady-state cost is one `if (unlikely(ht->snapshot_active))`
branch per mutation.

## 2. Two gaps found against the original design docs

1. **Structural-change freeze is incomplete if only `pause_rehash` is reused.**
   `pause_rehash` (via `hashtablePauseRehashing`, `hashtable.c:1409-1419`) only stops an
   **already-started** rehash from stepping (`rehashStepOnReadIfNeeded`/
   `rehashStepOnWriteIfNeeded` check it, `hashtable.c:726,736`). It does **not** stop a
   **new** rehash from starting: `hashtableExpandIfNeeded`/`hashtableShrinkIfNeeded` call
   `resize()` (`hashtable.c:746`) whenever `!hashtableIsRehashing(ht)`, which
   unconditionally sets `rehash_idx = 0` and allocates `tables[1]`. Left unfixed, enough
   inserts during an active snapshot silently start a rehash mid-snapshot, invalidating
   the "only `tables[0]` matters" invariant the whole scheme depends on. **Fix:** add a
   `snapshot_active` guard directly inside `resize()` (§4.3), the single choke point for
   expand/shrink/rightsize.
2. **Bucket chaining.** The `chained:1` bit (`hashtable.c:293-298`) means a bucket can
   overflow into further 64-byte "child" buckets (`getChildBucket`,
   `hashtable.c:238-256`). A "bucket" for versioning purposes must mean the whole chain
   rooted at a top-level slot, not just the head — otherwise entries in an overflow
   bucket are silently unversioned. Resolved by keying the version array on the
   *top-level* bucket index only (§4.1) — a mutation anywhere in a chain hits the same
   version slot.

Also confirmed and deliberately **not** fixed here: `dbSetValue`'s overwrite path
(`src/db.c:319-370`) bypasses every `hashtable.c` mutator (it uses the read-only
`hashtableFindRef` and then mutates in place or does a raw pointer write). This is a real
gap for whenever the RDB producer needs overwrite coverage, but is out of scope for this
primitive (see §7).

## 3. Verified anchors (this checkout)

- Bucket: `hashtable.c:293-301` — `chained:1`, `presence`, `hashes[7]`, `entries[7]`,
  `static_assert(sizeof(bucket) == HASHTABLE_BUCKET_SIZE)` (64 bytes, `hashtable.h:95`).
- `struct hashtable`: `hashtable.c:306-317` — `rehash_idx`, `tables[2]`, `used[2]`,
  `bucket_exp[2]`, `pause_rehash`/`pause_auto_shrink` (int16_t), `child_buckets[2]`,
  `safe_iterators`, `metadata[]` (flexible array, fixed-size at creation — not a fit for a
  lazily-sized version array; new fields go directly on the struct instead).
- `numBuckets(int exp)`: `hashtable.c:455`, static inline, bucket count for table `i` is
  `numBuckets(ht->bucket_exp[i])`.
- `expToMask(int exp)`: used alongside `numBuckets` for bucket-index masking.
- Rehash pause: `hashtablePauseRehashing`/`hashtableResumeRehashing`
  (`hashtable.c:1409-1419`, `static`) — increments `pause_rehash`, also calls
  `hashtablePauseAutoShrink`. `hashtableIsRehashing(ht)` (`:1427-1428`) checks
  `rehash_idx != -1`.
- Resize policy (`HASHTABLE_RESIZE_ALLOW/AVOID/FORBID`, `hashtable.h:87-90`) is a
  **global** policy via `updateDictResizePolicy` (`server.c:854-857`) — unrelated,
  untouched here. The per-table `pause_rehash` counter (plus the new `resize()` guard,
  §4.3) is the correct mechanism.
- `resize()`: `hashtable.c:746` onward; existing `HASHTABLE_RESIZE_FORBID` check around
  `:771-774`. Funnels every structural-change path: `expand()` (`:815-820`),
  `hashtableExpandIfNeeded` (`:1479-1501`), `hashtableShrinkIfNeeded` (`:1506-1517`),
  `hashtableRightsizeIfNeeded` (`:1565-1571`), `hashtableExpand`/`hashtableTryExpand`
  (`:1465-1474`).
- `hashtableCreate`: `hashtable.c:1260-1276` — struct is `zmalloc`'d, **not**
  zero-initialized (`:1263`), so every new field must be explicitly initialized.
- `hashtableEmpty`: `hashtable.c:1280-1281` onward — calls `resetTable()`, which sets
  `bucket_exp[table_index] = -1` (`:450`), invalidating any live version array's
  indexing. `hashtableRelease` (`:1326-1336`) calls `hashtableEmpty` internally
  (`:1328`).
- `hashtableMemUsage`: `hashtable.c:1380-1385`.
- Mutators and exact hook insertion points:
  - `insert()` (`hashtable.c:1092-1103`) — the shared primitive behind `hashtableAdd`
    and `hashtableAddOrFind`. Hook fires after `findBucketForInsert` resolves
    `table_index`/`pos_in_bucket` (`:1098`), before `b->entries[pos_in_bucket] = entry`
    (`:1099`).
  - `hashtableInsertAtPosition` (`:1713-1723`) — hook fires after `assert(!isPositionFilled(...))`
    (`:1718`), before `b->entries[pos_in_bucket] = entry` (`:1720`).
  - `hashtablePop` (`:1728-1748`, shared by `hashtableDelete`) — hook fires right after
    `findBucket` succeeds (`:1734`), before `b->presence &= ~(1 << pos_in_bucket)`
    (`:1736`).
  - `hashtableTwoPhasePopDelete` (`:1853-1876`) — hook fires at the top, using
    `p->bucket`/`p->pos_in_bucket`, before `b->presence &= ~(1 << pos_in_bucket)`
    (`:1862`).
  - Deliberately not hooked: `hashtableReplaceReallocatedEntry` (`:1766-1791` — a
    content-preserving pointer swap for reallocation, no capture needed) and the
    `db.c` overwrite path (§2, §7).

## 4. Design

### 4.1 New fields on `struct hashtable` (`hashtable.c:306-317`)

Inserted between `iter *safe_iterators;` and the flexible-array `metadata[]` member
(which must stay last):

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
    uint64_t snapshot_epoch;      /* Monotonic; incremented on every successful hashtableSnapshotStart(). Never reset. */
    uint64_t snapshot_cut;        /* = snapshot_epoch captured at the start of the *current* snapshot. */
    uint64_t *snapshot_versions;  /* NULL unless snapshot_active. One entry per top-level bucket in tables[0]. */
    hashtableSnapshotCaptureCallback snapshot_on_capture; /* NULL unless snapshot_active. */
    void *snapshot_privdata;
    void *metadata[];
};
```

`snapshot_versions` is indexed by top-level bucket index in `tables[0]` only — because a
snapshot can only start when `rehash_idx == -1` (§4.4) and the new `resize()` guard keeps
it that way for the whole snapshot, `tables[0]` is provably the only live table and its
`bucket_exp[0]` is stable for the duration. `table_index` is therefore always `0` at every
hook call site while a snapshot is active.

Version-array element type is `uint64_t` (matching `snapshot_epoch`/`snapshot_cut`), for
headroom and consistency, not `uint32_t` as the original proposal sketch suggested — cost
is ~8 B/bucket only while a snapshot is in flight, freed immediately after.

### 4.2 New public API (`hashtable.h`)

```c
/* Reason a hashtableSnapshotCaptureCallback fired. */
typedef enum {
    HASHTABLE_SNAPSHOT_CAPTURE_MUTATE, /* A write is about to touch this bucket for the first time since the cut. */
    HASHTABLE_SNAPSHOT_CAPTURE_WALK,   /* hashtableSnapshotWalkVisit() reached this bucket first. Reserved for a
                                         * future serializer walk driver; nothing in this codebase calls it yet. */
} hashtableSnapshotCaptureReason;

/* Callback registered with hashtableSnapshotStart(), invoked once per top-level bucket
 * the first time it is captured. 'table_index' is always 0 (a snapshot keeps the table
 * unrehashed for its duration). 'bucket_index' covers the whole chain rooted there. */
typedef void (*hashtableSnapshotCaptureCallback)(hashtable *ht, int table_index, size_t bucket_index,
                                                  hashtableSnapshotCaptureReason reason, void *privdata);

bool hashtableSnapshotStart(hashtable *ht, hashtableSnapshotCaptureCallback on_capture, void *privdata);
void hashtableSnapshotEnd(hashtable *ht);
bool hashtableSnapshotIsActive(hashtable *ht);
uint64_t hashtableSnapshotGetCut(hashtable *ht);
bool hashtableSnapshotWalkVisit(hashtable *ht, size_t bucket_index);
```

`hashtableSnapshotWalkVisit` is a driver-ready hook point for the (not-yet-built)
serializer walk — it lets the walker race the mutation hook for a given bucket using the
same "first touch, `<=` cut, wins" rule. Nothing calls it yet; the test suite exercises it
directly to prove the race is symmetric (§6).

### 4.3 Mutation hook + the `resize()` freeze

```c
/* Called just before a write touches the top-level bucket 'hash' maps to in tables[0].
 * No-op if no snapshot is active — the only steady-state cost. */
static inline void snapshotMutationHook(hashtable *ht, uint64_t hash) {
    if (likely(!ht->snapshot_active)) return;
    size_t bucket_index = hash & expToMask(ht->bucket_exp[0]);
    assert(bucket_index < numBuckets(ht->bucket_exp[0]));
    if (ht->snapshot_versions[bucket_index] <= ht->snapshot_cut) {
        ht->snapshot_versions[bucket_index] = ht->snapshot_cut + 1;
        if (ht->snapshot_on_capture) {
            ht->snapshot_on_capture(ht, 0, bucket_index, HASHTABLE_SNAPSHOT_CAPTURE_MUTATE, ht->snapshot_privdata);
        }
    }
}
```

In `resize()` (`hashtable.c:746`), immediately after the existing
`HASHTABLE_RESIZE_FORBID` check (`:771-774`):

```c
    if (ht->snapshot_active) {
        /* Structural changes are frozen for the snapshot's duration so top-level bucket
         * indices — and therefore the version array — stay valid. Resumes automatically
         * once the snapshot ends. */
        return false;
    }
```

This single choke point covers every expand/shrink/rightsize path (§3). It is what makes
"table 0 is the only table for the snapshot's duration" actually true, not just true of
already-in-flight rehashes — closing gap #1 from §2.

### 4.4 Snapshot lifecycle

```c
bool hashtableSnapshotStart(hashtable *ht, hashtableSnapshotCaptureCallback on_capture, void *privdata) {
    if (ht->snapshot_active) return false;
    if (hashtableIsRehashing(ht)) return false;
    if (ht->bucket_exp[0] < 0) return false; /* never sized — avoids deadlocking the first-ever insert's resize */

    size_t n = numBuckets(ht->bucket_exp[0]);
    uint64_t *versions = zcalloc_num(n, sizeof(uint64_t));
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, sizeof(uint64_t) * n);

    ht->snapshot_versions = versions;
    ht->snapshot_cut = ht->snapshot_epoch;
    ht->snapshot_epoch++;
    ht->snapshot_on_capture = on_capture;
    ht->snapshot_privdata = privdata;
    ht->snapshot_active = true;

    hashtablePauseRehashing(ht); /* also freezes bucket-chain compaction on delete (fillBucketHole gated by
                                  * hashtableIsRehashingPaused at :1738/:1868) — keeps chain layout stable, free. */
    return true;
}

void hashtableSnapshotEnd(hashtable *ht) {
    if (!ht->snapshot_active) return;
    size_t n = numBuckets(ht->bucket_exp[0]);
    zfree(ht->snapshot_versions);
    if (ht->type->trackMemUsage) ht->type->trackMemUsage(ht, -sizeof(uint64_t) * n);
    ht->snapshot_versions = NULL;
    ht->snapshot_on_capture = NULL;
    ht->snapshot_privdata = NULL;
    ht->snapshot_active = false;
    hashtableResumeRehashing(ht);
}

bool hashtableSnapshotIsActive(hashtable *ht) { return ht->snapshot_active; }

uint64_t hashtableSnapshotGetCut(hashtable *ht) {
    assert(ht->snapshot_active);
    return ht->snapshot_cut;
}

bool hashtableSnapshotWalkVisit(hashtable *ht, size_t bucket_index) {
    assert(ht->snapshot_active);
    assert(bucket_index < numBuckets(ht->bucket_exp[0]));
    if (ht->snapshot_versions[bucket_index] <= ht->snapshot_cut) {
        ht->snapshot_versions[bucket_index] = ht->snapshot_cut + 1;
        if (ht->snapshot_on_capture) {
            ht->snapshot_on_capture(ht, 0, bucket_index, HASHTABLE_SNAPSHOT_CAPTURE_WALK, ht->snapshot_privdata);
        }
        return true;
    }
    return false;
}
```

No new *exported* rehash-pause wrapper is needed: `hashtablePauseRehashing`/
`hashtableResumeRehashing` stay `static` and are called directly from
`hashtableSnapshotStart`/`End`, which live in the same translation unit and are themselves
the new exported symbols.

`zcalloc_num(num, size)` (`src/zmalloc.c:311-321`) is the existing overflow-checked
calloc wrapper — used instead of raw `zcalloc(n * sizeof(uint64_t))`.

**Rehash-in-progress decision:** reject (`hashtableSnapshotStart` returns `false`) if
`hashtableIsRehashing(ht)`. Simplest, safe v1 choice — the caller must wait for the
in-progress rehash to finish and retry. Finishing it synchronously or covering both
tables are follow-ups if this proves too restrictive in practice; not attempted here.

### 4.5 `hashtableCreate` / `hashtableEmpty` / `hashtableMemUsage`

`hashtableCreate` (`hashtable.c:1260-1276`) — struct is `zmalloc`'d, not zeroed; add
explicit initialization of all six new fields (`snapshot_active = false`, counters to
`0`, pointers to `NULL`) alongside the existing `safe_iterators = NULL;` line.

`hashtableEmpty` (`hashtable.c:1280-1281`) — insert as the *first* statement in the
function body:

```c
    if (ht->snapshot_active) {
        /* Emptying invalidates bucket indices; end any active snapshot rather than leak
         * the version array or leave a dangling capture callback. */
        hashtableSnapshotEnd(ht);
    }
```

Since `hashtableRelease` calls `hashtableEmpty` internally, this one change also makes
`hashtableRelease` safe to call with an active snapshot.

`hashtableMemUsage` (`hashtable.c:1380-1385`) — add the version array's current size:

```c
size_t hashtableMemUsage(const hashtable *ht) {
    size_t num_buckets = numBuckets(ht->bucket_exp[0]) + numBuckets(ht->bucket_exp[1]);
    num_buckets += ht->child_buckets[0] + ht->child_buckets[1];
    size_t metasize = ht->type->getMetadataSize ? ht->type->getMetadataSize() : 0;
    size_t snapshot_size = ht->snapshot_active ? numBuckets(ht->bucket_exp[0]) * sizeof(*ht->snapshot_versions) : 0;
    return sizeof(hashtable) + metasize + sizeof(bucket) * num_buckets + snapshot_size;
}
```

### 4.6 The four mutator call sites

**`insert()`** (`hashtable.c:1092-1103`, shared by `hashtableAdd`/`hashtableAddOrFind`) —
`hash` is already in scope, no recompute needed:

```c
static void insert(hashtable *ht, uint64_t hash, void *entry) {
    assert(ht->safe_iterators == NULL);
    hashtableExpandIfNeeded(ht);
    rehashStepOnWriteIfNeeded(ht);
    int pos_in_bucket;
    int table_index;
    bucket *b = findBucketForInsert(ht, hash, &pos_in_bucket, &table_index);
    snapshotMutationHook(ht, hash);                       /* NEW, before the write */
    b->entries[pos_in_bucket] = entry;
    b->presence |= (1 << pos_in_bucket);
    b->hashes[pos_in_bucket] = highBits(hash);
    ht->used[table_index]++;
}
```

**`hashtableInsertAtPosition`** (`hashtable.c:1713-1723`) — the opaque
`hashtablePosition` carries no hash, so recompute from the entry's key, gated behind the
snapshot-active check so steady state pays no extra hash computation:

```c
void hashtableInsertAtPosition(hashtable *ht, void *entry, hashtablePosition *pos) {
    position *p = positionFromOpaque(pos);
    bucket *b = p->bucket;
    int pos_in_bucket = p->pos_in_bucket;
    int table_index = p->table_index;
    assert(!isPositionFilled(b, pos_in_bucket));
    if (unlikely(ht->snapshot_active)) {                  /* NEW */
        const void *key = entryGetKey(ht, entry);
        snapshotMutationHook(ht, hashKey(ht, key));
    }
    b->presence |= (1 << pos_in_bucket);
    b->entries[pos_in_bucket] = entry;
    ht->used[table_index]++;
}
```

**`hashtablePop`** (`hashtable.c:1728-1748`, shared by `hashtableDelete`):

```c
bool hashtablePop(hashtable *ht, const void *key, void **popped) {
    if (hashtableSize(ht) == 0) return false;
    uint64_t hash = hashKey(ht, key);
    int pos_in_bucket = 0;
    int table_index = 0;
    bucket *b = findBucket(ht, hash, key, &pos_in_bucket, &table_index);
    if (b) {
        snapshotMutationHook(ht, hash);                    /* NEW, before presence-bit clear */
        if (popped) *popped = b->entries[pos_in_bucket];
        b->presence &= ~(1 << pos_in_bucket);
        ht->used[table_index]--;
        if (b->chained && !hashtableIsRehashingPaused(ht)) {
            fillBucketHole(ht, b, pos_in_bucket, table_index);
        }
        hashtableShrinkIfNeeded(ht);
        return true;
    }
    return 0;
}
```

**`hashtableTwoPhasePopDelete`** (`hashtable.c:1853-1876`) — the entry is still present
at `b->entries[pos_in_bucket]`, so its key can be recovered for the hash:

```c
void hashtableTwoPhasePopDelete(hashtable *ht, hashtablePosition *pos) {
    position *p = positionFromOpaque(pos);
    bucket *b = p->bucket;
    int pos_in_bucket = p->pos_in_bucket;
    int table_index = p->table_index;

    assert(isPositionFilled(b, pos_in_bucket));
    if (unlikely(ht->snapshot_active)) {                   /* NEW, before presence-bit clear */
        const void *key = entryGetKey(ht, b->entries[pos_in_bucket]);
        snapshotMutationHook(ht, hashKey(ht, key));
    }
    b->presence &= ~(1 << pos_in_bucket);
    ht->used[table_index]--;
    hashtablePauseAutoShrink(ht);
    hashtableResumeRehashing(ht);
    if (b->chained && !hashtableIsRehashingPaused(ht)) {
        fillBucketHole(ht, b, pos_in_bucket, table_index);
    }
    hashtableResumeAutoShrink(ht);
}
```

`hashtableResumeRehashing(ht)` here undoes the *pair-local* pause set up by the preceding
`hashtableTwoPhasePopFindRef` call — a different reason from any snapshot-level pause;
`pause_rehash` is a plain counter, so the two compose correctly without conflict.

## 5. Files touched

- `src/hashtable.h` — new `hashtableSnapshotCaptureReason` enum, capture-callback
  typedef, 5 new function prototypes.
- `src/hashtable.c` — 6 new struct fields; `snapshotMutationHook` static inline helper;
  one new guard in `resize()`; the 5 new exported functions; field init in
  `hashtableCreate`; defensive cleanup in `hashtableEmpty`; version-array accounting in
  `hashtableMemUsage`; hook call sites in `insert()`, `hashtableInsertAtPosition`,
  `hashtablePop`, `hashtableTwoPhasePopDelete`.
- `src/unit/test_hashtable_snapshot.cpp` — **new file**. `test_hashtable.cpp` is already
  57KB; the feature is cleanly separable and the Makefile auto-discovers new `.cpp` files
  by wildcard (`src/unit/Makefile:14`) — no registration needed. Mirror
  `test_hashtable.cpp`'s structure (`extern "C" { #include "hashtable.h" ... }`, a
  `static size_t mem_usage` + `trackmemusage()` callback, a fixture asserting
  `mem_usage == 0` in `TearDown`).

**Not touched:** `src/kvstore.c`, `src/db.c`, `src/rdb.c`, `src/server.c`,
`src/config.c`, `src/unit/test_hashtable.cpp`.

## 6. Test plan (`src/unit/test_hashtable_snapshot.cpp`)

Test-local fixtures needed: a `snapEntry{ long key; long value; }` type with the usual
`entryGetKey`/`hashFunction`/`keyCompare`/`trackMemUsage` wiring (same pattern as
`test_hashtable.cpp`'s `keyval_type`); a second `hashtableType` variant whose
`hashFunction` returns a constant, to force deterministic 100% bucket collisions for the
chaining test; a `CaptureLog` (`std::vector` of `{table_index, bucket_index, reason}`)
and a `testOnCapture` callback that appends to it.

1. **`SnapshotInactiveHasNoEffectOnNormalOps`** — never start a snapshot; run an
   add/find/delete loop; assert `hashtableSnapshotIsActive == false` throughout and no
   snapshot-attributable allocation ever occurs.
2. **`SnapshotStartRejectsUnsizedTable`** — a freshly created table (`bucket_exp[0] ==
   -1`) rejects `hashtableSnapshotStart`.
3. **`SnapshotStartRejectsWhileRehashing`** — force `hashtableIsRehashing(ht) == true`
   (populate past the resize threshold); `hashtableSnapshotStart` returns `false`.
4. **`SnapshotStartEndIdempotencySafety`** — `Start` twice in a row → second call
   returns `false`; `End` while inactive is a safe no-op (call twice, no double-free).
5. **`MutationHookCapturesExactlyOnceInPreCutState`** — delete key K (bucket B); assert
   the callback fired exactly once for B with reason `MUTATE`, and — from inside the
   callback — that K's old value is still visible via `hashtableFind` (proves the hook
   fires before the write); delete a second key also mapping to B; assert no second
   invocation (`<=` semantics: version is now `cut+1 > cut`).
6. **`WalkerWinsRaceAgainstLaterWriter`** — call `hashtableSnapshotWalkVisit(ht, B)`
   directly before any mutation touches B; assert it returns `true` with reason `WALK`;
   then delete a key mapping to B; assert the mutation hook does not fire again.
7. **`WriterWinsRaceAgainstLaterWalker`** (symmetric to #6) — mutate first (hook fires,
   `MUTATE`), then call `hashtableSnapshotWalkVisit(ht, B)`; assert it returns `false`
   and does not invoke the callback again. Together with #6, this is the test that would
   catch a `<` vs `<=` mistake.
8. **`ChainedBucketSharesOneVersionSlot`** — using the constant-hash type, insert enough
   entries to force chaining (verify via the existing `hashtableChainedBuckets` API);
   delete both a head-bucket entry and a chained-child entry; assert exactly one total
   callback invocation, for `bucket_index == 0`.
9. **`SnapshotStartEndTracksMemoryUsage`** — assert `mem_usage` increases by exactly
   `numBuckets(bucket_exp[0]) * sizeof(uint64_t)` on `Start` and returns exactly to the
   pre-`Start` value on `End` (reinforced by the fixture's blanket `mem_usage == 0` check
   after `hashtableRelease`).
10. **`InsertViaAddFiresHookBeforeEntryVisible`** — insert new key K; inside the
    callback, assert `hashtableFind(ht, K, ...)` returns `false` (proves the capture
    happens strictly before the new entry becomes visible).
11. **`InsertViaTwoPhasePositionFiresHookBeforeEntryVisible`** — same assertion via
    `hashtableFindPositionForInsert` + `hashtableInsertAtPosition`.
12. **`DeleteFiresHookBeforeRemoval`** — via `hashtableDelete`; inside the callback,
    assert the key is still present.
13. **`TwoPhasePopDeleteFiresHookBeforeRemoval`** — same via
    `hashtableTwoPhasePopFindRef` + `hashtableTwoPhasePopDelete`.
14. **`SnapshotFreezesResizeUntilEnd`** — note `hashtableBuckets(ht)`; insert enough to
    normally trigger a resize; assert the bucket count is unchanged and explicit
    `hashtableExpand`/`hashtableTryExpand` calls return `false` while active; after
    `End`, confirm a subsequent insert can resize normally.
15. **`EmptyDuringActiveSnapshotEndsItSafely`** — call `hashtableEmpty`/
    `hashtableRelease` directly without calling `End` first; assert
    `hashtableSnapshotIsActive == false` afterward and no leak.
16. **`SnapshotEpochIsMonotonicAcrossGenerations`** — `Start`/record `cut1`/`End`,
    `Start` again/record `cut2`; assert `cut2 > cut1` — the epoch persists and advances
    across separate, non-overlapping snapshot generations on the same table.

## 7. Deliberately deferred (not fixed here)

- **`dbSetValue` overwrite path** (`src/db.c:319-370`): bypasses every `hashtable.c`
  mutator. Needs either a new call site in `db.c` or a new `hashtableOverwriteAtRef`-style
  API — real work, deferred until the RDB producer stage actually needs overwrite
  coverage.
- **Defrag** (`hashtableReplaceReallocatedEntry`, `hashtable.c:1766-1791`, and
  `defrag.c`'s bucket-relocation callbacks): moves entries between buckets outside the
  four enumerated mutators. `hashtableReplaceReallocatedEntry` itself needs no capture
  (content-preserving pointer swap), but defrag's cross-bucket moves during an active
  snapshot are unaddressed here — flagged in
  [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §5 as needing
  verification, not resolved by this stage.
- **The resumable RDB producer, diskless fan-out, and replication integration** — all of
  [proposal-forkless-rdb.md](proposal-forkless-rdb.md).

## 8. Verification

- `make test-unit UNIT_TEST_PATTERN='*Snapshot*'` for the new suite, then full
  `make test-unit` to confirm no regression elsewhere.
- Confirm test 1 (steady-state) exercises real volume (thousands of ops) so "the hook
  branch is free" is substantiated, not just asserted.
- Re-read the four wired-in call sites after editing to confirm the hook fires strictly
  *before* the bucket-slot mutation in each — this ordering is the correctness property
  the whole design rests on.

## 9. References

- Primitive being implemented: [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §4 (P1)
- Mechanics/exact rules: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md)
- Consumer this unblocks (not built here): [proposal-forkless-rdb.md](proposal-forkless-rdb.md)
- Code: `src/hashtable.c`, `src/hashtable.h`, `src/zmalloc.c` (`zcalloc_num`)
