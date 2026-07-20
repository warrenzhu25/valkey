# 10 — Dragonfly's forkless snapshot model

**Status: external reference, not Valkey code.** Unlike notes 00–08, this has no
`file:line` anchors into *this* checkout — it documents **Dragonfly's** design so that
the snapshot/consistency requirements are captured alongside the
[Dashtable + slot-per-thread proposals](proposal-dragonfly-inspired-perf.md). Every
rule here is sourced from Dragonfly's own docs (links at the bottom); confidence is
"the vendor says so," not "I read the Valkey code."

Companion to [proposal-slot-per-thread.md](proposal-slot-per-thread.md) (Stage 4) and
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md), which lists
forkless snapshotting as **D3** and gates it on the Dashtable work (**D2**).

---

## 1. Why this note exists

Valkey's RDB gets a globally consistent point-in-time snapshot **for free** from
`fork()` — the child sees a COW copy of all memory at one instant (note 06,
`rdb.c:1673`). The price is the 2× RAM / COW page-storm problem, also note 06.

Dragonfly throws away the fork and rebuilds that consistency guarantee **in
software**, on top of Dashtable version stamps. Any Valkey design that adopts Dashtable
per shard (**D2/D3**) *also* throws away the fork, so it inherits Dragonfly's problem
and must replicate its solution. This note records exactly what that solution is.

## 2. The mechanism, precisely

**Version = a per-shard monotonic epoch counter**, bumped on every update. Each
Dashtable entry stores its last-update epoch.

**Snapshot start captures the cut** — the shard records its current epoch:

```
SnapshotShard.epoch = shard.epoch++;
```

Then two things run concurrently on the shard, and **both use `<=`** (not `<` — this is
the load-bearing detail):

**Serialization fiber** walks the Dashtable:

```
if (entry.version <= cut.epoch) {
    entry.version = cut.epoch + 1;     // mark serialized, lift above the cut
    SendToSerializationSink(entry);
}
```

**On-mutation hook** fires before any write applies:

```
if (entry.version <= cut.version) {
    // (conservative variant) push the CURRENT value to the sink first
    entry.version = shard.epoch++;     // bump above the cut, then mutate
}
```

**Result: every entry is captured exactly once.** Whoever reaches an entry first — the
fiber or a concurrent writer — serializes it and lifts its version above the cut; the
other side's `<=` test then fails and skips it. Using `<=` rather than `<` is what keeps
an entry sitting *exactly* at the cut epoch eligible for capture.

## 3. Why the hook is mandatory (not an optimization)

Dashtable's iteration only guarantees **convergence + coverage + "at most once."** It
explicitly does **not** guarantee "exactly once" — a segment split during traversal can
make the walk miss or revisit entries. The version scheme upgrades that to correct
exactly-once capture, and the mutation hook covers entries a writer would otherwise
clobber before the fiber reaches them.

Practical consequence for a Valkey port: **you cannot reuse SCAN-style reverse-binary
iteration to drive the snapshot.** The versioning is required for correctness, not for
speed.

**Bucket granularity:** blobs are always flushed at whole-bucket granularity — the
serializer never emits a blob covering only part of a Dash bucket.

## 4. Conservative vs. relaxed — the point-in-time knob

| Variant | Hook pushes… | Snapshot reflects state as of… | Cost |
|---|---|---|---|
| **Conservative** | the **previous** value, before the mutation applies | when snapshotting **started** | maintains a pre-image on every pre-cut write |
| **Relaxed** | nothing extra; the **new** value is serialized in place | when snapshotting **finished** | cheaper, no pre-image bookkeeping |

Conservative is the true point-in-time mode and the one that composes with the
replication journal for full sync. Relaxed is used where a strict instant isn't
required.

## 5. Cross-shard ordering — the part that surprises people

**The RDB snapshot itself does no cross-shard ordering.** Each shard captures its *own*
epoch independently. Blobs from different shards may arrive in **any order**, and each
blob is **"self-sufficient by itself."** There is a *virtual* per-shard cutpoint, not a
single global instant enforced across shards the way `fork()` gives one.

Global consistency for a coherent replica does **not** come from the snapshot ordering.
It comes from **combining the conservative snapshot with the journal stream** during
full sync: the snapshot gives each shard's point-in-time base, and the journal carries
everything after each shard's cut — including the transactional boundaries for
multi-shard commands (Dragonfly's **D4** transaction framework). The ordering guarantee
lives in the journal + transaction layer, not in the snapshot cut.

## 6. Implications for a Valkey Dashtable + slot-per-thread design

- **Add an epoch field per entry and a serialize-before-mutate hook on every write
  path.** This is the load-bearing mechanism; `<=` semantics matter for correctness.
- **Forkless snapshotting is strictly easier than fork in one way:** constant memory
  overhead via back-pressure on the sink, no COW page storm (note 06's failure mode
  disappears).
- **…but strictly harder in another:** you lose the "one `fork()` = one globally
  consistent instant" property. To make `BGSAVE`/full-sync coherent across slots you
  must build the **journal-based consistency layer** — i.e. Dragonfly's D4. That is the
  same transaction/replication machinery slot-per-thread already signs up for, so it is
  not extra scope; it is *the* scope.
- **This reinforces the proposal ordering:** D3 (forkless snapshot) genuinely depends on
  D2 (Dashtable version stamps) and is entangled with D4 (transactions/journal). It is
  not a standalone win you can land before the threading model settles.

## 7. Worked example: insert, remove, TTL — and where the version stamp lives

Every operation below is a **mutation**, and every mutation is exactly where §2's
`entry.version = shard.epoch++` bump (and, mid-snapshot, the serialize-before-mutate
hook) fires. So this doubles as a map of the snapshot hook's call sites.

**Structure recap (Dragonfly Dash).**
- A **directory** of pointers to **segments** (extendible hashing; global depth `G`).
- Each **segment** = an array of **buckets** + a few shared **stash** buckets; the
  segment has a local depth `L ≤ G`.
- Each **bucket** = a small fixed number of slots (~14 in Dragonfly **(approx)**), each
  carrying a **1-byte fingerprint** of its key for SIMD-scannable negative lookups.

### Insert — `SET user:42 …`

1. `h = hash(key)`. Top `G` bits index the directory → segment. Other bits pick a
   **home bucket** and a **neighbor bucket** in that segment (2-choice, like a
   bucketized cuckoo). `fp = low 8 bits` → fingerprint.
2. Place into the **less-full** of the two candidate buckets ("balanced insert" — keeps
   load even so splits stay rare). If both are full, fall back to the segment's
   **stash**.
3. If home + neighbor + stash are all full → the **segment is full → split just that
   segment**: allocate one new segment, bump its local depth, and rehash *only that
   segment's* keys across the two using the next hash bit. If `L` would exceed `G`,
   first **double the directory** — which only doubles a pointer array; existing
   segments are shared, not copied.
4. **Version:** `entry.version = shard.epoch++`. Mid-snapshot, if the target bucket's
   version `<=` the cut, its pre-image is flushed first (§2).

**This is the whole "better than a rehashing hashtable" story, concretely.** A Valkey
`hashtable` at load factor allocates a **second full table** and migrates entries
between the two over many subsequent ops — both tables coexist (a peak-memory bump) and
every op pays a migration tax until it drains. Dash never touches the whole table: it
splits **one segment** (a few KB), so a resize is O(one segment) in both time and extra
memory, with **no two-table window**. That bounded, incremental resize — plus the
per-bucket version stamp — is the *entire* remaining delta over Valkey's already
cache-line-bucketed `hashtable.c` (see
[proposal §2](proposal-dragonfly-inspired-perf.md): the raw-lookup gap is already
closed, so Dash is **not** a win on lookup speed, only on resize behavior and snapshots).

### Remove — `DEL user:42`

1. Locate segment → home/neighbor bucket → SIMD-match the fingerprint → confirm with a
   full key compare (a 1-byte fingerprint can collide).
2. Clear the slot. **No tombstone** — Dash probes only a bucket + its neighbor + stash
   (not a long linear run), so a freed slot is immediately reusable without breaking any
   probe chain. (Segment *merge* on underflow is possible but Dragonfly generally
   doesn't bother; tables rarely shrink in practice.)
3. **Version:** bump as above; mid-snapshot the pre-delete value is serialized first
   under the conservative variant, so the key still lands in the snapshot as it was at
   the cut.

Removal cost is comparable to Valkey's `hashtable` (also bucketed) — the delete itself
isn't the differentiator; the resize/shrink story is.

### TTL — `SET user:42 … EX 60` / `EXPIRE`

Dragonfly keeps expiry **out of the hot table**:

- Per shard there are **two** Dashtables: the **PrimeTable** (key → value) and a
  separate **ExpireTable** (key → expiry).
- The prime value (`CompactObj`) carries a single **`HasExpire` bit**. TTL-less keys —
  the common case — pay **zero** expiry bytes and never touch the ExpireTable.
- Setting a TTL: flip `HasExpire`, and insert/update the key in the ExpireTable. The
  timestamp is stored **relative to a per-DB base time**, compressed to ~4 bytes instead
  of an 8-byte absolute ms.
- **Lazy expiry:** on access, if `HasExpire` is set, consult the ExpireTable; if past,
  delete from **both** tables (each delete is a versioned mutation → snapshot-visible).
- **Active expiry:** piggybacks on bucket traversal — since a scan is already touching a
  cache-line bucket, it opportunistically purges expired neighbors in the same bucket
  rather than sampling random keys.

Valkey is structurally similar here — it already keeps a separate `db->expires` kvstore
per slot — so "two tables" is **not** the Dragonfly advantage. The refinements that
*are* Dragonfly-specific: the in-value **presence bit** (skip the expire lookup entirely
for non-volatile keys) and the **base-relative compressed timestamp**.

> Confidence: the two-table (Prime/Expire) split, the `HasExpire` bit, and base-relative
> timestamps are from Dragonfly's docs/source; exact per-bucket slot counts and whether
> segment-merge is enabled are **(approx)** — verify against `src/core/dash.h` /
> `dash_internal.h` before relying on the numbers.

## 8. Sources

- Dragonfly — [Point-in-Time Snapshotting Design](https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting)
- Dragonfly repo — [`docs/rdbsave.md`](https://github.com/dragonflydb/dragonfly/blob/main/docs/rdbsave.md)
- Dragonfly blog — [Balanced vs. Unbalanced](https://www.dragonflydb.io/blog/balanced-vs-unbalanced)
