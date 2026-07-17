# 05 — Expiration & Eviction

Two different mechanisms that beginners constantly conflate:

- **Expiration** — a key had a TTL and its time is up. It *should* die. Correctness.
- **Eviction** — we're over `maxmemory` and must kill *something*, TTL or not. Capacity.

Different code, different files, different triggers.

## Expiration

TTLs live in a separate `expires` kvstore per database (note 04), not on the object. A
key with a TTL is in *both* `keys` and `expires`.

The hard question: with 10 million keys, how do you notice that one expired without
scanning all of them? Valkey's answer is **two mechanisms, neither of which is a timer**.

### 1. Lazy expiry — on access

`expireIfNeeded` (declared `db.c:47`) is called from `lookupKey` (`db.c:81`) on **every
single lookup**. If the key is past its TTL, it's deleted right there and the lookup
reports a miss.

Cheap and exact for keys anyone touches. Useless for keys nobody touches — those would
leak forever, hence:

### 2. Active expiry — sampling in the background

`activeExpireCycle` (`expire.c:459`), with the actual work in `activeExpireCycleJob`
(`expire.c:199`) and the per-key delete in `activeExpireCycleTryExpire` (`expire.c:66`).

It is called from **two places**, and the difference matters:

- **Fast cycle** (`ACTIVE_EXPIRE_CYCLE_FAST`) — from `beforeSleep` (`server.c:1916`), i.e.
  every event loop iteration, and **only on a primary** (`iAmPrimary()`, note 01). Time
  budget `ACTIVE_EXPIRE_CYCLE_FAST_DURATION` = **1000 µs = 1 ms** (`expire.c:123`). It
  won't even start unless the previous cycle hit its time limit *or* the estimated stale
  fraction is above threshold (`expire.c:229`) — so on a lightly-expiring server it's
  nearly free.
- **Slow cycle** (`ACTIVE_EXPIRE_CYCLE_SLOW`) — from `databasesCron` on the `serverCron`
  timer. Budget is `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` = **25% of CPU** (`expire.c:124`),
  the bulk of the work.

**The algorithm** (`activeExpireCycleJob`, and the loop at `expire.c:339`–361), stated
precisely so you can stop hand-waving about it:

1. For each DB, sample up to `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` = **20** keys
   (`expire.c:122`) from `expires`, deleting the expired ones. It scans up to `num * 10`
   buckets to find them — scanning empty buckets is cheap because they're a sequential
   run of NULLs in one cache line (`expire.c:334`, and the comment there).
2. **Repeat within the same DB while the expired fraction stays high:**
   `(data.expired * 100 / data.sampled) > config_cycle_acceptable_stale` (`expire.c:361`),
   where the acceptable-stale threshold is `ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE` = **10%**
   (`expire.c:125`), *not* 25% — the 25% figure is the CPU cap above, a different knob
   people constantly confuse.
3. Stop when the fraction drops below ~10%, the DB is fully scanned, or the time budget
   is exhausted.

All four numbers (20 keys, 1 ms, 25% CPU, 10% stale) are scaled by the
`active-expire-effort` config via `activeExpireEffort()` (`expire.c:202`–204) — raising
effort samples more keys and tolerates fewer stragglers, at higher CPU cost.

The statistical consequence is a **bound on how many expired-but-not-yet-deleted keys can
accumulate**, not a guarantee that expired keys are gone instantly. **This is why `dbsize`
can report keys that are logically expired.**

**Hash-field TTLs are a second job type.** `activeExpireCycleJob` runs for both `KEYS`
(whole-key TTL) and `FIELDS` (per-field TTL from `HEXPIRE`, scanning
`db->keys_with_volatile_items`, `expire.c:283`). Field expiry checks the clock *every*
loop iteration rather than every 16th (`time_check_mask = 0x0`, `expire.c:289`) because a
single key can hold many fields and blow the budget. If you're reading this code and
wondering why there are two of everything, that's the `HEXPIRE` feature.

### Expiry on replicas — the rule that surprises everyone

**Replicas do not expire keys on their own.** A replica waits for the primary to send an
explicit `DEL`/`UNLINK` (propagated from the primary's expiry) and only then removes the
key. The code-level enforcement is the `iAmPrimary()` guard on the fast cycle
(`server.c:1915`) and equivalent guards on the slow path.

Why: if replicas expired independently, clock skew would make them diverge from the
primary, and replication would no longer be deterministic (note 02).

But a replica must not *return* a logically-expired value to a reader either. So it
tracks expiry logically: the key survives in the keyspace, but a **read** on a replica
returns nil for a key whose TTL has passed, while the key itself survives until the
primary's `DEL` arrives. This is exactly why `lookupKeyRead` and `lookupKeyWrite` are
distinct functions (note 04) — they differ in how they handle this case.

`expireGenericCommand` (`expire.c:763`) implements `EXPIRE`/`PEXPIRE`/`EXPIREAT`. Recall
from note 02 that these all propagate as absolute `PEXPIREAT` so replay is deterministic.

## Eviction

`evict.c`, entry point `performEvictions` (`evict.c:404`), called from `processCommand`
(note 02) when we're over the memory limit.

`getMaxmemoryState` (`evict.c:265`) answers "are we over, and by how much?" Note
`freeMemoryGetNotCountedMemory` (`evict.c:200`): replica output buffers and the AOF buffer
are **excluded** from the accounting, because evicting user data to make room for a
replica's backlog would be self-defeating.

### Policies

`noeviction` (default — reject writes with `-OOM`), `allkeys-lru`, `allkeys-lfu`,
`allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`.
`volatile-*` only consider keys that have a TTL.

### LRU here is approximate — and that's deliberate

True LRU needs a linked list of every key, reordered on every access: extra pointers per
object and pointer-chasing on the hot path. Valkey instead stores a small **24-bit clock**
in each `robj` and does **sampled eviction**: `evictionPoolPopulate` (`evict.c:113`) picks
`maxmemory-samples` random candidates (default **5**) per round via
`kvstoreHashtableSampleEntries` and merges the best ones into a persistent pool of size
`EVPOOL_SIZE` = **16** (`evict.c:54`), kept sorted by idle time across rounds. The worst
key in the pool gets evicted; the pool means good candidates spotted in one round aren't
thrown away if a slightly-worse key happened to be sampled next.

With 5 samples the result is very close to true LRU, at a fraction of the cost. This is
the same design instinct as approximate active expiry above and as incremental rehashing
in note 04: **bounded work per operation, statistically good enough, never stop the
world.** Once you see that pattern you'll recognize it all over the codebase.

**LFU** reuses the same 24 bits differently: ~8 bits of logarithmic counter plus a decay
timer, so it tracks *frequency* rather than *recency*. Better for workloads with a stable
hot set that occasional scans would otherwise flush out of an LRU.

## Exercise

Watch approximate active expiry lag behind reality. Load 100k keys each with a 5-second
TTL and *never read them*:

```
valkey-cli eval "for i=1,100000 do redis.call('set','k'..i,'v','px',5000) end" 0
```

Then poll `DBSIZE` and `INFO stats | grep expired` once a second. You'll see `DBSIZE` stay
well above zero for several seconds after the 5s mark — the fast cycle (1 ms/iteration) and
slow cycle (25% CPU) are collecting the dead keys in bounded batches, not all at once.
Now `CONFIG SET active-expire-effort 10` and repeat: the backlog drains faster and CPU
rises. You just moved the four constants from `expire.c` by turning one knob.

## Read next

Note 06 — how the data survives a restart.
