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
(`expire.c:199`) and `activeExpireCycleTryExpire` (`expire.c:66`).

It is called from **two places**, and the difference matters:

- **Fast cycle** — from `beforeSleep` (`server.c:1854`), i.e. every event loop iteration.
  Very short time budget. Keeps latency low by collecting recently-expired keys promptly.
- **Slow cycle** — from `databasesCron` (`server.c:1304`), i.e. on the `serverCron` timer.
  Larger time budget, does the bulk of the work.

(Verified: both call sites exist; `activeExpireCycle` appears in the body of `beforeSleep`
and of `databasesCron`.)

It is **probabilistic, not exhaustive**. Loosely: sample N random keys from `expires`;
delete the expired ones; if more than ~25% of the sample was expired, the database is
probably full of dead keys, so immediately sample again. Repeat until the expired
fraction drops below the threshold **or** a time budget is exhausted.

The time budget is the crucial part. Active expiry runs on the main thread, so it *must*
be bounded — it takes a small slice of CPU per cycle and yields. The statistical
consequence is a bound on how many expired-but-not-yet-deleted keys can accumulate, not a
guarantee that expired keys are gone instantly.

**This is why `dbsize` can report keys that are logically expired.** They're expired;
they just haven't been collected yet.

### Expiry on replicas — the rule that surprises everyone

**Replicas do not expire keys on their own.** A replica waits for the primary to send an
explicit `DEL` (actually `UNLINK`/`DEL` propagated from the primary's expiry) and only
then removes the key.

Why: if replicas expired independently, clock skew would make them diverge from the
primary, and replication would no longer be deterministic (note 02).

But a replica must not *return* a logically-expired value to a reader either. So it
tracks expiry logically: the key is still in the keyspace, but a **read** on a replica
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
in each `robj` and does **sampled eviction**: pick `maxmemory-samples` random candidates
(default 5), evict the best one. A pool of good candidates is kept across rounds to
improve quality.

With 5 samples the result is very close to true LRU, at a fraction of the cost. This is
the same design instinct as approximate active expiry above and as incremental rehashing
in note 04: **bounded work per operation, statistically good enough, never stop the
world.** Once you see that pattern you'll recognize it all over the codebase.

**LFU** reuses the same 24 bits differently: ~8 bits of logarithmic counter plus a decay
timer, so it tracks *frequency* rather than *recency*. Better for workloads with a stable
hot set that occasional scans would otherwise flush out of an LRU.

## Read next

Note 06 — how the data survives a restart.
