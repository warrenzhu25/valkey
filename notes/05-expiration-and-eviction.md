# 05 — Expiration & Eviction

Two mechanisms that beginners constantly conflate, because both end in "a key disappears,"
are in fact answers to completely different questions:

- **Expiration** — a key was given a TTL and its time is up. It *should* die; returning it
  would be wrong. This is about **correctness**.
- **Eviction** — the server is over `maxmemory` and must free space by killing *something*,
  whether or not it has a TTL. This is about **capacity**.

Different files, different triggers, different code. Expiration is `expire.c` and lazy checks
in `db.c`; eviction is `evict.c`. This chapter keeps them apart on purpose, and the payoff is
that you'll stop mixing up the "25% CPU" knob (an eviction-adjacent expiry *cap*) with the
"10% stale" knob (an expiry *target*) — a confusion the config names actively invite.

## Expiration

First, a correction to a widespread mental model (including older Valkey notes). The TTL
value does **not** live in a separate table keyed by name — it lives **inside the value
object**, in the embedded `expire` field guarded by `hasexpire` (chapter 04). The per-database
`expires` kvstore is a **secondary index**: a set of pointers to exactly those value objects
that carry a TTL. Every expiry check reads the timestamp with `objectGetExpire(val)`
(`db.c:363`, `db.c:2005`); the index exists only so the background sweep can iterate "just the
keys that have a TTL" without walking the whole keyspace.

The hard problem expiry solves: with 10 million keys, how do you notice that *one* of them
expired without scanning all 10 million? Valkey's answer is **two mechanisms, neither of which
is a per-key timer** (a timer per key would cost a timer heap the size of the keyspace).

### 1. Lazy expiry — on access

`expireIfNeeded` (`db.c:47`) is called from `lookupKey` (`db.c:81`) on **every single
lookup**. If the key is past its TTL, it's deleted right there and the lookup reports a miss.
This is cheap and exact for any key someone actually touches — the expiry happens precisely
when it would otherwise be observed. Its blind spot is the key nobody ever reads again: left
to lazy expiry alone, it would sit dead in memory forever. Hence the second mechanism.

### 2. Active expiry — sampling in the background

`activeExpireCycle` (`expire.c:459`), with the real work in `activeExpireCycleJob`
(`expire.c:199`) and the per-key delete in `activeExpireCycleTryExpire` (`expire.c:66`). It
runs from **two places**, and the distinction is the whole design:

- **Fast cycle** (`ACTIVE_EXPIRE_CYCLE_FAST`) — from `beforeSleep` (`server.c:1916`), i.e.
  every event-loop iteration, and **only on a primary** (`iAmPrimary()`). Its time budget is
  `ACTIVE_EXPIRE_CYCLE_FAST_DURATION` = **1000 µs = 1 ms** (`expire.c:123`). It won't even
  begin unless the previous cycle hit its limit or the estimated stale fraction is high — so
  on a lightly-expiring server it costs essentially nothing.
- **Slow cycle** (`ACTIVE_EXPIRE_CYCLE_SLOW`) — from `databasesCron` on the `serverCron`
  timer (chapter 01). Its budget is `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` = **25% of CPU**
  (`expire.c:124`). This is where the bulk of the reaping happens.

**The algorithm**, stated precisely so you can stop hand-waving:

1. For each database, sample up to `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` = **20** keys
   (`expire.c:122`) from the `expires` index, deleting the expired ones. It's willing to scan
   up to `num × 10` buckets to find candidates — scanning empty buckets is nearly free because
   they're a sequential run of NULLs sitting in one cache line.
2. **Repeat within the same database while the expired fraction stays high** — specifically
   while `(expired × 100 / sampled) > ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE` = **10%**
   (`expire.c:125`). Note: 10%, *not* 25%. The 25% is the CPU *cap* from the slow cycle; the
   10% is the "how many stragglers are we willing to tolerate" *target*. They are different
   knobs and people confuse them constantly.
3. Stop when the fraction falls below ~10%, the database is fully scanned, or the time budget
   is spent.

All four numbers (20 keys, 1 ms, 25% CPU, 10% stale) scale with the `active-expire-effort`
config via `activeExpireEffort()` — raising effort samples more keys and tolerates fewer
stragglers, at higher CPU cost.

The statistical consequence is a **bound on how many expired-but-not-yet-deleted keys can
pile up at once** — not a promise that an expired key vanishes the instant its TTL passes.
This is exactly why `DBSIZE` can report keys that are logically expired: they're dead, but the
sampler hasn't reached them yet.

**Hash-field TTLs are a second job type.** `activeExpireCycleJob` runs for both whole-key TTLs
(`KEYS`) and per-field TTLs from `HEXPIRE` (`FIELDS`, scanning `db->keys_with_volatile_items`).
Field expiry checks the clock on *every* loop iteration rather than every 16th, because a
single key can hold many volatile fields and blow the time budget. If you're reading this code
and wondering why there are two of everything, that's the `HEXPIRE` feature.

### Expiry on replicas — the rule that surprises everyone

**A replica never expires a key on its own initiative.** It waits for the primary to send an
explicit `DEL`/`UNLINK` (propagated from the primary's own expiry) and removes the key only
then. The enforcement is the `iAmPrimary()` guard on the fast cycle (`server.c:1915`) plus
equivalent guards on the slow path. The reason is determinism (chapter 02): if replicas
expired independently, clock skew between machines would delete keys at slightly different
times and the replica would drift out of sync with its primary.

But a replica must not *serve* a logically-expired value either. So it tracks expiry
**logically**: the key physically survives in the keyspace until the primary's `DEL` arrives,
yet a **read** on a replica returns nil for a key whose TTL has passed. This is precisely why
`lookupKeyRead` and `lookupKeyWrite` are distinct (chapter 04): a write path on a writable
replica may force-delete, while a read path on a read-only replica reports the key missing
without deleting it.

`expireGenericCommand` (`expire.c:763`) implements `EXPIRE`/`PEXPIRE`/`EXPIREAT`. Recall from
chapter 02 that these all propagate as an absolute `PEXPIREAT` so replay is deterministic
regardless of when the replica applies it.

## Eviction

`evict.c`, entry point `performEvictions` (`evict.c:404`), called from `processCommand`
(chapter 02) when a write would push the server over its memory limit.

`getMaxmemoryState` (`evict.c:265`) answers "are we over, and by how much?" One subtlety worth
knowing: `freeMemoryGetNotCountedMemory` (`evict.c:200`) **excludes** replica output buffers
and the AOF buffer from the accounting. Evicting user data to make room for a replica's backlog
would be self-defeating — those buffers drain on their own — so they don't count against the
limit.

### Policies

`noeviction` (the default — reject writes with `-OOM`), plus `allkeys-lru`, `allkeys-lfu`,
`allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, and `volatile-ttl`. The
`volatile-*` policies only ever consider keys that have a TTL; the `allkeys-*` policies consider
everything.

### LRU here is approximate — deliberately

True LRU needs a doubly linked list threading every key, reordered on every single access:
two extra pointers per object and pointer-chasing on the hottest path in the server. Valkey
refuses to pay that. Instead it stores a **24-bit clock** in each `robj` (the `lru` field from
chapter 04) and does **sampled eviction**:

- `evictionPoolPopulate` (`evict.c:113`) picks `maxmemory-samples` random candidates (default
  **5**) per round and merges the best of them into a persistent pool of size `EVPOOL_SIZE` =
  **16** (`evict.c:54`), kept sorted by idle time *across* rounds.
- The worst key in the pool is evicted. The pool matters because a genuinely good candidate
  spotted in one round isn't discarded just because the next round happened to sample
  slightly-worse keys.

With only 5 samples the choice lands very close to true LRU at a tiny fraction of the cost.
This is the same instinct as approximate active expiry above and incremental rehashing in
chapter 04: **bounded work per operation, statistically good enough, never stop the world.**
Once you see the pattern you'll find it everywhere in the codebase.

**LFU** reuses the same 24 bits differently — roughly 8 bits of logarithmic access counter
plus a decay timer — so it tracks *frequency* rather than *recency*. It's the better choice for
a workload with a stable hot set that an occasional big scan would otherwise flush out of a
pure-LRU cache.

## Worked example — one key, three ways it can die

Take a single key `session:42` with a 5-second TTL, and walk the three fates it can meet.

**Fate 1 — lazy expiry (someone reads it late).** At T+6s a client runs `GET session:42`.
`lookupKey` (`db.c:81`) finds the value object, then calls `expireIfNeeded` (`db.c:47`), which
reads `objectGetExpire(val)` = T+5s, sees `now > expire`, deletes the key, and propagates a
`DEL` to replicas and the AOF. `GET` returns nil. The expiry cost was paid exactly when the key
was next observed — zero background work.

**Fate 2 — active expiry (nobody ever reads it).** Nothing touches `session:42` after it's set.
At T+5s it's dead but still resident. On the next primary loop iteration the **fast cycle**
(`server.c:1916`, 1 ms budget) samples 20 keys from the `expires` index; if `session:42` is
among them, `activeExpireCycleTryExpire` (`expire.c:66`) deletes it and propagates a `DEL`. If
it isn't sampled this pass, the **slow cycle** (25% CPU, from `serverCron`) will reach it
soon. Between T+5s and that delete, `DBSIZE` still counts it — that's the stale bound, not a
bug. Raising `active-expire-effort` shortens the window by sampling harder.

**Fate 3 — eviction (memory pressure, TTL irrelevant).** Now suppose the server hits
`maxmemory` at T+2s, while `session:42` is still perfectly valid, under `allkeys-lru`. The next
write calls `processCommand` → `performEvictions` (`evict.c:404`). `getMaxmemoryState` says
we're 8 MB over. `evictionPoolPopulate` samples 5 random keys, scores them by idle time, and
merges them into the 16-slot pool; the idlest key in the pool is deleted (propagated as a
`DEL`), and the loop repeats until `getMaxmemoryState` reports we're back under the limit. If
`session:42` happens to be the coldest thing sampled, it dies here — three seconds *before* its
TTL — because this is capacity management, not correctness. Had the policy been `noeviction`,
the write itself would have been rejected with `-OOM` and `session:42` would have survived.

Same key, same TTL — but which mechanism kills it, and when, depends entirely on whether the
question was "is it still valid?" or "do we have room?"

## Try it yourself

Watch approximate active expiry lag behind reality. Load 100k keys, each with a 5-second TTL,
and never read them:

```
valkey-cli eval "for i=1,100000 do redis.call('set','k'..i,'v','px',5000) end" 0
```

Then poll `DBSIZE` and `INFO stats | grep expired` once a second. `DBSIZE` stays well above
zero for several seconds past the 5s mark — the fast cycle (1 ms/iteration) and slow cycle (25%
CPU) are collecting the dead keys in bounded batches, not all at once (Fate 2, at scale). Now
`CONFIG SET active-expire-effort 10` and repeat: the backlog drains faster and CPU rises. You
just moved all four `expire.c` constants at once by turning a single knob. For eviction, set a
low `maxmemory` with `--maxmemory-policy allkeys-lru`, flood writes, and watch
`INFO stats | grep evicted_keys` climb while `DBSIZE` holds near the memory ceiling — Fate 3.

## Read next

Chapter 06 — how the surviving data outlives a restart: fork-based RDB snapshots and the
append-only file.
