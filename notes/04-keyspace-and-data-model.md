# 04 — Keyspace & Data Model

A running Valkey server is, at bottom, one process holding a few hundred million
key/value pairs in RAM and answering questions about them in microseconds. Everything
in this chapter exists to serve two goals that pull against each other: **find any key
fast**, and **store each key cheaply**. Fast argues for big hash tables and rich
per-key metadata; cheap argues for tight, cache-friendly layouts with no wasted bytes.
The data model is the set of compromises Valkey strikes between them.

We build it up from the outside in — four nested layers — and then look closely at the
two that do the clever work: the object that wraps every value, and the hash table that
finds it.

```
serverDb            server.h:896   one per numbered database (0..N)
  └─ kvstore        kvstore.c      an array of hash tables (one per slot in cluster mode)
       └─ hashtable   hashtable.c  the table itself: 64-byte buckets, incremental rehash
            └─ robj    object.c    the value: type + encoding + refcount + payload
```

Read that top to bottom as "which database → which slot's table → which bucket → the
value." Every `GET`, every `SET`, every `SCAN` walks some portion of this ladder.

## `robj` — the universal value

Every value Valkey stores is a `robj` ("Redis/`serverObject`", `object.c`). Commands,
replication, RDB, eviction — they all pass `robj *` around and never look at raw bytes
directly. Getting this struct right is the single highest-leverage thing in the
memory footprint, because a server with 100 million keys has 100 million of them.

Here is the actual definition (`server.h:815`), and it is worth reading field by field
because it changed meaningfully in Valkey:

```c
struct serverObject {
    unsigned type : 4;        /* OBJ_STRING, OBJ_LIST, OBJ_HASH, ... */
    unsigned encoding : 4;    /* physical representation (see below) */
    unsigned lru : 24;        /* LRU clock OR LFU counter, for eviction */
    unsigned hasexpire : 1;   /* is a TTL embedded after the header? */
    unsigned hasembkey : 1;   /* is the key embedded after the header? */
    unsigned hasembval : 1;   /* is the value embedded (vs. via val_ptr)? */
    unsigned refcount : 29;   /* sharing count; a sentinel marks shared objects */
    void *val_ptr;            /* the payload — UNLESS hasembval says it's inline */
};
```

That is 8 bytes of bitfields plus an 8-byte pointer: **16 bytes**, and the code asserts
it (`server.h:826`, `sizeof <= 8 + sizeof(void*)`). Four things deserve comment.

- **`type` vs `encoding` are different questions.** `type` is what the user asked for
  (a hash, a sorted set). `encoding` is how Valkey physically stores it right now. One
  type has several encodings; that split is the whole next section.
- **`lru` is 24 bits doing double duty.** Under the default LRU policy it holds a coarse
  clock; under an LFU policy it holds an access-frequency counter. `lookupKey` bumps it
  on every access (`db.c:114`, `val->lru = lrulfu_touch(...)`). Chapter 05 spends the
  eviction budget on it.
- **`refcount` enables object sharing.** Normally 1. But the integers 0–9999 are created
  once at startup and marked with the sentinel `OBJ_SHARED_REFCOUNT` (`makeObjectShared`,
  `object.c:161`); `incrRefCount`/`decrRefCount` see the sentinel and refuse to touch the
  object. That is why `SET x 100` executed a million times does **not** allocate a million
  objects — they all point at the one shared `100`. (Shared objects are never stored *as
  keys' values in the main table with a live refcount race*; `lookupKey` even asserts a
  value it's about to LRU-touch is not shared, `db.c:113`.)
- **`hasexpire` / `hasembkey` / `hasembval` are the Valkey memory optimization.** They say
  which optional fields have been packed into the *same allocation* as the object header.
  This is new relative to classic Redis and it is why Valkey's per-key overhead is lower.

### The embedded layout — one malloc instead of four

Classically, a small string key had four separate heap allocations: the `robj`, the
key's sds string, the value's sds string, and (if it had a TTL) an entry in a separate
expires table. Four mallocs means four headers, four chances for fragmentation, and four
pointers to chase. Valkey collapses the common case — a small key with a small value —
into a **single allocation** laid out like this (`server.h:790`):

```
+------+----------+-----+------------+----------+--------+-----------------+---------+------------+
| type | encoding | lru | has* flags | refcount | expire | key_hdr_size    | key sds | value data |
+------+----------+-----+------------+----------+--------+-----------------+---------+------------+
        16-byte header ------------->  present    present if hasembkey       present if
                                       if           (1 length byte + sds)    hasembval
                                       hasexpire
```

The value replaces the `val_ptr` field and everything that follows it, which is why the
struct is described as "variable in size": the declared 16 bytes are a *minimum*, and the
real allocation runs past the end of the struct to hold the expire, key, and value inline
(`createEmbeddedStringObjectWithKeyAndExpire`, `object.c:182`).

**When does Valkey embed?** The rule is one line (`shouldEmbedStringObject`, `object.c:259`):
embed when the whole packed thing fits in **128 bytes** (two cache lines), and the value is
short enough for a one-byte-length sds header. The size it checks is exactly the sum of the
pieces above:

```
size  =  sizeof(robj) - sizeof(void*)          # 8  — the header minus the reused val_ptr slot
       + (hasexpire ? 8 : 0)                    # the TTL, an 8-byte timestamp
       + (hasembkey ? 1 + key_sds_size : 0)     # length byte + the key's sds
       + val_sds_size                           # the value's sds (always SDS_TYPE_8)
embed if size <= 128
```

Work a real one. `SET greeting hello` with a 60-second TTL, key `"greeting"` (8 bytes),
value `"hello"` (5 bytes):

```
base header (minus val_ptr) ............  8
expire (hasexpire) .....................  8
key: 1 length byte + sds("greeting")
     sds for 8 bytes uses a 1-byte header,
     so 1 + (1 + 8 + 1) = 11 ............ 11
value: sds("hello"), SDS_TYPE_8 header
     is 3 bytes, so 3 + 5 + 1 = 9 .......  9
                                    total  36  bytes   <= 128  →  EMBED
```

So that key, its value, and its expiry live in **one 36-byte allocation**. Contrast this
with the folklore number: old Redis embedded strings only up to "44 bytes," measured
differently and covering only the value. If a blog post quotes you 44, it is describing
Redis, not this code. Valkey's rule is a 128-byte *total-allocation* budget, and it packs
the key and TTL in too.

When the value is too big, Valkey falls back to `OBJ_ENCODING_RAW`: the `robj` holds a real
`val_ptr` to a separately allocated sds, and the layout is the second diagram in
`server.h:803` (expire and key can still be embedded even when the value is not).

## Type vs encoding — the table to memorize

A logical type has a **small** encoding for when it's tiny and a **large** encoding for
when it grows. Valkey starts small and upgrades in place as data is added:

| Type   | Small encoding                          | Large encoding                    | Upgrade trigger (default) |
|--------|-----------------------------------------|-----------------------------------|---------------------------|
| String | `int` (numeric) / `embstr` (short)      | `raw` (sds)                       | packed size > 128 bytes (see below) |
| Hash   | `listpack`                              | `hashtable`                       | > 512 fields **or** any field/value > 64 bytes |
| List   | `listpack`                              | `quicklist` (list of listpacks)   | node exceeds `list-max-listpack-size` (default −2 = 8 KB) |
| Set    | `intset` (all-integer) or `listpack`    | `hashtable`                       | > 512 ints, or > 128 non-int entries, or entry > 64 bytes |
| ZSet   | `listpack`                              | `skiplist` + hashtable            | > 128 members **or** any member > 64 bytes |

The thresholds are live configs — `hash-max-listpack-entries` (512),
`hash-max-listpack-value` (64), `set-max-intset-entries` (512),
`set-max-listpack-entries` (128), `zset-max-listpack-entries` (128), and the matching
`-value` limits — all defined in `config.c:3543` onward. The list config is the odd one:
`list-max-listpack-size` defaults to **−2**, and a *negative* value means "cap each node
by size, not count" (−1…−5 = 4/8/16/32/64 KB). A positive value would cap by entry count.

### Why the "slow" flat encoding is actually faster when small

The small encodings — `listpack`, `intset` — are **flat, contiguous arrays scanned
linearly**. That sounds strictly worse than a hash table, and for large N it is. For small
N it wins on every axis that matters:

- The whole structure fits in a cache line or two, so a linear scan is a handful of
  sequential reads the prefetcher loves — no pointer chasing, no cache misses.
- There is no hash table overhead: no bucket array, no per-entry pointers, no load-factor
  slack. A 5-field hash as a `listpack` is a few dozen bytes; as a `hashtable` it would be
  hundreds.
- Linear scan of 8 elements beats hashing-plus-indirection because hashing isn't free
  either.

So the encoding transition isn't "grow into the fast structure." It's "grow *out of* the
structure that was fast *because it was small*."

### What the small encodings actually look like in memory

You can picture the bytes, which makes `DEBUG OBJECT` and `MEMORY USAGE` readable.

**listpack** (`listpack.c`) — the workhorse behind small hashes, lists, zsets, and sets.
A 6-byte header (`LP_HDR_SIZE`, `listpack.c:48`: a 4-byte total-length and a 2-byte
element count) followed by back-to-back variable-length entries, terminated by a single
`0xFF` byte (`LP_EOF`, `listpack.c:97`):

```
+--------------+-----------+---------+---------+-----+------+
| total bytes  | num elems | entry 0 | entry 1 | ... | 0xFF |
|   4 bytes    |  2 bytes  |  <---  variable-length  --->   |
+--------------+-----------+---------+---------+-----+------+
```

Each entry stores its own length *twice* — once up front, once as a back-length suffix —
so the list can be walked **backwards** as cheaply as forwards (that back-length is how
`RPOP` and reverse range queries stay O(1) per step). A hash stores field and value as two
consecutive entries; a zset stores member and score as two consecutive entries.

**intset** (`intset.h:35`) — a set whose members are all integers. Just a sorted array:

```c
typedef struct intset {
    uint32_t encoding;   /* width of each slot: 2, 4, or 8 bytes */
    uint32_t length;     /* number of integers */
    int8_t   contents[]; /* the sorted integers, all the same width */
};
```

Because it's sorted, `SISMEMBER` is a binary search, and it upgrades the slot width
(2→4→8 bytes) only when an inserted value needs it. Add one non-integer member and the set
converts to `listpack` (or straight to `hashtable` if it's already large).

**quicklist** and **skiplist** are the large encodings, so a sentence each: a `quicklist`
is a doubly linked list whose *nodes are each a listpack* — you get the O(1) ends of a
linked list without one malloc per element. A `skiplist` (for large zsets) is the classic
probabilistic tower of forward pointers giving O(log n) ordered lookup, kept **alongside** a
hash table from member→score so `ZSCORE` is O(1) while `ZRANGE` stays ordered.

### Conversions are one-way

Growth upgrades the encoding; shrinking does **not** downgrade it. Delete fields from a
hash that became a `hashtable` and it stays a `hashtable` forever. `LPOP` a `quicklist`
back down to three elements and it stays a `quicklist`. This is a deliberate simplicity
trade — the check is cheap on the way up and skipped on the way down — but it's the answer
to "I deleted most of my keys, why didn't memory come back?" The conversion functions live
in the type files as `*TryConversion` / `*Convert` (e.g. `hashTypeTryConversion` in
`t_hash.c`); `OBJECT ENCODING <key>` shows you the current one.

## `serverDb` — a numbered database

A `serverDb` (`server.h:896`) is one logical database (the thing `SELECT 3` switches
between). Its important fields:

```c
kvstore *keys;                     /* every key → value in this DB          */
kvstore *expires;                  /* index of the keys that have a TTL     */
kvstore *keys_with_volatile_items; /* hashes with per-field TTLs (HEXPIRE)  */
dict    *blocking_keys;            /* keys clients are BLPOP-waiting on      */
dict    *watched_keys;             /* keys under MULTI/EXEC WATCH            */
```

One correction worth making loudly, because older notes (and older Redis) get it wrong:
**the TTL value now lives inside the object**, in that embedded `expire` field guarded by
`hasexpire`. So what is the `expires` kvstore for? It is a **secondary index**: a set of
pointers to exactly those value objects that carry a TTL. `setExpire` (`db.c:1957`) writes
the timestamp into the object via `objectSetExpire`, then adds the *same object pointer* to
`db->expires` (`db.c:1989`). `getExpire` looks the key up in `db->expires` and reads
`objectGetExpire` off the object it finds (`db.c:2003`). The index exists so the active
expiry cycle (chapter 05) can iterate "just the volatile keys" without scanning the whole
keyspace — not because the TTL is stored there.

## `kvstore` — why there's an *array* of tables

A `serverDb` doesn't hold one hash table; it holds a `kvstore`, which is an **array of hash
tables** (`kvstore.c`, top comment). In cluster mode there is **one table per hash slot** —
16,384 of them. In standalone mode the array has a single entry and the indirection costs
essentially nothing.

The payoff is locality of *slots*. "Give me every key in slot 4242" becomes "iterate one
small table," not "scan the entire keyspace and filter." That single capability is what
makes `CLUSTER COUNTKEYSINSLOT` cheap and, more importantly, what makes atomic slot
migration possible (`design-docs/atomic-slot-migration.md`) — moving a slot means handing
over one table.

The obvious worry — "16,384 hash tables on an empty node is a lot of empty allocations" — is
handled: the `kvstore` allocates per-slot tables **lazily** on first insert and keeps a
list of the non-empty ones, so iteration and memory both scale with slots-in-use, not slots-
possible. The cursor arithmetic reserves the low bits for the slot index and the high 48
bits for the in-table cursor (`kvstore.c:52`), which is how a single `SCAN` cursor threads
through many tables.

## `hashtable` — the table that finds the key

This is the heart of the chapter. Valkey's `hashtable.c` replaced the classic chained
`dict.c` with a **Swiss-table-inspired**, cache-line-optimized design (credited in the file
header to Viktor Söderqvist, with the bucket idea from Madelyn Olson and the scan algorithm
from Pieter Noordhuis). Two properties shape everything.

### 64-byte buckets with a top-hash filter

Entries are grouped into **buckets sized to exactly one 64-byte cache line**
(`static_assert`, `hashtable.c:301`). On a 64-bit build each bucket holds up to 7 entries
(`ENTRIES_PER_BUCKET`, `hashtable.c:164`) and looks like this (`hashtable.c:293`):

```c
typedef struct hashtableBucket {
    uint8_t chained  : 1;   /* is the last slot a pointer to a child bucket? */
    uint8_t presence : 7;   /* one bit per slot: occupied or free            */
    uint8_t hashes[7];      /* one "top hash" byte per slot                  */
    void   *entries[7];     /* the 7 entry pointers                          */
} bucket;                   /* 1 + 7 + 56 = 64 bytes, one cache line         */
```

The clever part is the `hashes` array. When you look up a key, Valkey computes its full
hash, takes one byte of it, and compares that byte against the seven `hashes` bytes — seven
single-byte comparisons against data already in the loaded cache line. Only for a slot whose
top-hash byte *matches* does it dereference `entries[i]` and do the expensive full key
comparison. In a bucket of 7 entries where at most one matches, that filter rejects the
other six **without following a single pointer or touching the key's memory**. That is the
whole Swiss-table trick: turn "chase 7 pointers and `memcmp` 7 keys" into "scan 7 bytes,
then chase at most one pointer."

When a bucket fills, the last slot is repurposed as a pointer to a separately allocated
**child bucket**, forming a short chain (`hashtable.c:238`). Chains stay short because the
table resizes long before buckets pile up.

### Incremental rehashing — never stop the world

The second property is that growing the table **never blocks**. When the table gets too
full, Valkey allocates a second, larger table and keeps *both* (`tables[2]` in
`struct hashtable`, `hashtable.c:309`). It then migrates a few buckets at a time from old to
new: a little on each insert, and a batch each tick from `databasesCron` (`server.c:1304`).
While a rehash is in progress, every lookup simply checks **both** tables.

Why go to this trouble? Because of the design constraint that governs the whole server: one
thread owns all the data (chapter 01). A classic stop-the-world rehash of a 100-million-key
table would move gigabytes of pointers in one shot, freezing every connected client for the
duration. Amortizing that work across millions of operations — a few buckets each — keeps
every single request fast. "Bounded work per operation" is the recurring instinct of this
codebase; incremental rehash is its purest expression.

Rehashing is also **paused while a child process is forked** (during `BGSAVE`), because
moving pointers around would dirty copy-on-write pages and blow up the child's memory
(`hashtable.c:141`). The resize policy machinery at the top of `hashtable.c` exists for
exactly this.

### One key, all the way through — add, rehash, delete

To make the two properties above concrete, follow a single key through the three
operations. Suppose the table currently has **4 buckets** (`bucket_exp[0] = 2`, so the low
2 bits of a hash pick the bucket) and we're adding `user:42`, whose value object lives at
address `0xA1B0`. SipHash gives `hash(user:42) = 0x…7C93`, and Valkey pulls two things from
it: the **bucket index** from the low bits (`0x7C93 & 3 = 3` → bucket 3) and the **top-hash
byte** from the high bits (say `0x2A`) for the filter.

**Add** (`insert`, `hashtable.c:1092`) writes exactly three fields into the first free slot
of bucket 3 — the pointer, the presence bit, and the hash byte — then bumps the counter:

```
b->entries[2] = 0xA1B0;   b->presence |= (1<<2);   b->hashes[2] = 0x2A;   used++;

bucket 3:  slot:   0     1      2     3 4 5 6
       presence:   1     1      1     0 ...
         hashes: [0x9F][0x11][ 0x2A ][-]      ← our byte at slot 2
        entries: [0x88][0xC4][0xA1B0][-]      ← our value at slot 2
```

No other key's memory was touched — that's the invariant every operation preserves.

**Rehash** starts when an add would overflow the table. At 28 entries (4 × 7, the
`MAX_FILL_PERCENT_SOFT` = 100% limit, `hashtable.c:1497`) the next insert calls `resize`: it
allocates a **new 8-bucket table** as `tables[1]`, sets `rehash_idx = 0`, and returns — no
entries have moved yet. From now on, new inserts land directly in the new table, and each
subsequent read or write runs one `rehashStep` (`hashtable.c:715`), migrating **one old
bucket at a time**:

```
step 1: migrate old bucket 0 → rehash_idx=1      A lookup mid-rehash checks the OLD
step 2: migrate old bucket 1 → rehash_idx=2      bucket if it isn't migrated yet
step 3: migrate old bucket 2 → rehash_idx=3      (idx >= rehash_idx), else the NEW one.
step 4: migrate old bucket 3 → old table empty → rehashingCompleted()
```

When bucket 3 is migrated, each entry is re-hashed against the new 3-bit mask. The one
extra bit the wider mask exposes decides where it lands: everything from old bucket 3 goes
to **new bucket 3 or new bucket 7** (`3` or `3|100b`). `user:42` (`0x7C93 & 7 = 3`) stays in
new bucket 3. That clean, bit-aligned split is exactly what lets `SCAN` (next section)
survive a rehash. `databasesCron` also runs migration batches on a timer
(`server.c:1359`), so the table finishes rehashing even under no traffic.

**Delete** (`hashtableDelete` → `hashtablePop`, `hashtable.c:1728`) is the cheapest of the
three. `findBucket` locates the slot using the top-hash filter (scan the 7 hash bytes, find
`0x2A`, *then* compare the key), and the delete is a single bit clear:

```c
b->presence &= ~(1 << 2);   // slot 2 is now "not there"; pointer & hash byte left as garbage
used--;
if (b->chained) fillBucketHole(...);   // pull one entry up from a child bucket, if any
hashtableShrinkIfNeeded(ht);           // if now < 13% full, start a shrinking rehash
freeEntry(0xA1B0);                     // free the value object
```

Clearing the presence bit *is* the deletion; the stale pointer and hash byte are simply
ignored because presence says the slot is empty. The only extra work is keeping bucket
chains dense (`fillBucketHole`) and, if the table has emptied out below
`MIN_FILL_PERCENT_SOFT` (13%, `hashtable.c:95`), kicking off a *shrinking* rehash — the same
two-table dance in reverse.

The thread that ties all three together is **bounded work**: an add writes three fields, a
rehash step moves one bucket, a delete clears one bit. Nothing ever walks the whole table in
one shot, because the single thread that owns the data can never afford to stall.

### Stateless `SCAN` — the reverse-binary cursor

`SCAN` has to give a real guarantee — *every key present for the whole scan is returned at
least once* — while using **no server-side state** (the cursor is just an integer the client
echoes back) and while the table may **rehash underneath it** between calls. That combination
sounds impossible. The trick is the order in which buckets are visited: not 0, 1, 2, 3… but
by **incrementing the cursor's high bits, reverse-binary** (`nextCursor`, `hashtable.c:532`):

```c
size_t nextCursor(size_t v, size_t mask) {
    v |= ~mask;   /* set the high (unmasked) bits            */
    v = rev(v);   /* reverse: those high bits become low bits */
    v++;          /* increment — carries ripple upward        */
    v = rev(v);   /* reverse back                             */
    return v;
}
```

Concretely, on a 4-bucket table (2-bit cursor) the visiting order is `00 → 10 → 01 → 11`
(0, 2, 1, 3). Now suppose the table **doubles to 8 buckets** partway through, right after
you visited cursor `10`. Each old bucket `b` splits into two new buckets, `b` and
`b | 100`, and — this is the point — the reverse-increment order visits both halves of an
already-seen bucket *before* it revisits, and never before it has finished the unseen ones.
An entry that was in old bucket `00` is now in new bucket `000` or `100`; both sort *after*
your current position in reverse-bit order, so you'll still reach it. An entry you already
emitted from `00` won't be emitted again from the *other* half. The formal check is
`hashtableScanHasPassedKey` (`hashtable.c:2055`): a bucket has been visited iff
`rev(bucket_idx) < rev(cursor_idx)`.

The guarantee this buys (spelled out in the comment at `hashtable.c:2007`):

- A key present for the entire scan is returned **at least once** — usually exactly once,
  occasionally twice.
- A key inserted or deleted mid-scan **may or may not** appear.
- Expansion and rehashing are safe. Only *shrinking* the table can break it, because a
  smaller table's bucket boundaries don't align with the cursor — so when strict
  no-duplicates is needed, callers pause shrinking for the scan's duration.

The practical takeaway for anyone *using* `SCAN`: you may see the same key twice, and you
may see keys added during the scan. Deduplicate on the client if you need a clean set. What
you will never do is miss a key that sat still the whole time.

## Looking a key up — `db.c`

Commands rarely touch the `kvstore` directly. They call one of the `lookupKey*` wrappers,
and the naming tells you the intent:

- `lookupKey(db, key, flags)` (`db.c:81`) — the core. Finds the value, runs lazy expiry,
  updates the LRU/LFU clock, and bumps keyspace hit/miss stats.
- `lookupKeyRead` (`db.c:144`) / `lookupKeyWrite` (`db.c:158`) — the read/write wrappers
  commands actually call. The distinction is not cosmetic: a write lookup passes
  `EXPIRE_FORCE_DELETE_EXPIRED`, while a read lookup on a **read-only replica** deliberately
  does *not* delete an expired key (deleting it locally would diverge the replica from its
  primary — chapter 05, `db.c:93`).
- `lookupKeyReadOrReply` (`db.c:162`) / `lookupKeyWriteOrReply` (`db.c:168`) — the same, but
  they emit the "no such key" reply and return `NULL` on a miss. This is why so many command
  implementations open with a single line that both fetches the value and handles the
  not-found case.
- `LOOKUP_NOTOUCH` (used e.g. at `db.c:948`) lets introspection commands like `OBJECT` and
  `TTL` peek at a value **without** disturbing its LRU/LFU stats — otherwise checking a key
  would make it look recently used.

**Every lookup checks expiry.** `lookupKey` calls `expireIfNeeded` before returning the
value (`db.c:97`); if the key's embedded TTL has passed, the value is deleted and the lookup
reports a miss. That is the "lazy" half of expiration — the other half, the background sweep,
is chapter 05.

## Worked example — watch the encodings shift

Run these against a scratch server (`valkey-server --save ''`) and read the output; each line
demonstrates one thing from this chapter.

```
> RPUSH L a b c
> OBJECT ENCODING L                    → "listpack"      # small list starts flat
> CONFIG SET list-max-listpack-size 3
> RPUSH L d
> OBJECT ENCODING L                    → "quicklist"     # crossed the node-size cap
> LPOP L ; LPOP L                       (back to 2 elems)
> OBJECT ENCODING L                    → "quicklist"     # one-way: never converts back

> SET s 12345
> OBJECT ENCODING s                    → "int"           # numeric string → shared-int path
> SET s hi
> OBJECT ENCODING s                    → "embstr"        # short → single-allocation embed
> SET s <a 200-char string>
> OBJECT ENCODING s                    → "raw"            # too big to embed → separate sds

> SADD nums 1 2 3
> OBJECT ENCODING nums                 → "intset"        # all-integer set → sorted array
> SADD nums hello
> OBJECT ENCODING nums                 → "listpack"      # a non-int forced the conversion
```

Two things to *interpret*, not just observe. First, the `int` → `embstr` → `raw` progression
is the 128-byte rule from earlier deciding, per value, whether the payload rides inside the
object's allocation or hangs off a pointer — confirm it with `MEMORY USAGE s` before and after
the `embstr`→`raw` jump and watch the reported bytes step up as a second allocation appears.
Second, the `quicklist` that refuses to become a `listpack` again is the one-way rule, and
it's the mechanism behind "I trimmed the list but memory didn't drop." `DEBUG OBJECT L` shows
the internal node structure (`ql_nodes`) so you can see the listpack-per-node reality of a
quicklist directly.

## Read next

Chapter 05 — how keys *die*: the lazy expiry every lookup above just triggered, the
background sampling cycle that reaps TTL'd keys you never touch, and what happens when
`maxmemory` forces the server to evict keys that haven't expired at all.
