# 04 — Keyspace & Data Model

Four layers, from the outside in:

```
serverDb           server.h:896     one per numbered database (0..15)
  └─ kvstore       kvstore.c        array of hash tables (one per slot in cluster mode)
       └─ hashtable  hashtable.c    the actual table: cache-line buckets, incremental rehash
            └─ robj    object.c     the value: type + encoding + refcount + ptr
```

## `robj` — the universal value (`object.c`)

Every value in the keyspace is an `robj` (`createObject`, `object.c:141`). It carries:

- **type** — `OBJ_STRING`, `OBJ_LIST`, `OBJ_SET`, `OBJ_ZSET`, `OBJ_HASH`, `OBJ_STREAM`...
- **encoding** — the *physical* representation, which is not 1:1 with type
- **refcount** — `incrRefCount` (`object.c:646`) / `decrRefCount` (`object.c:658`)
- **ptr** — the payload

### Type vs encoding is the thing to understand

A single logical type has several physical representations, chosen by size and content,
and **upgraded in place** as the value grows:

| Type | Small encoding | Large encoding |
|------|----------------|----------------|
| String | `int` (if numeric), `embstr` (≤44 bytes, one allocation) | `raw` (sds) |
| Hash | `listpack` (flat array, linear scan) | `hashtable` |
| List | `listpack` | `quicklist` (linked list of listpacks) |
| Set | `intset` (all-integer) or `listpack` | `hashtable` |
| ZSet | `listpack` | `skiplist` + hashtable |

The small encodings are **flat, contiguous arrays with linear scan**. That sounds slow
and is in fact faster for small N, because it fits in cache and skips pointer chasing —
and it uses dramatically less memory. Thresholds are configurable
(`hash-max-listpack-entries`, etc.).

Conversion is **one-way**: once a hash becomes a `hashtable`, deleting fields does not
convert it back. Worth knowing when you're debugging memory that won't come down.

`OBJ_ENCODING_*` conversions live in the `t_*.c` files; look for `*TryConversion` /
`*Convert` functions.

## `serverDb` (`server.h:896`)

Holds `keys` (a `kvstore` of key→value) and `expires` (a `kvstore` of key→expiry-ms),
plus blocking-key bookkeeping. Note that **expiry is a separate structure**, not a field
on the object — that matters in note 05.

## `kvstore` — why there's an array of tables

Read the top-of-file comment in `kvstore.c`; it's short and says it plainly. It is an
**array of hash tables**, and in cluster mode there is **one table per hash slot** (16,384).

The payoff: "give me every key in slot 4242" becomes iterating one small table instead of
scanning the entire keyspace. That is exactly what slot migration needs
(`design-docs/atomic-slot-migration.md`) and what `CLUSTER COUNTKEYSINSLOT` needs. In
non-cluster mode the kvstore has a single table and the indirection costs nearly nothing.

## `hashtable` — the actual table

Read the top-of-file comment in `hashtable.c` (lines ~8–40); it lists the design
properties directly. The two that shape everything else:

- **Cache-line sized buckets.** Entries are grouped so a bucket probe touches one cache
  line, not a pointer chain. This is a Swiss-table-style design, and it replaced the
  older `dict.c` chained hash table.
- **Incremental rehashing with two tables.** Growing does *not* stop the world. Both the
  old and new table exist simultaneously; each operation migrates a few buckets, and
  `databasesCron` (`server.c:1304`) migrates a few more per tick. Lookups check both
  tables while a rehash is in progress.

Incremental rehashing is the direct consequence of "one thread owns all the data" (note
01): a stop-the-world rehash of a 100M-key table would stall every client, so the cost is
amortized across operations instead.

**Stateless `scan`.** The `SCAN` cursor is a bucket index using reverse-binary iteration,
which is what allows it to give guarantees (every key present for the whole scan is
returned at least once) even while the table rehashes underneath it. The comment in
`hashtable.c` explains it; this is a genuinely clever piece of the codebase and worth
reading properly once.

## Lookups — `db.c`

The entry points you'll see everywhere:

- `lookupKey` (`db.c:81`) — the core, flag-driven.
- `lookupKeyRead` (`db.c:144`) / `lookupKeyWrite` (`db.c:158`) — the wrappers commands
  actually call. The read/write distinction matters: it affects keyspace hit/miss stats,
  and on a replica it changes expiry behavior (note 05).
- `lookupKeyReadOrReply` (`db.c:162`) / `lookupKeyWriteOrReply` (`db.c:168`) — the same,
  but they emit the reply and return NULL if missing. Most command implementations use
  these, which is why so many commands start with a single line that both looks up and
  error-handles.
- `dbAdd` (`db.c:228`), `dbDelete` (`db.c:554`).

**Every lookup checks expiry** — `lookupKey` calls `expireIfNeeded` (`db.c:47`) before
returning. This is the "lazy expiry" half of note 05.

## Read next

Note 05 — how keys die.
