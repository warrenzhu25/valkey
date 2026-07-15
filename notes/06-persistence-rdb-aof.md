# 06 — Persistence: RDB & AOF

Two mechanisms with opposite tradeoffs. They can run together.

|  | RDB | AOF |
|--|-----|-----|
| What | Point-in-time binary snapshot | Log of every write command |
| Restart speed | Fast (bulk load) | Slow (replay) |
| Data loss window | Minutes (between saves) | ≤1s (default fsync policy) |
| File size | Compact | Large, needs periodic rewrite |
| Also used for | **Replication full sync** (note 07) | — |

## RDB

### Saving

- `rdbSave` (`rdb.c:1634`) — **blocking**, in the main process. Used by `SAVE` and on shutdown.
- `rdbSaveBackground` (`rdb.c:1673`) — **forks**. Used by `BGSAVE` and by automatic saves.
- `rdbSaveRio` (`rdb.c:1481`) — the actual serialization loop, writing to a `rio` abstraction.
- `rdbSaveKeyValuePair` (`rdb.c:1190`) — one key: expiry, type byte, key, value.

**The fork is the whole trick.** The child gets a copy-on-write snapshot of the parent's
memory for free — a consistent point-in-time view with no locking and no pause. The child
walks the keyspace and writes it out while the parent keeps serving traffic.

The cost of that trick is the thing that bites people in production: **every page the
parent writes to during the save gets copied.** A write-heavy workload during a BGSAVE can
approach 2× memory usage. This is why `stat_cow_bytes` gets tracked (you'll see it in
slot migration too, `cluster_migrateslots.c`) and why `INFO` reports COW size.

`rio` (`rio.c`) is an I/O abstraction with the same interface over a file, a socket, or a
buffer. That's what lets the exact same serialization code write an RDB to disk *and*
stream it to a replica — see `rdbSaveToReplicasSockets` (`rdb.c:3756`), which is the
diskless-replication path.

> A design for doing all of this **without the fork** — trading the COW page storm for
> version-stamped, in-process serialization — is in
> [proposal-forkless-rdb.md](proposal-forkless-rdb.md).

### Loading

- `rdbLoad` (`rdb.c:3631`) → `rdbLoadRioWithLoadingCtx` (`rdb.c:3160`).

The format is a sequence of opcodes: metadata (aux fields), then `SELECTDB`, then
key/value records, each optionally preceded by an expiry. Values are encoded per type,
and the **encodings from note 04 are serialized directly** — a listpack-encoded hash goes
to disk as a listpack. That's why loading is fast: for small objects it's close to a
memcpy, not a rebuild.

Records are **version-tagged** (`RDB_VERSION`), and unknown opcodes are a hard error, not
something to skip. This matters: an RDB is a trusted-input format, and a malformed or
hostile file must be rejected rather than half-loaded. (Your `fix-slot-import-rdb-validation`
branch is exactly this class of problem — validating records that came in via the RDB
path.)

## AOF

### Writing — three stages, and people confuse them constantly

1. **`feedAppendOnlyFile`** (`aof.c:1446`) — during command execution, append the
   (possibly rewritten, note 02) command to an **in-memory buffer**. Nothing has left the
   process yet.
2. **`flushAppendOnlyFile`** (`aof.c:1178`) — in `beforeSleep` (note 01), `write(2)` the
   buffer to the OS. Now it's in the **page cache**, not on disk.
3. **`fsync`** — force the page cache to the physical device. Controlled by `appendfsync`:
   - `always` — fsync every write. Safe, slow.
   - `everysec` — fsync once a second, **in a background thread** (`bio.c`). The default.
     Data loss window: ~1s.
   - `no` — let the OS decide. Fast, unbounded loss window.

The distinction between "written" (stage 2) and "fsynced" (stage 3) is where all the
durability arguments live. A crash of the *process* loses nothing after stage 2. A crash
of the *machine* loses everything not yet fsynced.

### Rewrite

An AOF that logs every command grows forever (`INCR x` a million times = a million lines
for one key). `rewriteAppendOnlyFileBackground` (`aof.c:2590`) forks a child that writes a
*minimal* command sequence reconstructing the current dataset — same COW trick as RDB.

Writes that arrive **during** the rewrite must not be lost: the parent accumulates them in
a buffer and appends them to the new AOF once the child finishes.

### The manifest — the modern part

`loadAppendOnlyFiles(aofManifest *am)` (`aof.c:1774`) — note the argument. Valkey does not
use a single `appendonly.aof` file anymore. It uses a **multi-part AOF** in an `appenddirname`
directory, described by a **manifest** file:

- a **base** file (often an RDB-format snapshot — "RDB preamble"),
- one or more **incremental** files (the command log since the base),
- a **history** of superseded files, deleted after a successful rewrite.

So a rewrite doesn't rewrite a file in place; it writes a new base + new manifest and
retires the old parts. This is much safer against a crash mid-rewrite: the manifest either
points at the old consistent set or the new one, never a half-written file.

If you're debugging AOF, **read the manifest first** — `appendonlydir/*.manifest` is plain
text and tells you exactly which files the server thinks are live.

## Read next

Note 07 — where RDB gets reused for something completely different.
