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

### The file format is a flat opcode stream

Header `REDIS` + 4-digit version (`RDB_VERSION` = **80**, `rdb.h:52`), then a sequence of
one-byte **opcodes** (`rdb.h`) until `EOF`:

| Opcode | Value | Meaning |
|--------|-------|---------|
| `RDB_OPCODE_AUX` | 250 | aux metadata (redis-ver, bits, ctime, used-mem, repl-id/offset) |
| `RDB_OPCODE_SELECTDB` | 254 | following keys belong to DB N |
| `RDB_OPCODE_RESIZEDB` | 251 | hash-table size hint so load pre-sizes tables |
| `RDB_OPCODE_EXPIRETIME_MS` | 252 | absolute ms expiry, precedes the key it applies to |
| `RDB_OPCODE_IDLE` / `FREQ` | 248 / 249 | per-key LRU idle / LFU counter (note 05) — persisted so eviction quality survives a restart |
| `RDB_OPCODE_FUNCTION2` | 245 | FUNCTION library source |
| `RDB_OPCODE_SLOT_INFO` | 244 | per-slot key counts; **"safe to ignore"** on load |
| `RDB_OPCODE_EOF` | 255 | end, followed by a CRC64 of everything before it |

Everything else is a *type byte* (`RDB_TYPE_*`) introducing a key/value record. Two
consequences worth carrying:

- **Encodings from note 04 are serialized directly.** A listpack-encoded hash goes to disk
  *as* a listpack; loading small objects is close to a `memcpy`, not a rebuild. That's why
  RDB load is fast.
- **Unknown opcodes are a hard error, not skipped** (except the few explicitly marked "safe
  to ignore" like `SLOT_INFO`). An RDB is a trusted-input format; a malformed or hostile
  file must be rejected rather than half-loaded. (Your `fix-slot-import-rdb-validation`
  branch is exactly this class of problem — validating records arriving via the RDB path.)

### Loading

- `rdbLoad` (`rdb.c:3631`) → `rdbLoadRioWithLoadingCtx` (`rdb.c:3160`).

The loader is a `switch` over the opcodes above. It verifies the trailing CRC64 (unless
checksums are disabled) and rejects a version newer than it understands.

## AOF

### Writing — three stages, and people confuse them constantly

1. **`feedAppendOnlyFile`** (`aof.c:1446`) — during command execution, append the
   (possibly rewritten, note 02) command to an **in-memory buffer** (`server.aof_buf`).
   Nothing has left the process yet.
2. **`flushAppendOnlyFile`** (`aof.c:1178`) — in `beforeSleep` (note 01, `server.c:1962`),
   `write(2)` the buffer to the OS. Now it's in the **page cache**, not on disk.
3. **`fsync`** — force the page cache to the physical device. Controlled by `appendfsync`:
   - `always` — fsync every event-loop flush. Safe, slow. (This is why the ordering in
     note 01 puts the AOF flush *before* client writes — so a reply is never sent for a
     write that isn't yet durable.)
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
a buffer and appends them to the new AOF once the child finishes. (`serverCron` triggers an
auto-rewrite when the AOF has grown `auto-aof-rewrite-percentage` beyond its last base
size — note 01, `server.c:1661`.)

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

## Exercise

See the RDB opcode stream directly. Save a tiny dataset, then dump the header bytes:

```
valkey-cli set foo bar ; valkey-cli set n 123 ; valkey-cli save
xxd dump.rdb | head        # 'REDIS0011'-style magic, then AUX fields, then SELECTDB(0xFE)
```

For AOF, `CONFIG SET appendonly yes`, run `INCR c` a few times, and `cat
appendonlydir/*.incr.aof` — you'll see each command in RESP. Then `BGREWRITEAOF` and look
at the manifest and the new base file: the million-line log collapses to a single `SET c
<value>`. Watching the incremental file, the rewrite, and the manifest swap in real files
makes the three write stages concrete.

## Read next

Note 07 — where RDB gets reused for something completely different.
