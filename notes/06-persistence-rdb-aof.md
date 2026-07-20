# 06 — Persistence: RDB & AOF

An in-memory database has one obvious problem: memory doesn't survive a restart. Valkey
offers two ways to make data outlive the process, and they sit at opposite ends of the same
tradeoff. RDB takes an occasional photograph of the whole dataset — compact, fast to reload,
but everything since the last photo is lost in a crash. AOF writes down every command as it
happens — durable to within a second, but the log grows without bound and replay is slow.
They are not either/or; most production servers run both, and — the detail that ties this
chapter to the next — the RDB machinery is *also* how a replica gets its initial copy of the
data. Understand persistence and you've already understood half of replication.

|  | RDB | AOF |
|--|-----|-----|
| What | Point-in-time binary snapshot | Log of every write command |
| Restart speed | Fast (bulk load) | Slow (replay) |
| Data-loss window | Minutes (between saves) | ≤ 1 s (default fsync policy) |
| File size | Compact | Large; needs periodic rewrite |
| Also used for | **Replication full sync** (chapter 07) | — |

## RDB

### Saving

- `rdbSave` (`rdb.c:1634`) — **blocking**, in the main process. Used by `SAVE` and at shutdown.
- `rdbSaveBackground` (`rdb.c:1673`) — **forks a child**. Used by `BGSAVE` and automatic saves.
- `rdbSaveRio` (`rdb.c:1481`) — the actual serialization loop, writing to a `rio` abstraction.
- `rdbSaveKeyValuePair` (`rdb.c:1190`) — one key: its expiry, type byte, the key, the value.

**The fork is the whole trick.** When `rdbSaveBackground` calls `serverFork` (`rdb.c` in the
`rdbSaveBackground` body), the operating system gives the child a **copy-on-write** view of
the parent's entire address space. The child instantly has a *consistent, point-in-time
snapshot* of every key — with no locking, no pause, and no copy. It then walks the keyspace at
its leisure, serializing to disk, while the parent goes right back to serving clients. One
`fork()` call replaces what would otherwise be a stop-the-world dump or an elaborate
concurrent-snapshot algorithm.

The cost of the trick is the thing that bites in production: copy-on-write is free only for
pages nobody writes. **Every page the parent modifies while the child is saving must be
duplicated** so the child keeps seeing the old version. A write-heavy workload during a
`BGSAVE` can therefore push memory toward 2× the dataset size. This is why the server tracks
`stat_cow_bytes` and `INFO` reports the COW size — it's the single most important number to
watch when a save is causing memory pressure.

`rio` (`rio.c`) is an I/O abstraction presenting one interface over a file, a socket, or a
memory buffer. That indirection is what lets the *exact same* serialization code write an RDB
to disk and stream it directly to a replica over a socket — `rdbSaveToReplicasSockets`
(`rdb.c:3756`), the diskless-replication path of chapter 07.

> A design for doing all of this **without the fork** — trading the COW page storm for
> version-stamped, in-process serialization — is written up in
> [proposal-forkless-rdb.md](proposal-forkless-rdb.md).

### The file format is a flat opcode stream

The file opens with the magic `REDIS` plus a 4-digit version (`RDB_VERSION` = **80**,
`rdb.h:52`), then a sequence of one-byte **opcodes** until `EOF`:

| Opcode | Value | Meaning |
|--------|-------|---------|
| `RDB_OPCODE_AUX` | 250 | aux metadata (redis-ver, bits, ctime, used-mem, repl-id/offset) |
| `RDB_OPCODE_SELECTDB` | 254 | following keys belong to database N |
| `RDB_OPCODE_RESIZEDB` | 251 | hash-table size hint, so the loader pre-sizes its tables |
| `RDB_OPCODE_EXPIRETIME_MS` | 252 | absolute-ms expiry, precedes the key it applies to |
| `RDB_OPCODE_IDLE` / `FREQ` | 248 / 249 | per-key LRU idle / LFU counter (chapter 05), persisted so eviction quality survives a restart |
| `RDB_OPCODE_FUNCTION2` | 245 | `FUNCTION` library source |
| `RDB_OPCODE_SLOT_INFO` | 244 | per-slot key counts; explicitly **safe to ignore** on load |
| `RDB_OPCODE_EOF` | 255 | end, followed by a CRC64 of everything before it |

Everything else is a *type byte* (`RDB_TYPE_*`) introducing one key/value record. Two
consequences worth carrying forward:

- **The chapter-04 encodings are serialized directly.** A listpack-encoded hash goes to disk
  *as* a listpack; loading a small object is close to a `memcpy`, not a rebuild from scratch.
  That is why RDB load is so much faster than AOF replay.
- **Unknown opcodes are a hard error, not skipped** (apart from the few marked "safe to
  ignore" like `SLOT_INFO`). An RDB is a trusted-input format: a malformed or hostile file
  must be rejected outright rather than half-loaded, since a partial load would silently
  corrupt the keyspace. Validating records that arrive over the RDB path — including during
  cluster slot import — is a recurring source of real bugs precisely because this format
  assumes it's trusted.

### Loading

`rdbLoad` (`rdb.c:3631`) → `rdbLoadRioWithLoadingCtx` (`rdb.c:3160`) is a `switch` over the
opcodes above. It verifies the trailing CRC64 (unless checksums are disabled) and refuses a
version newer than it understands.

## AOF

### Writing — three stages people constantly confuse

1. **`feedAppendOnlyFile`** (`aof.c:1446`) — during command execution, append the (possibly
   rewritten — chapter 02) command to an **in-memory buffer**, `server.aof_buf`. Nothing has
   left the process yet.
2. **`flushAppendOnlyFile`** (`aof.c:1178`) — in `beforeSleep` (chapter 01, `server.c:1962`),
   `write(2)` that buffer to the OS. The data is now in the kernel **page cache** — but not
   yet on the physical disk.
3. **`fsync`** — force the page cache down to the device. Governed by `appendfsync`:
   - `always` — fsync on every event-loop flush. Safe, slow. (This is exactly why the
     chapter-01 ordering puts the AOF flush *before* client writes: a reply must never go out
     for a write that isn't yet durable.)
   - `everysec` — fsync about once a second, on a **background BIO thread** (`bio.c`). The
     default. Worst-case loss window ~1 s.
   - `no` — let the OS flush whenever it likes. Fastest, unbounded loss window.

The line between "written" (stage 2) and "fsynced" (stage 3) is where every durability
argument actually lives. A crash of the *process* loses nothing past stage 2 — the bytes are
in the kernel. A crash of the *machine* loses everything not yet through stage 3.

### Rewrite

An AOF that records every command grows without bound — `INCR x` a million times is a
million lines describing one key. `rewriteAppendOnlyFileBackground` (`aof.c:2590`) forks a
child (the same COW trick as RDB) that writes a *minimal* command sequence reconstructing the
current dataset — one `SET x <value>` instead of the million `INCR`s. Writes that arrive
**during** the rewrite mustn't be lost, so the parent buffers them and appends them to the
new AOF once the child finishes. `serverCron` triggers an automatic rewrite once the AOF has
grown `auto-aof-rewrite-percentage` beyond its last base size (chapter 01, `server.c:1661`).

### The manifest — the modern part

`loadAppendOnlyFiles(aofManifest *am)` (`aof.c:1774`) — note the argument type. Valkey no
longer uses a single `appendonly.aof`. It uses a **multi-part AOF** living in an
`appenddirname` directory, described by a plain-text **manifest**:

- a **base** file (usually an RDB-format snapshot — the "RDB preamble"),
- one or more **incremental** files (the command log accumulated since that base),
- a **history** of superseded files, deleted after a successful rewrite.

So a rewrite never edits a file in place. It writes a *new* base plus a *new* manifest and
retires the old parts. That is what makes it crash-safe: at any instant the manifest points
at either the old complete set of files or the new one, never a half-written frankenstein. If
you're ever debugging AOF, **read `appendonlydir/*.manifest` first** — it's text, and it
tells you exactly which files the server considers live.

## Worked example — a BGSAVE while writes keep coming

A server holds 4 GB of data and is taking ~10,000 writes/sec. A `save 300 100` rule matures
(chapter 01: 100+ changes and 300s elapsed), so `serverCron` calls `rdbSaveBackground`
(`rdb.c:1673`). Follow both processes.

**T+0 — fork.** `serverFork(CHILD_TYPE_RDB)` returns twice. The child sees a frozen 4 GB
snapshot; the parent gets the child's PID and immediately returns to the event loop. At this
instant almost no extra memory has been used — parent and child share every physical page,
marked read-only for copy-on-write.

**T+0…T+8s — the two diverge.**

```
CHILD (rdbSaveRio, rdb.c:1481)                PARENT (still serving clients)
  walk DB 0, DB 1, ...                           GET/SET/INCR as usual
  for each key: rdbSaveKeyValuePair (:1190)      a SET touches key "cart:99"
    write expiry, type byte, key, value            → that page was shared read-only
  listpack hash → written verbatim as listpack     → kernel copies the page (COW):
  ...                                                parent gets a private writable copy,
  write RDB_OPCODE_EOF + CRC64                       child still sees the OLD "cart:99"
  exitFromChild(0)                               server.stat_cow_bytes climbs
```

The crucial property: the child's snapshot is **consistent as of T+0**. Every write the
parent makes after the fork lands on a *copied* page the child never looks at, so the RDB on
disk is a clean point-in-time image — no write that happened after T+0 leaks into it, and no
write before it is missing. No lock was taken to achieve that; the MMU did the work.

**T+8s — completion.** The child writes the CRC64, `fsync`s, and exits. `serverCron`'s
`checkChildrenDone` (chapter 01) reaps it, renames the temp file into place, and clears the
save state. Peak extra memory was whatever fraction of the 4 GB the parent wrote to during
those 8 seconds — that's the COW cost, and it's exactly what `INFO`'s `rdb_last_cow_size`
reports. Had the parent been idle, the save would have cost almost no extra memory at all;
under the 10k writes/sec it cost real gigabytes. That sensitivity to write rate *is* the
BGSAVE production story, and it's the motivation for the forkless proposal linked above.

The AOF's version of "don't lose concurrent writes" is the mirror image: instead of COW-ing
pages away from a child, the parent *buffers* the commands arriving during a rewrite and
replays them onto the new base when the child is done. Same problem — a consistent snapshot
plus the delta since — solved with a fork in both cases.

## Try it yourself

See the RDB opcode stream directly:

```
valkey-cli set foo bar ; valkey-cli set n 123 ; valkey-cli save
xxd dump.rdb | head        # 'REDIS0080' magic, then AUX fields, then SELECTDB (0xFE)
```

For AOF, `CONFIG SET appendonly yes`, run `INCR c` a few times, and `cat
appendonlydir/*.incr.aof` — each command is there in RESP. Then `BGREWRITEAOF` and look at the
manifest and the new base file: the growing `INCR` log collapses to a single `SET c <value>`.
Watching the incremental file, the rewrite, and the manifest swap happen in real files makes
the three write stages concrete. To *feel* the COW cost from the worked example, run
`valkey-benchmark -t set -n 5000000` in one shell and `BGSAVE` in another, then watch
`INFO stats | grep cow` — the number is the parent's writes turned into copied pages.

## Read next

Chapter 07 — where the RDB snapshot gets reused for something entirely different: shipping the
whole dataset to a brand-new replica.
