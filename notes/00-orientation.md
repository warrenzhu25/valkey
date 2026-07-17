# 00 — Orientation

## The single most important habit

**Never read a Valkey source file top to bottom.** `cluster_legacy.c` is 8,600 lines,
`server.c` is 8,000, `module.c` is 15,500. Linear reading fails.

Instead, for any file, read in this order:

1. **The top-of-file comment.** Valkey is unusually good at these. `hashtable.c`,
   `kvstore.c`, and `io_threads.c` all open with a design summary that saves you an
   hour of reading.
2. **The structs and enums.** In a state machine like `cluster_migrateslots.c`, the
   `slotMigrationJobState` enum *is* the protocol. Once you've read ~75 lines of type
   definitions, the remaining 2,500 lines are just handlers you can read on demand. Same
   trick works for the replica state machine (`REPL_STATE_*`, note 07) and the client
   flags (`struct ClientFlags`, note 03).
3. **One function, traced end to end.** Pick a path (a `GET`, a replica connecting)
   and follow it. Breadth comes later.

## Map of `src/`

**Core loop and dispatch**
- `server.c` — `main()`, init, `serverCron`, `beforeSleep`, `processCommand`, `call`
- `ae.c` — the event loop itself (thin; `ae_epoll.c` / `ae_kqueue.c` are backends)
- `networking.c` — clients, input parsing, reply buffers
- `io_threads.c` — I/O offload workers → see `design-docs/io-threads.md`

**Keyspace**
- `db.c` — `lookupKey`, `dbAdd`, `dbDelete`; the database abstraction
- `object.c` — `robj`, refcounting, encodings
- `kvstore.c` — array of hash tables, one per slot in cluster mode
- `hashtable.c` — the actual hash table (cache-line buckets, incremental rehash)
- `t_string.c`, `t_hash.c`, `t_list.c`, `t_set.c`, `t_zset.c`, `t_stream.c` — type commands
- `expire.c`, `evict.c` — TTL and maxmemory

**Durability**
- `rdb.c` — snapshot format, save/load
- `aof.c` — append-only file, manifest, rewrite
- `replication.c` — PSYNC, full/partial sync, dual-channel

**Cluster**
- `cluster.c` — mode-agnostic front end; slot→node lookup, redirects
- `cluster_legacy.c` — the gossip protocol / cluster bus (the big one)
- `cluster_migrateslots.c` — atomic slot migration → see `design-docs/atomic-slot-migration.md`

**Extension**
- `module.c`, `script*.c`, `function*.c`, `acl.c`, `pubsub.c`

## Landmark functions — the 20 that anchor everything

If you know where these are, you can navigate the rest by following calls. All verified
against this checkout (grep the name if a line has drifted):

| Function | Location | What it anchors |
|----------|----------|-----------------|
| `main` | `server.c:7511` | startup (weak symbol; tests override it) |
| `initServer` | `server.c:2924` | loop, sockets, DBs, cron created here |
| `aeMain` | `ae.c:540` | the event loop, never returns |
| `beforeSleep` | `server.c:1854` | deferred/must-happen-now work each iteration (note 01) |
| `serverCron` | `server.c:1537` | periodic janitor at `hz` (note 01) |
| `readQueryFromClient` | `networking.c:4341` | socket → query buffer (note 03) |
| `processCommand` | `server.c:4315` | all gatekeeping (note 02) |
| `call` | `server.c:3875` | actual execution + propagation (note 02) |
| `addReply` | `networking.c:787` | fill reply buffer, don't touch socket (note 03) |
| `handleClientsWithPendingWrites` | `networking.c:3318` | drain buffers to sockets (note 03) |
| `lookupKey` | `db.c:81` | keyspace read + lazy expiry (notes 04, 05) |
| `createObject` | `object.c:141` | every value is an `robj` (note 04) |
| `activeExpireCycle` | `expire.c:459` | background TTL sampling (note 05) |
| `performEvictions` | `evict.c:404` | maxmemory enforcement (note 05) |
| `rdbSaveBackground` | `rdb.c:1673` | forked snapshot (note 06) |
| `flushAppendOnlyFile` | `aof.c:1178` | AOF write stage (note 06) |
| `syncCommand` | `replication.c:1108` | primary side of PSYNC (note 07) |
| `syncWithPrimary` | `replication.c:4149` | replica state machine (note 07) |
| `getNodeByQuery` | `cluster.c:1048` | slot ownership / redirects (note 08) |
| `clusterCron` | `cluster_legacy.c:6236` | gossip + failover driver (note 08) |

## Your first trace (do this before reading further)

Follow one `GET foo`. It touches half the landmarks above and makes notes 01–04 concrete:

```
readQueryFromClient (networking.c:4341)   bytes off the socket
  → processInputBuffer (networking.c:4203)  RESP → c->argv = ["GET","foo"]
    → processCommand (server.c:4315)        auth? ACL? slot? maxmemory? — all pass
      → call (server.c:3875)                wrapper: stats, propagation, notifications
        → getCommand (t_string.c)           the actual proc
          → lookupKeyRead (db.c:144)        → lookupKey → expireIfNeeded
            → addReply (networking.c:787)   append "$3\r\nbar\r\n" to c->buf
... beforeSleep (server.c:1854) ...
  → handleClientsWithPendingWrites          write(2) the buffer to the socket
```

The single most clarifying observation: **the reply is not written by `getCommand`.** It's
buffered during execution and flushed by the event loop afterward (note 03). Internalize
that and the whole architecture opens up.

## The two-layer cluster split

Worth internalizing early because it's confusing otherwise: `cluster.c` is the
*interface* (what the rest of the server calls — "which node owns this key?"), and
`cluster_legacy.c` is the *implementation* (the gossip protocol that maintains that
answer). "Legacy" doesn't mean deprecated; it names the original cluster bus protocol
as distinct from the abstraction layered on top of it.

## Conventions you'll hit immediately

- `robj` (`object.h`) is the universal value wrapper: type + encoding + refcount + ptr.
  Almost everything in the keyspace is an `robj *`.
- `client` (`server.h`) is enormous and carries all per-connection state, including
  reply buffers and replication state. Replicas are represented as clients. So is the
  AOF. So are fake clients used by Lua and modules. (`conn == NULL` marks a fake client.)
- `server` is a single global struct (`struct valkeyServer server`). Grep it constantly.
- `C_OK` / `C_ERR` are the return convention, not `0`/`-1`.
- `sds` is the string type (`sds.c`) — length-prefixed, `char *`-compatible.
- `run_with_period(ms) { ... }` gates a block to run at most every `ms` regardless of `hz`
  — you'll see it all over `serverCron` (note 01).
- **"Bounded work per operation" is the recurring design pattern.** Incremental rehash
  (note 04), sampled expiry and sampled eviction (note 05), incremental backlog trim
  (note 07) — all the same instinct: never stop the single thread that owns the data.

## Building and poking at it

```sh
make -j                       # build
make CFLAGS="-O0 -g"          # debug build, easier to step in gdb
./src/valkey-server --port 7000 --loglevel debug
./src/valkey-cli -p 7000

./runtest --single unit/type/hash          # one TCL test file
./src/valkey-server --daemonize no --save ''   # no persistence, easier to trace
```

`--loglevel debug` plus a single-file test is the fastest way to watch a subsystem
actually run, which beats reading it statically. Two more tools you'll reach for
constantly:

- **`DEBUG` subcommands** — `DEBUG OBJECT <key>` (encoding + internals), `DEBUG JMAP`,
  `DEBUG SLEEP`, `DEBUG SET-ACTIVE-EXPIRE 0` (freeze active expiry to study lazy expiry
  alone), `DEBUG RELOAD` (round-trip through RDB). These are your keyhole into the
  internals without a debugger.
- **`OBJECT ENCODING` / `MEMORY USAGE`** — watch the encoding transitions in note 04 live.
- **A one-line `serverLog(LL_WARNING, ...)`** dropped into a landmark function above, then
  `make -j`, tells you more in one run than an hour of static reading. The exercises at the
  end of each note lean on exactly this.

## Reading order

| # | Note | Why |
|---|------|-----|
| 01 | [Server lifecycle & event loop](01-server-lifecycle-and-event-loop.md) | The heartbeat everything else hangs off |
| 02 | [Command execution path](02-command-execution-path.md) | Follow one `GET` from socket to reply |
| 03 | [Clients & networking](03-clients-and-networking.md) | The `client` struct, buffers, reply flow |
| 04 | [Keyspace & data model](04-keyspace-and-data-model.md) | `robj`, `serverDb`, `kvstore`, `hashtable` |
| 05 | [Expiration & eviction](05-expiration-and-eviction.md) | Lazy vs active expiry, maxmemory |
| 06 | [Persistence: RDB & AOF](06-persistence-rdb-aof.md) | Fork-based snapshots, the AOF manifest |
| 07 | [Replication](07-replication.md) | PSYNC, full vs partial, dual-channel |
| 08 | [Cluster bus & failover](08-cluster-bus-and-failover.md) | Gossip, slot ownership, redirects |
