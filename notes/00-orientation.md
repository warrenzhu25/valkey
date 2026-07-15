# 00 — Orientation

## The single most important habit

**Never read a Valkey source file top to bottom.** `cluster_legacy.c` is 8,600 lines,
`server.c` is 8,000, `module.c` is 15,000. Linear reading fails.

Instead, for any file, read in this order:

1. **The top-of-file comment.** Valkey is unusually good at these. `hashtable.c`,
   `kvstore.c`, and `io_threads.c` all open with a design summary that saves you an
   hour of reading.
2. **The structs and enums.** In a state machine like `cluster_migrateslots.c`, the
   `slotMigrationJobState` enum *is* the protocol. Once you've read ~75 lines of type
   definitions, the remaining 2,500 lines are just handlers you can read on demand.
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
  AOF. So are fake clients used by Lua and modules.
- `server` is a single global struct (`struct valkeyServer server`). Grep it constantly.
- `C_OK` / `C_ERR` are the return convention, not `0`/`-1`.
- `sds` is the string type (`sds.c`) — length-prefixed, `char *`-compatible.

## Building and poking at it

```sh
make -j                       # build
./src/valkey-server --port 7000 --loglevel debug
./src/valkey-cli -p 7000

./runtest --single unit/type/hash          # one TCL test file
./src/valkey-server --daemonize no --save ''   # no persistence, easier to trace
```

`--loglevel debug` plus a single-file test is the fastest way to watch a subsystem
actually run, which beats reading it statically.
