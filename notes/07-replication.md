# 07 — Replication

Asynchronous, primary→replica, offset-based. `replication.c` (5,800 lines) contains both
sides of the protocol — primary logic and replica logic in the same file. That's the main
reason it reads as confusing. **Figure out which side a function is on before reading it.**

Rough rule: `*Command` functions and `replicationSetup*`/`primaryTry*` are the **primary**
side (responding to a replica). `syncWithPrimary*` and `readSyncBulkPayload` are the
**replica** side (driving its own connection to the primary).

## The core idea: a replication offset

The primary maintains a monotonically increasing byte offset of the replication stream
(`server.primary_repl_offset`). Every write it propagates advances it. Each replica reports
the offset it has processed.

That single number gives you: how far behind a replica is (`INFO replication`), whether a
reconnecting replica can resume, and who is most up to date during a failover (note 08).

The primary also keeps a **replication backlog** — a fixed-size circular buffer of the most
recent stream bytes (`repl-backlog-size`). It exists purely so a briefly-disconnected
replica can be caught up without a full resync. `beforeSleep` incrementally trims it
(`server.c:2003`) once no replica needs the old bytes.

## Establishing replication

`REPLICAOF <host> <port>` → `replicaofCommand` (`replication.c:4653`). This only sets state;
the actual connection is driven asynchronously.

### Replica side: `syncWithPrimary` (`replication.c:4149`) — an explicit state machine

Run as a connection handler: each time the socket becomes readable/writable it advances one
step. Don't look for a linear function — read the `server.repl_state` enum and the `switch`
(`replication.c:3800`+). The real progression (from that switch):

```
REPL_STATE_CONNECT
  → CONNECTING            TCP connect issued
  → RECEIVE_PING_REPLY    sent PING, awaiting PONG
  → SEND_HANDSHAKE        AUTH + REPLCONF listening-port + REPLCONF capa ...
  → RECEIVE_CAPA_REPLY
  → SEND_PSYNC
  → RECEIVE_PSYNC_REPLY   primary answered +FULLRESYNC or +CONTINUE
  → TRANSFER              receiving the RDB bulk payload (readSyncBulkPayload)
  → CONNECTED             streaming live commands, offset advancing
```

`syncWithPrimaryHandleError` (`replication.c:4059`) is the common failure exit — any step
failing resets the state and retries later from `replicationCron`. When you see a replica
"stuck", `INFO replication`'s `master_link_status` plus this enum tells you exactly which
step it died on.

### Primary side: `syncCommand` (`replication.c:1108`)

Handles the replica's `PSYNC <replid> <offset>` and decides between two outcomes:

**Partial resync.** `primaryTryPartialResynchronization` (`replication.c:885`). Granted only
if the replica's replication ID matches ours **and** the offset it asks for is still inside
the backlog. Reply `+CONTINUE`, then just stream the missing bytes from the backlog. Cheap.

**Full resync.** Everything else. `replicationSetupReplicaForFullResync`
(`replication.c:857`) replies `+FULLRESYNC <replid> <offset>`, then the primary produces an
RDB snapshot of the entire dataset and ships it. Expensive: a fork, potentially gigabytes
over the network, and the replica throws away its dataset to load it.

**The whole design of this subsystem is "avoid full resync."** The backlog, the replication
ID handoff, dual-channel — all of it exists to make partial resync possible more often.

### Why two replication IDs

A replica keeps `replid` and `replid2`. When a replica is promoted to primary, it keeps the
old ID as `replid2` and remembers the offset at which it changed. That way, *other* replicas
of the old primary can reconnect to the new one and still be granted a **partial** resync —
the new primary recognizes the old history as its own. Without this, every failover would
force a full resync of every surviving replica. This is subtle and worth reading the actual
code for (`primaryTryPartialResynchronization` checks *both* IDs).

## Streaming writes

`replicationFeedReplicas` (`replication.c:579`) — called from the propagation path (note 02)
after a command executes. It appends to the backlog and to every replica's output buffer.

Remember from note 03: **a replica is just a client**. Feeding a replica is `addReply` into
its reply buffer, and `beforeSleep` flushes it. Which also means `client-output-buffer-limit`
applies to replicas, and a replica that falls too far behind gets disconnected — then has to
resync.

Propagation is **asynchronous**: the primary does *not* wait for replica acks before
replying to the client. That is why Valkey can lose acknowledged writes on failover. `WAIT`
lets a client block until N replicas have acked a given offset — implemented via the
`get_ack_from_replicas` / `sendGetackToReplicas` path in `beforeSleep` (note 01,
`server.c:1933`) — but it is opt-in, and it is *not* a consensus protocol.

## Full sync: three flavors

1. **Disk-backed.** Fork, write RDB to disk, then send the file. `rdbSaveBackground`.
2. **Diskless.** Fork, child writes the RDB *directly to the replica sockets* —
   `rdbSaveToReplicasSockets` (`rdb.c:3756`). No disk round-trip. Made possible by the `rio`
   abstraction (note 06). Configured with `repl-diskless-sync`.
3. **Dual-channel.** The newer one; see below.

## Dual-channel replication

Key anchors: `dualChannelFullSyncWithPrimary` (`replication.c:3250`),
`dualChannelReplHandleHandshake` (`replication.c:3103`), `replicaReceiveRDBFromPrimaryToDisk`
(`replication.c:2763`), `receiveRDBinBioThread` (`replication.c:3011`).

**The problem it solves:** in a classic full sync, one connection carries the RDB *and then*
the subsequent command stream. While the replica is busy loading a large RDB, the primary
has nowhere to put the new writes accumulating for it except that replica's output buffer —
which can blow through `client-output-buffer-limit` and kill the sync. On big datasets this
turns into an infinite resync loop (the same failure mode as note 03).

**The fix:** use **two connections**. One carries the RDB snapshot; the other, opened in
parallel, immediately starts accumulating the live replication stream from the snapshot's
offset. The replica loads the RDB (via a **BIO thread**, `receiveRDBinBioThread` — so the
main thread stays responsive) and then applies the buffered stream from the second channel.

Note the thread interaction here, and how it connects to note 01: the RDB is received and
written to disk on a background thread, precisely so that the replica's single-threaded main
loop is not blocked for the duration of a multi-gigabyte transfer.

`replicationCron` (`replication.c:5317`) — the periodic side: pings to replicas, timeout
detection, retrying a failed connect, expiring the backlog.

## Exercise

Bring up a primary on 6379 and a replica on 6380 (`valkey-server --port 6380 --replicaof
127.0.0.1 6379`). On the replica, watch `INFO replication` — you'll catch
`master_link_status:down` → `up` and `master_sync_in_progress` flip as it walks the state
machine above. Now force the two paths: write a few keys, `DEBUG SLEEP` nothing, restart the
replica quickly → `master_repl_offset` still inside the backlog → look for `+CONTINUE`
(partial resync) in the logs. Then `CONFIG SET repl-backlog-size 16kb`, flood writes while
the replica is briefly down, reconnect → the requested offset has scrolled out of the
backlog → `+FULLRESYNC` and a fork. You just triggered both branches of `syncCommand` on
demand.

## Read next

Note 08 — replication is the foundation failover is built on.
