# 07 — Replication

Replication is how one Valkey server's data comes to exist on another: a **primary** streams
its writes to one or more **replicas**, which apply them in order and end up holding the same
dataset. It's what gives you read scaling (fan reads out to replicas), availability (promote a
replica when the primary dies — chapter 08), and hot backups. The model is deliberately
simple and deliberately *not* a consensus protocol: replication is **asynchronous** and
**offset-based**, the primary never waits for replicas before answering a client, and the
whole design bends toward one goal — *avoid re-sending the entire dataset*.

`replication.c` is ~5,800 lines holding **both** sides of the protocol — primary logic and
replica logic in one file — which is the main reason it reads as bewildering. The single most
useful habit: **decide which side a function is on before reading it.** Roughly, `*Command`
handlers and `replicationSetup*` / `primaryTry*` are the **primary** responding to a replica;
`syncWithPrimary*` and `readSyncBulkPayload` are the **replica** driving its own connection
upstream.

## The core idea: a replication offset

The primary maintains a monotonically increasing byte count of everything it has ever
streamed — `server.primary_repl_offset`. Every write it propagates advances it. Each replica
reports the offset it has applied. That one number answers a surprising number of questions:
how far behind a replica is (`INFO replication`), whether a briefly-disconnected replica can
resume where it left off, and which replica is most up to date when a failover has to pick a
new primary (chapter 08).

Alongside the offset the primary keeps a **replication backlog**: a fixed-size circular buffer
(`repl-backlog-size`) of the most recent stream bytes. Its entire purpose is to let a replica
that dropped off for a moment catch up from the buffer instead of reloading everything.
`beforeSleep` incrementally trims it (`server.c:2003`) once no replica still needs the old
bytes — the same "bounded work per iteration" instinct as everywhere else.

## Establishing replication

`REPLICAOF <host> <port>` → `replicaofCommand` (`replication.c:4653`) only *sets state*; the
connection is driven asynchronously afterward.

### Replica side: `syncWithPrimary` (`replication.c:4149`) — an explicit state machine

This runs as a connection handler: each time the socket becomes readable or writable, it
advances one step. Don't hunt for a linear function — read the `server.repl_state` enum
(`server.h:390`) and the `switch`. The progression:

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

`syncWithPrimaryHandleError` (`replication.c:4059`) is the common failure exit: any step that
fails resets the state and is retried later from `replicationCron`. When a replica appears
"stuck," `INFO replication`'s `master_link_status` plus this enum tell you exactly which step
it died on.

### Primary side: `syncCommand` (`replication.c:1108`)

The primary receives the replica's `PSYNC <replid> <offset>` and chooses between two outcomes:

**Partial resync** — `primaryTryPartialResynchronization` (`replication.c:885`). Granted only
if the replica's replication ID matches the primary's history **and** the offset it asks for
is still inside the backlog. The primary replies `+CONTINUE` and simply streams the missing
bytes from the backlog. Cheap — no fork, no snapshot.

**Full resync** — everything else. `replicationSetupReplicaForFullResync` (`replication.c:857`)
replies `+FULLRESYNC <replid> <offset>`, then the primary produces an RDB snapshot of the whole
dataset and ships it. Expensive: a fork, potentially gigabytes over the network, and the
replica discards its current dataset to load the new one.

**The entire subsystem is organized around avoiding that full resync.** The backlog, the
replication-ID handoff, and dual-channel replication all exist to make partial resync possible
in more situations.

### Why two replication IDs

A replica tracks both `replid` and `replid2`. When a replica is promoted to primary, it keeps
the *old* primary's ID as `replid2` and records the offset at which the switch happened. That
lets the *other* replicas of the dead primary reconnect to the newly promoted one and still be
granted a **partial** resync — the new primary recognizes the old history as its own. Without
this, every failover would force a full resync of every surviving replica. It's subtle enough
to be worth reading in the source: `primaryTryPartialResynchronization` checks *both* IDs.

## Streaming writes

`replicationFeedReplicas` (`replication.c:579`) is called from the propagation path
(chapter 02) after a command executes. It appends the command to the backlog and to every
replica's output buffer.

Recall from chapter 03 that **a replica is just a client.** Feeding a replica is literally
`addReply` into its reply buffer, drained by `beforeSleep` — which means
`client-output-buffer-limit` applies to replicas too, and a replica that falls too far behind
gets disconnected and must resync.

Propagation is **asynchronous**: the primary does not wait for replica acks before replying to
the client. That is precisely why Valkey can lose acknowledged writes on failover — a write
can be acked to the client and then lost if the primary dies before any replica received it.
`WAIT N timeout` lets a client block until N replicas have acked a given offset (via the
`sendGetackToReplicas` path in `beforeSleep`, chapter 01, `server.c:1933`), but it is opt-in
and it is *not* consensus — it reports how many replicas have the data, it doesn't guarantee
they will keep it.

## Full sync: three flavors

1. **Disk-backed** — fork, write the RDB to disk, then send the file. `rdbSaveBackground`.
2. **Diskless** — fork, and the child writes the RDB *straight to the replica sockets* via
   `rdbSaveToReplicasSockets` (`rdb.c:3756`), no disk round-trip. Enabled by the `rio`
   abstraction (chapter 06) and configured with `repl-diskless-sync`.
3. **Dual-channel** — the newer design, below.

## Dual-channel replication

Anchors: `dualChannelFullSyncWithPrimary` (`replication.c:3250`), `dualChannelReplHandleHandshake`
(`replication.c:3103`), `replicaReceiveRDBFromPrimaryToDisk` (`replication.c:2763`),
`receiveRDBinBioThread` (`replication.c:3011`).

**The problem it solves:** in a classic full sync a *single* connection carries the RDB and
then the command stream that follows it. While the replica is busy loading a large RDB, the
primary has nowhere to park the writes piling up for that replica except its output buffer —
which can blow through `client-output-buffer-limit` and kill the sync mid-flight. On large
datasets this becomes an infinite resync loop (the same failure mode as chapter 03).

**The fix:** two connections. One carries the RDB snapshot; the other, opened in parallel,
immediately begins accumulating the live replication stream from the snapshot's offset. The
replica loads the RDB on a **BIO thread** (`receiveRDBinBioThread`) so its single-threaded main
loop stays responsive through a multi-gigabyte transfer, then applies the buffered stream from
the second channel. This is the chapter-01 threading rule in action: the heavy I/O is pushed to
a background thread precisely so the main thread never stalls.

`replicationCron` (`replication.c:5317`) is the periodic side: pinging replicas, detecting
timeouts, retrying failed connects, and expiring the backlog.

## Worked example — a brand-new replica joins

Start an empty replica and point it at a primary that already holds 2 GB across
`primary_repl_offset = 500,000`. Watch both sides walk the machine.

**1. `REPLICAOF 127.0.0.1 6379`** on the replica → `replicaofCommand` sets `repl_state =
REPL_STATE_CONNECT`. Nothing has connected yet; `replicationCron` will drive it.

**2. Handshake.** The replica's `syncWithPrimary` (`replication.c:4149`) advances on each
socket event: CONNECT → CONNECTING → PING/PONG → sends `REPLCONF listening-port 6380` and
`REPLCONF capa eof capa psync2` → SEND_PSYNC. Having no prior history, it sends **`PSYNC ? -1`**
("I know no replication ID, give me offset −1").

**3. Primary decides — full resync.** `syncCommand` (`replication.c:1108`) reads `PSYNC ? -1`
and calls `primaryTryPartialResynchronization` (`replication.c:885`): the replica's ID is `?`,
which matches nothing, so partial is impossible. It falls through to
`replicationSetupReplicaForFullResync` (`replication.c:857`), which replies:

```
+FULLRESYNC 8b2e...f3a1 500000\r\n
```

handing over its replication ID and the offset the snapshot will correspond to. It then starts
a `BGSAVE` (fork + COW, chapter 06) — or, with `repl-diskless-sync`, has the child write the
RDB straight down the socket.

**4. Transfer.** The replica, in `RECEIVE_PSYNC_REPLY`, sees `+FULLRESYNC`, records the ID and
offset 500,000, and moves to `TRANSFER`. `readSyncBulkPayload` reads the RDB bulk payload;
critically, the replica **empties its own keyspace** and loads the snapshot in its place — the
old contents are gone. Meanwhile any writes the primary took *after* offset 500,000 are being
buffered for this replica (in its output buffer, or the second channel under dual-channel).

**5. `CONNECTED` — live streaming.** With the RDB loaded, the replica is now byte-identical to
the primary *as of* offset 500,000, and enters `REPL_STATE_CONNECTED`. Now every write on the
primary flows through `replicationFeedReplicas` (`replication.c:579`): a client runs `SET x 1`,
the primary propagates it, `primary_repl_offset` advances to, say, 500,042, and those 42 bytes
are `addReply`'d into this replica's buffer. The replica applies `SET x 1`, advances its own
`slave_repl_offset` to 500,042, and periodically `REPLCONF ACK 500042`s back. `INFO replication`
on the primary now shows this replica with `offset=500042`, lag ≈ 0.

**6. A blip, and why the backlog matters.** The replica's network hiccups for 200 ms and
reconnects. This time it sends `PSYNC 8b2e...f3a1 500042` — it *knows* the ID and its offset.
`primaryTryPartialResynchronization` sees the ID matches and offset 500,042 is still within the
backlog, replies **`+CONTINUE`**, and streams only the ~few kilobytes written during the blip.
No fork, no 2 GB reload. That is the payoff the whole subsystem is built around — and if the
blip had lasted long enough for offset 500,042 to scroll out of a small `repl-backlog-size`,
step 3's full resync would have repeated instead.

## Try it yourself

Bring up a primary on 6379 and a replica on 6380 (`valkey-server --port 6380 --replicaof
127.0.0.1 6379`). On the replica, watch `INFO replication`: you'll catch `master_link_status`
go `down` → `up` and `master_sync_in_progress` flip as it walks the state machine — that's
steps 2–5. Now force both branches of `syncCommand` on demand. Write a few keys, restart the
replica quickly: its requested offset is still inside the backlog → look for `+CONTINUE`
(partial, step 6) in the logs. Then `CONFIG SET repl-backlog-size 16kb`, flood writes while the
replica is briefly down, and reconnect: the requested offset has scrolled out of the backlog →
`+FULLRESYNC` and a fork (step 3). You just drove the entire worked example by hand.

## Read next

Chapter 08 — replication is the foundation that automatic failover is built on: how a cluster
detects a dead primary and promotes one of these replicas without an operator in the loop.
