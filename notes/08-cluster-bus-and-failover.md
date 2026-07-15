# 08 — Cluster Bus & Failover

This note covers the *gossip layer*: how nodes find each other, agree on who owns which
slot, and elect a new primary. It deliberately does **not** cover slot migration — that has
a real design doc (`design-docs/atomic-slot-migration.md`), which you should read after this.

## The two-file split (say it again)

- **`cluster.c`** (1,800 lines) — the interface the rest of the server uses. Mode-agnostic.
- **`cluster_legacy.c`** (8,600 lines) — the gossip protocol implementation. "Legacy" names
  the original cluster bus wire protocol; it is not deprecated.

Start in `cluster.c`. You may never need most of `cluster_legacy.c`.

## Slots

16,384 hash slots. `slot = CRC16(key) mod 16384`. Every slot is owned by exactly one primary.
**Keys are never sharded individually** — the slot is the unit of ownership, which is what
makes ownership a small, gossipable fact (16K entries, held as a bitmap) rather than a
distributed index of every key.

**Hash tags:** if the key contains `{...}`, only the substring inside the braces is hashed.
`{user1}:profile` and `{user1}:sessions` therefore land in the same slot, on the same node,
so a multi-key command over them is legal. This is the *only* mechanism for co-locating
related keys, and it's why cluster-aware schemas put a tag in the key name.

Recall from note 04 that `kvstore` keeps **one hash table per slot**. Slot ownership is
therefore not just metadata — it's reflected in the physical layout of the keyspace, which
is what makes "hand slot 4242 to another node" a tractable operation.

## Redirection — the client-facing half

`getNodeByQuery` (`cluster.c:1048`) is called from `processCommand` (note 02) before any
command executes. It extracts the key(s), computes the slot, and determines whether *this*
node can serve the request.

`clusterRedirectClient` (`cluster.c:1314`) emits the answer:

- **`-MOVED <slot> <ip:port>`** — "I don't own this slot; the owner is over there, and this
  is stable." The client should update its cached slot map.
- **`-ASK <slot> <ip:port>`** — "This slot is *currently migrating*; this particular key has
  already moved. Ask the target *for this one request only*, prefixed with `ASKING`." The
  client must **not** update its slot map.
- **`-CROSSSLOT`** — a multi-key command whose keys aren't all in one slot. Rejected outright.
- **`-CLUSTERDOWN`** — the cluster isn't in a servable state.

The MOVED/ASK distinction is the crux of cluster redirection: MOVED is a permanent topology
fact, ASK is a temporary per-key exception during migration. Getting a client library to
treat them the same is a classic bug.

## The cluster bus

Every node listens on a **second port** (`port + 10000`) speaking a **binary** protocol —
not RESP. Node-to-node only; clients never touch it. It uses `clusterLink`, not `client`
(the one major exception to note 03's "everything is a client").

### Gossip

`clusterCron` (`cluster_legacy.c:6236`) runs periodically and drives the whole thing:

- `clusterSendPing` (`cluster_legacy.c:4898`) — send a PING to a random subset of nodes.
- Every packet header carries the sender's view of the cluster: its slot bitmap, its config
  epoch, its state. Crucially, each PING also embeds a **gossip section** — a handful of
  *randomly chosen other nodes* and what the sender believes about their health.
- `clusterProcessPacket` (`cluster_legacy.c:3886`) — inbound packet dispatch.
- `clusterProcessGossipSection` (`cluster_legacy.c:2809`) — merge what other nodes claim.

So a node learns about the cluster **transitively**, without an all-to-all mesh of health
checks. This is what makes it scale to hundreds of nodes.

### Failure detection is two-phase

- **PFAIL** (*possible* failure) — *I* haven't heard from node X within `cluster-node-timeout`.
  A purely local suspicion. Not actionable.
- **FAIL** (*confirmed* failure) — enough other primaries have *also* reported X as PFAIL (via
  their gossip sections) that a **majority of primaries** now agree. Now it's actionable and
  gets broadcast.

The PFAIL→FAIL promotion is a quorum. This is what prevents a single node with a bad network
link from unilaterally declaring a healthy primary dead.

### Failover election

`clusterHandleReplicaFailover` (`cluster_legacy.c:5691`). When a replica sees its primary
marked FAIL:

1. **Wait.** A delay proportional to how far behind the primary the replica is (its
   replication offset, note 07) — so the *most up-to-date* replica tends to ask first, and
   thus tends to win. This is a ranking heuristic, not a guarantee.
2. **Request votes** from all primaries, at a new **config epoch**.
3. A primary grants at most one vote per epoch. Win a **majority of primaries** →
4. Claim the failed primary's slots, bump the config epoch, and broadcast the new
   configuration.

**Config epoch is the conflict resolver.** When two nodes disagree about who owns a slot, the
claim with the **higher config epoch wins**, unconditionally. That's the entire tiebreak rule,
and it's how the cluster converges without a consensus log.

`clusterUpdateState` (`cluster_legacy.c:6691`) decides whether the cluster is `ok` or `down`
(e.g. `cluster-require-full-coverage` — if any slot has no owner, refuse to serve).

## The honest caveat

This is **not** Raft or Paxos, and it does not claim to be. Replication is asynchronous
(note 07), so a failover **can lose acknowledged writes**: a primary can ack a write to a
client, die before propagating it, and a replica without that write can be elected. The
cluster protocol provides *availability and automatic recovery*, not linearizability. If you
need "no acknowledged write is ever lost," Valkey Cluster alone doesn't give it to you.

Understanding this is more valuable than memorizing the packet format.

## Read next

`design-docs/atomic-slot-migration.md` — now that you know how slot ownership is gossiped and
how config epochs resolve conflicts, the migration state machine will make sense.
