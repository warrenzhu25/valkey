# 08 — Cluster Bus & Failover

A single primary with replicas (chapter 07) gives you availability but not *scale* — one
machine still has to hold the whole dataset and take every write. Valkey Cluster spreads the
data across many primaries, each owning a slice of the keyspace, and keeps the whole thing
alive without an operator by detecting dead primaries and promoting their replicas
automatically. This chapter is the **gossip layer** that makes that work: how nodes discover
each other, agree on who owns which slice, and elect a new primary when one dies. It
deliberately does **not** cover slot *migration* — that has its own design doc
(`design-docs/atomic-slot-migration.md`), which will make far more sense once you've read this.

The thing to keep in mind throughout: this is a **gossip-and-quorum** system, not a consensus
protocol. It buys availability and automatic recovery, and it explicitly does *not* buy
"never lose an acknowledged write." The honest caveat at the end is the most important part of
the chapter.

## The two-file split (worth repeating)

- **`cluster.c`** (~1,800 lines) — the interface the rest of the server calls. Mode-agnostic:
  "which node owns this key?", "redirect this client."
- **`cluster_legacy.c`** (~8,600 lines) — the gossip protocol implementation. "Legacy" names
  the original cluster-bus wire protocol; it is *not* deprecated.

Start in `cluster.c`; you may never need most of `cluster_legacy.c`.

## Slots — the unit of ownership

There are 16,384 hash slots (`CLUSTER_SLOTS`, `cluster.h:10`). A key's slot is
`CRC16(key) mod 16384`, computed by `keyHashSlot` (`cluster.c:58`), and every slot is owned by
exactly one primary. **Keys are never sharded individually.** The slot is the unit of
ownership, and that choice is what makes ownership a small, gossipable fact — a 16 K-entry
bitmap each node can hold and exchange — instead of a distributed index over every key.

**Hash tags** let you force co-location. `keyHashSlot` checks for `{...}` and, if present,
hashes *only* the substring inside the braces. So `{user1}:profile` and `{user1}:sessions`
hash to the same slot, land on the same node, and can be touched together by a multi-key
command. This is the *only* mechanism for co-locating related keys, which is why
cluster-aware schemas deliberately put a tag in the key name.

Recall from chapter 04 that `kvstore` keeps **one hash table per slot**. Slot ownership is
therefore not merely metadata — it's mirrored in the physical layout of the keyspace, which is
exactly what makes "hand slot 4242 to another node" a tractable, bounded operation rather than
a full scan.

## Redirection — the client-facing half

`getNodeByQuery` (`cluster.c:1048`) is called from `processCommand` (chapter 02) *before* a
command executes. It extracts the key(s) via the command's key specs, computes the slot, and
decides whether *this* node can serve the request. `clusterRedirectClient` (`cluster.c:1314`)
emits the verdict:

- **`-MOVED <slot> <ip:port>`** — "I don't own this slot; the owner is there, and this is
  stable." The client should update its cached slot map.
- **`-ASK <slot> <ip:port>`** — "This slot is *currently migrating* and this particular key has
  already moved. Ask the target for *this one request*, prefixed with `ASKING`." The client
  must **not** update its slot map.
- **`-CROSSSLOT`** — a multi-key command whose keys aren't all in one slot. Rejected outright.
- **`-CLUSTERDOWN`** — the cluster isn't in a servable state.

The MOVED/ASK distinction is the crux of client-side cluster support: MOVED is a permanent
topology fact, ASK is a temporary per-key exception during migration. A client library that
treats them the same is a classic, subtle bug.

## The cluster bus

Every node listens on a **second port** — `port + CLUSTER_PORT_INCR`, i.e. `+10000`
(`cluster_legacy.h:5`) — speaking a **binary** protocol, not RESP. It is node-to-node only;
clients never touch it. It uses `clusterLink`, not `client` — the one major exception to
chapter 03's "everything is a client."

### Gossip

`clusterCron` (`cluster_legacy.c:6236`) runs at ~10 Hz and drives the whole thing:

- `clusterSendPing` (`cluster_legacy.c:4898`) — send a PING to a random subset of nodes.
- Every packet header carries the sender's view: its slot bitmap, its config epoch, its state.
  Crucially, each PING also embeds a **gossip section** — a handful of *randomly chosen other
  nodes* and what the sender believes about their health.
- `clusterProcessPacket` (`cluster_legacy.c:3886`) — inbound packet dispatch.
- `clusterProcessGossipSection` (`cluster_legacy.c:2809`) — merge what other nodes claim.

So a node learns about the cluster **transitively**, without an all-to-all mesh of health
checks. That's what lets it scale to hundreds of nodes: gossip traffic per node grows roughly
with `log(N)`, not `N`.

### Failure detection is two-phase

- **PFAIL** (*possible* failure) — *I* personally haven't heard from node X within
  `cluster-node-timeout`. A purely local suspicion; not actionable.
- **FAIL** (*confirmed* failure) — enough other primaries have *also* reported X as PFAIL (via
  their gossip sections) that a **majority of primaries** now agree. Now it's actionable, gets
  broadcast, and is set in `markNodeAsFailingIfNeeded` (`cluster_legacy.c:2622`).

The PFAIL→FAIL promotion is a quorum, and that's precisely what stops a single node with one
bad network link from unilaterally declaring a healthy primary dead.

### Failover election

`clusterHandleReplicaFailover` (`cluster_legacy.c:5691`). When a replica sees its primary
marked FAIL:

1. **Wait** a delay proportional to how far *behind* the replica is (by replication offset,
   chapter 07) — so the most up-to-date replica tends to ask first and thus tends to win. A
   ranking heuristic, not a guarantee.
2. **Request votes** from all primaries at a new **config epoch** (a `FAILOVER_AUTH_REQUEST`
   broadcast on the bus).
3. Each primary grants at most one vote per epoch. Win a **majority of primaries** →
4. Claim the failed primary's slots, bump the config epoch, and broadcast the new
   configuration.

**Config epoch is the conflict resolver.** When two nodes disagree about who owns a slot, the
claim carrying the **higher config epoch wins**, unconditionally. That single rule is the
entire tiebreak, and it's how the cluster converges without a consensus log.

`clusterUpdateState` (`cluster_legacy.c:6691`) decides whether the cluster is `ok` or `down`
(e.g. under `cluster-require-full-coverage`, if any slot has no owner, it refuses to serve).

## Worked example — a primary dies and a replica takes over

A six-node cluster: primaries **A, B, C** (A owns slots 0–5460), each with one replica —
**A′, B′, C′**. A′ is fully caught up with A. Now A's machine loses power. Trace the recovery.

**T+0 — silence.** A stops sending PINGs. Nothing happens yet; a missed ping isn't a failure.

**T+node-timeout — local suspicion (PFAIL).** B and C each notice they haven't heard from A
within `cluster-node-timeout` and mark A `PFAIL` locally (in their own node tables). A′ notices
too. These are three independent private suspicions — not yet actionable, and not yet shared as
fact.

**T+ε — gossip promotes it to FAIL.** On the next `clusterCron` tick, B's PING to C includes a
gossip section saying "I think A is PFAIL"; C's says the same to B. `clusterProcessGossipSection`
merges these, and when a node sees that a **majority of primaries** (here 2 of 3: itself + one
other) report A as PFAIL, `markNodeAsFailingIfNeeded` (`cluster_legacy.c:2622`) promotes A to
**FAIL** and broadcasts it. Now it's official cluster-wide. This quorum is why one node's bad
link couldn't have triggered this alone.

**T+delay — the election.** A′ sees its primary A marked FAIL and enters
`clusterHandleReplicaFailover` (`cluster_legacy.c:5691`). It waits a short delay — small because
A′ was fully caught up (a laggy replica would wait longer and likely lose) — then broadcasts a
`FAILOVER_AUTH_REQUEST` at a **new config epoch**, say epoch 7 (higher than any current). B and
C each check: have I voted in epoch 7 yet? No → grant one vote each. A′ collects 2 votes, a
majority of the 3 primaries.

**T+claim — the handoff.** Having won, A′:
- promotes itself from replica to primary,
- claims A's slots 0–5460 in its own slot bitmap,
- stamps them with config epoch 7,
- and broadcasts the new configuration on the bus.

Every node that hears it applies the **higher-epoch** claim, overwriting A's old ownership of
0–5460 unconditionally (the config-epoch tiebreak). Clients still holding a stale slot map and
sending slot-0 keys to A′... which now owns them, so no redirect — or to B/C, which reply
`-MOVED 0 A′:port`, and the client updates its map.

**T+recovery — A comes back.** When A's machine reboots and rejoins, it still believes it owns
0–5460 at its *old, lower* config epoch. It hears A′'s claim at epoch 7, loses the tiebreak,
and reconfigures itself as a **replica of A′**. The cluster has healed with no operator action.

**What was lost.** If A had acknowledged a write to a client and died *before* that write
reached A′, it's gone — A′ was elected without it, and A discarded it on rejoining. That's not
a bug in the protocol; it's the asynchronous-replication tradeoff, stated next.

## The honest caveat

This is **not** Raft or Paxos, and it doesn't claim to be. Because replication is asynchronous
(chapter 07), a failover **can lose acknowledged writes**: a primary can ack a write, die
before propagating it, and a replica lacking that write can still be elected — exactly the
"what was lost" step above. The cluster protocol provides *availability and automatic
recovery*, not linearizability. If your requirement is "no acknowledged write is ever lost,"
Valkey Cluster alone does not provide it. Understanding this tradeoff is worth more than
memorizing the packet format.

## Try it yourself

On a running cluster (or `utils/create-cluster/create-cluster start`), map the abstractions to
observable state:

```
valkey-cli -p 7000 cluster keyslot "{user1}:profile"    # CRC16 → slot; keyHashSlot in action
valkey-cli -p 7000 cluster keyslot "{user1}:sessions"   # same slot — proves the hash tag
valkey-cli -p 7000 cluster nodes                         # slot ranges, epochs, flags per node
valkey-cli -p 7000 -c set "{user1}:x" 1                  # -c follows a MOVED; drop -c to see it
```

Then watch the worked example happen live: freeze a *primary* with
`valkey-cli -p 7000 debug sleep 30`, and in another shell run
`watch valkey-cli -p 7001 cluster nodes`. You'll see the frozen node gain `fail?` (PFAIL),
then `fail` (FAIL) once a quorum agrees, then one of its replicas flip to `master` with a
**higher config epoch** — the exact PFAIL → FAIL → election → epoch-bump sequence above, right
there in the node-flags and epoch columns.

## Read next

`design-docs/atomic-slot-migration.md` — now that you know how slot ownership is gossiped and
how config epochs resolve conflicts, the migration state machine will make sense.
