# Personal Learning Notes — Valkey Internals

**These are NOT design docs. Do not upstream them. Do not put them in `design-docs/`.**

The `design-docs/` folder in this repo has a specific meaning: maintainer-reviewed
documents, issue-first, committed alongside the feature they describe. Its README
explicitly warns that overdocumentation leads to stale docs. These notes deliberately
live outside that folder because they are learning material for one person, written
after the fact by reading the code.

## What these are

Orientation notes for someone new to the Valkey codebase. Each note explains one
subsystem: what it does, the mental model, and — most importantly — **where to look
in the code**, with `file:line` anchors that were verified against this checkout at
the time of writing.

## How to use them

Read a note, then open the code it points at. The notes are a map, not a substitute
for the territory. Line numbers drift; if an anchor looks wrong, grep for the
function name.

## Reading order

| # | Note | Why |
|---|------|-----|
| 00 | [Orientation](00-orientation.md) | Map of `src/`, how to read a big C codebase |
| 01 | [Server lifecycle & event loop](01-server-lifecycle-and-event-loop.md) | The heartbeat everything else hangs off |
| 02 | [Command execution path](02-command-execution-path.md) | Follow one `GET` from socket to reply |
| 03 | [Clients & networking](03-clients-and-networking.md) | The `client` struct, buffers, reply flow |
| 04 | [Keyspace & data model](04-keyspace-and-data-model.md) | `robj`, `serverDb`, `kvstore`, `hashtable` |
| 05 | [Expiration & eviction](05-expiration-and-eviction.md) | Lazy vs active expiry, maxmemory |
| 06 | [Persistence: RDB & AOF](06-persistence-rdb-aof.md) | Fork-based snapshots, the AOF manifest |
| 07 | [Replication](07-replication.md) | PSYNC, full vs partial, dual-channel |
| 08 | [Cluster bus & failover](08-cluster-bus-and-failover.md) | Gossip, slot ownership, redirects |

## Proposals

Not learning notes, and not `design-docs/` material either (that folder is issue-first
and maintainer-reviewed). These are pre-issue drafts — the thing you write to decide
whether an issue is worth opening.

- [Dragonfly-inspired performance directions](proposal-dragonfly-inspired-perf.md) —
  what Dragonfly does, what Valkey already has, and a ranked plan.
- [Slot-per-thread command execution](proposal-slot-per-thread.md) — detailed design
  for the shard-per-thread stage.
- [Adopting Dashtable in Valkey](proposal-dashtable-adoption.md) — how to bring Dash's
  version stamps + fork-less snapshot into `hashtable.c` without a full port.
- [Fork-less RDB](proposal-forkless-rdb.md) — producing RDB / diskless full sync without
  `fork()`, on top of the version-stamp primitive.
- [Stage 0 measurement plan](proposal-stage0-measurement.md) — the benchmark that gates
  the roadmap: execution-bound (slot-per-thread) vs I/O-bound (io_uring).
- [VLL-style transactions](proposal-vll-transactions.md) — how Dragonfly's Very Lightweight
  Locking works, and how it could replace slot-per-thread's escalation barrier (step 7).
- [Per-slot memory tracking & memory-aware rebalancing](proposal-memory-aware-rebalance.md) —
  extend `CLUSTER SLOT-STATS` with per-slot bytes and rebalance by memory, not slot count.
  Independent of the threading work — the most immediately actionable idea here.

## External reference

Not a Valkey-code note (no `file:line` anchors), not a proposal — a written-up study of
another system's design, kept here because the proposals depend on it.

- [Dragonfly's forkless snapshot model](09-dragonfly-snapshot-model.md) — the exact
  version-stamp + serialize-before-mutate rules a Dashtable port would have to
  replicate for `BGSAVE`/full-sync consistency.

## Already documented upstream — read these instead of writing notes

Two subsystems already have real design docs. They are good. Do not duplicate them:

- **I/O threads** → `design-docs/io-threads.md`
- **Atomic slot migration** → `design-docs/atomic-slot-migration.md`

Notes 01–08 are written to *complement* those two, not overlap them.

## Confidence

Everything with a `file:line` anchor was checked against the code. Anything I inferred
rather than verified is marked **(inferred)**. If a note makes a claim with no anchor
and no marker, treat it as a summary I believed but did not prove — verify before you
rely on it.
