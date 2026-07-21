# Valkey Internals — a book about how Valkey works

**These are NOT design docs. Do not upstream them. Do not put them in `design-docs/`.**

The `design-docs/` folder in this repo has a specific meaning: maintainer-reviewed
documents, issue-first, committed alongside the feature they describe. Its README
explicitly warns that overdocumentation leads to stale docs. These chapters deliberately
live outside that folder because they are learning material, written after the fact by
reading the code.

## What these are

Book chapters for Valkey users and software engineers who want to understand how Valkey
works internally. Each chapter explains one subsystem in depth: the problem it solves, the
mechanism (byte layouts, algorithms, and design tradeoffs spelled out inline), and an
end-to-end **worked example** that follows one operation all the way through. The goal is
that after reading a chapter you understand the subsystem **without needing to open the
source** — though every claim is anchored to a `file:line` reference (verified against this
checkout at authoring time) for when you want to go deeper.

## How to use them

Read a chapter start to finish; it stands on its own. The `file:line` anchors are there if
you want to drop into the code, not homework you must do to follow along. Line numbers
drift; if an anchor looks wrong, grep for the function name.

**Prefer one file?** [`valkey-internals.md`](valkey-internals.md) is chapters 00–09
concatenated into a single searchable document (with a table of contents). Regenerate it
after editing any chapter by running [`build-combined.sh`](build-combined.sh).

## Reading order

| # | Chapter | Why |
|---|---------|-----|
| 00 | [Orientation](00-orientation.md) | Map of `src/`, how to read a big C codebase |
| 01 | [Server lifecycle & event loop](01-server-lifecycle-and-event-loop.md) | The heartbeat everything else hangs off |
| 02 | [Command execution path](02-command-execution-path.md) | Follow one `GET` from socket to reply |
| 03 | [Clients & networking](03-clients-and-networking.md) | The `client` struct, buffers, reply flow |
| 04 | [Keyspace & data model](04-keyspace-and-data-model.md) | `robj`, `serverDb`, `kvstore`, `hashtable` |
| 05 | [Expiration & eviction](05-expiration-and-eviction.md) | Lazy vs active expiry, maxmemory |
| 06 | [Persistence: RDB & AOF](06-persistence-rdb-aof.md) | Fork-based snapshots, the AOF manifest |
| 07 | [Replication](07-replication.md) | PSYNC, full vs partial, dual-channel |
| 08 | [Cluster bus & failover](08-cluster-bus-and-failover.md) | Gossip, slot ownership, redirects |
| 09 | [Threading & the I/O model](09-threading-and-io-model.md) | I/O worker pool, BIO threads, what stays single-threaded |

## Proposals

Not learning notes, and not `design-docs/` material either (that folder is issue-first
and maintainer-reviewed). These are pre-issue drafts — the thing you write to decide
whether an issue is worth opening.

- [Dragonfly-inspired performance directions](proposal-dragonfly-inspired-perf.md) —
  what Dragonfly does, what Valkey already has, and a ranked plan.
- [Slot-per-thread command execution](proposal-slot-per-thread.md) — detailed design
  for the shard-per-thread stage.
  - [Phase 2: `slot_to_shard[]` + `shard-threads`](proposal-slot-per-thread-phase2.md) —
    the ownership map + config as a provable no-op refactor.
  - [Phase 4: dispatch branch + REMOTE continuation](proposal-slot-per-thread-phase4.md) —
    the LOCAL/REMOTE/BARRIER branch point, mapping the cross-thread hop onto the blocking
    framework (`BLOCKED_SHARD`).
- [Read-only command execution on I/O threads](proposal-readonly-io-execution.md) — opens
  with a review of the `issue-2022` branch (why per-batch fork/join is structurally capped:
  the phases never overlap, and `prefetch-batch-max-size` is 16), then designs the
  alternative — persistent slot ownership + socket-less executor clients + `BLOCKED_IO_EXEC`,
  no per-thread event loops, barriers at cron rate rather than batch rate.
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
- [Main-thread CPU-time distribution](proposal-mainthread-cpu-distribution.md) — extend the
  existing event-loop duration buckets with I/O + idle so `INFO` answers Stage 0's
  execution-vs-I/O question in production, no profiler needed.
- [An io_uring I/O backend](proposal-io-uring-backend.md) — the D5 / I/O-bound branch of
  Stage 0: what io_uring would and wouldn't buy Valkey, the readiness→completion retrofit
  cost, and why it's a bounded, kernel-gated optional backend, not a transformation.
- [Tiered storage (SSD value offload)](proposal-tiered-storage.md) — Dragonfly-style tiering:
  keep keys in RAM, offload cold value bytes to SSD, fault them back in via the existing
  postpone/re-dispatch path. The `objectGetVal` seam makes the plumbing small; the SSD
  allocator is the real project.

## External reference

Not a Valkey-code note (no `file:line` anchors), not a proposal — a written-up study of
another system's design, kept here because the proposals depend on it.

- [Dragonfly's forkless snapshot model](10-dragonfly-snapshot-model.md) — the exact
  version-stamp + serialize-before-mutate rules a Dashtable port would have to
  replicate for `BGSAVE`/full-sync consistency.

## Already documented upstream — the authoritative specs

Two subsystems also have real, maintainer-reviewed design docs. Those are the *specs*; the
book chapters are the *narrative* that complements them, not a substitute:

- **I/O threads** → `design-docs/io-threads.md` (spec) / [chapter 09](09-threading-and-io-model.md) (narrative)
- **Atomic slot migration** → `design-docs/atomic-slot-migration.md`

Chapters 01–09 are written to *complement* those, not overlap them.

## Confidence

Everything with a `file:line` anchor was checked against the code. Anything inferred
rather than verified is marked **(inferred)**. If a chapter makes a claim with no anchor
and no marker, treat it as a summary believed but not proven — verify before you
rely on it.
