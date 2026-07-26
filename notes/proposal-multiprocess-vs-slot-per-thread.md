# Proposal — Multi-process vs. slot-per-thread on one many-core VM

**Status: pre-issue draft — a decision note, not a design.** The goal it serves: raise Valkey
throughput on a single large VM (16+ cores). There are two ways to use those cores, and one of
them ships today with zero engineering. This note compares them honestly, so the expensive one
is chosen for reasons that survive contact with an operator, not by default.

Companion to [proposal-slot-per-thread.md](proposal-slot-per-thread.md) (the expensive option,
in full detail) and [proposal-stage0-measurement.md](proposal-stage0-measurement.md) (the
measurement that gates it). This note adds the baseline that measurement is missing: **what
slot-per-thread has to beat.**

---

## 1. The two options

A single Valkey process is, for command execution, single-threaded: one thread runs `call()`.
On a 16-core VM that thread is one core, and the other 15 are idle for execution. Two ways to
fix it:

- **Multi-process (MP).** Run N Valkey instances on the one VM, in cluster mode, each owning a
  slice of the 16384 hash slots, each pinned to its own cores. **This works today. No code.**
- **Slot-per-thread (SPT).** One process, N execution threads, slots partitioned across
  threads. Does not exist; it is the multi-year build detailed in the companion note.

Both partition the keyspace by slot. Both reject cross-slot commands unless extra machinery
handles them (MP: never; SPT: via the escalation barrier). The difference is the **process
boundary** — and that boundary is the whole comparison.

## 2. The headline, stated plainly

**MP already gets the cores.** N pinned instances scale throughput near-linearly with N on a
uniform workload — that is what cluster mode is for, and running the cluster on one box instead
of N boxes changes nothing about the scaling.

So **SPT does not buy throughput over MP.** At the ceiling they are roughly a tie, and SPT will
in fact land slightly *below* a perfectly-partitioned MP run, because a client whose key lives
on another thread pays a cross-thread hop that a client connected to the right process does not.

What SPT buys is **lower operational cost and a shared memory pool.** The entire decision is
whether those are worth a multi-year build, and that depends on the deployment — which is why
this is a decision note and not a plan.

## 3. The comparison

| Axis | Multi-process (today) | Slot-per-thread (unbuilt) |
|---|---|---|
| **Throughput on 16 cores, uniform load** | Near-linear. The baseline | ≈ MP, minus the REMOTE-hop tax on misplaced clients |
| **Memory** | Hard-partitioned: each instance sized to RAM/N. **Skew wastes RAM** — one instance evicts/OOMs while others sit half-empty | **One shared pool.** No skew waste. SPT's single biggest structural win |
| **fork()/RDB blast radius** | A `BGSAVE` forks and stalls **one instance** (1/N of clients) | A fork stalls the **whole process** (all cores' clients) — until fork-less RDB |
| **RDB coordination** | N instances can fork *at once* → COW storm → OOM. Must stagger saves (see §4) | One save decision. Simpler to schedule |
| **Do instances interfere?** | Memory **isolated** (a wall per instance). CPU / L3 / memory-bandwidth / NIC / disk **shared** — needs pinning; a noisy instance still steals cache and bandwidth | Everything shared, including memory — **no per-workload memory wall**. A hot slot pegs one thread |
| **Fault isolation** | **Better.** One crash / bad Lua / OOM kills 1/N | Worse. One crash takes all threads |
| **Cross-slot `MGET`/`MULTI`/Lua** | Rejected forever (`-CROSSSLOT`) | Works, via the barrier (slower, but works) |
| **Control plane** | N configs, N monitoring targets, N replication streams, N RDB/AOF files, N cluster-bus members **gossiping per box** | One of each |
| **Client connections** | N× — a client pools to every instance | 1× |
| **Adding a core** | Reshard: migrate slots to a new instance | `shard-threads N` — config change |
| **Failover domain** | N primaries per box → **correlated failure**; replicas must live on *other* boxes, and slot placement must be box/rack-aware | One node per box |
| **Engineering cost** | **Zero** | Multi-year (companion note §10) |

Read the table as two columns of a single trade: MP trades **memory efficiency and a simple
control plane** for **isolation and zero cost**. SPT trades the reverse. This is the classic
processes-vs-threads dilemma, and Valkey does not get to escape it.

## 4. The RDB problem on one machine, concretely

The user named this, and it is the sharpest operational edge of MP, so spell it out.

**It is not a file conflict.** Each instance writes its own `dump.rdb`; give each a distinct
`dir` (or `dbfilename`) and the files never collide. That part is a config line.

**The real problem is coordinated forks.** `BGSAVE` (scheduled `save`, `bgrewriteaof`, or a
full-sync from a replica) calls `fork()`, and the child shares the parent's pages copy-on-write.
Under write load the parent dirties pages, each of which is then copied — so a saving instance's
RSS climbs toward 2× its dataset for the duration of the save. If **all N instances save at the
same time** — and the default `save` windows plus a synchronized replica attach make that
likely — the box's peak RSS spikes toward 2× *simultaneously across every instance*, and the VM
OOM-kills something. Plus N processes writing RDBs to the same disk at once saturate I/O, so
each save runs longer, so the COW window stays open longer.

**Mitigations, all operator effort:**
- Stagger `save` windows per instance, or drive saves from an external scheduler.
- Provision headroom for the worst simultaneous-save case, not the steady state — which *gives
  back* some of MP's memory-efficiency argument.
- Watch aggregate `current_cow_size` across instances, not per-instance.

**SPT's version of this problem is one fork, not N** — easier to schedule — but that one fork
stalls the whole 16-core instance at once, which is a worse single event even though there is
only one of it.

**The fix that helps both, and belongs first:** fork-less RDB
([proposal-forkless-rdb.md](proposal-forkless-rdb.md)) removes the fork entirely. It is
**independent of the threading work** and is the recommended first feature regardless of which
architecture wins here. It shrinks MP's fork-storm pain and simultaneously erases SPT's biggest
disadvantage vs. MP (the whole-instance stall) — so shipping it first makes the later
MP-vs-SPT decision cleaner and is never wasted.

## 5. "Do they affect each other?" — the isolation axis in detail

Two resources, opposite answers:

- **Memory: MP isolates, SPT shares.** Each MP instance's `maxmemory` is a hard wall — a
  workload that floods one instance evicts only its own keys, and cannot touch a neighbor's.
  SPT has one pool and no per-workload wall, so a single hot workload can consume the shared
  memory and force eviction of an unrelated workload's keys. **For multi-tenant or
  mixed-workload boxes, MP's isolation is a real safety property**, not just overhead.
- **CPU, cache, memory bandwidth, NIC, disk: both share, and only pinning helps.** Neither
  architecture gives a bandwidth or last-level-cache wall. Under MP you *must* pin instances to
  disjoint cores or they fight in the scheduler; even pinned, a scan-heavy instance thrashes the
  shared L3 and starves its neighbors' bandwidth. SPT has the same physics with one fewer thing
  to configure.

So the honest summary: **MP is the better isolation story (because of memory), SPT is the better
efficiency story (because of the shared pool).** Which one matters is a property of the
workload, not of Valkey.

## 6. When each wins

| If the target deployment… | Prefer |
|---|---|
| Runs a **predictable, uniform** workload where RAM/N is easy to size, and the ops org already runs cluster mode | **Multi-process, today.** SPT's shared pool buys little; do not build it for this |
| Has **unpredictable or skewed** memory across slots, so no static RAM/N split is safe | **SPT** — the shared pool is the win, and it is hard to get any other way |
| Needs **cross-slot commands** (`MGET`/`MSET` across keys, `MULTI`, Lua over multiple keys) | **SPT** — MP rejects these permanently |
| Wants the box to be **one logical node** — one config, one failover domain, one replication stream | **SPT** |
| Prioritizes **blast-radius isolation** and per-tenant memory walls | **Multi-process** |
| Is **latency-sensitive to tail** and cannot tolerate a cross-thread hop | **Multi-process** (no REMOTE tax) or SPT *with* connection migration (Phase 4a) |

Most single-tenant throughput goals land in the **first row**. That is the uncomfortable part
of this note: for the plain reading of "make Valkey faster on a big VM," MP is the pragmatic
answer and its pains are schedulable. SPT earns its cost on the *other* rows — memory skew,
cross-slot, single-node oper. The case for SPT should be made in those terms, because as a pure
throughput play it is competing with something free.

## 7. The measurement this implies — run it before building

[proposal-stage0-measurement.md](proposal-stage0-measurement.md) gates the whole roadmap on two
numbers but is **missing the MP baseline**, and that baseline is the cheapest and most decisive
experiment available:

1. **N-process baseline.** On the 16-core VM, launch 8–16 instances pinned to disjoint cores,
   drive each with a client hitting only its own key range, and sum throughput + record p99.
   No `perf`, no special hardware, no code — one shell script. This is the ceiling SPT must
   beat and the honest denominator for every future SPT benchmark.
2. **Single-instance today.** One instance, same box — the floor.
3. **The gap between them is the prize** SPT (or MP) is competing for. If (1) is comfortably
   linear and the operator is fine running N instances, the throughput case for SPT is settled
   *against* it before any threading code is written.

The N-process run also surfaces the §4 and §5 costs directly: trigger simultaneous `BGSAVE`s
and watch aggregate COW; run a skewed workload and watch one instance evict while others idle.
Those are the operational numbers that decide row-by-row in §6.

## 8. Recommendation

1. **Run the N-process baseline first** (§7.1). It is free and it may end the discussion.
2. **Ship fork-less RDB regardless** (§4) — it improves MP now and de-risks SPT later.
3. **Only commit to slot-per-thread if the deployment sits in §6's non-throughput rows** —
   memory skew, cross-slot needs, or single-node operation — *and* Stage 0 confirms the serving
   thread is execution-bound (companion note §12: if it is I/O-bound, neither MP nor SPT is the
   lever — io_uring is).

The one-line version: **multi-process is the throughput answer that already exists; build
slot-per-thread for the operations and the shared memory pool, or do not build it.**

## 9. This belongs in the kill criteria

The companion note's §12 ("What would make me abandon this") lists three stop conditions —
Amdahl ceiling, the ordering model, hot slots. It should list a fourth, which on the reasoning
above is the *most likely* reason to stop and the cheapest to test:

> **N processes on one VM are operationally acceptable for the target deployment.** If the
> memory-skew waste is tolerable, cross-slot commands are not needed, and staggered saves are
> manageable, then MP already delivers the throughput and slot-per-thread's years buy only
> convenience. Prove this false — with the §7 baseline and a real statement of the deployment's
> memory and cross-slot needs — before building.

## 10. References

- The option this weighs against: [proposal-slot-per-thread.md](proposal-slot-per-thread.md)
  (§11 honest risks, §12 abandonment criteria — this note proposes a fourth).
- The gating measurement, to be extended with §7's baseline:
  [proposal-stage0-measurement.md](proposal-stage0-measurement.md).
- The fork fix that helps both architectures:
  [proposal-forkless-rdb.md](proposal-forkless-rdb.md).
- Cross-slot handling in SPT: slot-per-thread §6 (the escalation barrier).
- Why I/O-bound would beat both: [proposal-io-uring-backend.md](proposal-io-uring-backend.md).
