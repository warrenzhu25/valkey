# Native Autoscaling for a Managed Cache Service: A Design Proposal

> **Sources.** Compiled from public documentation and the open-source Memorystore
> Cluster Autoscaler. A proposed design, not any vendor's roadmap, internal
> architecture, or pricing.

See also [memorystore_serverless_design.md](memorystore_serverless_design.md):
the controller proposed here is the third of three layers a serverless cache
needs, and that doc covers the two beneath it.

## 1. Problem

A managed cache is provisioned by capacity: the operator picks a node size and
shard count and revises them by hand. Cache load is rarely stable — diurnal
traffic, spiky events — and the costs are asymmetric. Over-provisioning is paid
continuously; under-provisioning arrives at once, as evictions or OOM.

Memorystore has no native autoscaling. Customers who want it deploy the
[Memorystore Cluster Autoscaler](https://github.com/GoogleCloudPlatform/memorystore-cluster-autoscaler),
an open-source tool they host and operate themselves. This document proposes
service-side autoscaling to replace it.

## 2. Landscape

**AWS ElastiCache** ships two models. *Serverless* hides topology behind a proxy
fleet and scales compute (ECPU), memory, and network continuously; operators cap
cost with max storage and ECPU limits. *Node-based clusters* use Application Auto
Scaling with target-tracking policies over CloudWatch metrics. Clients connect
directly to nodes, so a reshard is client-visible, and scale-in is hedged: a 25%
deadband below target, a 600s cooldown, no scale-in on missing data, scale-out
if *any* policy agrees but scale-in only if *all* do, and two hard refusals — no
shard removal when a slot holds an item over 256 MB post-serialization, or when
the resultant configuration lacks memory. AWS advises starting with scale-in
**disabled**.

**Azure** does not autoscale and
[recommends against it](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/cache-best-practices-scale),
because a scale operation itself consumes cache resources. This looks like an
objection to *reactive* scaling, but the evidence says otherwise: Azure Managed
Redis runs on Redis Enterprise, whose per-node proxy already hides topology — the
decoupling ElastiCache Serverless pays for — and the
[architecture page](https://learn.microsoft.com/en-us/azure/redis/architecture)
still states *"Scaling down isn't currently supported."* Not manually, not on a
schedule. So the objection is to scale-down's data movement itself, not its
timing. AWS reaches the same place from the other side: it ships scale-in, then
hedges it heavily.

Two vendors with the topology problem solved, both declining to treat shrinking
as routine. **Scale-in safety is the center of this design (§5), not an
appendix.**

The existing Memorystore Cluster Autoscaler is a poller → scaler → API control
loop. Its shape is right; its limits are operational — the customer hosts it,
pays for its compute, and maintains its Terraform.

## 3. Goals

- **Fully managed.** No customer-deployed infrastructure.
- **Zero-downtime.** No dropped connections, no lost data. Drive Memorystore's
  existing zero-downtime scaling primitive, don't add a new one.
- **Target tracking.** Utilization metrics (memory, CPU) with a target value,
  not threshold ladders.
- **Bounded.** Operator-defined min and max capacity.
- **Damped.** Cooldowns after each event, to prevent oscillation.
- **Safe to scale in (§5).**

## 4. Architecture

Move the poller/scaler loop into the service control plane; express the policy
as a resource on the instance.

```
  ┌──────────────────────────────────────────────────┐
  │  API layer (stateless, request-driven)           │
  │   • validates and persists the policy            │
  │   • runs no background loops                     │
  └───────────────────────┬──────────────────────────┘
                          │ policy
                          ▼
  ┌──────────────────────────────────────────────────┐
  │  Controller (background, stateful)               │
  │   • samples utilization metrics                  │
  │   • evaluates target-tracking policy             │
  │   • enforces bounds, cooldowns, safety checks    │
  │   • drives the existing scaling operation        │
  └───────────────────────┬──────────────────────────┘
                          ▼
                   data-plane scaling
```

The API layer is stateless, so the control loop cannot live there. The
controller owns the loop, the cooldown state, and the decision to act. The policy
is a declarative object on the instance — target metric, target value, min/max
capacity, cooldown. Concrete API surface belongs in an API review.

### 4.1 Trigger conditions

Given a target `T` and a deadband `d` (AWS uses `d = 0.25`), the controller
compares each utilization signal `u` against two thresholds:

| Direction | Condition | Sustain window |
|---|---|---|
| **Scale out** | `u ≥ T` | short (~1–3 min) |
| **Hold** | `T·(1−d) < u < T` | — |
| **Scale in** | `u ≤ T·(1−d)` *and* all §6.2 gates pass | long (~15–30 min) |

With `T = 0.70, d = 0.25`, scale-out fires at 70% and scale-in only below 52.5%.
The gap between the thresholds is what prevents oscillation; it is not optional.
The asymmetry is applied three ways at once — threshold (deadband), sustain
window (out reacts in minutes, in waits tens of minutes), and cooldown (scale-in
cooldown ≫ scale-out cooldown).

**The two directions watch different signals.**

- **Scale out** on the *worst* of several signals (logical OR — any one fires):
  - `used_memory_rss / maxmemory ≥ T_mem` — the urgent one; a late scale-out
    costs eviction or OOM.
  - primary engine-CPU `≥ T_cpu` — the single thread is saturated.
  - rising `evicted_keys` or rejected connections — *emergency* signals that
    bypass the sustain window.
- **Scale in** only when *all* signals agree it is safe (logical AND): memory and
  CPU both `≤ T·(1−d)`, sustained, plus the §6.2 feasibility and safety gate.

This out-OR / in-AND rule is ElastiCache's "scale out if any policy agrees, scale
in only if all do," generalized across metrics.

**Trigger on RSS, not `used_memory`.** For the memory signal use
`used_memory_rss / maxmemory`: RSS is what the kernel kills on and what bounds the
node. Triggering on `used_memory` alone lets fragmentation and buffers drive RSS
to the limit while the logical signal still reads safe — an OOM at "60%
utilization." (§5 develops the same RSS-vs-`used_memory` gap for the scale-in
guard.)

**Direction-neutral blocks.** Neither direction fires while a scale operation,
failover, or maintenance is in flight (yield, do not queue), before the relevant
cooldown elapses, or on missing data — `INSUFFICIENT_DATA` may permit scale-out
but never scale-in.

**Scale-out is not instant.** An ASM-based scale-out completes in minutes; memory
can fill faster than that. The emergency signals above bypass the sustain window
but not the migration itself, so a fast-filling workload can evict or OOM before
the new capacity is live. This is a floor on what *reactive* memory scaling can
promise, and part of why a proxy-fronted serverless tier — the layer beneath this
one — absorbs a transient a node-based cluster cannot. The controller narrows the
gap, it does not close it: trigger scale-out early (a lower `T_mem` than a CPU
target would take), and let the min-capacity floor and the survivor headroom
(§6.6) carry the burst while new capacity lands.

**At the ceiling there is nothing left to do.** `max` bounds cost (§3), but a
workload that keeps climbing at `max` reaches the exact failure the controller
exists to prevent — eviction, then OOM — with no scaling action remaining. The
controller must not pretend otherwise: at `max` it stops scaling and escalates
(alert, surface the saturation), rather than silently absorbing the overrun.
Sizing `max` is therefore a capacity decision, not just a cost cap — set it above
the worst forecast burst, not at the budget. This ceiling is the reactive model's
boundary; the serverless tier's continuous scaling is the structural answer to it.

## 5. Safety

Scale-out costs money; scale-in costs data. **A scale-in must be refused when the
survivors cannot hold the working set** — and the obvious check, comparing target
capacity against `used_memory`, is wrong:

> `used_memory` reports live allocations. The kernel kills on RSS. Between them
> sit allocator fragmentation, replication buffers, the AOF buffer, client I/O
> buffers, and copy-on-write pages from the snapshot the scale-in itself forks.

Sized against `used_memory`, a scale-in can land a node inside its logical limit
and still be OOM-killed. The guard must be sized against RSS plus transient
overhead, including the copy-on-write cost of the migration.

The engine will not catch this for us.
[Atomic slot migration](design-docs/atomic-slot-migration.md) rolls back on
target out-of-memory — but an OOM *error* is a `maxmemory` violation, a
`used_memory` event, while the kernel kills on RSS. If RSS crosses the container
limit before `used_memory` crosses `maxmemory`, there is no rollback, only a dead
primary and a failover.

So the guard is two guards:

- **Pre-flight admission** (controller): refuse the scale-in unless the resultant
  configuration holds RSS plus transient overhead. ElastiCache does the same,
  refusing to remove shards "if insufficient memory available on resultant shard
  configuration."
- **In-flight rollback** (engine): ASM's existing trigger, calibrated so the
  `maxmemory` guard fires before the OOM killer.

Azure ships one public calibration: ~20% of memory reserved "for noncache
operations, such as replication during failover." Whether 20% is right is
arguable; that a vendor reserves a fifth of memory for these terms is strong
evidence the gap is real.

Two more constraints:

- **Cooldown after every operation**, sized to exceed the operation's duration.
- **Asymmetric aggressiveness.** Scale out fast, scale in slow: a late scale-out
  costs latency, a hasty scale-in costs data.

## 6. Scale-In

§5 gives the principle. This section gives the mechanism, and two current-engine
blockers that make automated scale-in unsafe to ship today.

### 6.1 Two blockers in the current engine

**The engine cannot measure what a slot holds.** The admission check asks "how
many bytes will a survivor absorb from this slot range?" and the engine cannot
answer. `CLUSTER SLOT-STATS`
([src/cluster_slot_stats.h](src/cluster_slot_stats.h)) exposes four per-slot
metrics — `key-count`, `cpu-usec`, `network-bytes-in`, `network-bytes-out` — and
no memory. The only proxy, `key-count × mean key size`, assumes uniform value
size, which fails on the skewed workloads where scale-in is dangerous: one large
collection in one slot throws it off by orders of magnitude. This is likely why
ElastiCache refuses to migrate slots holding items over 256 MB — a crude stand-in
for a measurement AWS also cannot take.

**The engine's memory guard is unreachable on a typical cache.** On the import
target, an OOM on a slot-migration client reaches
[src/server.c:4564](src/server.c) → `clusterHandleSlotMigrationClientOOM()`,
which rolls back. But that branch runs only when `performEvictions()` returns
`EVICT_FAIL`, and [src/evict.c:422](src/evict.c) returns `EVICT_FAIL` only under
`maxmemory-policy noeviction` (or `import-mode`). Under `allkeys-lru` or any other
eviction policy, it evicts and returns `EVICT_OK`.

So on a normally-configured cache the target does not roll back when it runs out
of room. It **evicts its own working set to make room for the incoming slots,
silently, and reports success.** Scale-in costs data — by eviction, not OOM. This
is why AWS documents that its capacity metric "works best with maxmemory-policy
set to noeviction." And even under `noeviction`, the guard fires on `used_memory`
while the kernel kills on RSS — the §5 gap.

### 6.2 When: three gates, all must pass

- **Eligibility.** Utilization sustained below target with a deadband (AWS: 25%),
  over a window longer than a migration takes. Missing data is not low
  utilization. Cooldown since the last operation, either direction, has elapsed.
- **Feasibility.** No migration, failover, or maintenance in flight — yield, do
  not queue. Cluster healthy, every surviving shard has its replicas. No slot in
  the drain set holds an item large enough to overflow the source's client output
  buffer (an ASM rollback trigger).
- **Safety.** Every survivor *and its replicas* can hold post-migration RSS plus
  overhead, with headroom (~20%). Per §6.1, not computable on the current engine.

### 6.3 What to remove: replicas before shards

Scale-in has two dimensions, an order of magnitude apart in cost:

- **Replicas.** A replica leaving moves no data; it deregisters. Near-instant,
  reversible, low-risk. Costs read capacity and one HA copy.
- **Shards.** Removing a shard migrates its slots to survivors — the ASM path,
  with all the overhead of §5 and §6.1.

Remove a replica when the pressure is on something replicas serve (replica CPU,
read throughput) and primary/memory headroom is ample. Remove a shard only when
aggregate memory or primary-CPU headroom justifies giving up a shard's capacity.
ElastiCache exposes these as separate scalable dimensions for this reason.

**Choosing which shard.** The unit is a whole shard — primary and its replicas
leave together. In order:

1. **Feasibility filter.** Exclude shards owning an oversized-item slot, or whose
   removal drops below the min shard count or collapses zone spread.
2. **Least data.** Among the rest, pick the shard holding the fewest bytes
   (memory once the metric exists; `key-count` today). Moves least, least CoW.
3. **Simulate placement.** Spread the victim's slots across *all* survivors and
   reject the candidate if any survivor or its replicas would breach the safety
   threshold or become a hot shard.
4. **Balance.** Prefer the removal leaving the evenest slot distribution.

The victim's slots must be **spread**, not dumped on the nearest survivor —
otherwise scale-in trades a lightly-loaded cluster for one hot shard, worse than
not scaling.

### 6.4 How: drain slots in batches

The unit of movement is a batch of slots, not a shard:

1. Select the victim shard (§6.3) and the target slot assignment.
2. Run the §6.2 safety gate against every survivor and its replicas.
3. Migrate *k* slots. Re-run the gate. Repeat.
4. When the victim owns zero slots, `CLUSTER FORGET` it and release the nodes.
5. On rollback, enter cooldown and surface it. Never retry immediately.

Batching bounds transient CoW and replication overhead to ~`k/16384` of the
dataset instead of a whole shard's worth, lets the safety gate be re-checked
mid-drain, and makes rollback cost one batch instead of the whole operation.
Draining a shard atomically maximizes the overhead §5 guards against.

### 6.5 Reactive out, scheduled in

Diurnal troughs are predictable, and AWS notes scheduled scaling suits
deterministic workloads while target tracking suits the rest. A scale-in at a
known nightly trough is not performed under duress, which answers Azure's
objection (§2) operationally. Reactive scale-in is the specific thing both
vendors decline to recommend. This makes the §5 asymmetry an operating rule, not
a tuning parameter.

### 6.6 Defaults

- Scale-in defaults to **off**.
- Deadband at least 25% below target.
- Scale-in cooldown greater than measured p99 migration duration, and much
  longer than the scale-out cooldown.
- 20% memory headroom on survivors.
- Automated scale-in **refuses to run unless the memory guard is reachable** —
  `noeviction`, or the target in `import-mode` for the job.

That last default uses an existing primitive. `import-mode`
([src/config.c:3392](src/config.c)) forbids eviction and expiration on a primary
and forces `EVICT_FAIL` alongside `noeviction`
([src/evict.c:422](src/evict.c)). Setting it on the import target for a job makes
the OOM rollback fire even on an `allkeys-lru` cache. Cost: it also pauses
expiration, so a long migration accumulates expired keys to reclaim later — a
better trade than silently evicting the working set.

### 6.7 Engine prerequisites

Two engine-side changes gate safe automated scale-in. Both are small and
upstreamable to Valkey; until they land, the controller should *recommend*
scale-in for an operator to run, not perform it — where AWS and Azure have in
effect landed.

1. **Add a `memory-bytes` metric to `CLUSTER SLOT-STATS`.** Without it the
   admission gate (§6.2) is guesswork.
2. **Make the memory guard reachable during import** — require `noeviction`, or
   have ASM set `import-mode` on the target for the job.

## 7. Open Questions

- Which metric is primary? Memory is the safer trigger, CPU the more responsive;
  tracking both needs a conflict rule.
- Is per-slot `memory-bytes` (§6.7) cheap enough to keep exact on the hot path,
  or must it be sampled — reintroducing the skew error of §6.1?
- Does a long `import-mode` drain (§6.6) accumulate enough expired-but-unreclaimed
  keys to distort the memory measurement the safety gate depends on?
- Does scheduled scale-in (§6.5) need an operator-declared trough, or should the
  controller learn the diurnal pattern?
