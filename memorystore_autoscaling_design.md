# Native Autoscaling for a Managed Cache Service: A Design Proposal

> **Sources.** This document is compiled entirely from publicly available
> documentation and the public open-source Memorystore Cluster Autoscaler. It
> describes a proposed design, not any vendor's committed roadmap, internal
> architecture, or pricing. Nothing here reflects unannounced product plans.

See also [memorystore_serverless_design.md](memorystore_serverless_design.md),
which argues that the controller proposed here is the third of three layers a
serverless cache requires, and treats the two beneath it.

## 1. Problem

Managed cache services are typically provisioned by capacity: the operator picks
a node size and a shard count, and revises them by hand as load changes. Cache
workloads are rarely that stable. Traffic is diurnal, promotional events are
spiky, and the cost of over-provisioning is paid continuously while the cost of
under-provisioning arrives all at once, as evictions or as an out-of-memory
condition.

Google Cloud Memorystore does not currently expose native autoscaling in its
API. Scaling is performed manually via the Console, CLI, or API. Customers who
want automatic scaling deploy the
[Memorystore Cluster Autoscaler](https://github.com/GoogleCloudPlatform/memorystore-cluster-autoscaler),
an open-source companion tool that they must host and operate themselves.

This document proposes what native, service-side autoscaling would need to look
like.

## 2. Landscape

### AWS ElastiCache

Two distinct models, both publicly documented:

- **ElastiCache Serverless** abstracts topology entirely, behind a proxy layer:
  a set of proxy nodes behind a network load balancer, presenting a single
  endpoint. Decoupling clients from the underlying shards is what permits data
  to be redistributed without application disconnects. It monitors compute
  (measured in ElastiCache Processing Units), stored data, and network
  throughput, and scales continuously. Operators bound cost by setting maximum
  storage and ECPU limits; reaching the storage limit triggers eviction or OOM
  behavior, and reaching the ECPU limit triggers throttling. Minimums can be set
  to pre-warm ahead of anticipated spikes.
- **Node-based clusters** use
  [Application Auto Scaling](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoScaling.html)
  with target-tracking policies over CloudWatch metrics, adding or removing
  shards and replicas within operator-defined bounds. Clients here connect
  directly to nodes, so a reshard is client-visible, and the scale-in path is
  hedged accordingly: a 25% deadband below target, a 600-second default
  cooldown, a refusal to read missing metric data as low utilization, scale-out
  if *any* policy agrees but scale-in only if *all* do, and two hard refusals —
  no shard removal when a slot holds an item larger than 256 MB
  post-serialization, and none when the resultant configuration lacks memory.
  AWS's own best-practices page recommends starting with scale-in **disabled**.

### Azure

Azure Managed Redis and Azure Cache for Redis scale manually, and Microsoft
[explicitly recommends against automating it](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/cache-best-practices-scale),
on the grounds that a scale operation itself consumes cache resources — so
triggering one automatically under heavy load risks degrading the very
availability it was meant to protect.

The tempting reading is that this is an objection to *reactive* autoscaling,
answerable with cooldowns and headroom. The evidence does not support it.

Azure Managed Redis runs on the Redis Enterprise stack, in which a per-node
proxy process already hides shard topology from clients — the same decoupling
ElastiCache Serverless buys with its proxy fleet. Microsoft has the hard part
solved. And the
[architecture page](https://learn.microsoft.com/en-us/azure/redis/architecture)
states flatly: *"Scaling down isn't currently supported on Azure Managed
Redis."* Not "not automatically." Not at all — not manually, not on a schedule,
not with unlimited cooldown.

So the objection is to scale-down *as such*, to the data movement, and not to
its reactivity. AWS reaches the same place from the other direction: it ships
scale-in and then hedges it with a deadband, a cooldown, two hard refusals, and
documented advice to disable it.

Two vendors, both with the topology problem solved, both declining to treat
shrinking as routine. That is the single most important input to this design,
and it relocates the difficulty rather than dissolving it: **scale-in safety is
the center of this proposal, not an appendix to it.** §5 treats it directly.

### Memorystore Cluster Autoscaler (open source)

The existing tool is a decoupled control loop:

```
   ┌────────┐  metrics   ┌────────┐  scale op   ┌──────────────┐
   │ Poller │ ─────────► │ Scaler │ ──────────► │ Memorystore  │
   └────────┘            └────────┘             │     API      │
   Cloud Monitoring    threshold compare        └──────────────┘
```

It works, and its shape is the right one. Its limitations are operational rather
than architectural: the customer deploys it (Cloud Run or GKE), pays for the
compute it consumes, and maintains its Terraform.

## 3. Goals

- **Fully managed.** No customer-deployed infrastructure.
- **Zero-downtime.** Scaling must not drop connections or lose data. Memorystore
  clusters already support zero-downtime scaling; autoscaling should drive that
  existing primitive rather than introduce a new one.
- **Target tracking.** Policies expressed against utilization metrics — memory
  and CPU — with a target value, not hand-written threshold ladders.
- **Bounded.** Operator-defined minimum and maximum capacity. An autoscaler
  without a ceiling is a billing incident.
- **Damped.** Cooldown periods after each scaling event, to prevent oscillation.
- **Safe to scale in.** See §5.

## 4. Architecture

Move the poller/scaler loop from customer infrastructure into the service
control plane, and express the policy as a resource on the instance.

```
  ┌──────────────────────────────────────────────────┐
  │  API layer (stateless, request-driven)           │
  │   • validates and persists the autoscaling policy│
  │   • runs no background loops                     │
  └───────────────────────┬──────────────────────────┘
                          │ policy
                          ▼
  ┌──────────────────────────────────────────────────┐
  │  Control-plane controller (background, stateful) │
  │   • samples utilization metrics on an interval   │
  │   • evaluates target-tracking policy             │
  │   • enforces bounds, cooldowns, safety checks    │
  │   • drives the existing scaling operation        │
  └───────────────────────┬──────────────────────────┘
                          ▼
                   data-plane scaling
```

The split matters: the API layer is stateless and request-driven, so a
continuous control loop cannot live there. The controller owns the loop, the
cooldown state, and the decision to act.

The policy itself is a declarative object attached to the instance — target
metric, target value, min and max capacity, cooldown — created and updated
through the service's normal resource-update path. Concrete API surface is out
of scope here and belongs in an API review.

## 5. Safety Constraints

Scale-in is the dangerous direction. Scale-out costs money; scale-in costs data.

**A scale-in must be refused when the target capacity cannot hold the working
set.** The obvious formulation of that check — compare target capacity against
`used_memory` — is wrong, and the error is worth stating precisely, because it
is the same accounting gap that causes OOM kills in the engine itself:

> `used_memory` reports live allocations. The kernel terminates processes on
> RSS. Between them sit allocator fragmentation, replication buffers, the AOF
> buffer, client input and output buffers, and copy-on-write pages duplicated
> during any snapshot the scaling operation itself triggers.

A scale-in sized against `used_memory` can therefore land a node that is
comfortably within its logical limit and still be OOM-killed. The guard must be
sized against RSS plus expected transient overhead — including the copy-on-write
cost of the data movement the scale-in performs.

This is not hypothetical, and the engine will not save us.
[Atomic slot migration](design-docs/atomic-slot-migration.md) already rolls a
migration back when "out of memory error occurs on the target node." But an
out-of-memory *error* is a `maxmemory` violation — a `used_memory` accounting
event — while the kernel kills on RSS. If a target's RSS crosses the container
limit before its `used_memory` crosses `maxmemory`, there is no rollback. There
is a dead primary and a failover. The engine's own guard fires on the wrong
signal, so the control plane cannot delegate scale-in safety to it.

The guard is therefore two guards:

- **Pre-flight admission**, in the controller: refuse the scale-in unless the
  resultant configuration holds RSS plus expected transient overhead, including
  the copy-on-write cost of the migration the scale-in itself forks. ElastiCache
  implements the same idea, refusing to remove shards "if insufficient memory
  available on resultant shard configuration."
- **In-flight rollback**, in the engine: ASM's existing trigger, which must be
  calibrated so the `maxmemory` guard fires *before* the OOM killer does.

Azure ships one public calibration of the second: "approximately 20% of the
available memory is reserved as a buffer for noncache operations, such as
replication during failover and active geo-replication buffer." Whether 20% is
the right number is arguable. That a major vendor reserves a fifth of memory for
exactly the terms enumerated above is the strongest public evidence that the gap
is real and large.

Additional constraints:

- **Cooldown after every operation**, scale-out and scale-in alike, sized to
  exceed the duration of the operation itself.
- **Asymmetric aggressiveness.** Scale out quickly, scale in slowly. The cost of
  a late scale-out is latency; the cost of a hasty scale-in is eviction.
- **Do not scale under duress.** A scale operation competes with serving traffic,
  which is the narrow form of Microsoft's warning in §2. The controller should
  act on a *leading* signal — sustained utilization above target — rather than
  waiting for saturation, which is what a target-tracking policy with adequate
  headroom provides. Note that this answers the warning only for scale-*out*.
  The broader objection §2 identifies, that scale-down's data movement is
  dangerous regardless of when it is triggered, is answered by the two guards
  above and not by leading signals.

## 6. Open Questions

- Which metric is primary? Memory utilization is the safer trigger; CPU is the
  more responsive one. Tracking both requires a conflict rule when they disagree.
- Does scale-in ever run unattended, or does it default to opt-in? Given the
  asymmetry in §5, defaulting scale-in to off is defensible.
- How does the controller behave during an in-progress maintenance or failover?
  It must yield rather than queue.
