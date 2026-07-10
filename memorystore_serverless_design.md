# A Serverless Managed Cache: A Design Proposal

> **Sources.** This document is compiled entirely from publicly available
> documentation, published vendor benchmarks, and the Valkey source tree it
> lives in. It describes a proposed design, not any vendor's committed roadmap,
> internal architecture, or pricing. Nothing here reflects unannounced product
> plans.

Companion to [memorystore_autoscaling_design.md](memorystore_autoscaling_design.md),
which proposes native autoscaling for a capacity-provisioned cache. This
document argues that autoscaling is the *third* of three layers a serverless
cache requires, and that the two beneath it are where the design is decided.

## 1. Problem

A capacity-provisioned cache asks the operator two questions — node size and
shard count — and an autoscaler answers them automatically. A serverless cache
does not ask them at all. That sounds like the same product with the knobs
hidden. It is not.

Serverless makes exactly one promise:

> The provider may change the topology at any moment, unilaterally, and the
> application will not notice.

Every other property that gets marketed as serverless — pay-per-use, no capacity
planning, continuous scaling, scale-to-zero — is downstream of that promise. An
autoscaler that resharded a cluster while clients held direct connections to its
nodes would deliver topology changes straight into the application's error
budget. So the promise is not a feature of the autoscaler. It is a precondition
for having one that can run continuously.

## 2. What the promise costs

The promise decomposes into three layers, in dependency order:

```
  ┌────────────────────────────────────────────────────────┐
  │ 3. Capacity controller                                 │
  │    target tracking, bounds, cooldowns, safety guards   │
  │    (this is memorystore_autoscaling_design.md)         │
  └────────────────────────┬───────────────────────────────┘
                           │ requires
  ┌────────────────────────▼───────────────────────────────┐
  │ 2. Metering and ceilings                               │
  │    a billing unit observable without a node count      │
  └────────────────────────┬───────────────────────────────┘
                           │ requires
  ┌────────────────────────▼───────────────────────────────┐
  │ 1. Decoupling layer                                    │
  │    topology is invisible to the client                 │
  └────────────────────────────────────────────────────────┘
```

Layer 1 is the promise. Layer 2 is what makes it sellable, because you cannot
bill for nodes in a product whose premise is that the customer never sees nodes.
Layer 3 is the existing autoscaling proposal, which becomes both easier (the
client cannot observe a reshard) and harder (there are no operator-set bounds to
hide behind) once the layers below it exist.

## 3. Landscape

Three strategies exist in the market. The choice cascades into everything else.

### 3.1 Proxy over an unmodified engine

**Redis Enterprise** reached this design first. A proxy process runs on every
node — multi-threaded, C, cut-through, stateless — and
[consolidates client requests into inner pipelines](https://redis.io/blog/redis-enterprise-proxy/),
multiplexing thousands of client connections onto persistent shard connections.
Resharding (moving half the hash slots to a new shard) and shard failover are
invisible; clients keep the same endpoint. Whole-node failure is the exception:
clients disconnect and reconnect, and the endpoint is reassigned to a surviving
node's proxy. Published benchmarks put p50 at 0.18–0.41 ms across 2 to 32
shards at 2000 connections.

**ElastiCache Serverless** does the same with different topology — a separate
proxy fleet behind a network load balancer, rather than a proxy co-resident with
the shards. AWS states the purpose directly: "The proxy layer abstracts the
cluster topology and nodes from your client. This enables ElastiCache to
intelligently load balance, scale out and add new cache nodes, replace cache
nodes when they fail, and update software on the cache nodes, all without
availability impact to your application or having to reset connections."

**Azure Managed Redis** runs the Redis Enterprise stack and therefore the same
proxy, but is uniquely useful because it exposes both modes as a customer-facing
choice and documents the trade-off. Under *OSS clustering policy* clients speak
the Cluster API directly to shards — lowest latency, best throughput, no proxy
in the data path. Under *Enterprise clustering policy* a single endpoint routes
through a proxy node, and Microsoft notes plainly that "the single node proxy
can be a bottleneck in either compute utilization or network throughput."

The cost of the abstraction is also published. Under Enterprise clustering
policy the only multi-key commands permitted across slots are `DEL`, `MSET`,
`MGET`, `EXISTS`, `UNLINK`, and `TOUCH`. Everything else returns `CROSSSLOT`.
**A proxy design must decide which commands it refuses, on day one.**

### 3.2 Clean-sheet engine, or a new protocol

**Momento** built a cache engine from scratch rather than wrapping Redis, and is
explicit that this was the point: a new design permitted genuine multi-tenancy
with different economics, and both shared and dedicated tenancy per customer.
With no single-threaded topology-aware engine to hide, there is no proxy to
write.

**Upstash** keeps the Redis protocol but adds an HTTP/REST interface. That is
the actual serverless unlock rather than a convenience: a stateless HTTP call
requires no connection pooling, which is what makes it usable from Lambda and
edge runtimes where a persistent TCP connection per invocation is untenable.
Storage is tiered across RAM and SSD, with infrequently accessed entries evicted
from memory but retained on disk. Billing is per-request.

### 3.3 Scale in place, and decouple nothing

**Aurora Serverless v2** makes the opposite bet and it works. It scales in
0.5-ACU increments with sub-second granularity, measuring capacity every second
and tracking CPU, memory, and network together. The mechanism, in AWS's words:
"most scaling events keep the writer or reader on the same host. This in-place
approach means Aurora doesn't need to migrate data or create new instances
during scaling operations." It scales to zero and resumes in under 15 seconds.

No proxy. No resharding. No slot migration. Nothing to hide from the client,
because nothing moves.

### 3.4 Why Aurora's design does not transfer

Aurora can scale in place because it separated storage from compute. The compute
instance is a query processor over a page cache; durable data lives in a
distributed log-structured storage service. Resizing compute moves no data, so
scaling is a resource-limit adjustment on a running process.

**A cache has no such separation. Memory is the storage.**

That single fact is the deepest reason serverless caching is harder than
serverless SQL, and it explains the market: every cache vendor lands on either a
proxy (hide the movement) or a clean-sheet engine (avoid the topology).
ElastiCache Serverless splits the difference, growing a cache node vertically on
the fast path "while in parallel initiating a scale-out operation" — as close to
Aurora's in-place model as one can get without abandoning the engine.

It also explains why the tiering approaches keep reappearing — Upstash's
RAM+SSD, Azure's Flash Optimized tier (20% RAM / 80% flash, keys always in RAM),
ElastiCache data tiering. Each is a partial attempt to reintroduce a storage
layer beneath the cache. Note the consequence, which AWS documents: data-tiered
instance types are **excluded** from the memory-based autoscaling metric,
because they are expected to sit at 100% memory usage by design. Tiering
addresses the capacity problem and destroys the autoscaling signal.

## 4. Goals and non-goals

**Goals**

- **Topology invisibility.** No client-observable reshard, failover, node
  replacement, or software upgrade. This is the product.
- **Continuous scaling.** Both vertically and horizontally, and both at once.
- **Usage-based billing** on a unit that is computable without exposing nodes.
- **Bounded cost.** The operator sets ceilings, not capacity. An unbounded
  serverless cache is a billing incident.
- **Safe to scale in.** See §8.

**Non-goals**

- **Full command-surface compatibility.** §3.1 shows this is not achievable
  behind a proxy. Declare the refused set explicitly.
- **Data-plane multi-tenancy**, at least initially. See §5.3.
- **Scale-to-zero**, initially. Aurora's sub-15-second resume depends on its
  storage/compute split; a cache resuming from zero must reload its working set
  from somewhere, and that somewhere does not exist yet.

## 5. Layer 1: the decoupling layer

### 5.1 The rejected alternative: a smart client

Topology can be hidden in the client instead of the server. Valkey's own
[valkey-glide](https://github.com/valkey-io/valkey-glide) is a serious attempt —
a Rust core implementing topology handling, with thin bindings for Java, Python,
Go, Node, C#, PHP, Ruby, C++, and Swift. It directly answers the usual objection
to smart clients, which is that you cannot maintain one per language.

It should still be rejected for a serverless product, and the reason is not
technical quality. **A client-side design makes the provider's ability to
reshard contingent on the customer's library version.** A promise contingent on
the customer is not a promise. AWS pays a network hop for exactly this reason
and *still* requires cluster-mode-capable clients on top.

This alternative belongs in the record because it is the cheaper path to most of
the benefit, and because a reader who does not see it rejected will assume the
proxy was chosen by default.

### 5.2 The proxy

Adopt the server-side proxy. Budget for it being genuinely hard, and
specifically for the commands that pin state to a connection.

Connection multiplexing — thousands of client connections consolidated onto few
backend connections — is the economic point of a proxy fleet. It is broken by
`MULTI`/`EXEC`, `WATCH`, blocking commands (`BLPOP` and friends), `SUBSCRIBE`,
and scripts that hold state. Each forces either a dedicated backend connection
for the duration or an outright refusal. Azure's published cross-slot list
(§3.1) is what that decision looks like once shipped.

The proxy is also where throttling happens and where metering is computed, which
is the bridge to Layer 2.

### 5.3 Do not multi-tenant the data plane

The engine is single-threaded and has no meaningful CPU isolation. One `KEYS *`
or one slow script starves every co-tenant on the node. Keep cache nodes
dedicated per cache and let the proxy fleet be the shared, multi-tenant
component. ElastiCache Serverless's per-cache VPC endpoint and 99.99%
availability SLA imply the same choice.

Momento can multi-tenant because it did not inherit this engine. We do.

## 6. Choosing a proxy

### 6.1 The open-source field

Verified status as of this writing:

| Project | Lang | License | Last commit | Cluster mode |
|---|---|---|---|---|
| Envoy `redis_proxy` | C++ | Apache-2.0 | active | yes |
| camellia-redis-proxy | Java | MIT | 2026-07 | yes |
| Predixy | C++ | BSD-3 | 2024-01 | yes (+ sentinel) |
| redis-cluster-proxy | C | **AGPL-3.0** | 2023-08 | yes |
| twemproxy | C | Apache-2.0 | 2022-10 | **no** |
| Codis | Go | MIT | **2018-11** | own scheme |
| corvus | C | MIT | 2022-05 | yes |
| overlord | Go | MIT | 2023-07 | yes |

Three eliminations precede any architectural comparison. **Codis** has not taken
a commit since 2018 and is not a proxy but a cluster-management system built
around a forked Redis 3.2. **twemproxy** predates Redis Cluster, performs
client-side sharding, and has no concept of `MOVED` — a slot migration is
invisible to it in the worst sense. **redis-cluster-proxy is AGPL-3.0**, which
is disqualifying for a managed service: running it as a network service is
precisely the trigger the license exists to pull. RedisLabs published it and
stopped maintaining it in 2023.

Of the survivors, **camellia** is the only dedicated Redis proxy that is
genuinely alive, and it is Java — a garbage collector in the data path of a
product sold on sub-millisecond p99. Redis Enterprise's proxy is C and
cut-through for this reason; ElastiCache's is Rust. **Predixy** is the
best-designed of the C++ options and is effectively one maintainer with a last
commit in early 2024; adopting it means owning it.

**Envoy** is the pragmatic choice for a prototype, and not because its Redis
support is good. It is because Envoy already has the two things a *serverless*
proxy needs beyond routing: a rate-limit service for enforcing the compute
ceiling, and a stats and extension surface (`ext_proc`, Wasm) for per-command
metering. That is Layer 2, and no dedicated Redis proxy offers it.

Read Envoy's own framing carefully before relying on it. The Redis filter is
described as a **"best-effort cache"** that prioritizes "availability and
partition tolerance over consistency" and "will not try to reconcile
inconsistent data or keep a globally consistent view." That is an explicit
disclaimer of the correctness properties a managed cache service sells. It
refreshes topology by polling `CLUSTER SLOTS` on an interval, supports RESP2
only (`HELLO 3` errors), and has no blocking-command support.

### 6.2 Why every vendor wrote their own

The requirement is not "route commands to the right shard." It is **"absorb a
slot migration in flight without dropping a connection or a command."** Only the
first is what an off-the-shelf proxy solves.

Consider what
[atomic slot migration](design-docs/atomic-slot-migration.md) actually does. The
source node forks a child to serialize the slot's keys and streams them to the
target as AOF-format commands; it keeps serving and buffers incremental changes;
then it **pauses writes to the migrating slots** while the target takes over via
a failover-style handover; then topology propagates and the source unpauses.

A generic proxy experiences that sequence as a latency spike (the pause),
followed by `MOVED` errors (the handover), followed by a topology re-poll. It
survives, but it *exposes* both — the pause as tail latency, the handover as
errors the client sees. Envoy polling `CLUSTER SLOTS` on an interval is a retry
loop, not a decoupling layer.

To make it invisible, the proxy must understand the migration state machine: it
must hold requests for the affected slots during the pause window and
re-dispatch them against the new owner once `SYNCSLOTS FINISH` lands, so the
client observes nothing but a few milliseconds of added latency. That is
proxy-engine co-design, and it is why AWS wrote a Rust proxy and Redis wrote a C
one instead of shipping Envoy.

**Recommendation.** Assume we are writing it. Prototype on Envoy to de-risk the
surrounding machinery — TLS termination, metering hooks, rate limiting, and the
economics of connection multiplexing — and to measure empirically what the hop
costs. Redis Enterprise's published p50 of 0.18–0.41 ms is the number to beat.
Then write the real proxy against the ASM state machine.

## 7. Layer 2: metering and ceilings

The metering unit must be computable at the proxy, from bytes and command
identity, without instrumenting the engine.

ElastiCache's ECPU is worth copying in shape. It is the **maximum** of
normalized vCPU time and kilobytes transferred, not their sum, because both are
proxies for the same underlying cost and summing double-charges. Storage is
metered separately, sampled several times per minute and averaged into
gigabyte-hours.

The design content is in what happens at the ceilings, because once the capacity
knobs are gone the ceilings are the only remaining safety mechanism. AWS splits
them:

- **Storage ceiling** reached → eviction, or out-of-memory errors under
  `noeviction`.
- **Compute ceiling** reached → throttling.
- **Minimums** exist to pre-warm ahead of an anticipated spike.

The operator's API surface therefore inverts. It stops being "how many shards"
and becomes "what is my maximum bill, and what happens when I reach it."

## 8. Layer 3: the capacity controller

The architecture in the autoscaling proposal survives, with three amendments.

**Scale vertically and horizontally at the same time.** ElastiCache Serverless
grows a cache node in size *while in parallel* initiating a scale-out. Vertical
is immediate and moves no data; horizontal is slow and moves everything.
Vertical buys the minutes horizontal needs. The controller fires both.

**A warm pool is required, and it is the business model.** Continuous scaling is
only possible if node acquisition takes seconds. The *service* over-provisions
at fleet level so the *customer* need not at instance level. That is what the
margin pays for. AWS admits the failure mode in the node-based documentation:
"if there is not enough capacity available from EC2, ElastiCache Auto Scaling
would not scale and be delayed til the capacity is available."

**The primary-metric question dissolves.** The autoscaling doc asks whether
memory or CPU should be the primary trigger. In a serverless design there is no
choice: memory, compute, and network each have their own ceiling and each is
billed. Track all three and scale on whichever is most saturated. The conflict
rule is `max`, not a tiebreak. Aurora does exactly this — it tracks CPU, memory,
and network and scales up when constrained by any of them.

## 9. Scale-in safety

**The proxy hides the topology change. It does not pay for it.**

Scale-in still forks a child on the source node (copy-on-write pages) and still
inflates the target's memory by the migrated slot data plus replication and
output buffers. Every argument in the autoscaling proposal's scale-in section
survives serverless intact. A proxy makes scale-in *invisible*, not *free*.

### 9.1 The engine's own guard is miscalibrated

Atomic slot migration already has an automatic rollback. Its trigger list
includes "out of memory error occurs on the target node" and "client output
buffer on the source node grows too large."

But an out-of-memory *error* is a `maxmemory` violation — a `used_memory`
accounting event. **The kernel kills on RSS.** Between the two sit allocator
fragmentation, replication buffers, the AOF buffer, client input and output
buffers, and the copy-on-write pages duplicated by the snapshot the migration
itself forks.

If a target's RSS crosses the container limit before its `used_memory` crosses
`maxmemory`, there is no rollback. There is a dead primary and a failover.

This is not a hypothetical. It is a gap between two guards in shipping code, and
it means the control plane cannot delegate scale-in safety to the engine.

### 9.2 Two guards, not one

- **Pre-flight admission (control plane).** Refuse the scale-in unless the
  resultant shard configuration holds RSS plus expected transient overhead,
  including the copy-on-write cost of the migration the scale-in itself
  performs. ElastiCache implements the same idea, refusing to "remove shards if
  insufficient memory available on resultant shard configuration," and
  additionally refuses to migrate any slot holding an item larger than 256 MB
  post-serialization — a data-movement feasibility constraint, not a memory one.
- **In-flight rollback (engine).** ASM's existing trigger, which must be
  calibrated so that the `maxmemory` guard fires *before* the OOM killer does.

Azure ships a calibration for the second: "approximately 20% of the available
memory is reserved as a buffer for noncache operations, such as replication
during failover and active geo-replication buffer." Whether 20% is the right
number is arguable. That a major vendor reserves a fifth of memory for exactly
the terms enumerated above is the strongest public evidence that the gap is
real and large.

### 9.3 "Zero-downtime" needs an asterisk

ASM pauses writes to the migrating slots during handover. A serverless cache
that scales continuously is therefore pausing some slot's writes continuously.
The guarantee to state is *no dropped connections and no lost data*, not *no
latency impact*. Say so, rather than letting "zero-downtime" absorb the claim.

## 10. What the evidence says about scale-down

The autoscaling proposal reads Microsoft's published objection to cache
autoscaling as an objection to *reactive* autoscaling, answerable with cooldowns
and headroom. The evidence does not support that reading.

Azure Managed Redis runs on the Redis Enterprise stack. The proxy is already
there. Topology is already hidden. And the architecture page states flatly:

> Scaling down isn't currently supported on Azure Managed Redis.

Not "not automatically." Not at all — not manually, not on a schedule, not with
unlimited cooldown. Microsoft has the decoupling layer and declines to shrink.

Meanwhile AWS does ship scale-in for node-based clusters, and hedges it with a
25% deadband (no scale-in until the metric falls to 75% of target), a 600-second
default cooldown, a refusal to read `INSUFFICIENT_DATA` as low utilization, a
rule that scale-out proceeds if *any* policy agrees while scale-in requires
*all* of them, two hard refusals (§9.2), and a best-practices page recommending
customers **start with scale-in disabled** and shrink manually.

Two vendors, both with the topology problem solved, both refusing to treat
shrinking as routine. The honest conclusion is that the objection is to
scale-down *as such* — to the data movement — and not to its reactivity.

This strengthens rather than weakens the case for building it, but it relocates
the difficulty. **Scale-in safety is the center of this design, not a safety
appendix to it.** If we ship scale-in that is merely as good as ElastiCache's,
we have matched the state of the art. If we ship one that is provably safe
against RSS rather than `used_memory`, we have advanced it.

## 11. Open questions

- **Which commands do we refuse?** Azure's cross-slot list is one answer.
  Blocking commands, `WATCH`, and stateful scripts are the hard cases. This must
  be decided before the proxy is written, not discovered afterwards.
- **Where does the proxy run?** Co-resident with shards (Redis Enterprise) or a
  separate fleet behind a load balancer (ElastiCache)? Co-residency saves a hop
  and couples proxy failure to shard failure; a separate fleet scales
  independently and is a second thing to autoscale.
- **What is the metering unit's command-cost table, and who owns it?** It is a
  pricing artifact and an engineering artifact simultaneously.
- **Does scale-in default to on?** AWS defaults it on and documents advice to
  turn it off. Given §10, defaulting it off is defensible and honest.
- **Can the storage/compute split be approximated?** Tiering (§3.4) is the only
  public attempt, and it breaks the memory-based autoscaling signal. Is there a
  formulation that preserves it?
- **What is the proxy's behavior during maintenance and failover?** It must
  yield rather than queue, and it must do so without surfacing errors that the
  decoupling layer exists to hide.
