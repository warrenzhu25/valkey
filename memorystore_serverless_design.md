# A Serverless Managed Cache: A Design Proposal

> **Sources.** Compiled from public documentation, published vendor benchmarks,
> and the Valkey source tree. A proposed design, not any vendor's roadmap,
> internal architecture, or pricing.

Companion to [memorystore_autoscaling_design.md](memorystore_autoscaling_design.md).
That doc proposes autoscaling for a capacity-provisioned cache; this one argues
autoscaling is the *third* of three layers a serverless cache needs, and the two
beneath it decide the design.

## 1. Problem

A capacity-provisioned cache asks the operator for node size and shard count; an
autoscaler answers them automatically. A serverless cache does not ask at all —
which is not the same product with the knobs hidden.

Serverless makes one promise:

> The provider may change topology at any moment, unilaterally, and the
> application will not notice.

Every other serverless property — pay-per-use, no capacity planning, continuous
scaling, scale-to-zero — is downstream of it. An autoscaler that resharded while
clients held direct node connections would deliver topology changes into the
application's error budget. So the promise is not a feature of the autoscaler; it
is a precondition for running one continuously.

## 2. What the promise costs

Three layers, in dependency order:

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

Layer 1 is the promise. Layer 2 makes it sellable — you cannot bill for nodes in
a product whose premise is that the customer never sees nodes. Layer 3 is the
autoscaling proposal, which gets both easier (the client cannot observe a
reshard) and harder (no operator-set bounds to hide behind) once 1 and 2 exist.

## 3. Landscape

Three strategies, and the choice cascades.

### 3.1 Proxy over an unmodified engine

**Redis Enterprise** got here first. A proxy process runs on every node —
multi-threaded, C, cut-through, stateless — and
[consolidates client requests into inner pipelines](https://redis.io/blog/redis-enterprise-proxy/),
multiplexing thousands of client connections onto persistent shard connections.
Resharding and shard failover are invisible; clients keep the endpoint. The
exception is whole-node failure: clients reconnect, and the endpoint moves to a
surviving node's proxy. Published p50 is 0.18–0.41 ms across 2–32 shards at 2000
connections.

**ElastiCache Serverless** does the same with a separate proxy fleet behind a
network load balancer. AWS: "The proxy layer abstracts the cluster topology and
nodes from your client. This enables ElastiCache to intelligently load balance,
scale out and add new cache nodes, replace cache nodes when they fail, and update
software on the cache nodes, all without availability impact."

**Azure Managed Redis** runs the Redis Enterprise stack and exposes both modes as
a customer choice, documenting the trade-off. *OSS clustering policy*: clients
speak the Cluster API directly to shards — lowest latency, no proxy in the data
path. *Enterprise clustering policy*: a single endpoint routes through a proxy
node, which Microsoft notes "can be a bottleneck in either compute utilization or
network throughput."

The cost of the abstraction is also published: under Enterprise clustering
policy, the only cross-slot multi-key commands allowed are `DEL`, `MSET`, `MGET`,
`EXISTS`, `UNLINK`, `TOUCH`; everything else returns `CROSSSLOT`. **A proxy
design must decide which commands it refuses, on day one.**

### 3.2 Clean-sheet engine, or a new protocol

**Momento** built a cache engine from scratch rather than wrapping Redis, which
permitted genuine multi-tenancy and both shared and dedicated tenancy. With no
single-threaded topology-aware engine to hide, there is no proxy to write.

**Upstash** keeps the Redis protocol but adds an HTTP/REST interface — the real
serverless unlock, since a stateless HTTP call needs no connection pooling and so
works from Lambda and edge runtimes where a persistent TCP connection per
invocation is untenable. Storage is tiered across RAM and SSD; billing is
per-request.

### 3.3 Scale in place, decouple nothing

**Aurora Serverless v2** makes the opposite bet and it works: 0.5-ACU increments,
sub-second, measuring capacity every second across CPU, memory, and network. AWS:
"most scaling events keep the writer or reader on the same host. This in-place
approach means Aurora doesn't need to migrate data or create new instances during
scaling operations." It scales to zero and resumes in under 15 seconds. No proxy,
no resharding, no migration — nothing to hide, because nothing moves.

### 3.4 Why Aurora's design does not transfer

Aurora scales in place because it separated storage from compute: the compute
instance is a query processor over a page cache, and durable data lives in a
distributed storage service. Resizing compute moves no data.

**A cache has no such separation. Memory is the storage.**

That is the deepest reason serverless caching is harder than serverless SQL, and
it explains the market: every cache vendor lands on a proxy (hide the movement)
or a clean-sheet engine (avoid the topology). ElastiCache Serverless splits the
difference, growing a node vertically "while in parallel initiating a scale-out
operation."

It also explains the recurring tiering approaches — Upstash's RAM+SSD, Azure's
Flash Optimized tier (20% RAM / 80% flash, keys always in RAM), ElastiCache data
tiering — each a partial attempt to reintroduce a storage layer under the cache.
The consequence, per AWS: data-tiered instance types are **excluded** from the
memory-based autoscaling metric, since they are expected to run at 100% memory by
design. Tiering fixes capacity and breaks the autoscaling signal.

## 4. Goals and non-goals

**Goals**

- **Topology invisibility.** No client-observable reshard, failover, node
  replacement, or upgrade. This is the product.
- **Continuous scaling**, vertical and horizontal, both at once.
- **Usage-based billing** on a unit computable without exposing nodes.
- **Bounded cost.** The operator sets ceilings, not capacity.
- **Safe to scale in (§8).**

**Non-goals**

- **Full command compatibility.** §3.1 shows this is unachievable behind a proxy.
  Declare the refused set explicitly.
- **Data-plane multi-tenancy**, initially (§5.3).
- **Scale-to-zero**, initially. Aurora's fast resume rides on its storage/compute
  split; a cache resuming from zero must reload its working set from a store that
  does not yet exist here.

## 5. Layer 1: the decoupling layer

### 5.1 The rejected alternative: a smart client

Topology can be hidden in the client instead. Valkey's
[valkey-glide](https://github.com/valkey-io/valkey-glide) is a serious attempt —
a Rust core with thin bindings for Java, Python, Go, Node, C#, PHP, Ruby, C++,
Swift — answering the usual objection that you cannot maintain one client per
language.

Reject it anyway, and not on quality. **A client-side design makes the provider's
ability to reshard contingent on the customer's library version**, and a promise
contingent on the customer is not a promise. AWS pays a network hop for this
reason and *still* requires cluster-mode clients on top. Recorded here because it
is the cheaper path to most of the benefit, so its rejection should be explicit.

### 5.2 The proxy

Adopt the server-side proxy, and budget for the commands that pin state to a
connection. Connection multiplexing — thousands of client connections onto few
backend connections — is the economic point of a proxy fleet, and it is broken by
`MULTI`/`EXEC`, `WATCH`, blocking commands (`BLPOP`), `SUBSCRIBE`, and stateful
scripts. Each forces a dedicated backend connection or an outright refusal;
Azure's cross-slot list (§3.1) is that decision shipped. The proxy is also where
throttling and metering happen — the bridge to Layer 2.

### 5.3 Do not multi-tenant the data plane

The engine is single-threaded with no CPU isolation: one `KEYS *` or slow script
starves every co-tenant on the node. Keep cache nodes dedicated per cache; let
the proxy fleet be the shared component. ElastiCache Serverless's per-cache VPC
endpoint and 99.99% SLA imply the same choice. Momento can multi-tenant because
it did not inherit this engine.

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

Three eliminations first. **Codis** has no commit since 2018 and is a
cluster-management system around a forked Redis 3.2, not a proxy. **twemproxy**
predates Redis Cluster, does client-side sharding, and has no concept of `MOVED`.
**redis-cluster-proxy is AGPL-3.0** — disqualifying for a managed service, since
running it as a network service is exactly the license's trigger; unmaintained
since 2023.

Of the survivors, **camellia** is the only dedicated Redis proxy that is alive,
and it is Java — a GC in the data path of a product sold on sub-millisecond p99
(Redis Enterprise's proxy is C, ElastiCache's is Rust). **Predixy** is the best
C++ option and effectively one maintainer, last commit early 2024; adopting it
means owning it.

**Envoy** is the prototype choice — not for its Redis support, but because it
already has what a *serverless* proxy needs beyond routing: a rate-limit service
for the compute ceiling, and an extension surface (`ext_proc`, Wasm) for
per-command metering. That is Layer 2, and no dedicated Redis proxy has it. Its
limits are real, though: Envoy calls its Redis filter a **"best-effort cache"**
that prioritizes "availability and partition tolerance over consistency" and
"will not try to reconcile inconsistent data or keep a globally consistent view."
It refreshes topology by polling `CLUSTER SLOTS`, supports RESP2 only (`HELLO 3`
errors), and has no blocking-command support.

### 6.2 Why every vendor wrote their own

The requirement is not "route commands to the right shard." It is **"absorb a
slot migration in flight without dropping a connection or a command"** — and only
the first is what an off-the-shelf proxy solves.

[Atomic slot migration](design-docs/atomic-slot-migration.md): the source forks a
child to serialize the slot's keys, streams them to the target as AOF-format
commands, keeps serving and buffers incremental changes, then **pauses writes to
the migrating slots** while the target takes over via a failover-style handover;
then topology propagates.

A generic proxy sees that as a latency spike (the pause), then `MOVED` errors
(the handover), then a topology re-poll. It survives but *exposes* both — the
pause as tail latency, the handover as client-visible errors. Envoy polling
`CLUSTER SLOTS` is a retry loop, not a decoupling layer.

To make it invisible, the proxy must know the migration state machine: hold
requests for the affected slots during the pause and re-dispatch them to the new
owner once `SYNCSLOTS FINISH` lands, so the client sees only a few ms of latency.
That is proxy-engine co-design, which is why AWS wrote a Rust proxy and Redis a C
one instead of shipping Envoy.

**Recommendation.** Assume we write it. Prototype on Envoy to de-risk the
surrounding machinery — TLS termination, metering hooks, rate limiting,
multiplexing economics — and to measure the hop cost against Redis Enterprise's
0.18–0.41 ms p50. Then write the real proxy against the ASM state machine.

## 7. Layer 2: metering and ceilings

The metering unit must be computable at the proxy, from bytes and command
identity, without instrumenting the engine.

ElastiCache's ECPU is worth copying in shape: the **maximum** of normalized vCPU
time and kilobytes transferred, not their sum, since both proxy the same cost and
summing double-charges. Storage is metered separately, sampled several times per
minute into gigabyte-hours.

The design content is at the ceilings — once the capacity knobs are gone they are
the only safety mechanism. AWS splits them:

- **Storage ceiling** → eviction, or OOM errors under `noeviction`.
- **Compute ceiling** → throttling.
- **Minimums** → pre-warm ahead of a spike.

The operator's API surface inverts: not "how many shards" but "what is my maximum
bill, and what happens when I reach it."

## 8. Layer 3: the capacity controller

The autoscaling architecture survives, with three amendments.

**Scale vertically and horizontally at once.** ElastiCache Serverless grows a
node *while in parallel* scaling out. Vertical is immediate and moves no data;
horizontal is slow and moves everything. Vertical buys the minutes horizontal
needs. Fire both.

**A warm pool is required, and it is the business model.** Continuous scaling
needs node acquisition in seconds, so the *service* over-provisions at fleet
level so the *customer* need not at instance level — what the margin pays for.
AWS admits the failure mode: "if there is not enough capacity available from EC2,
ElastiCache Auto Scaling would not scale and be delayed til the capacity is
available."

**The primary-metric question dissolves.** The autoscaling doc asks whether
memory or CPU is the primary trigger. Serverless removes the choice: memory,
compute, and network each have a ceiling and each is billed. Track all three,
scale on the most saturated — the conflict rule is `max`. Aurora does exactly
this.

## 9. Scale-in safety

**The proxy hides the topology change. It does not pay for it.** Scale-in still
forks a child on the source (copy-on-write pages) and inflates the target's
memory by the migrated slot data plus replication and output buffers. Every
argument in the autoscaling proposal's scale-in section survives. A proxy makes
scale-in *invisible*, not *free*.

### 9.1 The engine's own guard is miscalibrated

ASM rolls back on "out of memory error occurs on the target node." But an OOM
*error* is a `maxmemory` violation — a `used_memory` event — while the kernel
kills on RSS. Between the two sit allocator fragmentation, replication buffers,
the AOF buffer, client I/O buffers, and copy-on-write pages from the snapshot the
migration forks. If RSS crosses the container limit before `used_memory` crosses
`maxmemory`, there is no rollback, only a dead primary and a failover. This is a
gap between two guards in shipping code: the control plane cannot delegate
scale-in safety to the engine.

### 9.2 Two guards, not one

- **Pre-flight admission (control plane).** Refuse the scale-in unless the
  resultant configuration holds RSS plus transient overhead, including the
  copy-on-write cost of the migration. ElastiCache does the same, refusing to
  "remove shards if insufficient memory available on resultant shard
  configuration," and additionally refuses to migrate any slot holding an item
  over 256 MB post-serialization — a data-movement constraint, not a memory one.
- **In-flight rollback (engine).** ASM's trigger, calibrated so the `maxmemory`
  guard fires before the OOM killer.

Azure ships a calibration: ~20% of memory reserved "for noncache operations, such
as replication during failover." Whether 20% is right is arguable; that a vendor
reserves a fifth of memory for these terms is strong evidence the gap is real.

### 9.3 "Zero-downtime" needs an asterisk

ASM pauses writes to the migrating slots during handover, so a continuously
scaling cache is continuously pausing some slot's writes. The guarantee is *no
dropped connections and no lost data*, not *no latency impact* — say so, rather
than letting "zero-downtime" absorb the claim.

## 10. What the evidence says about scale-down

The autoscaling proposal reads Microsoft's objection as being about *reactive*
autoscaling, answerable with cooldowns and headroom. The evidence disagrees.

Azure Managed Redis runs on Redis Enterprise — the proxy is there, topology is
already hidden — and the architecture page still states:

> Scaling down isn't currently supported on Azure Managed Redis.

Not "not automatically." Not manually, not on a schedule. Microsoft has the
decoupling layer and declines to shrink.

AWS does ship node-based scale-in, and hedges it: a 25% deadband (no scale-in
until the metric hits 75% of target), a 600s cooldown, no scale-in on
`INSUFFICIENT_DATA`, scale-out if *any* policy agrees but scale-in only if *all*
do, two hard refusals (§9.2), and advice to **start with scale-in disabled**.

Two vendors with the topology problem solved, both refusing to treat shrinking as
routine. The objection is to scale-down's data movement, not its reactivity. This
strengthens the case for building it and relocates the difficulty: **scale-in
safety is the center of this design.** Scale-in as good as ElastiCache's matches
the state of the art; scale-in provably safe against RSS rather than
`used_memory` advances it.

## 11. Open questions

- **Which commands do we refuse?** Azure's cross-slot list is one answer;
  blocking commands, `WATCH`, and stateful scripts are the hard cases. Decide
  before the proxy is written.
- **Where does the proxy run?** Co-resident with shards (Redis Enterprise) saves
  a hop but couples proxy failure to shard failure; a separate fleet
  (ElastiCache) scales independently but is a second thing to autoscale.
- **Who owns the metering command-cost table?** It is a pricing and an
  engineering artifact at once.
- **Does scale-in default to on?** AWS defaults it on and advises turning it off.
  Per §10, defaulting it off is defensible.
- **Can the storage/compute split be approximated?** Tiering (§3.4) is the only
  public attempt, and it breaks the memory-based autoscaling signal. Is there a
  formulation that preserves it?
- **Proxy behavior during maintenance and failover?** It must yield rather than
  queue, without surfacing the errors the decoupling layer exists to hide.
