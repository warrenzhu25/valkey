# Managed Cache Services: Public Competitive Landscape

> **Sources.** Every claim below is drawn from vendor documentation and other
> publicly available material, and is linked. This document contains no
> confidential information, no unannounced roadmap, and no pricing strategy. It
> is a factual capability comparison, not a positioning document.

This compares Google Cloud Memorystore against established managed cache
offerings (**AWS ElastiCache**, **Azure Managed Redis / Azure Cache for Redis**,
**Redis Cloud**) and serverless-first entrants (**Upstash**, **Momento**) across
four capability areas.

---

## 1. Serverless and Autoscaling

The category is shifting from provisioned instances toward fully abstracted,
no-ops services.

- **Google Cloud Memorystore** — no native autoscaling. Automatic scaling
  requires deploying the open-source
  [Memorystore Cluster Autoscaler](https://github.com/GoogleCloudPlatform/memorystore-cluster-autoscaler)
  on Cloud Run or GKE. Provisioning is node-based. Memorystore clusters do
  support zero-downtime scaling once a scale operation is issued.
- **AWS ElastiCache** — offers
  [ElastiCache Serverless](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoScaling.html),
  which scales compute (ECPUs), memory, and bandwidth behind a routing layer.
  Node-based clusters scale via Application Auto Scaling target tracking.
- **Azure** — manual. Microsoft
  [explicitly recommends against autoscaling a cache](https://learn.microsoft.com/en-us/azure/azure-cache-for-redis/cache-best-practices-scale),
  reasoning that a scale operation consumes cache resources and so is unsafe to
  trigger automatically under load.
- **Redis Cloud** — provisioned. Plans are sized by the customer; there is no
  no-ops serverless tier comparable to ElastiCache Serverless.
- **Upstash and Momento** — built serverless-first, with per-request billing and
  no base node fee.

## 2. Geo-Distribution

- **Google Cloud Memorystore** — regional, primary-replica. Cross-region
  replication is available; it is not multi-master.
- **Redis Cloud and Azure Enterprise tiers** — true
  [Active-Active geo-distribution](https://redis.io/docs/latest/operate/rs/databases/active-active/)
  built on conflict-free replicated data types (CRDTs), allowing concurrent
  writes in multiple regions with automatic conflict resolution. Redis Cloud
  requires a Pro subscription.
- **AWS ElastiCache** — Global Datastore is active-passive.
- **Upstash** — **not active-active.** The
  [Global Database](https://upstash.com/docs/redis/features/globaldatabase)
  designates a *single* primary region plus read regions. Writes from any region
  are routed to the primary and replicated asynchronously; the model is
  eventually consistent, and reads may return stale values. This is a
  low-latency read topology, not multi-master.
- **Momento** — markets a globally available caching service; public
  documentation does not describe a CRDT-based multi-master model, so it is left
  unrated here rather than credited.

## 3. Data Tiering (RAM + SSD)

Spilling colder data to NVMe reduces cost for large datasets.

- **Google Cloud Memorystore** — RAM only.
- **AWS ElastiCache** — data-tiering node families (e.g. `r6gd`) move cold data
  to NVMe.
- **Azure** — available on the **Enterprise Flash** tier only, not on Basic,
  Standard, or Premium.
- **Redis Cloud** — Auto Tiering, formerly Redis on Flash.
- **Upstash and Momento** — not applicable in the same form; storage mechanics
  are hidden behind per-request billing.

## 4. Modules and Connectivity

- **Google Cloud Memorystore** — vector similarity search is supported; the
  broader Redis Stack module set is not.
- **Redis Cloud and Azure Enterprise** — RediSearch, RedisJSON, RedisTimeSeries.
- **AWS ElastiCache** — no Redis Stack modules.
- **Upstash and Momento** — first-class HTTP/REST APIs alongside wire-protocol
  compatibility, which matters for edge runtimes (Cloudflare Workers, Vercel)
  where holding a TCP connection is awkward. Both offer vector search as a
  separate serverless product.

---

## Capability Matrix

| Capability | Memorystore | ElastiCache | Azure | Redis Cloud | Upstash | Momento |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Native autoscaling | ❌ (OSS tool) | ✅ | ❌ | ❌ | ✅ | ✅ |
| Serverless / no-ops tier | ❌ | ✅ | ❌ | ❌ | ✅ | ✅ |
| Per-request billing | ❌ | ✅ (ECPU) | ❌ | ❌ | ✅ | ✅ |
| Active-active (multi-master) | ❌ | ❌ (A/P) | ✅ (Enterprise) | ✅ | ❌ (primary + read regions) | — |
| Data tiering (RAM+SSD) | ❌ | ✅ | ✅ (Enterprise Flash) | ✅ (Auto Tiering) | n/a | n/a |
| Redis Stack modules | ❌ (vector only) | ❌ | ✅ (Enterprise) | ✅ | ➖ (equivalents) | ➖ (equivalents) |
| REST / HTTP API | ❌ | ❌ | ❌ | ❌ | ✅ | ✅ |

Legend: ✅ supported · ❌ not supported · ➖ partial or via a different mechanism ·
— insufficient public information · n/a not applicable to the model.

Tier qualifiers matter. Azure's active-active and module support exist **only**
on Enterprise tiers, and its data tiering **only** on Enterprise Flash. A row
marked ✅ for Azure does not describe the Basic, Standard, or Premium tiers most
users run.

---

## Summary

Memorystore's gaps against this field are consistent: no native autoscaling, no
serverless tier, no per-request billing, no multi-master geo-distribution, no
data tiering, and no Redis Stack modules. Its strengths — a 99.99% SLA,
zero-downtime scaling, Private Service Connect, managed backups, cross-region
replication — are operational rather than architectural, and none of them
distinguish it from ElastiCache.

The two corrections most worth carrying forward from earlier drafts of this
comparison:

1. **Upstash is not active-active.** Its Global Database is a single-primary,
   eventually consistent read-replica topology. Crediting it as multi-master
   overstates it and understates Redis Cloud's CRDT implementation, which is the
   genuine differentiator in that row.
2. **Redis Cloud is not serverless.** It is provisioned. Only Upstash, Momento,
   and ElastiCache Serverless belong in that column.
