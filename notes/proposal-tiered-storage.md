# Proposal — Tiered storage (SSD value offload) for Valkey

**Status: pre-issue draft.** Adapts Dragonfly's tiering design — keep all keys and metadata
in RAM, offload cold *value bytes* to SSD, fault them back in on access — to Valkey's object
model. Part of the Dragonfly-inspired series
([proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md)). Relates to two
existing proposals: [proposal-io-uring-backend.md](proposal-io-uring-backend.md) (an io_uring
backend would serve tier disk reads, §4.4) and
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) (per-shard tiers, lock-free, §8).
This is not a commitment to build it.

All `file:line` anchors were checked against this checkout.

---

## 1. The idea in one paragraph

A tiered Valkey keeps its **entire keyspace index in RAM** — every key, its TTL, its LRU/LFU
clock, its type — so lookups stay O(1) and the server always knows what exists. But the
**value bytes** of cold keys are written to an SSD tier and evicted from RAM; the object
retains only a small on-disk *locator*. On access, the value faults back in asynchronously.
The dataset can then exceed RAM by a large factor while hot data is still served at memory
speed — trading a disk round-trip on cold reads for a multiple of usable capacity per dollar.

## 2. Why Valkey is well-positioned (the seams already exist)

Three properties of the current code make this an *integration* project at the object layer,
not a rewrite — the storage engine (§6) is the hard part, but the plumbing into Valkey is
small.

1. **The value is already behind one accessor.** Every value lives at `robj.val_ptr`, and
   nothing dereferences it directly — all access goes through `objectGetVal()`
   (`src/object.c:294`) and `objectSetVal()` (`src/object.c:356`). That single indirection is
   exactly where "this value is on disk, fetch it" belongs. There is one door to instrument,
   not a scattered set of dereferences.
2. **There is a free encoding.** `robj.encoding` is 4 bits (`src/server.h:815`), values 0–11
   used (`src/server.h:761-772`). `OBJ_ENCODING_TIERED = 12` is available. A tiered object
   sets that encoding and repurposes `val_ptr` to point at a locator (§4.1) instead of value
   bytes.
3. **Valkey already suspends and re-runs a command.** `blockPostponeClient()`
   (`src/blocked.c:678`) blocks a client with `BLOCKED_POSTPONE` and sets
   `c->flag.pending_command`; on unblock, the command is re-dispatched from the top with
   `reexecuting_command` (`src/blocked.c:716-718`). This is precisely the primitive a
   fault-in needs: hit a tiered value → issue async read → postpone → re-run `GET` when the
   bytes arrive. No partial-command state machine to invent.

## 3. Goals / non-goals

**Goals**
- Dataset >> RAM, with the keyspace index bounded by RAM (keys + metadata only).
- Hot data served at today's latency; cold reads pay one async disk read.
- Zero behavior change when tiering is off (default). Invisible until enabled.
- Byte-compatible persistence and replication (a tiered primary and a non-tiered replica must
  interoperate — §8).

**Non-goals**
- Offloading collection *elements* (hash fields, zset members) individually — §7. Whole-value
  string offload first, exactly as Dragonfly shipped it.
- A general-purpose embedded KV store. The tier allocator (§6) is purpose-built for Valkey's
  access pattern, not a RocksDB replacement.
- Making RAM-resident workloads faster. Tiering only helps when a large fraction of data is
  genuinely cold (§11).

## 4. Architecture

### 4.1 The locator and the object

A tiered value's `robj` keeps its full header in RAM — type, encoding (now `TIERED`), LRU/LFU
clock, `hasexpire`/embedded TTL, embedded key — and points `val_ptr` at:

```c
typedef struct tierRef {          /* val_ptr target when encoding == OBJ_ENCODING_TIERED */
    uint64_t page_offset;         /* location in the tier file / device */
    uint32_t len;                 /* serialized value length */
    uint8_t  orig_encoding;       /* RAW / INT / ... — what to restore on fault-in */
    uint8_t  flags;               /* e.g. dirty, in-flight-read */
} tierRef;                        /* 16 bytes; smaller than most offloaded values */
```

Only values whose serialized size exceeds a threshold (`tiering-min-value-size`, default e.g.
64 bytes) are eligible — below that, the 16-byte locator plus a disk seek costs more than the
value. Integers (`OBJ_ENCODING_INT`) and shared objects are never offloaded.

### 4.2 Offload path (RAM → disk) — an eviction policy

Offload is a variant of eviction (chapter 05), reusing the victim-selection machinery:

- `performEvictions` (`src/evict.c:404`) + the sampled `evictionPoolPopulate`
  (`src/evict.c:113`) already find the coldest keys by idle time. A new policy
  `allkeys-tier` / `volatile-tier` *offloads* the victim instead of deleting it.
- Offload = serialize the value (the existing RDB per-value serializer, §8), write it to the
  tier allocator (§6), then `objectSetVal(o, tierRef)` and set `encoding = TIERED`. The RAM
  freed is the value bytes minus 16.
- Trigger is the same `maxmemory` pressure that drives eviction today; a separate low-water
  mark can proactively offload before hard pressure.

The elegance: victim selection, the sampling pool, and the `maxmemory` accounting are all
reused. Only the *action on the victim* changes from free to offload.

### 4.3 Fault-in path (disk → RAM)

One hook at the lookup seam. `lookupKeyReadWithFlags` (`src/db.c:137`) and
`lookupKeyWriteWithFlags` (`src/db.c:154`) both funnel through `lookupKey` (`src/db.c:81`):

```
lookupKey finds obj
  └─ if objectGetEncoding(obj) == OBJ_ENCODING_TIERED:
        if a read for this locator is already in flight → just postpone again
        else issue async read (tierRead(tierRef, buf), §6) and mark in-flight
        blockPostponeClient(c)            src/blocked.c:678   (client suspended)
        return "not ready" sentinel       (command does NOT proceed this pass)
     else return obj as today
```

When the async read completes, the completion handler rebuilds the real value object from the
bytes (restoring `orig_encoding`), `objectSetVal`s it back into place (now RAM-resident),
frees the `tierRef`, and unblocks the client — `unblockClient` re-dispatches the command
(`src/blocked.c:716`), which now finds a normal in-RAM value and runs to completion.

Reads leave the value resident (it just proved it's hot). Writes fault in, then mutate.

### 4.4 Async disk backend

- **Phase 1: BIO threads.** The background pool (chapter 09, `src/bio.c`) already does
  off-main-thread disk work. Add a `BIO_TIER_READ` job type (`src/bio.h:56`, extend
  `BIO_NUM_OPS`) that reads a locator's bytes and posts a completion back to the main thread.
  No new infrastructure; reuses the exact pattern of `BIO_RDB_SAVE`.
- **Phase N: io_uring.** Async **disk** I/O — thousands of small random reads with
  completion-based delivery — is what io_uring was built for, and it's a cleaner fit than the
  *network*-I/O case that [proposal-io-uring-backend.md](proposal-io-uring-backend.md)
  analyzes (that one is gated on Stage 0 showing socket syscalls dominate; tier reads need no
  such gate). A batched-submit / batched-complete io_uring backend replaces BIO for tier
  reads and cuts per-read syscall overhead dramatically. The two share a backend; tiering is
  the disk consumer, the event loop the network consumer.

## 5. The write path and dirty tracking

A tiered value that is *overwritten* (`SET k newval` on an offloaded `k`) must not fault in
just to discard: `dbReplaceValue` (`src/db.c:398`) can free the `tierRef` (releasing its tier
pages, §6) and install the new in-RAM value directly. A tiered value that is *deleted*
(`dbDelete`) frees its tier pages without a read. A *partial* mutation (`APPEND`, `SETRANGE`)
must fault in first, then mutate.

The locator's `dirty` flag lets a re-offloaded, unchanged value skip the disk write (its bytes
are already on disk at the old offset) — a meaningful win for read-mostly cold data that
oscillates across the water mark.

## 6. The tier storage engine — the actual hard part

Everything above is a few hundred lines of integration. **This section is the project.**

A naive "append each value at the next file offset" fragments catastrophically and
write-amplifies. Dragonfly built a purpose-built allocator; Valkey needs the equivalent:

- **Page-based, size-binned allocation.** The device is managed in pages (e.g. 4 KB);
  small values are packed into size-class bins within a page (small-object optimization) so a
  100-byte value doesn't burn a 4 KB page. Large values span pages.
- **Reclamation.** Freed locators leave holes; a compaction/GC pass reclaims fragmented pages,
  which itself generates disk writes and must be rate-limited against the serving path.
- **Crash consistency.** The tier file is *not* the source of truth for durability — RDB/AOF
  are (§8). But an in-flight tier must survive restart *or* be treated as a cold cache that's
  rebuilt from persistence. Simplest first cut: **treat the tier as volatile** — on restart,
  offloaded values are reloaded from RDB/AOF into RAM (or re-offloaded), and the tier file is
  discarded. This sidesteps tier-file crash recovery entirely for Phase 1.
- **Write amplification budget.** SSD endurance is a real operational limit; the offload rate
  and GC must be observable and capped.

Prior art to study before writing a line: **KeyDB FLASH** (RocksDB-backed — correct but heavy
write amplification and operational weight, which is *why* Dragonfly built custom) and
**Redis-on-Flash** (Redis Enterprise's commercial tier). The lesson both teach: bolting on a
general LSM store is the tempting shortcut and the wrong one; the allocator is worth building.

## 7. Interactions and the collection problem

| Area | Under tiering |
|---|---|
| **Strings** | The whole design above. First and possibly only target. |
| **Collections** (hash/zset/set/list) | Offloading a whole collection opaquely means `HGET` faults the *entire* hash — often worse than not tiering. Per-element offload is a much deeper design (a tiered listpack/hashtable). **Out of scope initially**; only whole small-ish values tier. |
| **Expiry** | A tiered key's TTL lives in its RAM header, so active/lazy expiry (chapter 05) works unchanged — expiring a tiered key frees its tier pages without a fault-in. |
| **`OBJECT ENCODING`** | Reports `tiered` (or the original encoding + a tiered flag) — observability for operators. |
| **`MEMORY USAGE`** | Must report RAM footprint (header + locator), with tier bytes surfaced separately. |
| **`DEBUG` / introspection** | `DEBUG OBJECT` shows the locator; a `DEBUG OFFLOAD <key>` / `DEBUG FAULTIN <key>` pair drives Phase-1 testing manually. |

## 8. Persistence, replication, cluster — the compatibility crux

Tiering is a **local storage decision** and must stay invisible to the wire formats.

- **RDB save (chapter 06).** The saver walks values via `objectGetVal`; a tiered value would
  fault the whole dataset into RAM during `BGSAVE`, defeating the purpose. The saver needs a
  **tier-aware path**: read a tiered value's bytes straight from the tier into the RDB stream
  without materializing a full `robj` in RAM. The RDB *bytes* are identical to a non-tiered
  save — a tiered instance and a normal instance produce byte-compatible RDBs. This is the
  hard persistence work and it interacts with [proposal-forkless-rdb.md](proposal-forkless-rdb.md):
  a forked child can't see the parent's in-flight tier reads, so **fork-less RDB and tiering
  are natural companions** (another reason that proposal matters).
- **AOF.** Writes are logged as commands as usual; offload/fault-in are not commands and are
  never logged. AOF rewrite has the same walk problem as RDB.
- **Replication.** The replication stream carries commands, not storage state, so a tiered
  primary and a **non-tiered replica interoperate** — the replica makes its own offload
  decisions (or none). Full sync ships an RDB, which is byte-compatible per above.
- **Cluster slot migration** (`design-docs/atomic-slot-migration.md`). Migrating a slot must
  fault in (or tier-copy) its offloaded values to serialize them to the target. Bounded by the
  slot's cold-value count.
- **Slot-per-thread** ([proposal-slot-per-thread.md](proposal-slot-per-thread.md)). If each
  shard owns its slots' tier region, offload/fault-in are **lock-free per shard** — exactly
  Dragonfly's shared-nothing tiering. Tiering and slot-per-thread reinforce each other; a
  per-shard tier is the clean long-term shape.

## 9. Config and observability

**Config:**
- `tiering {no|yes}` — default `no`. Off = today, exactly.
- `tiering-path <dir>` — where the tier file(s) live (SSD).
- `tiering-min-value-size <bytes>` — offload eligibility floor (default ~64).
- `tiering-max-size <bytes>` — cap on tier file size; hard eviction (real delete) resumes when
  the tier itself is full.
- New `maxmemory-policy` values `allkeys-tier` / `volatile-tier` (extend
  `maxmemory_policy_enum`, `src/config.c:60`).

**INFO (`# Tiering` section):**
- `tiering_enabled`, `tiering_offloaded_keys`, `tiering_tier_bytes`.
- `tiering_faultins_total`, `tiering_faultin_pending` (blocked clients awaiting a read).
- `tiering_offloads_total`, `tiering_write_amplification` (device bytes / logical bytes — the
  SSD-endurance number operators must watch).
- `tiering_faultin_latency` histogram (the tail that decides viability, §11).

## 10. Phasing

Each step is independently testable; the default-off gate keeps `main` safe throughout.

1. **`OBJ_ENCODING_TIERED` + locator + fault-in-on-read**, BIO-backed, **volatile tier**
   (§6, discarded on restart), strings only, manual `DEBUG OFFLOAD`/`FAULTIN`. Proves the
   object plumbing and the postpone/re-dispatch loop end to end. No persistence changes yet
   (offloaded keys fault in before any RDB save).
2. **Offload as an eviction policy** — `allkeys-tier`, reusing the eviction pool; write-path
   dirty tracking (§5).
3. **The real page/bin allocator + reclamation** (§6) — the hard, separable storage project.
4. **Tier-aware RDB/AOF/replication** (§8) so persistence doesn't fault everything into RAM.
5. **io_uring backend** (§4.4) and, in the threaded world, **per-shard tiers** (§8).

If the project stalls after step 2, Valkey has a working-but-simple SSD tier for large string
values behind an eviction policy — already useful, and a fine place to be stranded.

## 11. Honest risks

- **Fault-in tail latency.** The block-and-reprocess model adds a disk read to cold accesses.
  If the working set isn't actually hot, p99 collapses and users would be better served buying
  RAM. Must be measured against real access distributions, not uniform benchmarks — this is
  the primary go/no-go signal.
- **The allocator is most of the work (§6).** The Valkey integration is small; the
  storage engine, GC, and crash story are a multi-quarter effort with real SSD-endurance
  consequences. Underestimating this is how KeyDB FLASH ended up leaning on RocksDB.
- **Persistence walk (§8).** Making RDB/AOF/replication tier-aware without faulting the whole
  dataset into RAM is subtle and interacts with fork-less RDB. A wrong cut here silently
  destroys the memory benefit during every save.
- **Write amplification.** Offload + GC generate SSD writes; a churny cold set can burn
  endurance. Must be surfaced (`tiering_write_amplification`) and capped.
- **Collections don't tier well (§7).** The biggest values are often hashes/zsets, and those
  are exactly the ones this design can't offload usefully at first. The addressable win is
  narrower than "offload cold data" sounds.
- **Blast radius on the value path.** Every command that reads a value must tolerate the
  postpone/re-dispatch path. Commands that read many keys (`MGET`, `SUNION`) may block, fault
  several values, and re-run — the re-dispatch must be idempotent and cheap.

## 12. What would make me abandon this

Written down in advance:

- Target workloads have a **hot** working set (tiering only pays when much data is cold and
  rarely touched — measure first).
- Fault-in tail latency under the postpone model is unacceptable versus simply provisioning
  RAM at the target scale.
- The allocator + persistence-walk cost dwarfs the capacity benefit for realistic datasets —
  i.e. the engineering doesn't pay for itself against "add nodes / add RAM."

Any one means Valkey users are better served by horizontal scale or bigger boxes, and the
honest conclusion is to stop.

## 13. Ready-to-code (Phase 1)

The minimum end-to-end slice — volatile tier, BIO reads, strings, manual trigger — touching:

| File | Change |
|---|---|
| `src/server.h` | `#define OBJ_ENCODING_TIERED 12`; the `tierRef` struct (§4.1); `server` gains tier config + stat fields. |
| `src/tier.{c,h}` | **New.** The tier engine: `tierWrite(bytes,len) -> tierRef`, `tierReadAsync(tierRef, cb)`, `tierFree(tierRef)`. Phase 1 = append-only file + a free list; the real allocator is Phase 3. |
| `src/object.c` | `objectGetVal`/`objectSetVal` (`:294`,`:356`) understand `TIERED` (assert callers never read a tiered `val_ptr` as bytes — the encoding check catches misuse). Add `objectOffload(robj*)` / `objectFaultInFromBytes(robj*, buf, len)`. |
| `src/db.c` | In `lookupKey` (`:81`), the §4.3 hook: on `TIERED`, `blockPostponeClient(c)` + issue `tierReadAsync`; return the not-ready sentinel. `dbReplaceValue`/`dbDelete` free the `tierRef` without faulting (§5). |
| `src/bio.{c,h}` | `BIO_TIER_READ` job type (`bio.h:56`, bump `BIO_NUM_OPS`); the completion posts back to the main thread, which calls `objectFaultInFromBytes` + `unblockClient`. |
| `src/evict.c` | Phase 2: `allkeys-tier` calls `objectOffload` on the pool victim instead of `dbDelete`. |
| `src/config.c` | `tiering` bool + `tiering-path` + `tiering-min-value-size`; Phase 2 adds the `maxmemory-policy` enum values (`:60`). |
| `src/debug.c` | `DEBUG OFFLOAD <key>` / `DEBUG FAULTIN <key>` to drive tests before the eviction policy exists. |
| `tests/unit/type/tiering.tcl` | Offload a key, assert `OBJECT ENCODING` = tiered and RAM dropped; read it, assert correct value and encoding restored; `DEBUG RELOAD` round-trip; overwrite/delete of a tiered key frees tier space without a fault. |

**Gate for Phase 1:** a key offloaded and faulted back in is bit-identical; a tiered key
survives `DEBUG RELOAD` (via fault-in-before-save); no command returns a wrong or partial
value across the postpone/re-dispatch boundary.

## 14. Code anchors

| Thing | Where |
|---|---|
| Value indirection seam | `src/object.c:294` (`objectGetVal`), `:356` (`objectSetVal`); `val_ptr` `src/server.h:823` |
| Encoding field + free slot | `src/server.h:815` (4-bit `encoding`), defines `:761-772` (0–11 used) |
| Lookup / fault-in hook | `src/db.c:81` (`lookupKey`), `:137` (read), `:154` (write) |
| Postpone / re-dispatch primitive | `src/blocked.c:678` (`blockPostponeClient`), re-run at `:716-718` |
| Eviction victim machinery to reuse | `src/evict.c:404` (`performEvictions`), `:113` (`evictionPoolPopulate`), pool `:56` |
| Value overwrite / delete | `src/db.c:398` (`dbReplaceValue`), `:319` (`dbSetValue`) |
| Async disk backend | `src/bio.c` / `src/bio.h:56` (`BIO_NUM_OPS`); io_uring: [proposal-io-uring-backend.md](proposal-io-uring-backend.md) |
| RDB per-value serializer (tier-aware save) | `src/rdb.c:1190` (`rdbSaveKeyValuePair`) |
| maxmemory-policy enum | `src/config.c:60` (`maxmemory_policy_enum`), registered `:3437` |
