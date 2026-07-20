# Proposal — Slot-per-thread Phase 2: `slot_to_shard[]` + `shard-threads` config

**Status: pre-issue draft, ready to implement.** This is the concrete, line-anchored
implementation of **Phase 2** of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — the first real in-server
increment of slot-per-thread execution. It introduces the slot→shard ownership map and the
`shard-threads` config as a **pure, no-op refactor**: no threads, no behavior change at the
default `shard-threads 1`.

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

Phase 1 (the ordering-model harness, §10.1) is a throwaway validation experiment. Phase 2
is the first code that lands in the server:

> "Introduce `slot_to_shard[]` with `shard-threads 1`. Pure refactor. Every command is
> LOCAL. Should be a runtime no-op and fully testable against the existing suite." (§10.2)

The keyspace is already partitioned by hash slot — `kvstore` is per-slot
(`src/server.c:2902-2904`). Phase 2 adds the **ownership map** on top: an array assigning
each of the 16384 slots to an owning execution shard, plus the `shard-threads` config and a
routing accessor. With `shard-threads` at its default of 1, every slot maps to shard 0, so
every command is LOCAL and runs exactly as today. This is the seam later phases
(multi-threaded reads, per-shard journals) widen; here it must be a **provable no-op**.

**Out of scope:** any threading; LOCAL/REMOTE/BARRIER dispatch (Phase 4+); virtual slots
for standalone (Phase 3); the journal/sequencer (Phase 5); client-to-shard affinity and
connection migration. Phase 2 is the data structure + config + accessors only.

**Naming.** "Shard" is overloaded — in cluster topology a *shard* is a primary plus its
replicas. This feature's shard is an *execution shard* (a thread + the slots it owns). To
keep them distinct, the new module and symbols use the `slotShard*` prefix and the term
"slot-shard"; only the user-facing config keeps the proposal's committed name
`shard-threads`.

## 2. Verified anchors (this checkout)

- `CLUSTER_SLOTS` = `1 << CLUSTER_SLOT_MASK_BITS` = 16384 (`src/cluster.h:10`).
- Client slot field: `int slot;` (`src/server.h:1378`) — "the slot the client is executing
  against, -1 if none". Computed only in cluster mode by `clusterSlotByCommand()` inside
  `prepareCommandGeneric` (`src/server.c:4276`), reset to -1 in `unprepareCommand`
  (`src/server.c:4304`). In standalone it stays -1 (virtual slots are Phase 3).
- Config int-registration precedent — `io-threads` at `src/config.c:3457`:
  `createIntConfig("io-threads", NULL, DEBUG_CONFIG | MODIFIABLE_CONFIG, 1,
  IO_THREADS_MAX_NUM, server.io_threads_num, 1, INTEGER_CONFIG, NULL, updateIOThreads)`.
  Backing fields `io_threads_num`/`active_io_threads_num` at `src/server.h:1840-1841`.
- `createDatabase` (`src/server.c:2893`) — per-slot kvstore already exists; **not modified
  here**.
- `initServerConfig` (`src/server.c:2308`) seeds config-field defaults; `initServer`
  (`src/server.c:2924`) does subsystem init after config load — the init call site.
- INFO "# Server" block: `src/server.c:6182-6213`, `io_threads_active` printed at `:6212` —
  the spot for a `shard_threads` field.
- Build source lists are explicit in **both** systems: `src/Makefile:514` (`kvstore.o` in
  the object list) and `cmake/Modules/SourceFiles.cmake:14` (`kvstore.c`). A new module must
  be added to both.
- Unit tests link the whole server lib (`libvalkey.a` / `valkeylib-gtest` from
  `VALKEY_SERVER_SRCS`) and auto-glob `src/unit/*.cpp` (`src/unit/Makefile:15`,
  `src/unit/CMakeLists.txt:42,64`) — a new module is automatically visible to a new test
  file with no extra registration.

## 3. Design

### 3.1 New module — `src/slot_shard.h` / `src/slot_shard.c`

The map and partition math in one small, unit-testable place.

```c
/* slot_shard.h */
#define SLOT_SHARD_MAX CLUSTER_SLOTS   /* upper bound on shard count (one shard per slot) */

/* (Re)partition all CLUSTER_SLOTS slots across `num_shards` execution shards into
 * contiguous balanced ranges. num_shards is clamped to [1, SLOT_SHARD_MAX]. Idempotent;
 * safe to call again when `shard-threads` changes. */
void slotShardInit(int num_shards);

/* Owning execution shard of `slot` (0 <= slot < CLUSTER_SLOTS). */
int slotToShard(int slot);

/* Current number of execution shards (mirrors server.shard_threads_num after init). */
int slotShardCount(void);

/* Execution shard that will run client c's current command. Returns 0 when the client has
 * no slot (c->slot < 0: keyless, global, or standalone). Today, with one shard, always 0
 * (every command LOCAL). This is the routing seam later phases widen. */
int clientHomeShard(client *c);
```

Core implementation — a file-static map plus a balanced contiguous partition:

```c
/* slot_shard.c */
static uint16_t slot_to_shard[CLUSTER_SLOTS]; /* 32 KB, always fully sized */
static int shard_count = 1;

void slotShardInit(int num_shards) {
    if (num_shards < 1) num_shards = 1;
    if (num_shards > SLOT_SHARD_MAX) num_shards = SLOT_SHARD_MAX;
    shard_count = num_shards;
    for (int s = 0; s < CLUSTER_SLOTS; s++)
        slot_to_shard[s] = (uint16_t)((long long)s * num_shards / CLUSTER_SLOTS);
}
int slotToShard(int slot) { return slot_to_shard[slot]; }
int slotShardCount(void) { return shard_count; }
int clientHomeShard(client *c) { return (c->slot < 0) ? 0 : slot_to_shard[c->slot]; }
```

The partition `slot s → s * N / CLUSTER_SLOTS` produces N contiguous slot ranges, balanced
to within one slot. N=1 → all slots to shard 0 (today's behavior); N=CLUSTER_SLOTS →
identity. `uint16_t` holds shard ids up to 65535, above the 16384 max.

### 3.2 Config — `src/config.c` (near `:3457`) and `src/server.h` (near `:1840`)

- Add `int shard_threads_num;` to the config-field region of `struct valkeyServer`
  (`src/server.h`, beside `io_threads_num` at `:1840`).
- Register beside `io-threads`:
  ```c
  createIntConfig("shard-threads", NULL, MODIFIABLE_CONFIG, 1, SLOT_SHARD_MAX,
                  server.shard_threads_num, 1, INTEGER_CONFIG, NULL, updateShardThreads),
  ```
  Apply callback re-partitions the map so `CONFIG SET` is honest about the data structure:
  ```c
  static int updateShardThreads(const char **err) {
      UNUSED(err);
      slotShardInit(server.shard_threads_num);
      return 1;
  }
  ```
  **Dormant by design:** the value changes the map but has *no execution effect yet* —
  nothing dispatches by shard until Phase 4. Document this in the config comment and the
  `valkey.conf` reference so operators are not misled into thinking it scales anything
  today.

### 3.3 Init — `src/server.c` `initServer()` (`:2924`)

Call `slotShardInit(server.shard_threads_num)` early in `initServer()`, alongside the other
in-memory subsystem initializations (config-field defaults are already in effect). Add
`#include "slot_shard.h"` to `server.c`.

### 3.4 Observability — INFO + a DEBUG helper (so the map is testable end-to-end)

- INFO "# Server" (`src/server.c:6212`, next to `io_threads_active`):
  `"shard_threads:%i\r\n", server.shard_threads_num`.
- `DEBUG SLOT-SHARD <slot>` in `src/debug.c` — returns the owning shard for a slot via
  `slotToShard`. This is the call site that makes `slotToShard`/`clientHomeShard` live
  (non-dead) code and lets a TCL test verify the partition against `CLUSTER KEYSLOT`
  end-to-end. Keep it minimal, mirroring an existing simple `DEBUG` subcommand's
  arg-parsing/reply shape.

**Honest scope statement.** Phase 2 deliberately does *not* branch command execution on the
shard id — with one home thread and one shard, every command is LOCAL, so there is nothing
to route to yet. The deliverable is the ownership map + config + accessors, made live via
the INFO field and the DEBUG helper and proven by tests. The LOCAL/REMOTE dispatch split is
Phase 4, when a second shard exists.

## 4. Files touched

- `src/slot_shard.h`, `src/slot_shard.c` — **new**: map, partition, accessors.
- `src/server.h` — `int shard_threads_num;` config field.
- `src/config.c` — `shard-threads` int config + `updateShardThreads` apply callback.
- `src/server.c` — `#include "slot_shard.h"`; `slotShardInit()` call in `initServer`;
  `shard_threads` INFO field; `shard_threads_num` default in `initServerConfig` if the
  config framework does not already seed it.
- `src/debug.c` — `DEBUG SLOT-SHARD <slot>` subcommand.
- `src/Makefile` (add `slot_shard.o` near `:514`) and `cmake/Modules/SourceFiles.cmake`
  (add `slot_shard.c` near `:14`) — build wiring.
- `src/unit/test_slot_shard.cpp` — **new**: unit tests (auto-discovered, no registration).
- `tests/unit/shard-threads.tcl` — **new**: config + INFO + no-op behavior tests.
- `valkey.conf` — document `shard-threads` (dormant, default 1).

**Not touched:** `kvstore.c`, `db.c`, the command execution path in `server.c` (no dispatch
branching), replication, cluster routing.

## 5. Test plan

**Unit (`src/unit/test_slot_shard.cpp`)** — pure partition/accessor logic, no server boot:

1. `ShardThreadsOneMapsAllSlotsToZero` — `slotShardInit(1)`; every slot 0..16383 → 0;
   `slotShardCount()==1`.
2. `PartitionIsContiguousAndBalanced` — for N in {2,3,7,64}: every slot maps into [0,N);
   shard id is non-decreasing as slot increases (contiguous); largest and smallest shard's
   slot counts differ by at most 1.
3. `IdentityWhenShardPerSlot` — `slotShardInit(CLUSTER_SLOTS)`; slot s → s.
4. `ClampsOutOfRange` — `slotShardInit(0)` and a value > `SLOT_SHARD_MAX` both clamp to the
   valid range with no out-of-bounds writes.
5. `ReinitRepartitions` — init 4 then init 1; the map fully reverts to all-zero (proves
   `CONFIG SET` re-partition is clean, no stale entries).

**Integration (`tests/unit/shard-threads.tcl`)** — real server, proves no-op + wiring:

6. `CONFIG GET shard-threads` defaults to 1; `INFO server` shows `shard_threads:1`.
7. `CONFIG SET shard-threads 4` succeeds; INFO updates to 4; a normal SET/GET/DEL/MGET
   workload still behaves identically (the no-op guarantee at >1 shard).
8. Cluster mode: `DEBUG SLOT-SHARD [CLUSTER KEYSLOT foo]` agrees with the expected partition
   for the configured `shard-threads`, verifying the map end-to-end.
9. Regression smoke: with default config, a representative existing suite passes unchanged.

## 6. Verification

1. Build both ways: `make -C src` and a CMake build, confirming the new module compiles and
   links in each (both source lists updated).
2. `make -C src test-unit UNIT_TEST_PATTERN='SlotShard*'` — the partition unit tests.
3. `./runtest --single unit/shard-threads` — config/INFO/no-op integration tests.
4. Run a slice of the existing suite (e.g. `./runtest --single unit/type/string --single
   unit/keyspace`) with default config to confirm **zero behavior change**.
5. Manually: start `valkey-server`, `CONFIG GET shard-threads` → 1, `INFO server | grep
   shard_threads`, `CONFIG SET shard-threads 8`, confirm normal commands still work and the
   DEBUG helper reports the expected owning shard for a known slot.

## 7. References

- Parent design and phasing: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §4
  (ownership model), §9 (configuration), §10 (phasing).
- Code: `src/server.c` (createDatabase, initServer, prepareCommandGeneric, INFO),
  `src/config.c` (int config precedent), `src/cluster.h` (`CLUSTER_SLOTS`), `src/debug.c`.
