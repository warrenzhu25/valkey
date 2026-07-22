/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Execution-shard ownership map: which execution shard owns which hash slot.
 *
 * Note on terminology: "shard" is overloaded. In cluster topology a shard is a
 * primary plus its replicas. Here it is an *execution shard* -- eventually a
 * thread plus the set of slots it owns exclusively. The symbols in this module
 * use the slotShard* prefix to keep the two apart; only the user-facing config
 * is named `shard-threads`.
 */

#ifndef SLOT_SHARD_H
#define SLOT_SHARD_H

/* cluster.h, which defines CLUSTER_SLOTS, uses types declared in server.h and is
 * included after it everywhere in the tree. Keep that order here so this header
 * is self-contained. */
#include "server.h"
#include "cluster.h"

/* Upper bound on the number of execution shards: one shard per slot. */
#define SLOT_SHARD_MAX CLUSTER_SLOTS

/* (Re)partition all CLUSTER_SLOTS slots across `num_shards` execution shards into
 * contiguous, balanced ranges. `num_shards` is clamped to [1, SLOT_SHARD_MAX].
 * Idempotent; safe to call again when `shard-threads` changes. */
void slotShardInit(int num_shards);

/* The execution shard owning `slot`. Caller must pass 0 <= slot < CLUSTER_SLOTS. */
int slotToShard(int slot);

/* Current number of execution shards, i.e. the value slotShardInit() was last
 * called with, after clamping. */
int slotShardCount(void);

#endif /* SLOT_SHARD_H */
