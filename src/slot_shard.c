/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Execution-shard ownership map.
 *
 * The keyspace is already partitioned by hash slot -- kvstore is an array of
 * hash tables, one per slot. This module adds the ownership map on top: an
 * array assigning each of the CLUSTER_SLOTS slots to an owning execution shard.
 *
 * The map is read-mostly. Once execution shards are threads, it will be mutated
 * only while every shard is parked, which is what makes it safe to read without
 * a lock from any thread.
 *
 * At the default of one shard every slot maps to shard 0, so every command is
 * local and this module has no effect on behavior.
 */

#include "slot_shard.h"

#include <stdint.h>

/* slot -> owning execution shard. Always fully sized (32 KB), so lookups need
 * no bounds arithmetic beyond the caller's slot validity. */
static uint16_t slot_to_shard[CLUSTER_SLOTS];

static int shard_count = 1;

void slotShardInit(int num_shards) {
    if (num_shards < 1) num_shards = 1;
    if (num_shards > SLOT_SHARD_MAX) num_shards = SLOT_SHARD_MAX;
    shard_count = num_shards;

    /* slot s -> s * N / CLUSTER_SLOTS yields N contiguous ranges whose sizes
     * differ by at most one slot. N == 1 maps everything to shard 0 (today's
     * behavior); N == CLUSTER_SLOTS is the identity. */
    for (int s = 0; s < CLUSTER_SLOTS; s++) {
        slot_to_shard[s] = (uint16_t)((long long)s * num_shards / CLUSTER_SLOTS);
    }
}

int slotToShard(int slot) {
    return slot_to_shard[slot];
}

int slotShardCount(void) {
    return shard_count;
}
