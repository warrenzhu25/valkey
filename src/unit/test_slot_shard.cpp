/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <vector>

extern "C" {
#include "slot_shard.h"
}

/* The ownership map is process-global state, so every test sets it up itself and
 * the fixture restores the default afterwards. */
class SlotShardTest : public ::testing::Test {
  protected:
    void TearDown() override {
        slotShardInit(1);
    }

    /* Number of slots owned by each of the `num_shards` shards. */
    static std::vector<int> slotCountsPerShard(int num_shards) {
        std::vector<int> counts(num_shards, 0);
        for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
            int shard = slotToShard(slot);
            EXPECT_GE(shard, 0);
            EXPECT_LT(shard, num_shards);
            if (shard >= 0 && shard < num_shards) counts[shard]++;
        }
        return counts;
    }
};

/* The default: one shard owns everything, so every command is local and the map
 * has no effect on behavior. */
TEST_F(SlotShardTest, ShardThreadsOneMapsAllSlotsToZero) {
    slotShardInit(1);

    EXPECT_EQ(slotShardCount(), 1);
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        ASSERT_EQ(slotToShard(slot), 0) << "slot " << slot;
    }
}

TEST_F(SlotShardTest, PartitionIsContiguousAndBalanced) {
    for (int num_shards : {2, 3, 7, 64}) {
        slotShardInit(num_shards);
        ASSERT_EQ(slotShardCount(), num_shards);

        /* Contiguous: the owning shard never decreases as the slot increases, so
         * each shard owns one unbroken slot range. */
        int prev = slotToShard(0);
        ASSERT_EQ(prev, 0) << "shards " << num_shards;
        for (int slot = 1; slot < CLUSTER_SLOTS; slot++) {
            int shard = slotToShard(slot);
            ASSERT_GE(shard, prev) << "shards " << num_shards << ", slot " << slot;
            ASSERT_LE(shard, prev + 1) << "shards " << num_shards << ", slot " << slot;
            prev = shard;
        }
        ASSERT_EQ(prev, num_shards - 1) << "shards " << num_shards;

        /* Balanced: every shard owns at least one slot, and the largest and
         * smallest ranges differ by at most one. */
        /* Note: server.h defines min/max as macros, so std::min/std::max do not
         * compile here. Plain comparisons instead. */
        std::vector<int> counts = slotCountsPerShard(num_shards);
        int smallest = counts[0], largest = counts[0];
        for (int count : counts) {
            if (count < smallest) smallest = count;
            if (count > largest) largest = count;
        }
        EXPECT_GT(smallest, 0) << "shards " << num_shards;
        EXPECT_LE(largest - smallest, 1) << "shards " << num_shards;
    }
}

TEST_F(SlotShardTest, IdentityWhenShardPerSlot) {
    slotShardInit(CLUSTER_SLOTS);

    EXPECT_EQ(slotShardCount(), CLUSTER_SLOTS);
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        ASSERT_EQ(slotToShard(slot), slot);
    }
}

/* Out-of-range shard counts clamp instead of writing outside the map. Run under
 * ASan to also cover the "no out-of-bounds write" half of this. */
TEST_F(SlotShardTest, ClampsOutOfRange) {
    for (int num_shards : {0, -1, -CLUSTER_SLOTS}) {
        slotShardInit(num_shards);
        EXPECT_EQ(slotShardCount(), 1) << "requested " << num_shards;
        EXPECT_EQ(slotToShard(0), 0);
        EXPECT_EQ(slotToShard(CLUSTER_SLOTS - 1), 0);
    }

    for (int num_shards : {SLOT_SHARD_MAX + 1, SLOT_SHARD_MAX * 2}) {
        slotShardInit(num_shards);
        EXPECT_EQ(slotShardCount(), SLOT_SHARD_MAX) << "requested " << num_shards;
        EXPECT_EQ(slotToShard(CLUSTER_SLOTS - 1), CLUSTER_SLOTS - 1);
    }
}

/* Re-partitioning must leave no stale entries behind, which is what makes
 * `CONFIG SET shard-threads` honest about the data structure. */
TEST_F(SlotShardTest, ReinitRepartitions) {
    slotShardInit(4);
    ASSERT_EQ(slotShardCount(), 4);
    ASSERT_EQ(slotToShard(CLUSTER_SLOTS - 1), 3);

    slotShardInit(1);
    EXPECT_EQ(slotShardCount(), 1);
    for (int slot = 0; slot < CLUSTER_SLOTS; slot++) {
        ASSERT_EQ(slotToShard(slot), 0) << "slot " << slot;
    }
}
