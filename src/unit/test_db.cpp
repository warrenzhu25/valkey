/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "cluster.h"
#include "server.h"
}

class DbSlotCacheTest : public ::testing::Test {
  protected:
    int saved_cluster_enabled;
    int saved_shard_threads_num;
    int saved_enable_debug_assert;
    client *saved_current_client;

    void SetUp() override {
        saved_cluster_enabled = server.cluster_enabled;
        saved_shard_threads_num = server.shard_threads_num;
        saved_enable_debug_assert = server.enable_debug_assert;
        saved_current_client = server_current_client;
    }

    void TearDown() override {
        server.cluster_enabled = saved_cluster_enabled;
        server.shard_threads_num = saved_shard_threads_num;
        server.enable_debug_assert = saved_enable_debug_assert;
        server_current_client = saved_current_client;
    }
};

TEST_F(DbSlotCacheTest, StandaloneExecutingCommandReusesCachedSlot) {
    sds key = sdsnew("cached-slot-key");
    int hashed_slot = keyHashSlot(key, (int)sdslen(key));
    int cached_slot = (hashed_slot + 1) % CLUSTER_SLOTS;
    client c = {0};
    c.slot = cached_slot;
    c.flag.executing_command = 1;

    server.cluster_enabled = 0;
    server.shard_threads_num = 4;
    server.enable_debug_assert = 0;
    server_current_client = &c;

    EXPECT_NE(hashed_slot, cached_slot);
    EXPECT_EQ(getKVStoreIndexForKey(key), cached_slot);

    sdsfree(key);
}
