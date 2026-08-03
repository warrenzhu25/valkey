/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef DRAGONFLY_BPTREE_ORDERED_INDEX_H
#define DRAGONFLY_BPTREE_ORDERED_INDEX_H

/* fbtree (flat B+ tree) backend for the OrderedIndex interface.
 *
 * This file declares the fbtree-specific implementations of all OrderedIndex
 * operations. These are called by ordered_index.c (the dispatch layer)
 * and should not be called directly.
 *
 * The fbtree stores [8-byte normalized score][element] as its key, enabling
 * lexicographic byte comparison to match numeric score ordering. */

#include "ordered_index.h"
#ifdef __cplusplus
extern "C" {
#endif

/* Lifecycle */
OrderedIndex *dragonfly_bptreeOICreate(void);
void dragonfly_bptreeOIFree(OrderedIndex *oi);

/* Modification */
OrderedIndexItem *dragonfly_bptreeOIInsert(OrderedIndex *oi, double score, const char *ele, size_t len);
void dragonfly_bptreeOIDelete(OrderedIndex *oi, OrderedIndexItem *item);
OrderedIndexItem *dragonfly_bptreeOIUpdateScore(OrderedIndex *oi, OrderedIndexItem *item, double newscore);
OrderedIndexItem *dragonfly_bptreeOIGetFirst(OrderedIndex *oi);
OrderedIndexItem *dragonfly_bptreeOIGetLast(OrderedIndex *oi);
OrderedIndexItem *dragonfly_bptreeOIPopFirst(OrderedIndex *oi);
OrderedIndexItem *dragonfly_bptreeOIPopLast(OrderedIndex *oi);
void dragonfly_bptreeOIFreeItem(OrderedIndexItem *item);
OrderedIndexItem *dragonfly_bptreeOICreateDetached(double score, const char *ele, size_t len);
void dragonfly_bptreeOIDetachedSetScore(OrderedIndexItem *item, double score);
OrderedIndexItem *dragonfly_bptreeOIInsertDetached(OrderedIndex *oi, OrderedIndexItem *item);
unsigned long dragonfly_bptreeOIDeleteRangeByScore(OrderedIndex *oi, double min, double max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx);
unsigned long dragonfly_bptreeOIDeleteRangeByIndex(OrderedIndex *oi, unsigned long start, unsigned long end, OrderedIndexOnDelete on_delete, void *ctx);
unsigned long dragonfly_bptreeOIDeleteRangeByLex(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx);

/* Query */
unsigned long dragonfly_bptreeOILength(OrderedIndex *oi);
OrderedIndexItem *dragonfly_bptreeOIGetByIndex(OrderedIndex *oi, unsigned long index);
unsigned long dragonfly_bptreeOIGetIndex(OrderedIndex *oi, const OrderedIndexItem *item);
void dragonfly_bptreeOIGetElementRaw(const OrderedIndexItem *item, const char **ptr, size_t *len);
double dragonfly_bptreeOIGetScore(const OrderedIndexItem *item);
unsigned long dragonfly_bptreeOICountScoreRange(OrderedIndex *oi, double min, double max, int min_ex, int max_ex);
unsigned long dragonfly_bptreeOICountLexRange(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex);

/* Iterator */
void dragonfly_bptreeOIInitIterator(OrderedIndexIterator *iter, OrderedIndex *oi);
void dragonfly_bptreeOIResetIterator(OrderedIndexIterator *iter);
OrderedIndexItem *dragonfly_bptreeOINext(OrderedIndexIterator *iter);
OrderedIndexItem *dragonfly_bptreeOIPrev(OrderedIndexIterator *iter);
void dragonfly_bptreeOISeekToIndex(OrderedIndexIterator *iter, unsigned long index);
void dragonfly_bptreeOISeekToScoreRange(OrderedIndexIterator *iter, double min, double max, int min_ex, int max_ex, long offset);
void dragonfly_bptreeOISeekToLexRange(OrderedIndexIterator *iter, const_sds min, const_sds max, int min_ex, int max_ex, long offset);

/* Memory */
void dragonfly_bptreeOIDismissMemory(OrderedIndex *oi);
size_t dragonfly_bptreeOIEstimateMemory(OrderedIndex *oi, size_t sample_size);

/* Defrag */
OrderedIndex *dragonfly_bptreeOIDefragInternals(OrderedIndex *oi, void *(*defragfn)(void *));
unsigned long dragonfly_bptreeOIScanDefrag(OrderedIndex *oi, unsigned long cursor, OrderedIndexDefragCallback callback, void *ctx, void *(*defragfn)(void *));

/* Debug */
int dragonfly_bptreeOIVerifyIntegrity(OrderedIndex *oi, char *errmsg, size_t errmsg_len);

#endif /* DRAGONFLY_BPTREE_ORDERED_INDEX_H */
#ifdef __cplusplus
}
#endif
