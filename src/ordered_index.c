/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* OrderedIndex implementation — delegates to the fbtree backend. */

#include "ordered_index.h"

#include "dragonfly_bptree_ordered_index.h"

/* Lifecycle */

OrderedIndex *orderedIndexCreate(void) {
    return dragonfly_bptreeOICreate();
}

void orderedIndexFree(OrderedIndex *oi) {
    dragonfly_bptreeOIFree(oi);
}

/* Modification */

OrderedIndexItem *orderedIndexInsert(OrderedIndex *oi, double score, const char *ele, size_t len) {
    return dragonfly_bptreeOIInsert(oi, score, ele, len);
}

void orderedIndexDelete(OrderedIndex *oi, OrderedIndexItem *item) {
    dragonfly_bptreeOIDelete(oi, item);
}

OrderedIndexItem *orderedIndexUpdateScore(OrderedIndex *oi, OrderedIndexItem *item, double newscore) {
    return dragonfly_bptreeOIUpdateScore(oi, item, newscore);
}

OrderedIndexItem *orderedIndexPopFirst(OrderedIndex *oi) {
    return dragonfly_bptreeOIPopFirst(oi);
}

OrderedIndexItem *orderedIndexPopLast(OrderedIndex *oi) {
    return dragonfly_bptreeOIPopLast(oi);
}

void orderedIndexFreeItem(OrderedIndexItem *item) {
    dragonfly_bptreeOIFreeItem(item);
}

OrderedIndexItem *orderedIndexCreateDetached(double score, const char *ele, size_t len) {
    return dragonfly_bptreeOICreateDetached(score, ele, len);
}

void orderedIndexDetachedSetScore(OrderedIndexItem *item, double score) {
    dragonfly_bptreeOIDetachedSetScore(item, score);
}

OrderedIndexItem *orderedIndexInsertDetached(OrderedIndex *oi, OrderedIndexItem *item) {
    return dragonfly_bptreeOIInsertDetached(oi, item);
}

unsigned long orderedIndexDeleteRangeByScore(OrderedIndex *oi, double min, double max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx) {
    return dragonfly_bptreeOIDeleteRangeByScore(oi, min, max, min_ex, max_ex, on_delete, ctx);
}

unsigned long orderedIndexDeleteRangeByIndex(OrderedIndex *oi, unsigned long start, unsigned long end, OrderedIndexOnDelete on_delete, void *ctx) {
    return dragonfly_bptreeOIDeleteRangeByIndex(oi, start, end, on_delete, ctx);
}

unsigned long orderedIndexDeleteRangeByLex(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx) {
    return dragonfly_bptreeOIDeleteRangeByLex(oi, min, max, min_ex, max_ex, on_delete, ctx);
}

/* Query */

unsigned long orderedIndexLength(OrderedIndex *oi) {
    return dragonfly_bptreeOILength(oi);
}

OrderedIndexItem *orderedIndexGetByIndex(OrderedIndex *oi, unsigned long index) {
    return dragonfly_bptreeOIGetByIndex(oi, index);
}

OrderedIndexItem *orderedIndexGetFirst(OrderedIndex *oi) {
    return dragonfly_bptreeOIGetFirst(oi);
}

OrderedIndexItem *orderedIndexGetLast(OrderedIndex *oi) {
    return dragonfly_bptreeOIGetLast(oi);
}

unsigned long orderedIndexGetIndex(OrderedIndex *oi, const OrderedIndexItem *item) {
    return dragonfly_bptreeOIGetIndex(oi, item);
}

void orderedIndexGetElementRaw(const OrderedIndexItem *item, const char **ptr, size_t *len) {
    dragonfly_bptreeOIGetElementRaw(item, ptr, len);
}

double orderedIndexGetScore(const OrderedIndexItem *item) {
    return dragonfly_bptreeOIGetScore(item);
}

unsigned long orderedIndexCountScoreRange(OrderedIndex *oi, double min, double max, int min_ex, int max_ex) {
    return dragonfly_bptreeOICountScoreRange(oi, min, max, min_ex, max_ex);
}

unsigned long orderedIndexCountLexRange(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex) {
    return dragonfly_bptreeOICountLexRange(oi, min, max, min_ex, max_ex);
}

/* Iterator */

void orderedIndexInitIterator(OrderedIndexIterator *iter, OrderedIndex *oi) {
    dragonfly_bptreeOIInitIterator(iter, oi);
}

void orderedIndexResetIterator(OrderedIndexIterator *iter) {
    dragonfly_bptreeOIResetIterator(iter);
}

OrderedIndexItem *orderedIndexNext(OrderedIndexIterator *iter) {
    return dragonfly_bptreeOINext(iter);
}

OrderedIndexItem *orderedIndexPrev(OrderedIndexIterator *iter) {
    return dragonfly_bptreeOIPrev(iter);
}

void orderedIndexSeekToIndex(OrderedIndexIterator *iter, unsigned long index) {
    dragonfly_bptreeOISeekToIndex(iter, index);
}

void orderedIndexSeekToScoreRange(OrderedIndexIterator *iter, double min, double max, int min_ex, int max_ex, long offset) {
    dragonfly_bptreeOISeekToScoreRange(iter, min, max, min_ex, max_ex, offset);
}

void orderedIndexSeekToLexRange(OrderedIndexIterator *iter, const_sds min, const_sds max, int min_ex, int max_ex, long offset) {
    dragonfly_bptreeOISeekToLexRange(iter, min, max, min_ex, max_ex, offset);
}

/* Memory */

void orderedIndexDismissMemory(OrderedIndex *oi) {
    dragonfly_bptreeOIDismissMemory(oi);
}

size_t orderedIndexEstimateMemory(OrderedIndex *oi, size_t sample_size) {
    return dragonfly_bptreeOIEstimateMemory(oi, sample_size);
}

OrderedIndex *orderedIndexDefragInternals(OrderedIndex *oi, void *(*defragfn)(void *)) {
    return dragonfly_bptreeOIDefragInternals(oi, defragfn);
}

unsigned long orderedIndexScanDefrag(OrderedIndex *oi, unsigned long cursor, OrderedIndexDefragCallback callback, void *ctx, void *(*defragfn)(void *)) {
    return dragonfly_bptreeOIScanDefrag(oi, cursor, callback, ctx, defragfn);
}

/* Not declared in ordered_index.h — debug-only introspection. */
int orderedIndexGetDepth(OrderedIndex *oi) {
    (void)oi;
    return 0; /* TODO: expose fbtree depth */
}
