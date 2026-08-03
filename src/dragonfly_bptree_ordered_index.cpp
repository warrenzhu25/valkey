/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* fbtree adapter for OrderedIndex interface.
 * Maps OrderedIndex operations to fbtree calls, handling score normalization
 * and [score][element] packing/unpacking.
 *
 * The fbtree stores packed keys: [8-byte normalized score][element bytes].
 * Each packed sds is marked via aux bit 0 so the hashtable can identify it
 * and hash/compare only the element portion (skipping the score prefix). */

extern "C" {
#include "sds.h"
#include "zmalloc.h"
extern "C" { sds dragonfly_get_shared_minstring(void); sds dragonfly_get_shared_maxstring(void); }
#define UNUSED(V) ((void) V)

#include "ordered_index.h"
#include "endianconv.h"
}




#undef min
#undef max
#include <memory_resource>
#include "dragonfly/bptree_set.h"
#include <cstdio>
#include <algorithm>

struct SdsComparePolicy {
    using KeyT = sds;
    struct KeyCompareTo {
        int operator()(const sds& a, const sds& b) const {
            int cmp = memcmp(a, b, std::min(sdslen(a), sdslen(b)));
            if (cmp != 0) return cmp < 0 ? -1 : 1;
            if (sdslen(a) < sdslen(b)) return -1;
            if (sdslen(a) > sdslen(b)) return 1;
            return 0;
        }
    };
};

class ValkeyMemoryResource : public std::pmr::memory_resource {
protected:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override { return zmalloc(bytes); }
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override { zfree(p); }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};
static ValkeyMemoryResource valkey_mr;
extern "C" { void init_dragonfly_bptree_mr() __attribute__((constructor)); void init_dragonfly_bptree_mr() { dfly::InitTLStatelessAllocMR(&valkey_mr); } }

using fbtreeIndex = dfly::BPTree<sds, SdsComparePolicy>;

struct fbtreeIterator {
    fbtreeIndex* tree;
    fbtreeIndex::BPTreePath path;
    bool is_initialized;
    bool is_eof;
    bool is_dead;
};
static_assert(sizeof(OrderedIndexIterator) >= sizeof(fbtreeIterator), "Need more size");

inline fbtreeIndex* fbtreeCreate() { return new fbtreeIndex(); }
inline void fbtreeFree(fbtreeIndex* fbt) { delete fbt; }
inline sds fbtreeInsert(fbtreeIndex* fbt, sds item) { fbt->Insert(item); return item; }
inline void fbtreeDelete(fbtreeIndex* fbt, const_sds item) { fbtreeIndex::BPTreePath p = fbt->GEQ((sds)item); if (!p.Empty() && sdscmp(p.Terminal(), (sds)item) == 0) fbt->Delete(p); }
inline sds fbtreePeekMin(fbtreeIndex* fbt) { if (fbt->Empty()) return NULL; fbtreeIndex::BPTreePath p; fbt->ToRank(0, &p); return p.Terminal(); }
inline sds fbtreePeekMax(fbtreeIndex* fbt) { if (fbt->Empty()) return NULL; fbtreeIndex::BPTreePath p; fbt->ToRank(fbt->Size()-1, &p); return p.Terminal(); }
inline sds fbtreePopMin(fbtreeIndex* fbt) { if (fbt->Empty()) return NULL; fbtreeIndex::BPTreePath p; fbt->ToRank(0, &p); sds v = p.Terminal(); fbt->Delete(p); return v; }
inline sds fbtreePopMax(fbtreeIndex* fbt) { if (fbt->Empty()) return NULL; fbtreeIndex::BPTreePath p; fbt->ToRank(fbt->Size()-1, &p); sds v = p.Terminal(); fbt->Delete(p); return v; }
inline unsigned long fbtreeLength(fbtreeIndex* fbt) { return fbt->Size(); }
inline sds fbtreeGetAtRank(fbtreeIndex* fbt, unsigned long rank) { if (rank >= fbt->Size()) return NULL; fbtreeIndex::BPTreePath p; fbt->ToRank(rank, &p); return p.Terminal(); }
inline long fbtreeGetRankOfItem(fbtreeIndex* fbt, sds item) { auto r = fbt->GetRank(item); if (!r) return -1; return *r; }
inline fbtreeIndex* fbtreeIteratorGetIndex(fbtreeIterator* iter) { return iter->tree; }

inline unsigned long fbtreeDeleteRangeByRank(fbtreeIndex* fbt, unsigned long start, unsigned long end, void (*cb)(sds, void*), void* ctx) {
    if (start >= fbt->Size()) return 0;
    if (end >= fbt->Size()) end = fbt->Size() - 1;
    if (start > end) return 0;
    unsigned long deleted = end - start + 1;
    for (unsigned long i = 0; i < deleted; i++) {
        fbtreeIndex::BPTreePath p; fbt->ToRank(start, &p);
        sds item = p.Terminal();
        if (cb) cb((sds)item, ctx);
        fbt->Delete(p);
    }
    return deleted;
}
inline unsigned long fbtreeDeleteRangeByScore(fbtreeIndex* fbt, const char* min_sortable, const char* max_sortable, int min_ex, int max_ex, void (*cb)(sds, void*), void* ctx) {
    uint64_t min_native, max_native;
    memcpy(&min_native, min_sortable, 8);
    memcpy(&max_native, max_sortable, 8);
    
    if (min_ex) {
        uint64_t n = ntohu64(min_native);
        n++;
        min_native = htonu64(n);
    }
    
    uint64_t max_search;
    if (!max_ex) {
        uint64_t n = ntohu64(max_native);
        n++;
        max_search = htonu64(n);
    } else {
        max_search = max_native;
    }
    
    sds min_tmp = sdsnewlen(&min_native, 8);
    fbtreeIndex::BPTreePath p_start = fbt->GEQ(min_tmp);
    sdsfree(min_tmp);
    unsigned long start = p_start.Empty() ? fbt->Size() : p_start.Rank();
    
    sds max_tmp = sdsnewlen(&max_search, 8);
    fbtreeIndex::BPTreePath p_end = fbt->GEQ(max_tmp);
    sdsfree(max_tmp);
    unsigned long end_bound = p_end.Empty() ? fbt->Size() : p_end.Rank();
    
    if (start >= end_bound) return 0;
    return fbtreeDeleteRangeByRank(fbt, start, end_bound - 1, cb, ctx);
}
inline unsigned long fbtreeDeleteRangeByValue(fbtreeIndex* fbt, sds min_packed, sds max_packed, int min_ex, int max_ex, void (*cb)(sds, void*), void* ctx) {
    fbtreeIndex::BPTreePath p = fbt->GEQ(min_packed); if (p.Empty()) return 0;
    unsigned long start = p.Rank();
    if (min_ex && p.Terminal() && sdscmp(p.Terminal(), min_packed) == 0) start++;
    fbtreeIndex::BPTreePath p2 = fbt->LEQ(max_packed); if (p2.Empty()) return 0;
    unsigned long end = p2.Rank();
    if (max_ex && p2.Terminal() && sdscmp(p2.Terminal(), max_packed) == 0) { if (end == 0) return 0; end--; }
    if (start > end) return 0; return fbtreeDeleteRangeByRank(fbt, start, end, cb, ctx);
}

inline void fbtreeInitIterator(fbtreeIterator* iter, fbtreeIndex* fbt) {
    iter->tree = fbt; iter->is_initialized = false; iter->is_eof = false; iter->is_dead = false; iter->path.Clear();
}
inline void fbtreeResetIterator(fbtreeIterator* iter) {
    iter->is_initialized = true; iter->is_eof = true; iter->is_dead = true; iter->path.Clear();
}

inline bool fbtreeNext(fbtreeIterator* iter, const_sds* pos) {
    if (iter->is_dead) return false;
    if (iter->tree->Empty()) return false;
    if (!iter->is_initialized) {
        iter->tree->ToRank(0, &iter->path);
        iter->is_initialized = true; iter->is_eof = false;
    }
    if (iter->is_eof || iter->path.Empty()) return false;
    *pos = iter->path.Terminal();
    if (!iter->path.Next()) iter->is_eof = true;
    return true;
}

inline bool fbtreePrev(fbtreeIterator* iter, const_sds* pos) {
    if (iter->is_dead) return false;
    if (iter->tree->Empty()) return false;
    if (!iter->is_initialized) {
        iter->path.Clear(); iter->is_initialized = true; iter->is_eof = true;
    }
    if (iter->is_eof) {
        iter->path.Clear();
        iter->tree->ToRank(iter->tree->Size()-1, &iter->path);
        iter->is_eof = false;
        if (iter->path.Empty()) return false;
    } else {
        if (!iter->path.Prev()) return false;
    }
    *pos = iter->path.Terminal();
    return true;
}

inline void fbtreeSeekToRank(fbtreeIterator* iter, unsigned long rank) {
    if (rank >= iter->tree->Size()) { 
        iter->path.Clear(); iter->is_initialized = true; iter->is_eof = true; return; 
    }
    iter->path.Clear(); iter->tree->ToRank(rank, &iter->path); 
    iter->is_initialized = true; iter->is_eof = false;
}

inline void fbtreeSeekToValue(const_sds item, fbtreeIterator* iter) {
    iter->path = iter->tree->GEQ((sds)item);
    iter->is_initialized = true; iter->is_eof = iter->path.Empty();
}


inline void fbtreeDismissMemory(fbtreeIndex* fbt) {}
inline unsigned long fbtreeDefragScan(fbtreeIndex* oi, unsigned long cursor, void (*callback)(char*, char*, void*), void* ctx, void* (*defragfn)(void *)) { return 0; }
inline int fbtreeDebugValidate(fbtreeIndex* oi, bool verbose) { return 1; }


inline long fbtreeSeekToScore(const char* sortable, fbtreeIterator* iter) {
    sds tmp = sdsnewlen(sortable, 8); iter->path = iter->tree->GEQ(tmp); sdsfree(tmp);
    iter->is_initialized = true; iter->is_eof = iter->path.Empty();
    if (iter->path.Empty()) return iter->tree->Size();
    return iter->path.Rank();
}




#define SCORE_SIZE 8

/* ========== Score Normalization ==========
 * Converts IEEE 754 double to a sortable 8-byte big-endian representation.
 * Lexicographic byte comparison matches numeric order after transformation. */

static inline uint64_t scoreToSortable(double score) {
    uint64_t bits;
    memcpy(&bits, &score, sizeof(bits));
    if (bits & (1ULL << 63)) {
        bits = ~bits;
    } else {
        bits ^= (1ULL << 63);
    }
    return htonu64(bits);
}

static inline double sortableToScore(uint64_t be) {
    uint64_t bits = ntohu64(be);
    if (bits & (1ULL << 63)) {
        bits ^= (1ULL << 63);
    } else {
        bits = ~bits;
    }
    double score;
    memcpy(&score, &bits, sizeof(score));
    return score;
}

/* Pack score and element into sds: [8-byte sortable score][element] */
static sds packScoreElement(double score, const char *ele, size_t ele_len) {
    uint64_t sortable = scoreToSortable(score);
    size_t total = SCORE_SIZE + ele_len;
    sds packed = sdsnewlen(NULL, total);
    memcpy(packed, &sortable, SCORE_SIZE);
    memcpy(packed + SCORE_SIZE, ele, ele_len);
    return packed;
}

static inline const char *unpackElement(const_sds packed, size_t *len) {
    *len = sdslen(packed) - SCORE_SIZE;
    return packed + SCORE_SIZE;
}

static inline double unpackScore(const_sds packed) {
    uint64_t sortable;
    memcpy(&sortable, packed, SCORE_SIZE);
    return sortableToScore(sortable);
}

/* ========== Lifecycle ========== */

extern "C" {
OrderedIndex *dragonfly_bptreeOICreate(void) {
    return (OrderedIndex *)fbtreeCreate();
}

void dragonfly_bptreeOIFree(OrderedIndex *oi) {
    fbtreeFree((fbtreeIndex *)oi);
}

/* ========== Modification ========== */

OrderedIndexItem *dragonfly_bptreeOIInsert(OrderedIndex *oi, double score, const char *ele, size_t len) {
    sds packed = packScoreElement(score, ele, len);
    return (OrderedIndexItem *)fbtreeInsert((fbtreeIndex *)oi, packed);
}

void dragonfly_bptreeOIDelete(OrderedIndex *oi, OrderedIndexItem *item) {
    fbtreeDelete((fbtreeIndex *)oi, (sds)item);
}

OrderedIndexItem *dragonfly_bptreeOIUpdateScore(OrderedIndex *oi, OrderedIndexItem *item, double newscore) {
    const_sds packed = (const_sds)item;
    size_t ele_len;
    const char *ele = unpackElement(packed, &ele_len);
    sds new_packed = packScoreElement(newscore, ele, ele_len);
    fbtreeDelete((fbtreeIndex *)oi, packed);
    sdsfree((sds)packed);
    return (OrderedIndexItem *)fbtreeInsert((fbtreeIndex *)oi, new_packed);
}

OrderedIndexItem *dragonfly_bptreeOIGetFirst(OrderedIndex *oi) {
    return (OrderedIndexItem *)fbtreePeekMin((fbtreeIndex *)oi);
}

OrderedIndexItem *dragonfly_bptreeOIGetLast(OrderedIndex *oi) {
    return (OrderedIndexItem *)fbtreePeekMax((fbtreeIndex *)oi);
}

OrderedIndexItem *dragonfly_bptreeOIPopFirst(OrderedIndex *oi) {
    return (OrderedIndexItem *)fbtreePopMin((fbtreeIndex *)oi);
}

OrderedIndexItem *dragonfly_bptreeOIPopLast(OrderedIndex *oi) {
    return (OrderedIndexItem *)fbtreePopMax((fbtreeIndex *)oi);
}

void dragonfly_bptreeOIFreeItem(OrderedIndexItem *item) {
    sdsfree((sds)item);
}

OrderedIndexItem *dragonfly_bptreeOICreateDetached(double score, const char *ele, size_t len) {
    return (OrderedIndexItem *)packScoreElement(score, ele, len);
}

void dragonfly_bptreeOIDetachedSetScore(OrderedIndexItem *item, double score) {
    uint64_t sortable = scoreToSortable(score);
    memcpy((char *)item, &sortable, SCORE_SIZE);
}

OrderedIndexItem *dragonfly_bptreeOIInsertDetached(OrderedIndex *oi, OrderedIndexItem *item) {
    return (OrderedIndexItem *)fbtreeInsert((fbtreeIndex *)oi, (sds)item);
}

/* Helper: range delete with on_delete callback.
 * The fbtree itself frees the sds after this callback returns,
 * so we must NOT free here -- only notify the caller. */
typedef struct {
    OrderedIndexOnDelete on_delete;
    void *user_ctx;
} rangeDeleteArgs;

static void rangeDeleteCallback(sds item, void *ctx) {
    rangeDeleteArgs *args = (rangeDeleteArgs *)ctx;
    if (args->on_delete) {
        args->on_delete((OrderedIndexItem *)item, args->user_ctx);
    }
}

unsigned long dragonfly_bptreeOIDeleteRangeByScore(OrderedIndex *oi, double min, double max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx) {
    uint64_t min_sortable = scoreToSortable(min);
    uint64_t max_sortable = scoreToSortable(max);
    rangeDeleteArgs args = {on_delete, ctx};
    return fbtreeDeleteRangeByScore((fbtreeIndex *)oi, (const char *)&min_sortable, (const char *)&max_sortable, min_ex, max_ex, rangeDeleteCallback, &args);
}

unsigned long dragonfly_bptreeOIDeleteRangeByIndex(OrderedIndex *oi, unsigned long start, unsigned long end, OrderedIndexOnDelete on_delete, void *ctx) {
    rangeDeleteArgs args = {on_delete, ctx};
    return fbtreeDeleteRangeByRank((fbtreeIndex *)oi, start, end, rangeDeleteCallback, &args);
}

unsigned long dragonfly_bptreeOIDeleteRangeByLex(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex, OrderedIndexOnDelete on_delete, void *ctx) {
    /* Lex range: all items share the same score (zset lex semantics). */
    fbtreeIndex *fbt = (fbtreeIndex *)oi;
    if (fbtreeLength(fbt) == 0) return 0;
    if (max == dragonfly_get_shared_minstring() || min == dragonfly_get_shared_maxstring()) return 0;

    const_sds first = fbtreeGetAtRank(fbt, 0);
    if (!first) return 0;
    uint64_t score_prefix;
    memcpy(&score_prefix, first, SCORE_SIZE);

    sds min_packed, max_packed;

    if (min == dragonfly_get_shared_minstring()) {
        min_packed = sdsnewlen(NULL, SCORE_SIZE);
        memcpy(min_packed, &score_prefix, SCORE_SIZE);
    } else {
        min_packed = sdsempty();
        min_packed = sdsMakeRoomFor(min_packed, SCORE_SIZE + sdslen(min));
        memcpy(min_packed, &score_prefix, SCORE_SIZE);
        memcpy(min_packed + SCORE_SIZE, min, sdslen(min));
        sdsIncrLen(min_packed, SCORE_SIZE + sdslen(min));
    }

    if (max == dragonfly_get_shared_maxstring()) {
        max_packed = sdsnewlen(NULL, SCORE_SIZE + 1);
        memcpy(max_packed, &score_prefix, SCORE_SIZE);
        memset(max_packed + SCORE_SIZE, 0xFF, 1);
    } else {
        max_packed = sdsempty();
        max_packed = sdsMakeRoomFor(max_packed, SCORE_SIZE + sdslen(max));
        memcpy(max_packed, &score_prefix, SCORE_SIZE);
        memcpy(max_packed + SCORE_SIZE, max, sdslen(max));
        sdsIncrLen(max_packed, SCORE_SIZE + sdslen(max));
    }

    rangeDeleteArgs args = {on_delete, ctx};
    unsigned long deleted = fbtreeDeleteRangeByValue(fbt, min_packed, max_packed, min_ex, max_ex, rangeDeleteCallback, &args);
    sdsfree(min_packed);
    sdsfree(max_packed);
    return deleted;
}

/* ========== Query ========== */

unsigned long dragonfly_bptreeOILength(OrderedIndex *oi) {
    return fbtreeLength((fbtreeIndex *)oi);
}

OrderedIndexItem *dragonfly_bptreeOIGetByIndex(OrderedIndex *oi, unsigned long index) {
    return (OrderedIndexItem *)fbtreeGetAtRank((fbtreeIndex *)oi, index);
}

unsigned long dragonfly_bptreeOIGetIndex(OrderedIndex *oi, const OrderedIndexItem *item) {
    long rank = fbtreeGetRankOfItem((fbtreeIndex *)oi, (sds)item);
    return (unsigned long)rank;
}

void dragonfly_bptreeOIGetElementRaw(const OrderedIndexItem *item, const char **ptr, size_t *len) {
    *ptr = unpackElement((const_sds)item, len);
}

double dragonfly_bptreeOIGetScore(const OrderedIndexItem *item) {
    return unpackScore((const_sds)item);
}

unsigned long dragonfly_bptreeOICountScoreRange(OrderedIndex *oi, double min, double max, int min_ex, int max_ex) {
    /* Use iterator to count — seek to start, iterate until past end */
    fbtreeIterator iter;
    fbtreeInitIterator(&iter, (fbtreeIndex *)oi);
    uint64_t min_sortable = scoreToSortable(min);
    if (min_ex) {
        uint64_t native = ntohu64(min_sortable);
        native++;
        min_sortable = htonu64(native);
    }
    fbtreeSeekToScore((const char *)&min_sortable, &iter);

    unsigned long count = 0;
    const_sds pos;
    while (fbtreeNext(&iter, (const_sds*)&pos)) {
        double score = unpackScore(pos);
        if (max_ex ? score >= max : score > max) break;
        count++;
    }
    return count;
}

/* ========== Range Seek Helpers ========== */

/* Unified seek helper: position iterator at a range boundary.
 *
 * After return, the iterator is positioned such that:
 *   - Forward (reverse=0): fbtreeNext() returns the first in-range element
 *   - Reverse (reverse=1): fbtreePrev() returns the last in-range element
 *
 * 'packed' is the [score][element] boundary value to seek to.
 * 'inclusive' means the boundary element itself is in-range. */
static void seekForBound(fbtreeIterator *fbt_iter, sds packed, int reverse, int inclusive) {
    fbtreeSeekToValue(packed, fbt_iter);

    if (!reverse && !inclusive) {
        /* Forward + exclusive: if positioned at exact match, advance past it. */
        const_sds pos;
        if (fbtreeNext(fbt_iter, (const_sds*)&pos)) {
            if (sdscmp(pos, packed) != 0) {
                /* First element > bound — re-seek so next() returns it. */
                fbtreeSeekToValue(pos, fbt_iter);
            }
            /* Else: was exact match, consumed it. next() returns next element. */
        }
    } else if (reverse && inclusive) {
        /* Reverse + inclusive: seek is at first >= bound.
         * If bound exists, advance past so prev() returns bound.
         * If not, re-seek to first > bound so prev() returns last < bound. */
        const_sds pos;
        if (fbtreeNext(fbt_iter, (const_sds*)&pos)) {
            if (sdscmp(pos, packed) != 0) {
                /* Not exact match — re-seek so prev() returns last < bound */
                fbtreeSeekToValue(pos, fbt_iter);
            }
            /* Else: exact match consumed, prev() now returns bound. */
        }
    }
    /* Forward + inclusive: seek already at first >= bound. next() returns it. ✓ */
    /* Reverse + exclusive: seek at first >= bound. prev() returns last < bound. ✓ */
}

/* Skip N elements in the given direction. */
static void skipElements(fbtreeIterator *fbt_iter, long count, int reverse) {
    const_sds pos;
    for (long i = 0; i < count; i++) {
        if (reverse) {
            if (!fbtreePrev(fbt_iter, (const_sds*)&pos)) return;
        } else {
            if (!fbtreeNext(fbt_iter, (const_sds*)&pos)) return;
        }
    }
}

/* Pack a lex element with a score prefix into a temporary sds for seeking. */
static sds packLexBound(uint64_t score_prefix, const_sds element) {
    sds packed = sdsempty();
    packed = sdsMakeRoomFor(packed, SCORE_SIZE + sdslen(element));
    memcpy(packed, &score_prefix, SCORE_SIZE);
    memcpy(packed + SCORE_SIZE, element, sdslen(element));
    sdsIncrLen(packed, SCORE_SIZE + sdslen(element));
    return packed;
}

unsigned long dragonfly_bptreeOICountLexRange(OrderedIndex *oi, const_sds min, const_sds max, int min_ex, int max_ex) {
    fbtreeIndex *fbt = (fbtreeIndex *)oi;
    unsigned long len = fbtreeLength(fbt);
    if (len == 0) return 0;

    /* Handle sentinels: dragonfly_get_shared_minstring()/maxstring represent -inf/+inf */

        if (min == dragonfly_get_shared_minstring() && max == dragonfly_get_shared_maxstring()) return len;

    if (max == dragonfly_get_shared_minstring() || min == dragonfly_get_shared_maxstring()) return 0;

    const_sds first = fbtreeGetAtRank(fbt, 0);
    if (!first) return 0;
    uint64_t score_prefix;
    memcpy(&score_prefix, first, SCORE_SIZE);

    fbtreeIterator iter;
    fbtreeInitIterator(&iter, fbt);

    /* Seek to min bound */
    if (min == dragonfly_get_shared_minstring()) {
        fbtreeSeekToRank(&iter, 0);
    } else {
        sds packed = packLexBound(score_prefix, min);
        seekForBound(&iter, packed, 0, !min_ex);
        sdsfree(packed);
    }

    unsigned long count = 0;
    const_sds pos;
    while (fbtreeNext(&iter, (const_sds*)&pos)) {
        /* Check max bound */
        if (max != dragonfly_get_shared_maxstring()) {
            size_t ele_len;
            const char *ele = unpackElement(pos, &ele_len);
            int cmp = memcmp(ele, max, ele_len < sdslen(max) ? ele_len : sdslen(max));
            if (cmp == 0) cmp = (int)ele_len - (int)sdslen(max);
            if (max_ex ? cmp >= 0 : cmp > 0) break;
        }
        count++;
    }
    return count;
}

/* ========== Iterator ========== */

void dragonfly_bptreeOIInitIterator(OrderedIndexIterator *iter, OrderedIndex *oi) {
    fbtreeInitIterator((fbtreeIterator *)iter, (fbtreeIndex *)oi);
}

void dragonfly_bptreeOIResetIterator(OrderedIndexIterator *iter) {
    fbtreeResetIterator((fbtreeIterator *)iter);
}

OrderedIndexItem *dragonfly_bptreeOINext(OrderedIndexIterator *iter) {
    const_sds pos;
    if (fbtreeNext((fbtreeIterator *)iter, (const_sds*)&pos)) {
        return (OrderedIndexItem *)pos;
    }
    return NULL;
}

OrderedIndexItem *dragonfly_bptreeOIPrev(OrderedIndexIterator *iter) {
    const_sds pos;
    if (fbtreePrev((fbtreeIterator *)iter, (const_sds*)&pos)) {
        return (OrderedIndexItem *)pos;
    }
    return NULL;
}

void dragonfly_bptreeOISeekToIndex(OrderedIndexIterator *iter, unsigned long index) {
    fbtreeSeekToRank((fbtreeIterator *)iter, index + 1);
}

void dragonfly_bptreeOISeekToScoreRange(OrderedIndexIterator *iter, double min, double max, int min_ex, int max_ex, long offset) {
    fbtreeIterator *fbt_iter = (fbtreeIterator *)iter;
    fbtreeIndex *fbt = fbtreeIteratorGetIndex(fbt_iter);
    if (!fbt) return;

    if (min > max || (min == max && (min_ex || max_ex))) {
        fbtreeResetIterator(fbt_iter);
        return;
    }

    uint64_t sortable;
    if (offset >= 0) {
        sortable = scoreToSortable(min);
        if (min_ex) {
            uint64_t native = ntohu64(sortable);
            native++;
            sortable = htonu64(native);
        }
    } else {
        sortable = scoreToSortable(max);
        if (!max_ex) {
            uint64_t native = ntohu64(sortable);
            native++;
            sortable = htonu64(native);
        }
    }
    unsigned long len = fbtreeLength(fbt);
    long base = fbtreeSeekToScore((const char *)&sortable, fbt_iter);
    long target = offset + base;

    if (target < 0 || (unsigned long)target >= len) {
        fbtreeResetIterator(fbt_iter);
        return;
    }

    /* Validate the element at target is within [min, max]. */
    const_sds item = fbtreeGetAtRank(fbt, (unsigned long)target);
    if (item) {
        double score = unpackScore(item);
        if (score > max || (max_ex && score == max) ||
            score < min || (min_ex && score == min)) {
            fbtreeResetIterator(fbt_iter);
            return;
        }
    }

    /* For reverse (offset<0), fbtreePrev decrements before returning,
     * so position one past the target for prev() to return it. */
    fbtreeSeekToRank(fbt_iter, (unsigned long)target + (offset < 0 ? 1 : 0));
}

/* Unified seek helper: position iterator at a range boundary.
 *
 * After return, the iterator is positioned such that:
 *   - Forward (reverse=0): fbtreeNext() returns the first in-range element
 *   - Reverse (reverse=1): fbtreePrev() returns the last in-range element
 *
 * 'packed' is the [score][element] boundary value to seek to.
 * 'inclusive' means the boundary element itself is in-range. */
void dragonfly_bptreeOISeekToLexRange(OrderedIndexIterator *iter, const_sds min, const_sds max, int min_ex, int max_ex, long offset) {
    fbtreeIterator *fbt_iter = (fbtreeIterator *)iter;
    fbtreeIndex *fbt = fbtreeIteratorGetIndex(fbt_iter);
    if (!fbt || fbtreeLength(fbt) == 0) return;

    /* Get score prefix from first element (all share same score in lex zsets) */
    const_sds first = fbtreeGetAtRank(fbt, 0);
    if (!first) return;
    uint64_t score_prefix;
    memcpy(&score_prefix, first, SCORE_SIZE);

    unsigned long len = fbtreeLength(fbt);
    int reverse = (offset < 0);

    if (!reverse) {
        /* Forward: seek to min bound */
        if (min == dragonfly_get_shared_minstring()) {
            fbtreeSeekToRank(fbt_iter, 0);
        } else {
            sds packed = packLexBound(score_prefix, min);
            seekForBound(fbt_iter, packed, 0, !min_ex);
            sdsfree(packed);
        }
        skipElements(fbt_iter, offset, 0);
    } else {
        /* Reverse: seek to max bound */
        if (max == dragonfly_get_shared_maxstring()) {
            fbtreeSeekToRank(fbt_iter, len);
        } else {
            sds packed = packLexBound(score_prefix, max);
            seekForBound(fbt_iter, packed, 1, !max_ex);
            sdsfree(packed);
        }
        skipElements(fbt_iter, -(offset + 1), 1);
    }
}

/* ========== Memory ========== */

void dragonfly_bptreeOIDismissMemory(OrderedIndex *oi) {
    fbtreeDismissMemory((fbtreeIndex *)oi);
}

size_t dragonfly_bptreeOIEstimateMemory(OrderedIndex *oi, size_t sample_size) {
    /* TODO: implement proper memory estimation by sampling nodes.
     * For now, approximate: each item is ~(SCORE_SIZE + avg_ele_len + sds_header + node_overhead). */
    UNUSED(sample_size);
    unsigned long len = fbtreeLength((fbtreeIndex *)oi);
    /* Rough estimate: 64 bytes per item (sds + node slot overhead) */
    return len * 64;
}

/* ========== Defrag ========== */

OrderedIndex *dragonfly_bptreeOIDefragInternals(OrderedIndex *oi, void *(*defragfn)(void *)) {
    /* fbtree nodes are fixed-size allocations — defrag the index struct itself. */
    void *newptr = defragfn(oi);
    return newptr ? (OrderedIndex *)newptr : oi;
}

/* Wrapper context to bridge fbtree's (sds,sds) callback to OrderedIndexDefragCallback. */
typedef struct {
    OrderedIndexDefragCallback callback;
    void *ctx;
} defragBridgeCtx;

static void defragBridgeCallback(sds old_item, sds new_item, void *ctx) {
    defragBridgeCtx *bridge = (defragBridgeCtx *)ctx;
    bridge->callback((OrderedIndexItem *)old_item, (OrderedIndexItem *)new_item, bridge->ctx);
}

unsigned long dragonfly_bptreeOIScanDefrag(OrderedIndex *oi, unsigned long cursor, OrderedIndexDefragCallback callback, void *ctx, void *(*defragfn)(void *)) {
    defragBridgeCtx bridge = {callback, ctx};
    return fbtreeDefragScan((fbtreeIndex *)oi, cursor, defragBridgeCallback, &bridge, defragfn);
}

/* ========== Debug ========== */

int dragonfly_bptreeOIVerifyIntegrity(OrderedIndex *oi, char *errmsg, size_t errmsg_len) {
    if (fbtreeDebugValidate((fbtreeIndex *)oi, false)) {
        errmsg[0] = '\0';
        return 1;
    }
    snprintf(errmsg, errmsg_len, "fbtree integrity check failed");
    return 0;
}

}
