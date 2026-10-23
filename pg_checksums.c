/*-------------------------------------------------------------------------
 *
 * pg_checksums.c
 *    Implementation of multi-level checksum functionality for PostgreSQL
 *
 * This module provides checksum computation at six distinct levels,
 * each with both physical and logical variants (except column level):
 *
 * 1. PAGE LEVEL (pg_page_checksum):
 *    - Physical only (wraps PostgreSQL's built-in checksums)
 *    - Uses PostgreSQL's page checksum algorithm
 *    - Detects physical page corruption
 *
 * 2. CELL LEVEL (pg_cell_checksum):
 *    - Logical only (depends only on cell value and attribute number)
 *    - NULL values return CHECKSUM_NULL (0xFFFFFFFF)
 *    - Handles all PostgreSQL data types
 *    - Same value in same column -> same checksum
 *
 * 3. TUPLE LEVEL:
 *    A. Physical (pg_tuple_physical_checksum):
 *       - Depends on physical location (block, offset)
 *       - Includes tuple header (optional)
 *       - Changes after VACUUM, CLUSTER, physical moves
 *       - Detects physical tuple corruption
 *
 *    B. Logical (pg_tuple_logical_checksum):
 *       - Hashes canonical column values, without relation OIDs or TIDs
 *       - No primary key required
 *       - Stable across VACUUM, CLUSTER, REINDEX
 *
 * 4. TABLE LEVEL:
 *    A. Physical (pg_table_physical_checksum):
 *       - Aggregates physical tuple checksums
 *       - Order-independent aggregation
 *       - Includes relation OID for uniqueness
 *       - Changes after physical reorganization
 *
 *    B. Logical (pg_table_logical_checksum):
 *       - Aggregates canonical logical row checksums
 *       - No primary key and no relation OID
 *       - Sum-plus-count preserves duplicate rows
 *       - Stable across VACUUM, CLUSTER, REINDEX
 *
 * 5. INDEX LEVEL:
 *    A. Physical (pg_index_physical_checksum):
 *       - Depends on physical index structure
 *       - Supports all index types (B-tree, Hash, GiST, GIN, SP-GiST, BRIN)
 *       - Changes after REINDEX, physical reorganization
 *       - Detects index corruption
 *
 *    B. Logical (pg_index_logical_checksum):
 *       - Derived from visible table rows and the index definition
 *       - Works for every access method
 *       - No heap TIDs or dead entries
 *       - Stable across VACUUM, CLUSTER, REINDEX
 *
 * 6. DATABASE LEVEL:
 *    A. Physical (pg_database_physical_checksum):
 *       - Aggregates physical relation checksums
 *       - Superuser-only for security
 *       - Optional filtering of system catalogs
 *       - Changes after any physical reorganization
 *
 *    B. Logical (pg_database_logical_checksum):
 *       - Aggregates canonical logical relation checksums
 *       - Superuser-only for security
 *       - No primary key requirement
 *       - Optional logical index checksums
 *       - Stable across physical reorganization of entire database
 *
 * Algorithm Details:
 * - Uses FNV-1a 32-bit hash (same as PostgreSQL's built-in checksums)
 * - Hash formula: hash = (hash ^ byte) * FNV_PRIME_32
 * - Different seeds for different contexts ensure uniqueness
 * - Order-independent aggregation ensures stability across scans
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <math.h>
#include "fmgr.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/genam.h"
#include "access/nbtree.h"
#include "access/brin.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_index.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h" 
#include "catalog/pg_inherits.h"
#include "catalog/pg_am.h"          /* *_AM_OID (all supported versions) */
#include "catalog/index.h"          /* IndexGetRelation, BuildIndexInfo, FormIndexDatum */
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

/*
 * The OID of the pg_index_indrelid_index index is exposed differently
 * depending on the release: PostgreSQL 13 and older declare it in
 * catalog/indexing.h, while PostgreSQL 14 and newer generate it into
 * catalog/pg_index_d.h.
 */
#if PG_VERSION_NUM >= 140000
#include "catalog/pg_index_d.h"     /* IndexIndrelidIndexId */
#else
#include "catalog/indexing.h"       /* IndexIndrelidIndexId */
#endif
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#if PG_VERSION_NUM >= 140000
#include "utils/multirangetypes.h"
#endif
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rangetypes.h"
#include "utils/rel.h"
#include "utils/typcache.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"
#include "port/atomics.h"
#include "miscadmin.h"              
#include "storage/checksum.h" 

#include "pg_checksums.h"

PG_MODULE_MAGIC;

/*-------------------------------------------------------------------------
 * Parallelism configuration
 *-------------------------------------------------------------------------
 */

/*
 * pg_checksums_workers - GUC controlling the number of parallel workers
 *
 * A value of 0 (the default) disables parallelism, preserving the original
 * single-backend behavior. A positive value requests a specific number of
 * workers, clamped by max_parallel_workers and by the number of blocks (or
 * relations) being scanned. Parallelism is also skipped when we are already
 * running inside a parallel worker, since workers cannot be nested.
 */
static int pg_checksums_workers = 0;

/*
 * pg_checksums_strict - GUC controlling database-scan skip behavior
 *
 * When true, a relation that cannot be checksummed (no primary key,
 * unsupported index access method, error) aborts the database checksum
 * instead of being skipped with a warning or silently omitted.
 */
static bool pg_checksums_strict = false;

/*
 * Cached type metadata for canonical logical encoding.  Type names and I/O
 * function OIDs are stable until a syscache invalidation, which clears the
 * cache.
 */
typedef struct LogicalTypeInfo
{
    Oid         typid;
    uint32      tag_hash;
    Oid         sendfunc;
    Oid         outfunc;
    bool        has_send;
} LogicalTypeInfo;

static HTAB *logical_type_cache = NULL;

static LogicalTypeInfo *logical_type_info(Oid typid);
static void logical_type_cache_invalidate(Datum arg, int cacheid,
                                          uint32 hashvalue);

/* Upper bound for the GUC; mirrors the sanity limit of parallel workers. */
#define PG_CHECKSUMS_MAX_WORKERS 1024

/* Number of blocks claimed per atomic fetch during a parallel scan. */
#define PG_CHECKSUMS_BLOCK_CHUNK 16

/*
 * _PG_init - extension load-time initialization
 *
 * Registers the GUC variable that controls the degree of parallelism.
 */
void
_PG_init(void)
{
    DefineCustomIntVariable("pg_checksums.workers",
                            "Number of parallel workers for checksum computation.",
                            "Zero disables parallelism, a positive value "
                            "requests a specific number of workers.",
                            &pg_checksums_workers,
                            0, 0, PG_CHECKSUMS_MAX_WORKERS,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_checksums.strict",
                             "Fail instead of skipping relations.",
                             "When enabled, database checksum functions abort "
                             "if a relation cannot be checksummed (missing "
                             "primary key, unsupported index AM, or error).",
                             &pg_checksums_strict,
                             false,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    CacheRegisterSyscacheCallback(TYPEOID, logical_type_cache_invalidate, 0);
}

/*
 * pg_checksums_worker_count - determine how many parallel workers to use
 *
 * Returns 0 when the scan should run in a single backend. This happens
 * when the GUC is non-positive and max_parallel_workers is zero, when we
 * are already inside a parallel worker (nesting is not allowed), or when
 * there is nothing to parallelize.
 */
static int
pg_checksums_worker_count(void)
{
    int         workers = pg_checksums_workers;

    if (workers <= 0)
        return 0;

    if (workers > max_parallel_workers)
        workers = max_parallel_workers;

    /*
     * Custom parallel contexts consume slots from max_worker_processes, not
     * from the per-query max_parallel_workers allowance; clamp to both.
     */
    if (workers > max_worker_processes)
        workers = max_worker_processes;

    /* Parallel workers cannot launch nested parallel workers. */
    if (IsInParallelMode())
        return 0;

    return workers;
}

/*
 * pg_checksums_get_snapshot - return the snapshot used for heap scans
 *
 * Uses the active snapshot when one is available and falls back to the
 * transaction snapshot otherwise. Both the serial and parallel code
 * paths use this so that they observe identical tuple sets.
 */
static Snapshot
pg_checksums_get_snapshot(void)
{
    Snapshot    snapshot = GetActiveSnapshot();

    if (snapshot == NULL)
        snapshot = GetTransactionSnapshot();

    return snapshot;
}

/*-------------------------------------------------------------------------
 * Internal type definitions
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Order-independent aggregation state
 *-------------------------------------------------------------------------
 */

/*
 * ChecksumAccum - running order-independent aggregate of 32-bit hashes
 *
 * To make aggregation fully order- and partition-independent (so that the
 * result is identical regardless of how many parallel workers split the
 * work, or of the physical ordering of the scanned objects), each hash is
 * passed through an avalanche finalizer and then combined with XOR. XOR
 * is commutative and associative, so any partitioning yields the same
 * result. The count is kept separately so that an empty scan can still be
 * distinguished from a non-empty one.
 */
typedef struct ChecksumAccum
{
    uint32      partial;        /* XOR of mixed hashes */
    uint64      count;          /* Number of hashes accumulated */
} ChecksumAccum;

/*
 * ChecksumAccum64 - order-independent aggregate of 64-bit hashes (database)
 */
typedef struct ChecksumAccum64
{
    uint64      partial;        /* XOR of mixed hashes */
    uint64      count;          /* Number of items accumulated */
} ChecksumAccum64;

/*
 * ChecksumLogicalAccum - aggregate for canonical logical checksums
 *
 * Uses addition plus a count so duplicate rows/index entries are preserved.
 * The final value is still a 32-bit FNV-1a checksum.
 */
typedef struct ChecksumLogicalAccum
{
    uint64      sum;
    uint64      count;
} ChecksumLogicalAccum;

/*
 * ChecksumWorkerResult - fixed-size result returned by each worker
 *
 * Each worker writes a single entry of this type into shared memory, so
 * no streaming or variable-sized shared memory is required. For 32-bit
 * scans partial holds the uint32 aggregate zero-extended to 64 bits.
 * found_invalid is set only by logical table scans to report a tuple with
 * a NULL primary key value.  had_error is set by database scans when one or
 * more relations had to be skipped.
 */
typedef struct ChecksumWorkerResult
{
    uint64      partial;
    uint64      count;
    bool        found_invalid;
    bool        had_error;
} ChecksumWorkerResult;

/*
 * ChecksumTaskType - identifies which checksum a parallel scan computes
 */
typedef enum ChecksumTaskType
{
    CHECKSUM_TASK_TABLE_PHYSICAL = 1,
    CHECKSUM_TASK_TABLE_LOGICAL,
    CHECKSUM_TASK_INDEX_PHYSICAL,
    CHECKSUM_TASK_DATABASE_PHYSICAL,
    CHECKSUM_TASK_DATABASE_LOGICAL
} ChecksumTaskType;

/*
 * ChecksumRelationItem - a single relation processed during a database scan
 */
typedef struct ChecksumRelationItem
{
    Oid         reloid;
    char        relkind;
} ChecksumRelationItem;

/*
 * ChecksumParallelState - shared state for a parallel checksum scan
 *
 * This structure lives in dynamic shared memory and is used both by the
 * leader and by the parallel workers. nitems holds the number of items
 * (blocks for table/index scans, relations for database scans) and
 * next_item is the atomic counter used to claim work in a dynamic,
 * self-balancing fashion.
 */
typedef struct ChecksumParallelState
{
    ChecksumTaskType task_type;
    Oid         reloid;
    bool        include_header;
    bool        include_system;
    bool        include_toast;
    uint32      nitems;
    uint32      snapshot_size;
    pg_atomic_uint32 next_item;
} ChecksumParallelState;

/* Keys for the shm_toc used by the parallel scan. */
#define KEY_STATE       0
#define KEY_SNAPSHOT    1
#define KEY_RESULTS     2
#define KEY_ITEMS       3

/*-------------------------------------------------------------------------
 * Helper function declarations
 *-------------------------------------------------------------------------
 */
static uint32 pg_tuple_logical_checksum_internal(Relation rel,
                                                 HeapTuple tuple);
static bool index_supports_checksum(Oid amoid);
static uint64 compute_database_checksum_internal(bool physical,
                                                 bool include_system,
                                                 bool include_toast,
                                                 bool include_indexes);
static uint32 pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value);
static uint32 pg_tuple_physical_checksum_internal(Relation rel, Page page,
                                                  OffsetNumber offnum,
                                                  BlockNumber blkno,
                                                  bool include_header);
static uint32 combine_checksums(uint32 current, uint32 new_val);

/* Privilege, relkind and storage validation helpers */
static void pg_checksums_require_select(Oid reloid);
static void pg_checksums_check_relation_kind(Relation rel, bool allow_toast,
                                             bool allow_partitioned);
static bool pg_checksums_relation_has_storage(Relation rel);
static bool pg_checksums_page_header_sane(Page page);
static void pg_checksums_check_page(Relation rel, BlockNumber blkno, Page page);
static bool pg_checksums_item_is_sane(Page page, ItemId lp);
static void pg_checksums_check_heap_tuple(Relation rel, BlockNumber blkno,
                                         OffsetNumber offnum,
                                         HeapTupleHeader tuple, Size len);
static void scan_relation_raw_pages(Relation rel, BlockNumber start,
                                    BlockNumber end, ChecksumAccum *acc);

/* Table checksum implementations (shared by the SQL wrappers and partitions) */
static uint64 pg_table_physical_checksum_calc(Oid reloid, bool include_header,
                                              bool check_privileges);
static int64 pg_table_logical_checksum_calc(Oid reloid, bool check_privileges,
                                            bool *is_null);

/* Index checksum implementations (shared with the database scan) */
static uint32 pg_index_physical_checksum_calc(Oid indexoid,
                                              bool check_privileges,
                                              bool *is_null);
static uint32 pg_index_logical_checksum_calc(Oid indexoid,
                                             bool check_privileges,
                                             bool *is_null);
static uint32 pg_index_physical_partitioned_checksum(Oid indexoid,
                                                     bool *is_null);
static void pg_index_logical_checksum_accum(Oid indexoid,
                                              bool check_privileges,
                                              ChecksumLogicalAccum *acc,
                                              bool *is_null);

static uint32 hash_mix32(uint32 h);
static uint64 hash_mix64(uint64 h);
static void checksum_accum_add(ChecksumAccum *acc, uint32 h);
static void checksum_accum_merge(ChecksumAccum *acc, uint64 partial, uint64 count);
static uint32 checksum_accum_finalize(ChecksumAccum *acc);
static void checksum_accum64_add(ChecksumAccum64 *acc, uint64 h);
static void checksum_accum64_merge(ChecksumAccum64 *acc, uint64 partial, uint64 count);
static uint64 checksum_accum64_finalize(ChecksumAccum64 *acc);

static void logical_accum_add(ChecksumLogicalAccum *acc, uint32 h);
static uint32 logical_accum_finalize(ChecksumLogicalAccum *acc);

static void scan_table_blocks_physical(Relation rel, BlockNumber start,
                                       BlockNumber end, bool include_header,
                                       Snapshot snapshot, ChecksumAccum *acc);
static void scan_table_blocks_logical(Relation rel, BlockNumber start,
                                      BlockNumber end, Snapshot snapshot,
                                      ChecksumLogicalAccum *acc);
static void pg_table_logical_checksum_accum(Oid reloid, bool check_privileges,
                                            ChecksumLogicalAccum *acc,
                                            bool *is_null);
static void scan_index_blocks_physical(Relation idxRel, BlockNumber start,
                                       BlockNumber end, bool is_brin,
                                       ChecksumAccum *acc);
static void compute_database_item(Oid relid, char relkind, bool physical,
                                  uint64 *rel_checksum, bool *skip);
static void pg_checksums_report_skipped_relation(Oid relid);
static void compute_database_items(ChecksumRelationItem *items, uint32 start,
                                   uint32 end, bool physical,
                                   ChecksumAccum64 *acc,
                                   ChecksumLogicalAccum *logical_acc,
                                   bool *had_error);

static void pg_checksums_parallel_scan_blocks(ChecksumTaskType task_type,
                                              Oid reloid, bool include_header,
                                              uint32 nblocks, int workers,
                                              ChecksumAccum *acc,
                                              ChecksumLogicalAccum *logical_acc);
static void pg_checksums_parallel_scan_database(bool physical,
                                                bool include_system,
                                                bool include_toast,
                                                ChecksumRelationItem *items,
                                                uint32 nitems, int workers,
                                                ChecksumAccum64 *acc,
                                                ChecksumLogicalAccum *logical_acc,
                                                bool *had_error);

/* Parallel worker entry point (must be a globally visible symbol). */
PGDLLEXPORT void pg_checksums_parallel_worker_main(dsm_segment *seg, shm_toc *toc);

/*-------------------------------------------------------------------------
 * FNV-1a Hash Implementation
 *-------------------------------------------------------------------------
 */

/*
 * fnv1a_32_buf - continue an FNV-1a hash over a byte range
 *
 * Unlike fnv1a_32_hash(), the incoming hash is used as-is (an all-zero value
 * is a legitimate state), so calls may be chained over several buffers and
 * still produce exactly what a single pass over their concatenation would.
 */
static inline uint32
fnv1a_32_buf(const unsigned char *bytes, size_t len, uint32 hash)
{
    for (size_t i = 0; i < len; i++)
    {
        hash ^= bytes[i];
        hash *= FNV_PRIME_32;
    }
    
    return hash;
}

/*
 * fnv1a_32_hash - FNV-1a 32-bit hash implementation
 *
 * This function implements the FNV-1a 32-bit hash algorithm, which is
 * consistent with PostgreSQL's internal checksum implementation. The
 * algorithm processes each byte of the input data, XORing it with the
 * current hash and then multiplying by the FNV prime.
 *
 * Parameters:
 *   data: Pointer to the data to hash
 *   len: Length of the data in bytes
 *   seed: Initial hash value (0 uses FNV_BASIS_32)
 *
 * Returns: 32-bit FNV-1a hash value
 */
static inline uint32
fnv1a_32_hash(const void *data, size_t len, uint32 seed)
{
    return fnv1a_32_buf((const unsigned char *) data, len,
                        seed == 0 ? FNV_BASIS_32 : seed);
}

/*
 * pg_checksum_data_custom - Wrapper for FNV-1a 32-bit hash
 *
 * Public wrapper function that provides the FNV-1a hash algorithm
 * to other parts of the extension. This maintains consistency with
 * PostgreSQL's internal checksum functions.
 */
uint32
pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value)
{
    return fnv1a_32_hash(data, len, init_value);
}

/*-------------------------------------------------------------------------
 * Checksum Combination Functions
 *-------------------------------------------------------------------------
 */

/*
 * combine_checksums - Combine two 32-bit hash values using FNV-1a
 *
 * This function combines two hash values in a way that maintains the
 * avalanche effect of the FNV-1a algorithm. It processes the new hash
 * value byte by byte, ensuring that each bit of the input affects
 * multiple bits of the output.
 *
 * Parameters:
 *   current: Current hash value
 *   new_val: New hash value to combine
 *
 * Returns: Combined 32-bit hash value
 */
uint32
combine_checksums(uint32 current, uint32 new_val)
{
    uint32 hash = current;
    
    /* Combine using FNV-1a with 4-byte chunks */
    hash ^= (new_val >> 24) & 0xFF;
    hash *= FNV_PRIME_32;
    hash ^= (new_val >> 16) & 0xFF;
    hash *= FNV_PRIME_32;
    hash ^= (new_val >> 8) & 0xFF;
    hash *= FNV_PRIME_32;
    hash ^= new_val & 0xFF;
    hash *= FNV_PRIME_32;
    
    return hash;
}

/*
 * combine_checksums_64 - Combine two 64-bit hash values using FNV-1a
 *
 * Similar to combine_checksums but for 64-bit values. Uses the 64-bit
 * FNV prime for proper avalanche effect.
 */
static uint64
combine_checksums_64(uint64 current, uint64 new_val)
{
    uint64 hash = current;
    uint64 prime = UINT64CONST(1099511628211);
    uint8 bytes[8];
    int i;
    
    /* Convert new_val to bytes in little-endian order */
    for (i = 0; i < 8; i++)
    {
        bytes[i] = (new_val >> (i * 8)) & 0xFF;
    }
    
    /* Process each byte with FNV-1a */
    for (i = 0; i < 8; i++)
    {
        hash ^= bytes[i];
        hash *= prime;
    }
    
    return hash;
}

/*-------------------------------------------------------------------------
 * Order-Independent Aggregation
 *-------------------------------------------------------------------------
 */

/*
 * hash_mix32 - 32-bit avalanche finalizer
 *
 * Applies a final mixing step (based on MurmurHash3's fmix) to spread the
 * bits of a hash before it is combined into an order-independent
 * aggregate. This ensures that even highly correlated input hashes
 * contribute uniformly to the final result.
 */
static uint32
hash_mix32(uint32 h)
{
    h ^= h >> 16;
    h *= 0x85EBCA6Bu;
    h ^= h >> 13;
    h *= 0xC2B2AE35u;
    h ^= h >> 16;
    return h;
}

/*
 * hash_mix64 - 64-bit avalanche finalizer
 */
static uint64
hash_mix64(uint64 h)
{
    h ^= h >> 33;
    h *= UINT64CONST(0xFF51AFD7ED558CCD);
    h ^= h >> 33;
    h *= UINT64CONST(0xC4CEB9FE1A85EC53);
    h ^= h >> 33;
    return h;
}

/*
 * checksum_accum_add - fold a single raw 32-bit hash into an accumulator
 */
static void
checksum_accum_add(ChecksumAccum *acc, uint32 h)
{
    acc->partial ^= hash_mix32(h);
    acc->count++;
}

/*
 * checksum_accum_merge - merge another accumulator's partial result
 *
 * The supplied partial is expected to already be the XOR of mixed hashes,
 * so it is combined without re-mixing. Used by the leader to fold the
 * fixed-size results written by parallel workers.
 */
static void
checksum_accum_merge(ChecksumAccum *acc, uint64 partial, uint64 count)
{
    acc->partial ^= (uint32) partial;
    acc->count += count;
}

/*
 * checksum_accum_finalize - convert an accumulator into the final checksum
 *
 * Preserves the historical convention that an empty scan yields
 * FNV_BASIS_32 and guarantees that a non-empty scan never produces the
 * empty sentinel or zero.
 */
static uint32
checksum_accum_finalize(ChecksumAccum *acc)
{
    if (acc->count == 0)
        return FNV_BASIS_32;

    if (acc->partial == 0 || acc->partial == FNV_BASIS_32)
        return 0x7FFFFFFF;

    return acc->partial;
}

/*
 * checksum_accum64_add - fold a single raw 64-bit hash into an accumulator
 */
static void
checksum_accum64_add(ChecksumAccum64 *acc, uint64 h)
{
    acc->partial ^= hash_mix64(h);
    acc->count++;
}

/*
 * checksum_accum64_merge - merge another 64-bit accumulator's partial result
 */
static void
checksum_accum64_merge(ChecksumAccum64 *acc, uint64 partial, uint64 count)
{
    acc->partial ^= partial;
    acc->count += count;
}

/*
 * checksum_accum64_finalize - convert a 64-bit accumulator into the final value
 *
 * The number of items is folded in so that adding or removing relations
 * changes the result even in degenerate cases.
 */
static uint64
checksum_accum64_finalize(ChecksumAccum64 *acc)
{
    if (acc->count == 0)
        return UINT64CONST(14695981039346656037); /* FNV-1a 64-bit basis */

    return combine_checksums_64(acc->partial, acc->count);
}

/*
 * logical_accum_add - fold one logical item into an accumulator
 */
static void
logical_accum_add(ChecksumLogicalAccum *acc, uint32 h)
{
    acc->sum += hash_mix64((uint64) h);
    acc->count++;
}

/*
 * logical_accum_finalize - fold an accumulator into a 32-bit checksum
 */
static uint32
logical_accum_finalize(ChecksumLogicalAccum *acc)
{
    uint64      mixed;
    uint32      result;

    if (acc->count == 0)
        return FNV_BASIS_32;

    mixed = hash_mix64(acc->sum) ^ hash_mix64(acc->count);
    result = hash_mix32((uint32) mixed ^ (uint32) (mixed >> 32));

    if (result == 0 || result == CHECKSUM_NULL)
        result = hash_mix32(result ^ FNV_BASIS_32);

    return result;
}

/*-------------------------------------------------------------------------
 * Privilege, relkind and storage validation helpers
 *-------------------------------------------------------------------------
 */

/*
 * pg_checksums_require_select - enforce SELECT privilege and RLS
 *
 * The checksum functions read relation storage directly, without going
 * through the executor.  They must therefore perform the same privilege
 * check the executor would perform, and they cannot apply row-level
 * security policies: a relation protected by RLS must be rejected rather
 * than scanned behind the policy's back.
 *
 * Indexes carry no ACL of their own, so the privilege check for an index
 * is performed on the underlying heap relation.
 */
static void
pg_checksums_require_select(Oid reloid)
{
    Oid         acl_relid = reloid;
    char        relkind = get_rel_relkind(reloid);
    AclResult   aclresult;
    ObjectType  objtype = OBJECT_TABLE;
    char       *relname;

    if (relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX)
        acl_relid = IndexGetRelation(reloid, false);

    aclresult = pg_class_aclcheck(acl_relid, GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK)
    {
        relname = get_rel_name(acl_relid);
        if (relname == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                     errmsg("relation with OID %u does not exist", acl_relid)));

        if (get_rel_relkind(acl_relid) == RELKIND_SEQUENCE)
            objtype = OBJECT_SEQUENCE;
        aclcheck_error(aclresult, objtype, relname);
    }

    /*
     * check_enable_rls() accounts for superusers, roles with BYPASSRLS and
     * table owners (including FORCE ROW LEVEL SECURITY).
     */
    if (check_enable_rls(acl_relid, GetUserId(), true) == RLS_ENABLED)
    {
        relname = get_rel_name(acl_relid);
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("cannot compute checksum of relation \"%s\" because row-level security is enabled",
                        relname ? relname : "?"),
                 errhint("Use a role with BYPASSRLS, the table owner, or disable RLS for this operation.")));
    }
}

/*
 * pg_checksums_check_relation_kind - reject relations we cannot checksum
 * with the heap tuple routines.
 */
static void
pg_checksums_check_relation_kind(Relation rel, bool allow_toast,
                                 bool allow_partitioned)
{
    char        relkind = rel->rd_rel->relkind;

    if (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW)
        return;

    if (relkind == RELKIND_PARTITIONED_TABLE)
    {
        if (allow_partitioned)
            return;

        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("partitioned table \"%s\" has no storage of its own",
                        RelationGetRelationName(rel))));
    }

    if (relkind == RELKIND_TOASTVALUE)
    {
        if (allow_toast)
            return;

        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("TOAST relation \"%s\" is not a heap relation",
                        RelationGetRelationName(rel))));
    }

    switch (relkind)
    {
        case RELKIND_INDEX:
        case RELKIND_PARTITIONED_INDEX:
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("relation \"%s\" is an index, not a table",
                            RelationGetRelationName(rel))));
            break;
        case RELKIND_SEQUENCE:
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("relation \"%s\" is a sequence, not a table",
                            RelationGetRelationName(rel))));
            break;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("relation \"%s\" is not a table with storage",
                            RelationGetRelationName(rel))));
            break;
    }
}

/*
 * pg_checksums_relation_has_storage - can pg_page_checksum() read pages?
 */
static bool
pg_checksums_relation_has_storage(Relation rel)
{
    switch (rel->rd_rel->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_TOASTVALUE:
            return true;
        default:
            return false;
    }
}

/*
 * pg_checksums_page_header_sane - cheap structural validation of a page
 *
 * This mirrors the header checks of PageIsVerified(), without the checksum
 * part.  It is used before following line pointers, so that a corrupted
 * page produces a clean error instead of crashing the backend that is
 * supposed to detect the corruption.
 */
static bool
pg_checksums_page_header_sane(Page page)
{
    PageHeader  phdr = (PageHeader) page;

    return (phdr->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
        phdr->pd_lower >= SizeOfPageHeaderData &&
        phdr->pd_lower <= phdr->pd_upper &&
        phdr->pd_upper <= phdr->pd_special &&
        phdr->pd_special <= BLCKSZ &&
        phdr->pd_special == MAXALIGN(phdr->pd_special);
}

/*
 * pg_checksums_check_page - raise a clean error for an insane page header
 */
static void
pg_checksums_check_page(Relation rel, BlockNumber blkno, Page page)
{
    if (PageIsNew(page))
        return;

    if (!pg_checksums_page_header_sane(page))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupt page header in relation \"%s\" at block %u",
                        RelationGetRelationName(rel), blkno)));
}

/*
 * pg_checksums_item_is_sane - validate an item pointer against the page
 *
 * Besides ItemIdIsNormal(), check that the tuple lies between pd_upper and
 * pd_special and that its length does not run past the end of that area.
 */
static bool
pg_checksums_item_is_sane(Page page, ItemId lp)
{
    PageHeader  phdr = (PageHeader) page;
    char       *item;
    Size        len;

    if (!ItemIdIsNormal(lp))
        return false;

    len = ItemIdGetLength(lp);
    item = (char *) PageGetItem(page, lp);

    if (item < (char *) page + phdr->pd_upper)
        return false;
    if (item + len > (char *) page + phdr->pd_special)
        return false;

    return true;
}

/*
 * pg_checksums_check_heap_tuple - validate the parts of a heap tuple header
 * that the checksum routines are about to follow.
 */
static void
pg_checksums_check_heap_tuple(Relation rel, BlockNumber blkno,
                              OffsetNumber offnum, HeapTupleHeader tuple,
                              Size len)
{
    if (len < SizeofHeapTupleHeader ||
        tuple->t_hoff < SizeofHeapTupleHeader ||
        tuple->t_hoff > len)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupt tuple header in relation \"%s\" at block %u, offset %u",
                        RelationGetRelationName(rel), blkno, offnum)));
}

/*
 * scan_relation_raw_pages - checksum a relation as a sequence of raw pages
 *
 * Used for TOAST relations, whose tuples are not HeapTupleHeader structures
 * and therefore cannot go through the regular heap tuple path.
 */
static void
scan_relation_raw_pages(Relation rel, BlockNumber start, BlockNumber end,
                        ChecksumAccum *acc)
{
    BlockNumber blkno;
    BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        uint32      page_hash;

        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                    bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        page_hash = fnv1a_32_hash((const unsigned char *) page, BLCKSZ, 0);
        page_hash = combine_checksums(page_hash, blkno);
        checksum_accum_add(acc, page_hash);

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*-------------------------------------------------------------------------
 * PAGE LEVEL FUNCTIONS (Physical only)
 *-------------------------------------------------------------------------
 */

/*
 * pg_page_checksum - SQL function for page-level checksums
 *
 * Wraps PostgreSQL's built-in page checksum functionality. This provides
 * a SQL interface to compute physical page checksums for any page in a
 * relation. Useful for detecting low-level storage corruption.
 *
 * Returns 0 for new (uninitialized) pages.
 */
PG_FUNCTION_INFO_V1(pg_page_checksum);

Datum
pg_page_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    int32       blkno;
    Relation    rel;
    Buffer      buffer;
    Page        page;
    BlockNumber nblocks;
    char       *pagecopy;
    uint16      checksum;

    reloid = PG_GETARG_OID(0);
    blkno = PG_GETARG_INT32(1);

    /* Open relation with AccessShareLock to prevent concurrent modifications */
    rel = relation_open(reloid, AccessShareLock);

    pg_checksums_require_select(reloid);

    if (!pg_checksums_relation_has_storage(rel))
    {
        char       *relname = pstrdup(RelationGetRelationName(rel));

        relation_close(rel, AccessShareLock);
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("relation \"%s\" has no storage", relname)));
    }

    nblocks = RelationGetNumberOfBlocks(rel);

    /*
     * BlockNumber is unsigned, and P_NEW (0xFFFFFFFF) is what a negative
     * int4 turns into.  ReadBuffer(P_NEW) would extend the relation, so the
     * range must be validated before reading.
     */
    if (blkno < 0 || (uint32) blkno >= nblocks)
    {
        char       *relname = pstrdup(RelationGetRelationName(rel));

        relation_close(rel, AccessShareLock);

        if (nblocks == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("relation \"%s\" has no pages", relname)));

        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("block number %d is out of range for relation \"%s\" (0..%u)",
                        blkno, relname, nblocks - 1)));
    }

    /* Read the page from the buffer manager */
    buffer = ReadBuffer(rel, (BlockNumber) blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    if (PageIsNew(page))
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_INT32(0);
    }

    /*
     * pg_checksum_page() transiently zeroes pd_checksum.  It must not run on
     * a page from the shared buffer pool: a concurrent caller could observe
     * the zeroed field, and an error between the two stores would leave the
     * page with a bogus checksum.  Copy the page and compute on the copy.
     */
    pagecopy = (char *) palloc(BLCKSZ);
    memcpy(pagecopy, page, BLCKSZ);
    UnlockReleaseBuffer(buffer);

    checksum = pg_checksum_page(pagecopy, (BlockNumber) blkno);

    pfree(pagecopy);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32) checksum);
}

/*-------------------------------------------------------------------------
 * Helper functions for column checksum computation
 *-------------------------------------------------------------------------
 */


/*
 * Canonical logical value encoding
 *
 * The goal is a checksum that depends on the value, not on relation or type
 * OIDs.  Scalar values use their binary send function (network byte order);
 * arrays, composites, ranges and numeric are handled recursively so that
 * embedded OIDs and insignificant numeric scale do not leak into the hash.
 */

static uint32 logical_hash_datum(Datum value, bool isnull, Oid typid,
                                 int32 typmod, int attnum, int depth);

static uint32
logical_hash_bytes(uint32 h, const void *data, size_t len)
{
    return fnv1a_32_hash(data, len, h);
}

static void
logical_type_cache_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
    (void) arg;
    (void) cacheid;
    (void) hashvalue;

    if (logical_type_cache != NULL)
    {
        hash_destroy(logical_type_cache);
        logical_type_cache = NULL;
    }
}

static LogicalTypeInfo *
logical_type_info(Oid typid)
{
    LogicalTypeInfo *info;
    bool        found;

    if (logical_type_cache == NULL)
    {
        HASHCTL     ctl;

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(LogicalTypeInfo);
        ctl.hcxt = TopMemoryContext;
        logical_type_cache = hash_create("pg_checksums logical type cache",
                                         64, &ctl,
                                         HASH_ELEM | HASH_BLOBS);
    }

    info = (LogicalTypeInfo *) hash_search(logical_type_cache, &typid,
                                           HASH_ENTER, &found);
    if (!found)
    {
        HeapTuple   type_tuple;
        Form_pg_type type_form;
        char       *tag;

        type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if (!HeapTupleIsValid(type_tuple))
            elog(ERROR, "cache lookup failed for type %u", typid);

        type_form = (Form_pg_type) GETSTRUCT(type_tuple);
        tag = format_type_be(typid);

        info->typid = typid;
        info->tag_hash = fnv1a_32_hash(tag, strlen(tag), FNV_BASIS_32);
        info->sendfunc = type_form->typsend;
        info->outfunc = type_form->typoutput;
        info->has_send = OidIsValid(type_form->typsend);

        pfree(tag);
        ReleaseSysCache(type_tuple);
    }

    return info;
}

static uint32
logical_hash_type_tag(uint32 h, Oid typid)
{
    LogicalTypeInfo *info = logical_type_info(typid);

    return combine_checksums(h, info->tag_hash);
}

static uint32
logical_hash_array(Datum value, Oid typid, int attnum, int depth)
{
    ArrayType  *arr = DatumGetArrayTypeP(value);
    Oid         elemtype = ARR_ELEMTYPE(arr);
    int16       elemlen;
    bool        elembyval;
    char        elemalign;
    Datum      *elems;
    bool       *nulls;
    int         nelems;
    int         ndim = ARR_NDIM(arr);
    int         i;
    uint32      h = FNV_BASIS_32;

    (void) typid;
    (void) attnum;

    /* Hash the shape first: an empty array differs from an empty string. */
    h = logical_hash_bytes(h, &ndim, sizeof(ndim));
    if (ndim > 0)
    {
        h = logical_hash_bytes(h, ARR_DIMS(arr), ndim * sizeof(int));
        h = logical_hash_bytes(h, ARR_LBOUND(arr), ndim * sizeof(int));
    }

    get_typlenbyvalalign(elemtype, &elemlen, &elembyval, &elemalign);
    deconstruct_array(arr, elemtype, elemlen, elembyval, elemalign,
                      &elems, &nulls, &nelems);

    h = logical_hash_bytes(h, &nelems, sizeof(nelems));

    for (i = 0; i < nelems; i++)
    {
        uint32      elem_hash;

        elem_hash = logical_hash_datum(elems[i], nulls[i], elemtype, -1,
                                       i + 1, depth + 1);
        h = combine_checksums(h, elem_hash);
    }

    pfree(elems);
    pfree(nulls);

    if ((Pointer) arr != DatumGetPointer(value))
        pfree(arr);

    return h;
}

static uint32
logical_hash_composite(Datum value, Oid typid, int32 typmod, int depth)
{
    HeapTupleHeader rec = DatumGetHeapTupleHeader(value);
    Oid         tupType = typid;
    int32       tupTypmod = typmod;
    TupleDesc   tupdesc;
    HeapTupleData tuple;
    Datum      *values;
    bool       *nulls;
    uint32      h = FNV_BASIS_32;
    int         non_dropped = 0;
    int         i;

    if (typid == RECORDOID)
    {
        tupType = HeapTupleHeaderGetTypeId(rec);
        tupTypmod = HeapTupleHeaderGetTypMod(rec);
    }

    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    for (i = 0; i < tupdesc->natts; i++)
    {
        if (!TupleDescAttr(tupdesc, i)->attisdropped)
            non_dropped++;
    }

    h = logical_hash_bytes(h, &non_dropped, sizeof(non_dropped));

    values = palloc(sizeof(Datum) * tupdesc->natts);
    nulls = palloc(sizeof(bool) * tupdesc->natts);

    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    ItemPointerSetInvalid(&tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_data = rec;

    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        uint32      field_hash;

        if (att->attisdropped)
            continue;

        field_hash = logical_hash_datum(values[i], nulls[i],
                                        att->atttypid, att->atttypmod,
                                        i + 1, depth + 1);
        h = combine_checksums(h, field_hash);
    }

    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);

    if ((Pointer) rec != DatumGetPointer(value))
        pfree(rec);

    return h;
}

static uint32
logical_hash_range(Datum value, Oid typid, int depth)
{
    RangeType  *range = DatumGetRangeTypeP(value);
    TypeCacheEntry *typcache = lookup_type_cache(typid, TYPECACHE_RANGE_INFO);
    RangeBound  lower;
    RangeBound  upper;
    bool        empty;
    uint32      h = FNV_BASIS_32;

    range_deserialize(typcache, range, &lower, &upper, &empty);

    h = logical_hash_bytes(h, &empty, sizeof(empty));

    if (!empty)
    {
        h = logical_hash_bytes(h, &lower.infinite, sizeof(lower.infinite));
        h = logical_hash_bytes(h, &lower.inclusive, sizeof(lower.inclusive));
        if (!lower.infinite)
        {
            uint32      lower_hash;

            lower_hash = logical_hash_datum(lower.val, false,
                                            typcache->rngelemtype->type_id,
                                            -1, 1, depth + 1);
            h = combine_checksums(h, lower_hash);
        }

        h = logical_hash_bytes(h, &upper.infinite, sizeof(upper.infinite));
        h = logical_hash_bytes(h, &upper.inclusive, sizeof(upper.inclusive));
        if (!upper.infinite)
        {
            uint32      upper_hash;

            upper_hash = logical_hash_datum(upper.val, false,
                                            typcache->rngelemtype->type_id,
                                            -1, 2, depth + 1);
            h = combine_checksums(h, upper_hash);
        }
    }

    if ((Pointer) range != DatumGetPointer(value))
        pfree(range);

    return h;
}

#if PG_VERSION_NUM >= 140000
static uint32
logical_hash_multirange(Datum value, Oid typid, int depth)
{
    MultirangeType *mr = DatumGetMultirangeTypeP(value);
    TypeCacheEntry *typcache = lookup_type_cache(typid,
                                                 TYPECACHE_MULTIRANGE_INFO);
    uint32      count = mr->rangeCount;
    uint32      h = FNV_BASIS_32;
    uint32      i;

    h = logical_hash_bytes(h, &count, sizeof(count));

    for (i = 0; i < count; i++)
    {
        RangeBound  lower;
        RangeBound  upper;
        uint32      lower_hash;
        uint32      upper_hash;

        multirange_get_bounds(typcache->rngtype, mr, i, &lower, &upper);

        h = logical_hash_bytes(h, &lower.infinite, sizeof(lower.infinite));
        h = logical_hash_bytes(h, &lower.inclusive, sizeof(lower.inclusive));
        if (!lower.infinite)
        {
            lower_hash = logical_hash_datum(lower.val, false,
                                            typcache->rngtype->rngelemtype->type_id,
                                            -1, 1, depth + 1);
            h = combine_checksums(h, lower_hash);
        }

        h = logical_hash_bytes(h, &upper.infinite, sizeof(upper.infinite));
        h = logical_hash_bytes(h, &upper.inclusive, sizeof(upper.inclusive));
        if (!upper.infinite)
        {
            upper_hash = logical_hash_datum(upper.val, false,
                                            typcache->rngtype->rngelemtype->type_id,
                                            -1, 2, depth + 1);
            h = combine_checksums(h, upper_hash);
        }
    }

    if ((Pointer) mr != DatumGetPointer(value))
        pfree(mr);

    return h;
}
#endif

static uint32
logical_hash_datum(Datum value, bool isnull, Oid typid,
                   int32 typmod, int attnum, int depth)
{
    Oid         orig_typid = typid;
    char        typtype;
    Oid         elemtype;
    bool        specialized = false;
    uint32      h = FNV_BASIS_32;

    if (depth > 32)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("logical checksum recursion is too deep")));

    check_stack_depth();

    h = logical_hash_type_tag(h, orig_typid);
    h = combine_checksums(h, (uint32) attnum);

    if (isnull)
        return CHECKSUM_NULL;

    typid = getBaseTypeAndTypmod(typid, &typmod);
    typtype = get_typtype(typid);

    elemtype = get_element_type(typid);
    if (OidIsValid(elemtype))
    {
        h = combine_checksums(h,
                              logical_hash_array(value, elemtype, attnum,
                                                 depth));
        specialized = true;
    }

    if (typtype == TYPTYPE_COMPOSITE || typid == RECORDOID)
    {
        h = combine_checksums(h,
                              logical_hash_composite(value, typid, typmod,
                                                     depth));
        specialized = true;
    }

    if (typtype == TYPTYPE_RANGE)
    {
        h = combine_checksums(h, logical_hash_range(value, typid, depth));
        specialized = true;
    }

#if PG_VERSION_NUM >= 140000
    if (typtype == TYPTYPE_MULTIRANGE)
    {
        h = combine_checksums(h,
                              logical_hash_multirange(value, typid, depth));
        specialized = true;
    }
#endif

    if (!specialized && typid == NUMERICOID)
    {
        Numeric     num = DatumGetNumeric(value);
        char       *str = numeric_normalize(num);

        h = logical_hash_bytes(h, str, strlen(str));
        pfree(str);

        if ((Pointer) num != DatumGetPointer(value))
            pfree(num);
    }
    else if (!specialized && typid == FLOAT4OID)
    {
        float4      f = DatumGetFloat4(value);

        if (isnan(f))
            h = logical_hash_bytes(h, "NaN", 3);
        else if (f == 0)
        {
            uint32      zero = 0;

            h = logical_hash_bytes(h, &zero, sizeof(zero));
        }
        else
        {
            Oid         sendfunc;
            bool        isvarlena;
            bytea      *bytes;

            getTypeBinaryOutputInfo(typid, &sendfunc, &isvarlena);
            bytes = OidSendFunctionCall(sendfunc, value);
            h = logical_hash_bytes(h, VARDATA(bytes),
                                   VARSIZE(bytes) - VARHDRSZ);
            pfree(bytes);
        }
    }
    else if (!specialized && typid == FLOAT8OID)
    {
        float8      f = DatumGetFloat8(value);

        if (isnan(f))
            h = logical_hash_bytes(h, "NaN", 3);
        else if (f == 0)
        {
            uint64      zero = 0;

            h = logical_hash_bytes(h, &zero, sizeof(zero));
        }
        else
        {
            Oid         sendfunc;
            bool        isvarlena;
            bytea      *bytes;

            getTypeBinaryOutputInfo(typid, &sendfunc, &isvarlena);
            bytes = OidSendFunctionCall(sendfunc, value);
            h = logical_hash_bytes(h, VARDATA(bytes),
                                   VARSIZE(bytes) - VARHDRSZ);
            pfree(bytes);
        }
    }
    else if (!specialized)
    {
        LogicalTypeInfo *info = logical_type_info(typid);

        if (info->has_send)
        {
            bytea      *bytes = OidSendFunctionCall(info->sendfunc, value);

            h = logical_hash_bytes(h, VARDATA(bytes),
                                   VARSIZE(bytes) - VARHDRSZ);
            pfree(bytes);
        }
        else if (OidIsValid(info->outfunc))
        {
            char       *str = OidOutputFunctionCall(info->outfunc, value);

            h = logical_hash_bytes(h, str, strlen(str));
            pfree(str);
        }
        else
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("no output function available for type %s",
                            format_type_be(typid))));
    }

    if (h == 0 || h == CHECKSUM_NULL)
        h = hash_mix32(h ^ FNV_BASIS_32 ^ (uint32) attnum);

    return h;
}

/*
 * pg_cell_checksum_canonical - canonical 32-bit logical cell checksum
 */
static uint32
pg_cell_checksum_canonical(Datum value, bool isnull, Oid typid,
                           int32 typmod, int attnum)
{
    return logical_hash_datum(value, isnull, typid, typmod, attnum, 0);
}

/*
 * pg_cell_checksum - SQL function for cell-level checksums
 *
 * Public SQL-callable function that computes checksum for a specific
 * cell of a specific tuple identified by its TID (tuple identifier).
 *
 * Parameters:
 *   relname: OID of the relation (table)
 *   tid: TID (block number and offset) identifying the tuple
 *   attnum: Attribute number (1-based) of the column to checksum
 *
 * Returns: 32-bit checksum, or NULL if the tuple doesn't exist or
 *          attnum is out of range
 */
PG_FUNCTION_INFO_V1(pg_cell_checksum);

Datum
pg_cell_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    ItemPointer tid;
    int32       attnum_arg;
    Relation    rel;
    Buffer      buffer;
    Page        page;
    HeapTupleHeader tuple;
    ItemId      lp;
    Datum       value;
    bool        isnull;
    HeapTupleData heapTuple;
    TupleDesc   tupdesc;
    Form_pg_attribute attr;
    uint32      checksum;
    BlockNumber blkno;
    OffsetNumber offnum;
    Size        tuple_len;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        PG_RETURN_NULL();

    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);
    attnum_arg = PG_GETARG_INT32(2);

    if (attnum_arg <= 0 || !ItemPointerIsValid(tid))
        PG_RETURN_NULL();

    /* Open relation and get tuple descriptor */
    rel = relation_open(reloid, AccessShareLock);

    pg_checksums_require_select(reloid);
    pg_checksums_check_relation_kind(rel, false, false);

    tupdesc = RelationGetDescr(rel);

    /* Validate attribute number */
    if (attnum_arg > tupdesc->natts)
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    if (blkno >= RelationGetNumberOfBlocks(rel))
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    /* Read the page containing the tuple */
    buffer = ReadBuffer(rel, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);

    if (PageIsNew(page))
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    pg_checksums_check_page(rel, blkno, page);

    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    lp = PageGetItemId(page, offnum);

    /* Only normal heap tuples carry data; skip dead/redirect/unused items */
    if (!ItemIdIsNormal(lp))
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    if (!pg_checksums_item_is_sane(page, lp))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupt line pointer in relation \"%s\" at block %u, offset %u",
                        RelationGetRelationName(rel), blkno, offnum)));

    tuple = (HeapTupleHeader) PageGetItem(page, lp);
    tuple_len = ItemIdGetLength(lp);

    pg_checksums_check_heap_tuple(rel, blkno, offnum, tuple, tuple_len);

    /* Create temporary HeapTuple structure for heap_getattr */
    heapTuple.t_len = tuple_len;
    heapTuple.t_data = tuple;
    heapTuple.t_tableOid = reloid;
    heapTuple.t_self = *tid;

    /* Get attribute value using the tuple descriptor */
    attr = TupleDescAttr(tupdesc, attnum_arg - 1);

    if (attr->attisdropped)
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    value = heap_getattr(&heapTuple, attnum_arg, tupdesc, &isnull);

    checksum = pg_cell_checksum_canonical(value, isnull,
                                          attr->atttypid,
                                          attr->atttypmod,
                                          attnum_arg);

    /* Clean up resources */
    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32) checksum);
}

/*-------------------------------------------------------------------------
 * TUPLE LEVEL FUNCTIONS (Physical and Logical)
 *-------------------------------------------------------------------------
 */

/*
 * pg_tuple_physical_checksum_internal - Core physical tuple checksum
 *
 * Computes a physical checksum for a tuple that depends on its physical
 * location (block number and offset). This checksum will change if the
 * tuple moves (e.g., after VACUUM FULL, CLUSTER).
 *
 * The checksum includes:
 * 1. Physical location (blkno << 16 | offnum) as seed
 * 2. Tuple data (with or without header as specified)
 * 3. MVCC information (xmin/xmax) when header is excluded
 *
 * This is useful for detecting physical corruption and verifying that
 * tuples haven't moved unexpectedly.
 */
static uint32
pg_tuple_physical_checksum_internal(Relation rel, Page page,
                                   OffsetNumber offnum,
                                   BlockNumber blkno, bool include_header)
{
    ItemId      lp;
    HeapTupleHeader tuple;
    char       *data;
    Size        len;
    uint32      checksum;
    uint64      location;
    uint32      location_hash;
    uint32      page_info;
    uint32      mvcc_info;
    uint32      itemid_info;

    if (PageIsNew(page))
        return 0;

    pg_checksums_check_page(rel, blkno, page);

    /* Validate offset number range */
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
        return 0;

    lp = PageGetItemId(page, offnum);

    /* Only normal heap tuples carry data */
    if (!ItemIdIsNormal(lp))
        return 0;

    if (!pg_checksums_item_is_sane(page, lp))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("corrupt line pointer in relation \"%s\" at block %u, offset %u",
                        RelationGetRelationName(rel), blkno, offnum)));

    tuple = (HeapTupleHeader) PageGetItem(page, lp);
    len = ItemIdGetLength(lp);

    pg_checksums_check_heap_tuple(rel, blkno, offnum, tuple, len);

    if (include_header)
    {
        /* Include entire tuple (header + data) in checksum */
        data = (char *) tuple;
    }
    else
    {
        /* Skip header, checksum only the tuple data */
        data = (char *) tuple + tuple->t_hoff;
        len -= tuple->t_hoff;
    }

    /* Create 64-bit location from block number and offset */
    location = ((uint64)blkno << 32) | (uint64)offnum;
    
    /* Calculate 32-bit hash of the location */
    location_hash = fnv1a_32_hash(&location, sizeof(location), FNV_BASIS_32);
    
    /*
     * Include relation identity and storage identity.  relfilenode changes
     * when VACUUM FULL, CLUSTER or ALTER TABLE SET TABLESPACE rewrite the
     * relation, so a tuple that happens to land at the same (block, offset)
     * still has a different physical checksum.  The relation OID keeps
     * checksums from different relations distinct.
     */
    location_hash = combine_checksums(location_hash,
                                      (uint32) RelationGetRelid(rel));
    location_hash = combine_checksums(location_hash,
                                      (uint32) rel->rd_rel->relfilenode);
    location_hash = combine_checksums(location_hash,
                                      (uint32) rel->rd_rel->reltablespace);

    /*
     * Include the page size/layout version for additional uniqueness.  Do
     * not include the page LSN, pd_checksum or pd_flags: they change when a
     * concurrent scan sets hint bits, which made a physical checksum depend
     * on who happened to read the page first.
     */
    page_info = PageGetPageSize(page) | (PageGetPageLayoutVersion(page) << 16);
    location_hash = combine_checksums(location_hash, page_info);

    /* Calculate checksum using location_hash as the initial value */
    checksum = pg_checksum_data_custom(data, len, location_hash);
    
    /* Incorporate MVCC information and other physical metadata */
    mvcc_info = (HeapTupleHeaderGetRawXmin(tuple) ^ 
                HeapTupleHeaderGetRawXmax(tuple) ^
                HeapTupleHeaderGetRawCommandId(tuple));
    checksum ^= mvcc_info;
    
    /* Include ItemId information */
    itemid_info = (ItemIdGetFlags(lp) << 24) | ItemIdGetLength(lp);
    checksum = combine_checksums(checksum, itemid_info);
    
    /* Guarantee checksum never equals CHECKSUM_NULL */
    if (checksum == CHECKSUM_NULL)
        checksum = (CHECKSUM_NULL ^ location_hash) & 0xFFFFFFFE;

    if (checksum == 0) {
        checksum = (location_hash ^ mvcc_info ^ FNV_BASIS_32) & 0xFFFFFFFE;
    }
    
    return checksum;
}

/*
 * pg_tuple_physical_checksum - SQL function for physical tuple checksums
 *
 * Public interface for computing physical tuple checksums.
 * Uses exception handling to ensure resources are cleaned up
 * even if an error occurs.
 */
PG_FUNCTION_INFO_V1(pg_tuple_physical_checksum);

Datum
pg_tuple_physical_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    ItemPointer tid;
    bool        include_header;
    Relation    rel;
    volatile Buffer buffer = InvalidBuffer;
    Page        page;
    volatile uint32 checksum = 0;
    volatile bool lock_held = false;
    BlockNumber blkno;
    OffsetNumber offnum;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();

    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);
    include_header = PG_GETARG_BOOL(2);

    if (!ItemPointerIsValid(tid))
        PG_RETURN_NULL();

    rel = relation_open(reloid, AccessShareLock);

    pg_checksums_require_select(reloid);
    pg_checksums_check_relation_kind(rel, false, false);

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    if (blkno >= RelationGetNumberOfBlocks(rel))
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    PG_TRY();
    {
        /* Read the page */
        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;

        page = BufferGetPage(buffer);

        /* Compute tuple checksum */
        checksum = pg_tuple_physical_checksum_internal(rel, page, offnum,
                                                      blkno, include_header);
    }
    PG_CATCH();
    {
        /* Clean up resources in case of error */
        if (lock_held && BufferIsValid(buffer))
            UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Clean up */
    if (lock_held && BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32) checksum);
}

/*-------------------------------------------------------------------------
 * Primary Key Helper Functions
 *-------------------------------------------------------------------------
 */

/*
 * pg_tuple_logical_checksum_internal - Core logical tuple checksum
 *
 * Computes a logical checksum for a tuple that depends only on its
 * primary key values and data content, not on physical location.
 *
 * This checksum is stable across:
 * - VACUUM FULL
 * - CLUSTER
 * - Physical data movement
 * - Storage reorganization
 *
 * Returns 0 if:
 * 1. Table has no primary key
 * 2. Failed to compute seed from PK values
 */
static uint32
pg_tuple_logical_checksum_internal(Relation rel, HeapTuple tuple)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    uint32      checksum = FNV_BASIS_32;
    int         i;

    for (i = 1; i <= tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
        Datum       value;
        bool        isnull;
        uint32      col_hash;

        if (attr->attisdropped)
            continue;

        value = heap_getattr(tuple, i, tupdesc, &isnull);

        col_hash = pg_cell_checksum_canonical(value, isnull,
                                              attr->atttypid,
                                              attr->atttypmod,
                                              i);

        checksum = combine_checksums(checksum, col_hash);
    }

    if (checksum == CHECKSUM_NULL || checksum == 0)
        checksum = hash_mix32(checksum ^ FNV_BASIS_32);

    return checksum;
}

/*
 * pg_tuple_logical_checksum - SQL function for logical tuple checksums
 *
 * Public interface for computing logical tuple checksums.
 * Returns NULL if the table has no primary key.
 */
PG_FUNCTION_INFO_V1(pg_tuple_logical_checksum);

Datum
pg_tuple_logical_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    ItemPointer tid;
    Relation    rel;
    volatile Buffer buffer = InvalidBuffer;
    Page        page;
    ItemId      lp;
    HeapTupleData heapTuple;
    HeapTupleHeader tuple;
    volatile uint32 checksum = 0;
    volatile bool lock_held = false;
    volatile bool missing = false;
    BlockNumber blkno;
    OffsetNumber offnum;
    Size        tuple_len;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();

    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);

    if (!ItemPointerIsValid(tid))
        PG_RETURN_NULL();

    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);

    pg_checksums_require_select(reloid);
    pg_checksums_check_relation_kind(rel, true, false);

    /*
     * A TOAST relation has no primary key and its tuples are not heap tuples;
     * its logical content is already covered when the parent table detoasts
     * the values.
     */
    if (rel->rd_rel->relkind == RELKIND_TOASTVALUE)
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);

    if (blkno >= RelationGetNumberOfBlocks(rel))
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    PG_TRY();
    {
        /* Read page */
        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;

        page = BufferGetPage(buffer);

        if (PageIsNew(page))
            missing = true;
        else
        {
            pg_checksums_check_page(rel, blkno, page);

            if (offnum < FirstOffsetNumber ||
                offnum > PageGetMaxOffsetNumber(page))
                missing = true;
            else
            {
                lp = PageGetItemId(page, offnum);

                if (!ItemIdIsNormal(lp))
                    missing = true;
                else if (!pg_checksums_item_is_sane(page, lp))
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             errmsg("corrupt line pointer in relation \"%s\" at block %u, offset %u",
                                    RelationGetRelationName(rel), blkno, offnum)));
                else
                {
                    tuple = (HeapTupleHeader) PageGetItem(page, lp);
                    tuple_len = ItemIdGetLength(lp);

                    pg_checksums_check_heap_tuple(rel, blkno, offnum, tuple,
                                                  tuple_len);

                    /* Create temporary HeapTuple structure */
                    heapTuple.t_len = tuple_len;
                    heapTuple.t_data = tuple;
                    heapTuple.t_tableOid = reloid;
                    heapTuple.t_self = *tid;

                    /* Compute logical checksum */
                    checksum = pg_tuple_logical_checksum_internal(rel, &heapTuple);
                }
            }
        }
    }
    PG_CATCH();
    {
        if (lock_held && BufferIsValid(buffer))
            UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (lock_held && BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    if (missing || checksum == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT32((int32) checksum);
}

/*-------------------------------------------------------------------------
 * Partitioned Table Helpers
 *-------------------------------------------------------------------------
 */

/*
 * pg_checksums_leaf_partitions - return the OIDs of all leaf partitions
 * of a partitioned table
 *
 * Uses find_all_inheritors to collect every descendant of the given
 * relation and keeps only the leaf partitions (ordinary tables). Foreign
 * table partitions are skipped because they have no local storage to
 * checksum. The list is empty for a partitioned table with no partitions.
 */
static List *
pg_checksums_leaf_partitions(Oid reloid)
{
    List       *children;
    List       *leaves = NIL;
    ListCell   *lc;

    children = find_all_inheritors(reloid, AccessShareLock, NULL);

    foreach(lc, children)
    {
        Oid         childoid = lfirst_oid(lc);

        if (childoid == reloid)
            continue;

        if (get_rel_relkind(childoid) == RELKIND_RELATION)
            leaves = lappend_oid(leaves, childoid);
    }

    list_free(children);

    return leaves;
}

/*
 * pg_checksums_leaf_indexes - return the OIDs of all leaf indexes of a
 * partitioned index
 *
 * Intermediate partitioned indexes are skipped; only ordinary indexes have
 * storage to checksum.  The list is empty for a partitioned index with no
 * partitions.
 */
static List *
pg_checksums_leaf_indexes(Oid indexoid)
{
    List       *children;
    List       *leaves = NIL;
    ListCell   *lc;

    children = find_all_inheritors(indexoid, AccessShareLock, NULL);

    foreach(lc, children)
    {
        Oid         childoid = lfirst_oid(lc);

        if (childoid == indexoid)
            continue;

        if (get_rel_relkind(childoid) == RELKIND_INDEX)
            leaves = lappend_oid(leaves, childoid);
    }

    list_free(children);

    return leaves;
}

/*-------------------------------------------------------------------------
 * TABLE LEVEL FUNCTIONS (Physical and Logical)
 *-------------------------------------------------------------------------
 */

/*
 * pg_table_physical_checksum - SQL function for physical table checksums
 *
 * Computes a physical checksum for an entire table by aggregating
 * physical checksums of all tuples. Uses order-independent aggregation
 * to ensure the same checksum regardless of scan order.
 *
 * Returns a 64-bit value that combines:
 * 1. Aggregate checksum of all tuples (32 bits, shifted left)
 * 2. Relation OID (32 bits)
 *
 * This ensures that even empty tables have a unique checksum.
 */
PG_FUNCTION_INFO_V1(pg_table_physical_checksum);

Datum
pg_table_physical_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    bool        include_header;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    reloid = PG_GETARG_OID(0);
    include_header = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

    PG_RETURN_INT64((int64)
                    pg_table_physical_checksum_calc(reloid, include_header,
                                                    true));
}

/*
 * pg_table_physical_checksum_calc - implementation shared with partitions
 *
 * check_privileges is false for leaf partitions reached through a
 * partitioned table: PostgreSQL checks the privilege on the parent when a
 * partitioned table is queried, and requiring SELECT on every partition
 * would reject perfectly valid calls.
 */
static uint64
pg_table_physical_checksum_calc(Oid reloid, bool include_header,
                                bool check_privileges)
{
    Relation    rel;
    ChecksumAccum acc = {0, 0};
    uint32      aggregate;
    uint64      final_checksum;
    uint32      relation_hash;
    uint32      nblocks;
    int         workers;

    rel = relation_open(reloid, AccessShareLock);

    if (check_privileges)
        pg_checksums_require_select(reloid);

    pg_checksums_check_relation_kind(rel, true, true);

    /*
     * A partitioned table has no storage of its own: its data lives in the
     * leaf partitions. Aggregate the physical checksums of all leaf
     * partitions into a single order-independent 64-bit value.
     */
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        List       *parts = pg_checksums_leaf_partitions(reloid);
        ChecksumAccum64 acc64 = {0, 0};
        ListCell   *lc;

        foreach(lc, parts)
        {
            Oid         partoid = lfirst_oid(lc);
            uint64      part_checksum;

            part_checksum = pg_table_physical_checksum_calc(partoid,
                                                            include_header,
                                                            false);

            checksum_accum64_add(&acc64,
                                 combine_checksums_64(part_checksum,
                                                      (uint64) partoid));
        }

        list_free(parts);
        relation_close(rel, AccessShareLock);

        final_checksum = checksum_accum64_finalize(&acc64);
        final_checksum = combine_checksums_64(final_checksum, (uint64) reloid);

        return final_checksum;
    }

    /*
     * TOAST tuples are not HeapTupleHeader structures.  Checksum the raw
     * pages of a TOAST relation instead of trying to interpret them as heap
     * tuples.
     */
    if (rel->rd_rel->relkind == RELKIND_TOASTVALUE)
    {
        nblocks = RelationGetNumberOfBlocks(rel);
        scan_relation_raw_pages(rel, 0, nblocks, &acc);

        aggregate = checksum_accum_finalize(&acc);

        if (aggregate == FNV_BASIS_32)
            aggregate = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);

        if (aggregate == 0)
            aggregate = 0x7FFFFFFF;

        aggregate = combine_checksums(aggregate,
                                      (uint32) rel->rd_rel->relfilenode);
        aggregate = combine_checksums(aggregate,
                                      (uint32) rel->rd_rel->reltablespace);

        relation_close(rel, AccessShareLock);

        return ((uint64) aggregate << 32) | reloid;
    }

    /*
     * Include the relation and storage identity (but not relpages/reltuples:
     * planner statistics are not part of the physical data and ANALYZE must
     * not change the checksum).  relfilenode changes on VACUUM FULL, CLUSTER
     * and SET TABLESPACE, so even an empty rewritten table gets a new
     * checksum.
     */
    relation_hash = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    relation_hash = combine_checksums(relation_hash,
                                      (uint32) rel->rd_rel->relfilenode);
    relation_hash = combine_checksums(relation_hash,
                                      (uint32) rel->rd_rel->reltablespace);

    nblocks = RelationGetNumberOfBlocks(rel);
    workers = pg_checksums_worker_count();
    if (nblocks < (uint32) workers)
        workers = (int) nblocks;

    if (workers < 1)
    {
        Snapshot    snapshot = pg_checksums_get_snapshot();

        scan_table_blocks_physical(rel, 0, nblocks, include_header,
                                   snapshot, &acc);
    }
    else
    {
        pg_checksums_parallel_scan_blocks(CHECKSUM_TASK_TABLE_PHYSICAL,
                                          reloid, include_header, nblocks,
                                          workers, &acc, NULL);
    }

    relation_close(rel, AccessShareLock);

    /* Compute order-independent aggregate */
    aggregate = checksum_accum_finalize(&acc);

    /* Combine with relation metadata */
    aggregate = combine_checksums(aggregate, relation_hash);

    if (aggregate == FNV_BASIS_32)
    {
        /* Guarantee hash for empty tables */
        aggregate = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    }

    if (aggregate == 0)
        aggregate = 0x7FFFFFFF;

    /* Combine aggregate hash (upper 32 bits) with OID (lower 32 bits) */
    final_checksum = ((uint64) aggregate << 32) | reloid;

    return final_checksum;
}

/*
 * pg_table_logical_checksum - SQL function for logical table checksums
 *
 * Computes a logical checksum for an entire table by aggregating
 * logical checksums of all tuples. Requires the table to have a
 * primary key.
 *
 * Returns NULL if the table has no primary key.
 * Returns a 64-bit value for tables with primary key.
 */
PG_FUNCTION_INFO_V1(pg_table_logical_checksum);

Datum
pg_table_logical_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    bool        is_null = false;
    int64       result;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    reloid = PG_GETARG_OID(0);

    result = pg_table_logical_checksum_calc(reloid, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_INT64(result);
}

/*
 * pg_table_logical_checksum_calc - implementation shared with partitions
 *
 * Sets *is_null when the checksum is undefined (no primary key, a NULL key
 * value, or a TOAST relation).
 */
static int64
pg_table_logical_checksum_calc(Oid reloid, bool check_privileges,
                               bool *is_null)
{
    ChecksumLogicalAccum acc = {0, 0};

    pg_table_logical_checksum_accum(reloid, check_privileges, &acc, is_null);

    if (*is_null)
        return 0;

    return (int64) logical_accum_finalize(&acc);
}

/*
 * pg_table_logical_checksum_accum - accumulate canonical logical rows
 *
 * There is no primary key requirement and no relation OID in the result.
 * Partitioned tables accumulate all leaf rows into the same accumulator, so
 * the result does not depend on how the table is partitioned.
 *
 * *is_null is set for TOAST relations, whose content is covered through the
 * parent table.
 */
static void
pg_table_logical_checksum_accum(Oid reloid, bool check_privileges,
                                ChecksumLogicalAccum *acc, bool *is_null)
{
    Relation    rel;
    char        relkind;

    *is_null = false;

    rel = relation_open(reloid, AccessShareLock);

    if (check_privileges)
        pg_checksums_require_select(reloid);

    pg_checksums_check_relation_kind(rel, true, true);

    relkind = rel->rd_rel->relkind;

    if (relkind == RELKIND_TOASTVALUE)
    {
        relation_close(rel, AccessShareLock);
        *is_null = true;
        return;
    }

    if (relkind == RELKIND_PARTITIONED_TABLE)
    {
        List       *parts;
        ListCell   *lc;
        bool        any_null = false;

        parts = pg_checksums_leaf_partitions(reloid);

        foreach(lc, parts)
        {
            Oid         partoid = lfirst_oid(lc);
            bool        part_null = false;

            pg_table_logical_checksum_accum(partoid, false, acc, &part_null);
            if (part_null)
                any_null = true;
        }

        list_free(parts);
        relation_close(rel, AccessShareLock);

        if (any_null)
            *is_null = true;
        return;
    }

    {
        Snapshot    snapshot = pg_checksums_get_snapshot();
        BlockNumber nblocks = RelationGetNumberOfBlocks(rel);
        int         workers = pg_checksums_worker_count();

        if (nblocks < (BlockNumber) workers)
            workers = (int) nblocks;

        if (workers < 1)
            scan_table_blocks_logical(rel, 0, nblocks, snapshot, acc);
        else
            pg_checksums_parallel_scan_blocks(CHECKSUM_TASK_TABLE_LOGICAL,
                                              reloid, false, nblocks, workers,
                                              NULL, acc);
    }

    relation_close(rel, AccessShareLock);
}

/*-------------------------------------------------------------------------
 * INDEX LEVEL FUNCTIONS (Physical and Logical)
 *-------------------------------------------------------------------------
 */

/*
 * Index Support Functions
 */

/*
 * index_supports_checksum - Check if index access method supports checksums
 *
 * Returns true for supported index types: B-tree, Hash, GiST, GIN,
 * SP-GiST, and BRIN. Other index types (like Bloom, Rum) are not
 * supported due to their specialized structures.
 */
static bool
index_supports_checksum(Oid amoid)
{
    return (amoid == BTREE_AM_OID ||
            amoid == HASH_AM_OID ||
            amoid == GIST_AM_OID ||
            amoid == GIN_AM_OID ||
            amoid == SPGIST_AM_OID ||
            amoid == BRIN_AM_OID);
}

/*-------------------------------------------------------------------------
 * Physical Index Checksum Functions
 *-------------------------------------------------------------------------
 */

/*
 * pg_index_physical_checksum - SQL function for physical index checksums
 *
 * Public interface for computing physical index checksums.
 * Supports all major index types. Returns NULL for unsupported index types
 * and for partitioned indexes.  Empty indexes are still distinguished by
 * their relation and storage identity.
 */
PG_FUNCTION_INFO_V1(pg_index_physical_checksum);

Datum
pg_index_physical_checksum(PG_FUNCTION_ARGS)
{
    Oid         indexoid;
    bool        is_null = false;
    uint32      index_checksum;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    indexoid = PG_GETARG_OID(0);

    index_checksum = pg_index_physical_checksum_calc(indexoid, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_INT32((int32) index_checksum);
}

/*
 * pg_index_physical_partitioned_checksum - aggregate leaf index checksums
 */
static uint32
pg_index_physical_partitioned_checksum(Oid indexoid, bool *is_null)
{
    List       *parts = pg_checksums_leaf_indexes(indexoid);
    ChecksumAccum acc = {0, 0};
    ListCell   *lc;

    *is_null = false;

    foreach(lc, parts)
    {
        Oid         partoid = lfirst_oid(lc);
        bool        part_null = false;
        uint32      part_checksum;

        part_checksum = pg_index_physical_checksum_calc(partoid, false,
                                                        &part_null);
        if (part_null)
            *is_null = true;
        else
            checksum_accum_add(&acc,
                               combine_checksums(part_checksum,
                                                 (uint32) partoid));
    }

    list_free(parts);

    if (*is_null)
        return 0;

    return combine_checksums(checksum_accum_finalize(&acc),
                             (uint32) indexoid);
}

/*
 * pg_index_physical_checksum_calc - implementation shared with database scan
 *
 * Sets *is_null for unsupported access methods.
 */
static uint32
pg_index_physical_checksum_calc(Oid indexoid, bool check_privileges,
                                bool *is_null)
{
    Relation    idxRel;
    ChecksumAccum acc = {0, 0};
    uint32      index_checksum = 0;
    uint32      index_type_hash;
    bool        is_brin;
    uint32      nblocks;
    int         workers;

    *is_null = false;

    /* Open index */
    idxRel = index_open(indexoid, AccessShareLock);

    /* Indexes derive their ACL from the underlying heap relation. */
    if (check_privileges)
        pg_checksums_require_select(indexoid);

    /*
     * A partitioned index has no storage of its own; aggregate the checksums
     * of its leaf indexes, as the table functions do for partitions.
     */
    if (idxRel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
    {
        bool        supported = index_supports_checksum(idxRel->rd_rel->relam);

        index_close(idxRel, AccessShareLock);

        if (!supported)
        {
            *is_null = true;
            return 0;
        }

        return pg_index_physical_partitioned_checksum(indexoid, is_null);
    }

    /* Check index type support */
    if (!index_supports_checksum(idxRel->rd_rel->relam))
    {
        index_close(idxRel, AccessShareLock);
        *is_null = true;
        return 0;
    }

    is_brin = (idxRel->rd_rel->relam == BRIN_AM_OID);
    nblocks = RelationGetNumberOfBlocks(idxRel);
    workers = pg_checksums_worker_count();
    if (nblocks < (uint32) workers)
        workers = (int) nblocks;

    if (workers < 1)
        scan_index_blocks_physical(idxRel, 0, nblocks, is_brin, &acc);
    else
        pg_checksums_parallel_scan_blocks(CHECKSUM_TASK_INDEX_PHYSICAL,
                                          indexoid, false, nblocks, workers,
                                          &acc, NULL);

    index_checksum = checksum_accum_finalize(&acc);

    /* Generic indexes also fold in their access-method metadata. */
    if (!is_brin)
    {
        index_type_hash = fnv1a_32_hash(&idxRel->rd_rel->relam, sizeof(Oid), FNV_BASIS_32);
        index_type_hash = combine_checksums(index_type_hash,
                                            (uint32) idxRel->rd_rel->relnatts);
        index_checksum = combine_checksums(index_checksum, index_type_hash);
    }

    /*
     * Index rewrites (REINDEX, VACUUM FULL, CLUSTER) assign a new
     * relfilenode.  Include it so a rebuilt index gets a different physical
     * checksum even when its pages happen to be byte-identical.
     */
    index_checksum = combine_checksums(index_checksum,
                                       (uint32) idxRel->rd_rel->relfilenode);
    index_checksum = combine_checksums(index_checksum,
                                       (uint32) idxRel->rd_rel->reltablespace);

    /* Close index */
    index_close(idxRel, AccessShareLock);

    return index_checksum;
}

/*-------------------------------------------------------------------------
 * Logical Index Checksum Functions
 *-------------------------------------------------------------------------
 */

/*
 * pg_index_logical_checksum - SQL function for logical index checksums
 *
 * Public interface for computing logical index checksums.
 * Returns NULL for unsupported index types.
 */
PG_FUNCTION_INFO_V1(pg_index_logical_checksum);

Datum
pg_index_logical_checksum(PG_FUNCTION_ARGS)
{
    Oid         indexoid;
    bool        is_null = false;
    uint32      index_checksum;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    indexoid = PG_GETARG_OID(0);

    index_checksum = pg_index_logical_checksum_calc(indexoid, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();

    PG_RETURN_INT32((int32) index_checksum);
}

/*
 * pg_index_logical_checksum_accum - heap-derived logical index content
 *
 * A logical index checksum describes the index entries that the table data
 * implies, not the physical index pages.  We evaluate the index definition
 * (plain columns, expressions, partial predicate and INCLUDE columns) on each
 * visible heap row.  This is independent of the access method, so it works
 * for B-tree, Hash, GiST, GIN, SP-GiST, BRIN and custom AMs, and it does not
 * contain heap TIDs, relation OIDs or type OIDs.
 *
 * A physical index checksum remains the tool for detecting missing/corrupt
 * index entries; this logical checksum is a fingerprint of the expected
 * logical index content.
 */
static void
pg_index_logical_checksum_accum(Oid indexoid, bool check_privileges,
                                ChecksumLogicalAccum *acc, bool *is_null)
{
    Relation    idxRel;
    Relation    heapRel;
    Oid         heapoid;
    IndexInfo  *indexInfo;
    EState     *estate;
    ExprContext *econtext;
    ExprState  *predicate;
    TupleTableSlot *slot;
    TableScanDesc scan;
    Snapshot    snapshot;
    Datum      *values;
    bool       *nulls;
    int         nattrs;
    int         i;
    bool        any_null = false;
    MemoryContext index_context;
    MemoryContext oldcontext;

    *is_null = false;

    idxRel = index_open(indexoid, AccessShareLock);

    if (check_privileges)
        pg_checksums_require_select(indexoid);

    if (idxRel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
    {
        List       *parts = pg_checksums_leaf_indexes(indexoid);
        ListCell   *lc;

        foreach(lc, parts)
        {
            Oid         partoid = lfirst_oid(lc);
            bool        part_null = false;
            uint32      part_checksum;

            part_checksum = pg_index_logical_checksum_calc(partoid, false,
                                                           &part_null);
            if (part_null)
                any_null = true;
            else
                logical_accum_add(acc, part_checksum);
        }

        list_free(parts);
        index_close(idxRel, AccessShareLock);

        if (any_null)
            *is_null = true;
        return;
    }

    if (!idxRel->rd_index->indisvalid ||
        !idxRel->rd_index->indisready ||
        !idxRel->rd_index->indislive)
    {
        index_close(idxRel, AccessShareLock);
        *is_null = true;
        return;
    }

    heapoid = IndexGetRelation(indexoid, false);
    heapRel = relation_open(heapoid, AccessShareLock);

    if (heapRel->rd_rel->relkind != RELKIND_RELATION &&
        heapRel->rd_rel->relkind != RELKIND_MATVIEW &&
        heapRel->rd_rel->relkind != RELKIND_TOASTVALUE)
    {
        relation_close(heapRel, AccessShareLock);
        index_close(idxRel, AccessShareLock);
        *is_null = true;
        return;
    }

    snapshot = pg_checksums_get_snapshot();

    index_context = AllocSetContextCreate(CurrentMemoryContext,
                                          "pg_checksums index logical",
                                          ALLOCSET_SMALL_SIZES);
    oldcontext = MemoryContextSwitchTo(index_context);

    indexInfo = BuildIndexInfo(idxRel);
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
    slot = table_slot_create(heapRel, NULL);

    nattrs = indexInfo->ii_NumIndexAttrs;
    values = palloc(sizeof(Datum) * nattrs);
    nulls = palloc(sizeof(bool) * nattrs);

    scan = table_beginscan(heapRel, snapshot, 0, NULL);

    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        uint32      row_hash = FNV_BASIS_32;

        ResetExprContext(econtext);
        econtext->ecxt_scantuple = slot;

        if (predicate != NULL && !ExecQual(predicate, econtext))
            continue;

        FormIndexDatum(indexInfo, slot, estate, values, nulls);

        for (i = 0; i < nattrs; i++)
        {
            Form_pg_attribute att = TupleDescAttr(RelationGetDescr(idxRel), i);
            uint32      col_hash;

            col_hash = pg_cell_checksum_canonical(values[i], nulls[i],
                                                  att->atttypid,
                                                  att->atttypmod,
                                                  i + 1);
            row_hash = combine_checksums(row_hash, col_hash);
        }

        logical_accum_add(acc, row_hash);
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);
    pfree(values);
    pfree(nulls);
    relation_close(heapRel, AccessShareLock);
    index_close(idxRel, AccessShareLock);

    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(index_context);
}

/*
 * pg_index_logical_checksum_calc - implementation shared with database scan
 */
static uint32
pg_index_logical_checksum_calc(Oid indexoid, bool check_privileges,
                               bool *is_null)
{
    ChecksumLogicalAccum acc = {0, 0};

    pg_index_logical_checksum_accum(indexoid, check_privileges, &acc,
                                    is_null);

    if (*is_null)
        return 0;

    return logical_accum_finalize(&acc);
}

/*-------------------------------------------------------------------------
 * PARALLEL SCAN IMPLEMENTATION
 *-------------------------------------------------------------------------
 *
 * The serial and parallel code paths share the block-range (or item-range)
 * scan functions below. In the serial case the whole range is processed by
 * a single backend; in the parallel case the range is split dynamically
 * among the leader and its workers using an atomic counter. Because the
 * aggregation is order- and partition-independent, both paths produce
 * identical results.
 */

/*
 * pg_checksums_consume - claim the next range of items
 *
 * Atomically advances the shared counter and returns the resulting range
 * [start, end). Returns false when all items have been claimed. This
 * self-balancing scheme keeps all participants busy regardless of how many
 * workers actually started.
 */
static bool
pg_checksums_consume(ChecksumParallelState *state, uint32 chunk,
                     uint32 *start, uint32 *end)
{
    uint32      first = pg_atomic_fetch_add_u32(&state->next_item, chunk);

    if (first >= state->nitems)
        return false;

    *start = first;

    /*
     * Compute the end without first + chunk, which could wrap around for a
     * relation with close to 2^32 blocks.
     */
    if (chunk >= state->nitems - first)
        *end = state->nitems;
    else
        *end = first + chunk;

    return true;
}

/*
 * scan_table_blocks_physical - physical checksum over a range of heap blocks
 *
 * Iterates the raw pages of the relation, applying the same visibility
 * rules as a sequential scan so that the resulting tuple set is identical
 * to what the serial path would have seen.
 */
static void
scan_table_blocks_physical(Relation rel, BlockNumber start, BlockNumber end,
                           bool include_header, Snapshot snapshot,
                           ChecksumAccum *acc)
{
    BlockNumber blkno;
    BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;
        OffsetNumber offnum;

        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                    bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        if (PageIsNew(page))
        {
            UnlockReleaseBuffer(buffer);
            if ((blkno & 63) == 0)
                CHECK_FOR_INTERRUPTS();
            continue;
        }

        pg_checksums_check_page(rel, blkno, page);

        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId      lp = PageGetItemId(page, offnum);
            HeapTupleHeader htup;
            HeapTupleData tuple;
            Size        tuple_len;
            uint32      tuple_checksum;

            if (!ItemIdIsNormal(lp))
                continue;

            if (!pg_checksums_item_is_sane(page, lp))
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg("corrupt line pointer in relation \"%s\" at block %u, offset %u",
                                RelationGetRelationName(rel), blkno, offnum)));

            htup = (HeapTupleHeader) PageGetItem(page, lp);
            tuple_len = ItemIdGetLength(lp);

            pg_checksums_check_heap_tuple(rel, blkno, offnum, htup, tuple_len);

            tuple.t_len = tuple_len;
            tuple.t_data = htup;
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            if (!HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer))
                continue;

            tuple_checksum = pg_tuple_physical_checksum_internal(rel, page,
                                                                 offnum, blkno,
                                                                 include_header);
            if (tuple_checksum != 0)
                checksum_accum_add(acc, tuple_checksum);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*
 * scan_table_blocks_logical - logical checksum over a range of heap blocks
 */
static void
scan_table_blocks_logical(Relation rel, BlockNumber start, BlockNumber end,
                          Snapshot snapshot, ChecksumLogicalAccum *acc)
{
    BlockNumber blkno;
    BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;
        OffsetNumber offnum;

        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                    bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        if (PageIsNew(page))
        {
            UnlockReleaseBuffer(buffer);
            if ((blkno & 63) == 0)
                CHECK_FOR_INTERRUPTS();
            continue;
        }

        pg_checksums_check_page(rel, blkno, page);

        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId      lp = PageGetItemId(page, offnum);
            HeapTupleHeader htup;
            HeapTupleData tuple;
            Size        tuple_len;
            uint32      tuple_checksum;

            if (!ItemIdIsNormal(lp))
                continue;

            if (!pg_checksums_item_is_sane(page, lp))
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg("corrupt line pointer in relation \"%s\" at block %u, offset %u",
                                RelationGetRelationName(rel), blkno, offnum)));

            htup = (HeapTupleHeader) PageGetItem(page, lp);
            tuple_len = ItemIdGetLength(lp);

            pg_checksums_check_heap_tuple(rel, blkno, offnum, htup, tuple_len);

            tuple.t_len = tuple_len;
            tuple.t_data = htup;
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            if (!HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer))
                continue;

            tuple_checksum = pg_tuple_logical_checksum_internal(rel, &tuple);
            logical_accum_add(acc, tuple_checksum);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*
 * scan_index_blocks_physical - physical checksum over a range of index blocks
 *
 * Handles both generic index types and BRIN, preserving the per-page hash
 * computation used by the serial path.
 */
static void
scan_index_blocks_physical(Relation idxRel, BlockNumber start, BlockNumber end,
                           bool is_brin, ChecksumAccum *acc)
{
    BlockNumber blkno;
    Size        page_size;
    BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        uint32      page_hash;

        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buffer);

        if (!PageIsNew(page))
        {
            PageHeader phdr = (PageHeader) page;

            /*
             * Always hash a complete block.  PageGetPageSize() comes from
             * the page header and a corrupted value could make us read past
             * the buffer.
             */
            page_size = BLCKSZ;
            page_hash = fnv1a_32_hash((char *) page, page_size, 0);

            if (is_brin)
            {
                uint8      *special_space;
                Size        special_size;

                page_hash = combine_checksums(page_hash, (uint32) page_size);

                /*
                 * The BRIN special area can only be inspected when the page
                 * header is sane; the raw page bytes are still hashed above.
                 */
                if (pg_checksums_page_header_sane(page))
                {
                    special_space = (uint8 *) PageGetSpecialPointer(page);
                    special_size = PageGetSpecialSize(page);
                    if (special_size >= 4)
                    {
                        uint32      brin_info = 0;

                        memcpy(&brin_info, special_space, 4);
                        page_hash = combine_checksums(page_hash, brin_info);
                    }
                }

                page_hash = combine_checksums(page_hash, blkno);
                page_hash = combine_checksums(page_hash, phdr->pd_lower);
                page_hash = combine_checksums(page_hash, phdr->pd_upper);
                page_hash = combine_checksums(page_hash, phdr->pd_flags);
            }
            else
            {
                page_hash = combine_checksums(page_hash, phdr->pd_lower);
                page_hash = combine_checksums(page_hash, phdr->pd_upper);
                page_hash = combine_checksums(page_hash, phdr->pd_special);
                page_hash = combine_checksums(page_hash, blkno);
                page_hash = combine_checksums(page_hash, phdr->pd_flags);
                page_hash = combine_checksums(page_hash, (uint32) page_size);
            }

            checksum_accum_add(acc, page_hash);
        }
        else
        {
            page_hash = fnv1a_32_hash(is_brin ? "BRIN_NEW" : "NEW_PAGE", 8, blkno);
            page_hash = combine_checksums(page_hash, (uint32) PageGetPageSize(page));
            checksum_accum_add(acc, page_hash);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*
 * compute_database_item - compute the checksum of one relation
 *
 * Sets *skip when the relation has no usable checksum (unsupported AM,
 * partitioned index, table without a primary key in logical mode, or TOAST
 * relation in logical mode).
 */
static void
compute_database_item(Oid relid, char relkind, bool physical,
                      uint64 *rel_checksum, bool *skip)
{
    *rel_checksum = 0;
    *skip = false;

    if (relkind == RELKIND_INDEX)
    {
        bool        is_null = false;
        uint32      value;

        if (physical)
            value = pg_index_physical_checksum_calc(relid, false, &is_null);
        else
            value = pg_index_logical_checksum_calc(relid, false, &is_null);

        if (is_null)
            *skip = true;
        else
            *rel_checksum = (uint64) value;
    }
    else
    {
        if (physical)
            *rel_checksum = pg_table_physical_checksum_calc(relid, false,
                                                            false);
        else
        {
            bool        is_null = false;
            int64       value;

            value = pg_table_logical_checksum_calc(relid, false, &is_null);
            if (is_null)
                *skip = true;
            else
                *rel_checksum = (uint64) value;
        }
    }
}

/*
 * pg_checksums_report_skipped_relation - strict-mode error for skipped relation
 */
static void
pg_checksums_report_skipped_relation(Oid relid)
{
    char       *relname;

    if (!pg_checksums_strict)
        return;

    relname = get_rel_name(relid);

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("relation \"%s\" cannot be checksummed",
                    relname ? relname : "?"),
             errdetail("The relation has no usable checksum (missing primary key, unsupported index access method, or a scan error)."),
             errhint("Set pg_checksums.strict = false to skip relations without a usable checksum.")));
}

/*
 * compute_database_items - database checksum over a range of relations
 */
static void
compute_database_items(ChecksumRelationItem *items, uint32 start, uint32 end,
                       bool physical, ChecksumAccum64 *acc,
                       ChecksumLogicalAccum *logical_acc, bool *had_error)
{
    uint32      i;

    for (i = start; i < end; i++)
    {
        Oid         relid = items[i].reloid;
        char        relkind = items[i].relkind;
        uint64      rel_checksum = 0;
        bool        skip = false;
        MemoryContext oldcontext;
        ResourceOwner oldowner;

#if PG_VERSION_NUM < 170000
        /*
         * PostgreSQL 12-16 prohibit internal subtransactions while in
         * parallel mode.  The leader of a parallel database scan is in
         * parallel mode too, so neither the leader nor a worker can use the
         * subtransaction-based error recovery below.  Run the relation
         * checksum directly; an error then aborts the parallel scan.  There
         * is no safe way to catch and continue without a subtransaction.
         */
        if (IsInParallelMode())
        {
            compute_database_item(relid, relkind, physical, &rel_checksum,
                                  &skip);
            if (skip)
                pg_checksums_report_skipped_relation(relid);
            else if (logical_acc != NULL)
                logical_accum_add(logical_acc, (uint32) rel_checksum);
            else
                checksum_accum64_add(acc,
                                     combine_checksums_64(rel_checksum,
                                                          (uint64) relid));
            continue;
        }
#endif

        /*
         * Each relation is checksummed in an internal subtransaction.  If a
         * relation cannot be processed (dropped concurrently, corrupt,
         * unsupported, ...), the subtransaction rollback releases its locks
         * and buffer pins, and the scan can safely continue.  Catching an
         * error without a subtransaction would leave resources behind.
         */
        oldcontext = CurrentMemoryContext;
        oldowner = CurrentResourceOwner;

        BeginInternalSubTransaction(NULL);
        /* Run statements inside the caller's memory context */
        MemoryContextSwitchTo(oldcontext);

        PG_TRY();
        {
            compute_database_item(relid, relkind, physical, &rel_checksum,
                                  &skip);

            if (skip)
                pg_checksums_report_skipped_relation(relid);
            else if (logical_acc != NULL)
                logical_accum_add(logical_acc, (uint32) rel_checksum);
            else
                checksum_accum64_add(acc,
                                     combine_checksums_64(rel_checksum,
                                                          (uint64) relid));

            /* Commit the inner transaction, return to outer xact context */
            ReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcontext);
            CurrentResourceOwner = oldowner;
        }
        PG_CATCH();
        {
            int         sqlerrcode = geterrcode();
            ErrorData  *edata;

            /*
             * Cancellation, shutdown and out-of-memory errors mean that the
             * result cannot be trusted; propagate them instead of silently
             * returning a partial database checksum.
             */
            /*
             * OOM and shutdown errors are propagated immediately.  Other
             * errors, including cancellation and strict-mode failures, first
             * release the internal subtransaction: a caller (for example a
             * PL/pgSQL exception block) may catch the error and continue, and
             * an open parallel child transaction would keep
             * IsInParallelMode() true.
             */
            if (sqlerrcode == ERRCODE_OUT_OF_MEMORY ||
                sqlerrcode == ERRCODE_ADMIN_SHUTDOWN ||
                sqlerrcode == ERRCODE_CRASH_SHUTDOWN ||
                sqlerrcode == ERRCODE_CANNOT_CONNECT_NOW)
                PG_RE_THROW();

            MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            FlushErrorState();

            /* Abort the inner transaction */
            RollbackAndReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcontext);
            CurrentResourceOwner = oldowner;

            if (had_error != NULL)
                *had_error = true;

            if (pg_checksums_strict || sqlerrcode == ERRCODE_QUERY_CANCELED)
                ReThrowError(edata);

            /*
             * Parallel workers log to the server log; the leader emits a
             * summary warning when it folds the workers' results.
             */
            if (IsParallelWorker())
                elog(LOG,
                     "skipping relation %u while computing database checksum: %s",
                     relid, edata->message);
            else
                ereport(WARNING,
                        (errmsg("skipping relation %u while computing database checksum",
                                relid),
                         errdetail("%s", edata->message)));

            FreeErrorData(edata);
        }
        PG_END_TRY();
    }
}

/*
 * pg_checksums_parallel_worker_main - entry point for parallel workers
 *
 * Restores the shared scan state and snapshot, then consumes items from the
 * shared atomic counter until none remain, writing a fixed-size result back
 * to shared memory before exiting.
 */
void
pg_checksums_parallel_worker_main(dsm_segment *seg, shm_toc *toc)
{
    ChecksumParallelState *state = shm_toc_lookup(toc, KEY_STATE, false);
    ChecksumWorkerResult *results = shm_toc_lookup(toc, KEY_RESULTS, false);
    int         worker_number = ParallelWorkerNumber;
    ChecksumWorkerResult *myresult = &results[worker_number];
    Snapshot    snapshot = NULL;
    uint32      start;
    uint32      end;

    (void) seg;

    myresult->partial = 0;
    myresult->count = 0;
    myresult->found_invalid = false;
    myresult->had_error = false;

    if (state->snapshot_size > 0)
    {
        char       *snap = shm_toc_lookup(toc, KEY_SNAPSHOT, false);

        snapshot = RestoreSnapshot(snap);
    }

    switch (state->task_type)
    {
        case CHECKSUM_TASK_TABLE_PHYSICAL:
            {
                Relation    rel = relation_open(state->reloid, AccessShareLock);
                ChecksumAccum acc = {0, 0};

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_table_blocks_physical(rel, start, end,
                                               state->include_header, snapshot,
                                               &acc);

                relation_close(rel, AccessShareLock);
                myresult->partial = acc.partial;
                myresult->count = acc.count;
                break;
            }
        case CHECKSUM_TASK_TABLE_LOGICAL:
            {
                Relation    rel = relation_open(state->reloid, AccessShareLock);
                ChecksumLogicalAccum acc = {0, 0};

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_table_blocks_logical(rel, start, end, snapshot, &acc);

                relation_close(rel, AccessShareLock);
                myresult->partial = acc.sum;
                myresult->count = acc.count;
                break;
            }
        case CHECKSUM_TASK_INDEX_PHYSICAL:
            {
                Relation    idxRel = index_open(state->reloid, AccessShareLock);
                bool        is_brin = (idxRel->rd_rel->relam == BRIN_AM_OID);
                ChecksumAccum acc = {0, 0};

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_index_blocks_physical(idxRel, start, end, is_brin, &acc);

                index_close(idxRel, AccessShareLock);
                myresult->partial = acc.partial;
                myresult->count = acc.count;
                break;
            }
        case CHECKSUM_TASK_DATABASE_PHYSICAL:
        case CHECKSUM_TASK_DATABASE_LOGICAL:
            {
                ChecksumRelationItem *items = shm_toc_lookup(toc, KEY_ITEMS, false);
                bool        physical = (state->task_type == CHECKSUM_TASK_DATABASE_PHYSICAL);
                ChecksumAccum64 acc64 = {0, 0};
                ChecksumLogicalAccum lacc = {0, 0};
                bool        pushed = false;

                /*
                 * The relation checksum functions rely on GetActiveSnapshot(),
                 * so push the leader's snapshot to keep the whole database
                 * consistent.
                 */
                if (snapshot != NULL)
                {
                    PushActiveSnapshot(snapshot);
                    pushed = true;
                }

                while (pg_checksums_consume(state, 1, &start, &end))
                    compute_database_items(items, start, end, physical, &acc64,
                                           physical ? NULL : &lacc,
                                           &myresult->had_error);

                if (pushed)
                    PopActiveSnapshot();

                if (physical)
                {
                    myresult->partial = acc64.partial;
                    myresult->count = acc64.count;
                }
                else
                {
                    myresult->partial = lacc.sum;
                    myresult->count = lacc.count;
                }
                break;
            }
        default:
            elog(ERROR, "unknown checksum parallel task type %d",
                 (int) state->task_type);
    }
}

/*
 * pg_checksums_parallel_scan_blocks - run a block-based scan in parallel
 *
 * Sets up the shared memory segment, launches the requested workers, lets the
 * leader participate in the scan, and folds the workers' results into the
 * caller's accumulator.  Physical/index scans use acc; table logical scans
 * use logical_acc.
 */
static void
pg_checksums_parallel_scan_blocks(ChecksumTaskType task_type, Oid reloid,
                                  bool include_header, uint32 nblocks,
                                  int workers, ChecksumAccum *acc,
                                  ChecksumLogicalAccum *logical_acc)
{
    ParallelContext *pcxt;
    ParallelContext *volatile pcxt_cleanup = NULL;
    shm_toc_estimator e;
    ChecksumParallelState *state;
    ChecksumWorkerResult *results;
    Snapshot    snapshot = NULL;
    uint32      snapshot_size = 0;
    bool        is_table = (task_type == CHECKSUM_TASK_TABLE_PHYSICAL ||
                            task_type == CHECKSUM_TASK_TABLE_LOGICAL);
    uint32      start;
    uint32      end;
    int         i;

    if (is_table)
    {
        snapshot = pg_checksums_get_snapshot();
        if (snapshot == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("no active snapshot while starting a parallel checksum scan")));
        snapshot_size = (uint32) EstimateSnapshotSpace(snapshot);
    }

    /*
     * CreateParallelContext() requires parallel mode to be active.  It also
     * makes IsInParallelMode() true in the leader, which prevents the leader
     * from starting a nested parallel scan when it processes its own share
     * of a database scan.
     */
    EnterParallelMode();

    PG_TRY();
    {
        pcxt = CreateParallelContext("pg_checksums",
                                     "pg_checksums_parallel_worker_main",
                                     workers);
        pcxt_cleanup = pcxt;

        shm_toc_initialize_estimator(&e);
        shm_toc_estimate_chunk(&e, sizeof(ChecksumParallelState));
        shm_toc_estimate_chunk(&e, sizeof(ChecksumWorkerResult) * workers);
        if (snapshot_size > 0)
            shm_toc_estimate_chunk(&e, snapshot_size);
        shm_toc_estimate_keys(&e, 3);
        pcxt->estimator = e;

        InitializeParallelDSM(pcxt);

        state = shm_toc_allocate(pcxt->toc, sizeof(ChecksumParallelState));
        shm_toc_insert(pcxt->toc, KEY_STATE, state);
        state->task_type = task_type;
        state->reloid = reloid;
        state->include_header = include_header;
        state->include_system = false;
        state->include_toast = false;
        state->nitems = nblocks;
        state->snapshot_size = snapshot_size;
        pg_atomic_init_u32(&state->next_item, 0);

        results = shm_toc_allocate(pcxt->toc, sizeof(ChecksumWorkerResult) * workers);
        shm_toc_insert(pcxt->toc, KEY_RESULTS, results);
        memset(results, 0, sizeof(ChecksumWorkerResult) * workers);

        if (snapshot_size > 0)
        {
            char       *snap = shm_toc_allocate(pcxt->toc, snapshot_size);

            SerializeSnapshot(snapshot, snap);
            shm_toc_insert(pcxt->toc, KEY_SNAPSHOT, snap);
        }

        LaunchParallelWorkers(pcxt);

        if (pcxt->nworkers_launched > 0)
            WaitForParallelWorkersToAttach(pcxt);

        /* The leader processes its own share of the work. */
        switch (task_type)
        {
            case CHECKSUM_TASK_TABLE_PHYSICAL:
                {
                    Relation    rel = relation_open(reloid, AccessShareLock);

                    while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                                &start, &end))
                        scan_table_blocks_physical(rel, start, end, include_header,
                                                   snapshot, acc);

                    relation_close(rel, AccessShareLock);
                    break;
                }
            case CHECKSUM_TASK_TABLE_LOGICAL:
                {
                    Relation    rel = relation_open(reloid, AccessShareLock);

                    while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                                &start, &end))
                        scan_table_blocks_logical(rel, start, end, snapshot,
                                                  logical_acc);

                    relation_close(rel, AccessShareLock);
                    break;
                }
            case CHECKSUM_TASK_INDEX_PHYSICAL:
                {
                    Relation    idxRel = index_open(reloid, AccessShareLock);
                    bool        is_brin = (idxRel->rd_rel->relam == BRIN_AM_OID);

                    while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                                &start, &end))
                        scan_index_blocks_physical(idxRel, start, end, is_brin, acc);

                    index_close(idxRel, AccessShareLock);
                    break;
                }
            default:
                break;
        }

        if (pcxt->nworkers_launched > 0)
            WaitForParallelWorkersToFinish(pcxt);

        /* Fold in the workers' results. */
        for (i = 0; i < workers; i++)
        {
            if (pcxt->worker[i].bgwhandle != NULL)
            {
                if (task_type == CHECKSUM_TASK_TABLE_LOGICAL)
                {
                    logical_acc->sum += results[i].partial;
                    logical_acc->count += results[i].count;
                }
                else
                    checksum_accum_merge(acc, results[i].partial,
                                         results[i].count);
            }
        }
    }
    PG_CATCH();
    {
        if (pcxt_cleanup != NULL)
            DestroyParallelContext(pcxt_cleanup);
        ExitParallelMode();
        PG_RE_THROW();
    }
    PG_END_TRY();

    DestroyParallelContext(pcxt);
    ExitParallelMode();
}

/*
 * pg_checksums_parallel_scan_database - run a database scan in parallel
 */
static void
pg_checksums_parallel_scan_database(bool physical, bool include_system,
                                    bool include_toast,
                                    ChecksumRelationItem *items, uint32 nitems,
                                    int workers, ChecksumAccum64 *acc,
                                    ChecksumLogicalAccum *logical_acc,
                                    bool *had_error)
{
    ParallelContext *pcxt;
    ParallelContext *volatile pcxt_cleanup = NULL;
    shm_toc_estimator e;
    ChecksumParallelState *state;
    ChecksumWorkerResult *results;
    Snapshot    snapshot;
    uint32      snapshot_size;
    uint32      start;
    uint32      end;
    bool        local_had_error = false;
    int         i;

    /*
     * Propagate a consistent snapshot to the workers so that every relation
     * is checksummed as of the same point in time, exactly like the serial
     * path.
     */
    snapshot = pg_checksums_get_snapshot();
    if (snapshot == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("no active snapshot while starting a parallel database checksum")));
    snapshot_size = (uint32) EstimateSnapshotSpace(snapshot);

    EnterParallelMode();

    PG_TRY();
    {
        pcxt = CreateParallelContext("pg_checksums",
                                     "pg_checksums_parallel_worker_main",
                                     workers);
        pcxt_cleanup = pcxt;

        shm_toc_initialize_estimator(&e);
        shm_toc_estimate_chunk(&e, sizeof(ChecksumParallelState));
        shm_toc_estimate_chunk(&e, sizeof(ChecksumWorkerResult) * workers);
        shm_toc_estimate_chunk(&e, sizeof(ChecksumRelationItem) * nitems);
        shm_toc_estimate_chunk(&e, snapshot_size);
        shm_toc_estimate_keys(&e, 4);
        pcxt->estimator = e;

        InitializeParallelDSM(pcxt);

        state = shm_toc_allocate(pcxt->toc, sizeof(ChecksumParallelState));
        shm_toc_insert(pcxt->toc, KEY_STATE, state);
        state->task_type = physical ? CHECKSUM_TASK_DATABASE_PHYSICAL
                                    : CHECKSUM_TASK_DATABASE_LOGICAL;
        state->reloid = InvalidOid;
        state->include_header = false;
        state->include_system = include_system;
        state->include_toast = include_toast;
        state->nitems = nitems;
        state->snapshot_size = snapshot_size;
        pg_atomic_init_u32(&state->next_item, 0);

        results = shm_toc_allocate(pcxt->toc, sizeof(ChecksumWorkerResult) * workers);
        shm_toc_insert(pcxt->toc, KEY_RESULTS, results);
        memset(results, 0, sizeof(ChecksumWorkerResult) * workers);

        {
            char       *snap = shm_toc_allocate(pcxt->toc, snapshot_size);

            SerializeSnapshot(snapshot, snap);
            shm_toc_insert(pcxt->toc, KEY_SNAPSHOT, snap);
        }

        {
            ChecksumRelationItem *ditems = shm_toc_allocate(pcxt->toc,
                                                            sizeof(ChecksumRelationItem) * nitems);

            memcpy(ditems, items, sizeof(ChecksumRelationItem) * nitems);
            shm_toc_insert(pcxt->toc, KEY_ITEMS, ditems);
        }

        LaunchParallelWorkers(pcxt);

        if (pcxt->nworkers_launched > 0)
            WaitForParallelWorkersToAttach(pcxt);

        /* The leader processes its own share of the relations. */
        while (pg_checksums_consume(state, 1, &start, &end))
            compute_database_items(items, start, end, physical, acc,
                                   logical_acc, &local_had_error);

        if (pcxt->nworkers_launched > 0)
            WaitForParallelWorkersToFinish(pcxt);

        for (i = 0; i < workers; i++)
        {
            if (pcxt->worker[i].bgwhandle != NULL)
            {
                if (physical)
                    checksum_accum64_merge(acc, results[i].partial,
                                           results[i].count);
                else
                {
                    logical_acc->sum += results[i].partial;
                    logical_acc->count += results[i].count;
                }

                if (results[i].had_error)
                    local_had_error = true;
            }
        }

    }
    PG_CATCH();
    {
        if (pcxt_cleanup != NULL)
            DestroyParallelContext(pcxt_cleanup);
        ExitParallelMode();
        PG_RE_THROW();
    }
    PG_END_TRY();

    DestroyParallelContext(pcxt);
    ExitParallelMode();

    if (had_error != NULL)
        *had_error = local_had_error;
}

/*-------------------------------------------------------------------------
 * DATABASE LEVEL FUNCTIONS (Physical and Logical)
 *-------------------------------------------------------------------------
 */

/*
 * compute_database_checksum_internal - Core database checksum calculation
 *
 * Computes a checksum for the entire database by aggregating checksums
 * of all relations. This is a resource-intensive operation that should
 * be run by superusers only.
 *
 * Features:
 * 1. Uses a consistent snapshot for data consistency
 * 2. Can filter out system catalogs and TOAST tables
 * 3. Skips relations that cannot be processed (with error handling)
 * 4. Handles both physical and logical checksum modes
 *
 * Returns a 64-bit FNV-1a hash.
 */
static uint64
compute_database_checksum_internal(bool physical, bool include_system,
                                  bool include_toast, bool include_indexes)
{
    Relation    pg_class_rel;
    TableScanDesc scan;
    HeapTuple   classTuple;
    Snapshot    snapshot;
    ChecksumRelationItem *items;
    uint32      nitems = 0;
    uint32      capacity = 64;
    ChecksumAccum64 acc = {0, 0};
    ChecksumLogicalAccum lacc = {0, 0};
    bool        had_error = false;
    int         workers;

    /* Use a consistent snapshot */
    snapshot = pg_checksums_get_snapshot();
    if (snapshot == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("no active snapshot while computing database checksum")));

    /* Collect the matching relations first. */
    items = (ChecksumRelationItem *) palloc(capacity * sizeof(ChecksumRelationItem));

    pg_class_rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan(pg_class_rel, snapshot, 0, NULL);

    while ((classTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
        Oid         relid = classForm->oid;
        Oid         relnamespace = classForm->relnamespace;
        char        relkind = classForm->relkind;

        /*
         * Temporary relations are session-local: they are not part of the
         * persistent database, other sessions must not read them, and a
         * parallel worker is not allowed to touch them at all. Skipping them
         * keeps the database checksum stable and independent of the calling
         * session's temporary objects.
         */
        if (isAnyTempNamespace(relnamespace))
            continue;

        /*
         * Shared catalogs belong to the cluster rather than to this
         * database.  They are deliberately not part of a per-database
         * checksum, even when include_system is true.
         */
        if (classForm->relisshared)
            continue;

        /*
         * TOAST relations use a tuple format of their own.  A physical
         * checksum can hash their raw pages; a logical checksum already
         * covers the toast content when the parent table detoasts it.
         */
        if (relnamespace == PG_TOAST_NAMESPACE)
        {
            if (!physical || !include_toast)
                continue;
        }
        else if (!include_system && relnamespace == PG_CATALOG_NAMESPACE)
            continue;

        /* Skip non-table/index relations */
        if (relkind != RELKIND_RELATION &&
            relkind != RELKIND_INDEX &&
            relkind != RELKIND_MATVIEW &&
            relkind != RELKIND_TOASTVALUE)
            continue;

        if (!include_indexes && relkind == RELKIND_INDEX)
            continue;

        if (nitems >= capacity)
        {
            capacity *= 2;
            items = (ChecksumRelationItem *)
                repalloc(items, capacity * sizeof(ChecksumRelationItem));
        }

        items[nitems].reloid = relid;
        items[nitems].relkind = relkind;
        nitems++;

        CHECK_FOR_INTERRUPTS();
    }

    table_endscan(scan);
    table_close(pg_class_rel, AccessShareLock);

    workers = pg_checksums_worker_count();
    if (nitems < (uint32) workers)
        workers = (int) nitems;

    if (workers < 1)
        compute_database_items(items, 0, nitems, physical, &acc,
                               physical ? NULL : &lacc, &had_error);
    else
        pg_checksums_parallel_scan_database(physical, include_system,
                                            include_toast, items, nitems,
                                            workers, &acc,
                                            physical ? NULL : &lacc,
                                            &had_error);

    pfree(items);

    if (had_error)
        ereport(WARNING,
                (errmsg("database checksum skipped one or more relations"),
                 errhint("Inspect the server log for the affected relations.")));

    if (physical)
        return checksum_accum64_finalize(&acc);

    return (uint64) logical_accum_finalize(&lacc);
}

/*
 * pg_database_physical_checksum - SQL function for physical database checksums
 *
 * Public interface for computing physical database checksums.
 * Requires superuser privileges for security.
 *
 * Parameters:
 *   include_system: Include system catalogs in checksum
 *   include_toast: Include TOAST tables in checksum
 */
PG_FUNCTION_INFO_V1(pg_database_physical_checksum);

Datum
pg_database_physical_checksum(PG_FUNCTION_ARGS)
{
    bool        include_system = false;
    bool        include_toast = false;
    uint64      db_checksum;
    
    /* Parse optional parameters */
    if (PG_NARGS() >= 1)
        include_system = PG_GETARG_BOOL(0);
    if (PG_NARGS() >= 2)
        include_toast = PG_GETARG_BOOL(1);

    /* Security check: only superusers can checksum the entire database */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to compute database checksum")));

    /* Compute physical database checksum */
    db_checksum = compute_database_checksum_internal(true, include_system,
                                                     include_toast, true);

    PG_RETURN_INT64((int64)db_checksum);
}

/*
 * pg_database_logical_checksum - SQL function for logical database checksums
 *
 * Public interface for computing logical database checksums.
 * Requires superuser privileges.  There is no primary-key requirement.
 */
PG_FUNCTION_INFO_V1(pg_database_logical_checksum);

Datum
pg_database_logical_checksum(PG_FUNCTION_ARGS)
{
    bool        include_system = false;
    bool        include_indexes = true;
    uint64      db_checksum;

    /* Parse optional parameters */
    if (PG_NARGS() >= 1)
        include_system = PG_GETARG_BOOL(0);
    if (PG_NARGS() >= 2)
        include_indexes = PG_GETARG_BOOL(1);

    /* Security check: only superusers can checksum the entire database */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to compute database checksum")));

    /* Compute logical database checksum */
    db_checksum = compute_database_checksum_internal(false, include_system,
                                                     false, include_indexes);

    PG_RETURN_INT64((int64)db_checksum);
}

/*-------------------------------------------------------------------------
 * UTILITY FUNCTIONS
 *-------------------------------------------------------------------------
 */

/*
 * pg_data_checksum - Generic data checksum utility function
 *
 * Computes a checksum for arbitrary binary data. Useful for application
 * developers who want to checksum their own data using the same algorithm.
 *
 * Parameters:
 *   data: Binary data to checksum (bytea)
 *   seed: Initial hash value (can be used for different contexts)
 */
PG_FUNCTION_INFO_V1(pg_data_checksum);

Datum
pg_data_checksum(PG_FUNCTION_ARGS)
{
    bytea *data;
    uint32 seed;
    uint32 checksum;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    data = PG_GETARG_BYTEA_PP(0);
    seed = PG_GETARG_INT32(1);
    
    checksum = pg_checksum_data_custom(VARDATA_ANY(data), 
                                       VARSIZE_ANY_EXHDR(data), 
                                       seed);
    
    PG_RETURN_INT32((int32)checksum);
}

/*
 * pg_text_checksum - Text data checksum utility function
 *
 * Convenience wrapper for checksumming text data. Useful for string
 * comparison and text content verification.
 */
PG_FUNCTION_INFO_V1(pg_text_checksum);

Datum
pg_text_checksum(PG_FUNCTION_ARGS)
{
    text *input_text;
    uint32 seed;
    uint32 checksum;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    input_text = PG_GETARG_TEXT_PP(0);
    seed = PG_GETARG_INT32(1);
    
    checksum = pg_checksum_data_custom(VARDATA_ANY(input_text), 
                                       VARSIZE_ANY_EXHDR(input_text), 
                                       seed);
    
    PG_RETURN_INT32((int32)checksum);
}
