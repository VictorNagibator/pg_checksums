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
 *       - Depends on primary key values (if exists)
 *       - NULL if no primary key
 *       - Stable across physical reorganization
 *       - Same logical row -> same checksum
 *
 * 4. TABLE LEVEL:
 *    A. Physical (pg_table_physical_checksum):
 *       - Aggregates physical tuple checksums
 *       - Order-independent aggregation
 *       - Includes relation OID for uniqueness
 *       - Changes after physical reorganization
 *
 *    B. Logical (pg_table_logical_checksum):
 *       - Aggregates logical tuple checksums
 *       - Requires primary key
 *       - Stable across VACUUM FULL, CLUSTER
 *       - Empty tables: checksum of OID
 *
 * 5. INDEX LEVEL:
 *    A. Physical (pg_index_physical_checksum):
 *       - Depends on physical index structure
 *       - Supports all index types (B-tree, Hash, GiST, GIN, SP-GiST, BRIN)
 *       - Changes after REINDEX, physical reorganization
 *       - Detects index corruption
 *
 *    B. Logical (pg_index_logical_checksum):
 *       - Depends on index key values and heap TIDs
 *       - Ignores physical index structure
 *       - Stable across REINDEX with same data
 *       - Requires index key extraction support
 *
 * 6. DATABASE LEVEL:
 *    A. Physical (pg_database_physical_checksum):
 *       - Aggregates physical relation checksums
 *       - Superuser-only for security
 *       - Optional filtering of system catalogs
 *       - Changes after any physical reorganization
 *
 *    B. Logical (pg_database_logical_checksum):
 *       - Aggregates logical relation checksums
 *       - Superuser-only for security
 *       - Requires primary keys for all tables
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
#include "fmgr.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/genam.h"
#include "access/nbtree.h"
#include "access/brin.h"
#include "access/brin_revmap.h"
#include "access/brin_page.h"
#include "access/xlogdefs.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_index.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_class.h" 
#include "catalog/pg_inherits.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
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

/* Structure for index logical checksum entries */
typedef struct IndexLogicalEntry
{
    Datum       *key_values;      /* Array of index key values */
    bool        *key_nulls;       /* Array of null flags */
    uint32      *key_hashes;      /* Array of precomputed key hashes */
    int         nkeys;            /* Number of key columns */
    ItemPointerData tid;          /* Heap tuple ID */
    uint32      entry_hash;       /* Precomputed hash of this entry */
} IndexLogicalEntry;

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
 * ChecksumWorkerResult - fixed-size result returned by each worker
 *
 * Each worker writes a single entry of this type into shared memory, so
 * no streaming or variable-sized shared memory is required. For 32-bit
 * scans partial holds the uint32 aggregate zero-extended to 64 bits.
 * found_invalid is set only by logical table scans to report a tuple with
 * a NULL primary key value.
 */
typedef struct ChecksumWorkerResult
{
    uint64      partial;
    uint64      count;
    bool        found_invalid;
} ChecksumWorkerResult;

/*
 * ChecksumTaskType - identifies which checksum a parallel scan computes
 */
typedef enum ChecksumTaskType
{
    CHECKSUM_TASK_TABLE_PHYSICAL = 1,
    CHECKSUM_TASK_TABLE_LOGICAL,
    CHECKSUM_TASK_INDEX_PHYSICAL,
    CHECKSUM_TASK_INDEX_LOGICAL,
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
static List *find_primary_key_columns(Oid reloid);
static uint32 pg_tuple_logical_checksum_internal(Relation rel, HeapTuple tuple, bool include_header);
static bool index_supports_checksum(Oid amoid);
static uint32 pg_cell_checksum_internal(Datum value, bool isnull, Oid typid,
                                          int32 typmod, int attnum);
static uint64 compute_database_checksum_internal(bool physical, bool include_system, bool include_toast);
static uint32 compute_typlen_byval_checksum(Datum value, Oid typid, int len, int attnum);
static uint32 compute_checksum_for_data(const char *data, int len, int attnum);
static uint32 pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value);
static uint32 pg_tuple_physical_checksum_internal(Page page, OffsetNumber offnum, 
                                                  BlockNumber blkno, bool include_header);
static uint32 combine_checksums(uint32 current, uint32 new_val);

static uint32 hash_mix32(uint32 h);
static uint64 hash_mix64(uint64 h);
static void checksum_accum_add(ChecksumAccum *acc, uint32 h);
static void checksum_accum_merge(ChecksumAccum *acc, uint64 partial, uint64 count);
static uint32 checksum_accum_finalize(ChecksumAccum *acc);
static void checksum_accum64_add(ChecksumAccum64 *acc, uint64 h);
static void checksum_accum64_merge(ChecksumAccum64 *acc, uint64 partial, uint64 count);
static uint64 checksum_accum64_finalize(ChecksumAccum64 *acc);

static void scan_table_blocks_physical(Relation rel, BlockNumber start,
                                       BlockNumber end, bool include_header,
                                       Snapshot snapshot, ChecksumAccum *acc);
static void scan_table_blocks_logical(Relation rel, BlockNumber start,
                                      BlockNumber end, Snapshot snapshot,
                                      ChecksumAccum *acc,
                                      bool *found_invalid);
static void scan_index_blocks_physical(Relation idxRel, BlockNumber start,
                                       BlockNumber end, bool is_brin,
                                       ChecksumAccum *acc);
static void scan_index_blocks_logical(Relation idxRel, BlockNumber start,
                                      BlockNumber end, ChecksumAccum *acc);
static void compute_database_items(ChecksumRelationItem *items, uint32 start,
                                   uint32 end, bool physical,
                                   ChecksumAccum64 *acc);

static void pg_checksums_parallel_scan_blocks(ChecksumTaskType task_type,
                                              Oid reloid, bool include_header,
                                              uint32 nblocks, int workers,
                                              ChecksumAccum *acc,
                                              bool *found_invalid);
static void pg_checksums_parallel_scan_database(bool physical,
                                                bool include_system,
                                                bool include_toast,
                                                ChecksumRelationItem *items,
                                                uint32 nitems, int workers,
                                                ChecksumAccum64 *acc);

/* Parallel worker entry point (must be a globally visible symbol). */
PGDLLEXPORT void pg_checksums_parallel_worker_main(dsm_segment *seg, shm_toc *toc);

/*-------------------------------------------------------------------------
 * FNV-1a Hash Implementation
 *-------------------------------------------------------------------------
 */

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
static uint32
fnv1a_32_hash(const void *data, size_t len, uint32 seed)
{
    const unsigned char *bytes = (const unsigned char *)data;
    uint32 hash = seed == 0 ? FNV_BASIS_32 : seed;
    
    for (size_t i = 0; i < len; i++)
    {
        hash ^= bytes[i];
        hash *= FNV_PRIME_32;
    }
    
    return hash;
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
    Oid reloid;
    int32 blkno;
    Relation rel;
    Buffer buffer;
    Page page;
    uint16 checksum;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    blkno = PG_GETARG_INT32(1);
    
    /* Open relation with AccessShareLock to prevent concurrent modifications */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Read the page from buffer manager */
    buffer = ReadBuffer(rel, (BlockNumber)blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    
    page = BufferGetPage(buffer);
    
    /* Use built-in pg_checksum_page function */
    if (PageIsNew(page))
        checksum = 0;
    else
        checksum = pg_checksum_page((char *)page, (BlockNumber)blkno);
    
    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT32((int32)checksum);
}

/*-------------------------------------------------------------------------
 * Helper functions for column checksum computation
 *-------------------------------------------------------------------------
 */

/*
 * compute_checksum_for_data - Compute checksum for raw data
 *
 * Internal helper that computes checksum for arbitrary data with
 * appropriate seed based on attribute number.
 */
static uint32
compute_checksum_for_data(const char *data, int len, int attnum)
{
    return pg_checksum_data_custom(data, len, (uint32)attnum);
}

/*
 * compute_typlen_byval_checksum - Compute checksum for pass-by-value types
 *
 * Handles fixed-length pass-by-value types (typbyval = true). These
 * include integer types, floating point types, boolean, and other
 * types that fit in a Datum.
 *
 * Special handling is required for floating-point types to ensure
 * consistent representation across different architectures.
 */
static uint32
compute_typlen_byval_checksum(Datum value, Oid typid, int len, int attnum)
{
    char buffer[8];
    
    /* We only support typlen 1, 2, 4, 8 for pass-by-value types */
    switch (len)
    {
        case 1:
            {
                /* char, bool, and other 1-byte types */
                int8 val = DatumGetChar(value);
                memcpy(buffer, &val, 1);
            }
            break;
        case 2:
            {
                /* int2, smallint */
                int16 val = DatumGetInt16(value);
                memcpy(buffer, &val, 2);
            }
            break;
        case 4:
            if (typid == FLOAT4OID)
            {
                /* float4: use exact binary representation */
                float4 val = DatumGetFloat4(value);
                memcpy(buffer, &val, 4);
            }
            else
            {
                /* int4, date, and other 4-byte integer types */
                int32 val = DatumGetInt32(value);
                memcpy(buffer, &val, 4);
            }
            break;
        case 8:
            if (typid == FLOAT8OID || typid == TIMESTAMPTZOID || 
                typid == TIMESTAMPOID || typid == TIMEOID)
            {
                /* float8 and timestamp types */
                int64 val = DatumGetInt64(value);
                memcpy(buffer, &val, 8);
            }
            else
            {
                /* int8 (bigint) and other 8-byte types */
                int64 val = DatumGetInt64(value);
                memcpy(buffer, &val, 8);
            }
            break;
        default:
            elog(ERROR, "unexpected typlen for typbyval type: %d", len);
    }
    
    return compute_checksum_for_data(buffer, len, attnum);
}

/*-------------------------------------------------------------------------
 * CELL LEVEL FUNCTIONS (Logical only)
 *-------------------------------------------------------------------------
 */

/*
 * pg_cell_checksum_internal - Core cell checksum calculation
 *
 * This is the central function for computing cell-level checksums.
 * It handles all PostgreSQL data types, including NULL values.
 *
 * Key design decisions:
 * 1. NULL values return CHECKSUM_NULL (0xFFFFFFFF)
 * 2. Same value in same column always produces same checksum
 * 3. Different columns (different attnum) produce different checksums
 *    even for identical values
 * 4. Non-NULL values never return CHECKSUM_NULL or 0
 *
 * The function categorizes types into four groups:
 * 1. Fixed-length pass-by-value (typbyval = true)
 * 2. Variable-length (typlen = -1, varlena types)
 * 3. C-string (typlen = -2)
 * 4. Fixed-length pass-by-reference (other positive typlen)
 */
static uint32
pg_cell_checksum_internal(Datum value, bool isnull, Oid typid,
                            int32 typmod, int attnum)
{
    Form_pg_type typeForm;
    HeapTuple   typeTuple;
    char       *data;
    int         data_len;
    uint32      checksum;

    /* Handle NULL values - ALWAYS return special sentinel value */
    if (isnull)
        return CHECKSUM_NULL;

    /* Get type information from system cache */
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);
    
    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    if (typeForm->typbyval && typeForm->typlen > 0)
    {
        /* Fixed-length pass-by-value type */
        checksum = compute_typlen_byval_checksum(value, typid, 
                                                typeForm->typlen, attnum);
    }
    else if (typeForm->typlen == -1)
    {
        /* varlena type (text, bytea, arrays, etc.) */
        struct varlena *varlena;
        
        /* Detoast if necessary */
        varlena = PG_DETOAST_DATUM(value);
        
        data = (char *) varlena;
        data_len = VARSIZE_ANY(varlena);
        
        checksum = compute_checksum_for_data(data, data_len, attnum);
        
        /* Free detoasted copy if it was different from the original */
        if (varlena != (struct varlena *) DatumGetPointer(value))
            pfree(varlena);
    }
    else if (typeForm->typlen == -2)
    {
        /* cstring type (null-terminated string) */
        data = DatumGetCString(value);
        data_len = strlen(data);
        checksum = compute_checksum_for_data(data, data_len, attnum);
    }
    else
    {
        /* Fixed-length pass-by-reference type */
        data = DatumGetPointer(value);
        data_len = typeForm->typlen;
        
        if (data == NULL)
            elog(ERROR, "invalid pointer for fixed-length reference type");
            
        checksum = compute_checksum_for_data(data, data_len, attnum);
    }

    ReleaseSysCache(typeTuple);
    
    if (checksum == CHECKSUM_NULL)
    {
        /* Generate deterministic alternative checksum */
        checksum = (attnum * 100003) ^ (typid * 100019);
        
        /* Ensure it's not CHECKSUM_NULL or 0 */
        if (checksum == CHECKSUM_NULL)
            checksum = 0x7FFFFFFE; /* A safe positive value */
        if (checksum == 0)
            checksum = 0x7FFFFFFD;
    }

    if (!isnull && checksum == 0)
    {
        checksum = (attnum * 100019) ^ (typid * 100003);
        if (checksum == 0)
            checksum = 0x7FFFFFFC;
    }
    
    return checksum;
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
    uint32      checksum = 0;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);
    attnum_arg = PG_GETARG_INT32(2);
    
    if (attnum_arg <= 0)
        PG_RETURN_NULL();
    
    /* Open relation and get tuple descriptor */
    rel = relation_open(reloid, AccessShareLock);
    tupdesc = RelationGetDescr(rel);
    
    /* Validate attribute number */
    if (attnum_arg > tupdesc->natts)
    {
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }
    
    /* Read the page containing the tuple */
    buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);
    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    
    /* Verify the tuple exists and is valid */
    if (!ItemIdIsUsed(lp))
    {
        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }

    tuple = (HeapTupleHeader) PageGetItem(page, lp);

    /* Create temporary HeapTuple structure for heap_getattr */
    heapTuple.t_len = ItemIdGetLength(lp);
    heapTuple.t_data = tuple;
    heapTuple.t_tableOid = reloid;
    heapTuple.t_self = *tid;

    /* Get attribute value using the tuple descriptor */
    attr = TupleDescAttr(tupdesc, attnum_arg - 1);
    value = heap_getattr(&heapTuple, attnum_arg, tupdesc, &isnull);
    
    /* Compute cell checksum using the internal function */
    checksum = pg_cell_checksum_internal(value, isnull, 
                                          attr->atttypid, 
                                          attr->atttypmod,
                                          attnum_arg);

    /* Clean up resources */
    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32)checksum);
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
pg_tuple_physical_checksum_internal(Page page, OffsetNumber offnum, 
                                   BlockNumber blkno, bool include_header)
{
    ItemId      lp;
    HeapTupleHeader tuple;
    char       *data;
    uint32      len;
    uint32      checksum;
    uint64      location;
    uint32      location_hash;
    uint32      page_info;
    uint32      mvcc_info;
    uint32      itemid_info;
    PageHeader phdr = (PageHeader)page;
    XLogRecPtr  page_lsn;
    uint32      lsn_hi, lsn_lo;
    
    /* Validate offset number range */
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
        return 0;
    
    lp = PageGetItemId(page, offnum);
    
    /* Skip unused ItemIds */
    if (!ItemIdIsUsed(lp))
        return 0;
    
    tuple = (HeapTupleHeader) PageGetItem(page, lp);
    len = ItemIdGetLength(lp);
    
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
        
        if (len <= 0)
            return 0;
    }

    /* Create 64-bit location from block number and offset */
    location = ((uint64)blkno << 32) | (uint64)offnum;
    
    /* Calculate 32-bit hash of the location */
    location_hash = fnv1a_32_hash(&location, sizeof(location), FNV_BASIS_32);
    
    /* Include page header information for additional uniqueness */
    page_info = PageGetPageSize(page) | (PageGetPageLayoutVersion(page) << 16);
    location_hash = combine_checksums(location_hash, page_info);
    
    /* Include Page LSN (64-bit) split into two 32-bit parts */
    page_lsn = PageGetLSN(page); 
    lsn_hi = (uint32)(page_lsn >> 32);
    lsn_lo = (uint32)(page_lsn & 0xFFFFFFFF);
    location_hash = combine_checksums(location_hash, lsn_hi);
    location_hash = combine_checksums(location_hash, lsn_lo);
    
    /* Include other page metadata */
    location_hash = combine_checksums(location_hash, phdr->pd_checksum);
    location_hash = combine_checksums(location_hash, phdr->pd_flags);
    
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
    Buffer      buffer;
    Page        page;
    uint32      checksum = 0;
    bool        lock_held = false;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);
    include_header = PG_GETARG_BOOL(2);
    
    PG_TRY();
    {
        /* Open relation */
        rel = relation_open(reloid, AccessShareLock);
        
        /* Read the page */
        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;
        
        page = BufferGetPage(buffer);
        
        /* Compute tuple checksum */
        checksum = pg_tuple_physical_checksum_internal(page,
                                                      ItemPointerGetOffsetNumber(tid),
                                                      ItemPointerGetBlockNumber(tid),
                                                      include_header);
    }
    PG_CATCH();
    {
        /* Clean up resources in case of error */
        if (lock_held && BufferIsValid(buffer))
            UnlockReleaseBuffer(buffer);
        if (rel)
            relation_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    /* Clean up */
    if (lock_held && BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
    if (rel)
        relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT32((int32)checksum);
}

/*-------------------------------------------------------------------------
 * Primary Key Helper Functions
 *-------------------------------------------------------------------------
 */

/*
 * find_primary_key_columns - Find primary key columns for a table
 * 
 * Scans pg_index to find the primary key index for a given table OID.
 * Returns a list of attribute numbers (1-based) for PK columns.
 * Returns NIL if no primary key exists.
 *
 * Note: PostgreSQL allows only one primary key per table, so we
 * return as soon as we find a valid primary key index.
 */
static List *
find_primary_key_columns(Oid reloid)
{
    List *pk_columns = NIL;
    Relation pg_index_rel;
    SysScanDesc scan;
    HeapTuple index_tuple;
    
    /* Open pg_index relation */
    pg_index_rel = table_open(IndexRelationId, AccessShareLock);
    
    /* Scan all indexes in pg_index */
    scan = systable_beginscan(pg_index_rel, IndexIndrelidIndexId, true,
                              NULL, 0, NULL);
    
    /* Look for the primary key index for our table */
    while (HeapTupleIsValid(index_tuple = systable_getnext(scan)))
    {
        Form_pg_index index_form = (Form_pg_index) GETSTRUCT(index_tuple);
        
        /* Check if this index belongs to our table and is a valid primary key */
        if (index_form->indrelid == reloid && 
            index_form->indisprimary && 
            index_form->indisvalid)
        {
            int2vector *indkey = &(index_form->indkey);
            
            /* Extract all key attributes */
            for (int i = 0; i < index_form->indnkeyatts; i++)
            {
                AttrNumber attnum = indkey->values[i];
                
                /* Only include user columns (attnum > 0) */
                if (attnum > 0)
                {
                    pk_columns = lappend_int(pk_columns, attnum);
                }
            }
            break; /* Table can have only one primary key */
        }
    }
    
    systable_endscan(scan);
    table_close(pg_index_rel, AccessShareLock);
    
    return pk_columns;
}

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
pg_tuple_logical_checksum_internal(Relation rel, HeapTuple tuple, bool include_header)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    List *pk_columns = find_primary_key_columns(RelationGetRelid(rel));
    uint32 checksum = FNV_BASIS_32;
    int i;
    ListCell *lc;
    
    if (pk_columns == NIL)
    {
        return 0; /* No PK, can't compute logical checksum */
    }
    
    /* First, hash all PK columns to create a deterministic base */
    foreach(lc, pk_columns)
    {
        int attnum = lfirst_int(lc);
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
        Datum value;
        bool isnull;
        uint32 col_hash;
        
        value = heap_getattr(tuple, attnum, tupdesc, &isnull);
        
        if (isnull)
        {
            /* PK can't have NULLs, but handle gracefully */
            list_free(pk_columns);
            return 0;
        }
        
        col_hash = pg_cell_checksum_internal(value, false,
                                              attr->atttypid,
                                              attr->atttypmod,
                                              attnum);
        
        checksum = combine_checksums(checksum, col_hash);
    }
    
    list_free(pk_columns);
    
    if (include_header)
    {
        /* Include tuple header in the checksum */
        HeapTupleHeader tup = tuple->t_data;
        char *header_data = (char *)tup;
        uint32 header_len = tup->t_hoff; /* Just the header part */
        
        /* Hash the header */
        uint32 header_hash = fnv1a_32_hash(header_data, header_len, checksum);
        checksum = header_hash;
    }
    
    /* Always include all column values */
    for (i = 1; i <= tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
        Datum value;
        bool isnull;
        uint32 col_hash;
        
        value = heap_getattr(tuple, i, tupdesc, &isnull);
        
        col_hash = pg_cell_checksum_internal(value, isnull,
                                              attr->atttypid,
                                              attr->atttypmod,
                                              i);
        
        checksum = combine_checksums(checksum, col_hash);
    }
    
    /* Ensure non-zero and non-NULL result */
    if (checksum == CHECKSUM_NULL || checksum == 0)
    {
        checksum = (checksum ^ FNV_BASIS_32) & 0xFFFFFFFE;
    }
    
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
    bool        include_header;
    Relation    rel;
    Buffer      buffer;
    Page        page;
    ItemId      lp;
    HeapTupleData heapTuple;
    uint32      checksum = 0;
    bool        lock_held = false;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    tid = PG_GETARG_ITEMPOINTER(1);
    include_header = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);
    
    PG_TRY();
    {
        /* Read page */
        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;
        
        page = BufferGetPage(buffer);
        lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
        
        if (!ItemIdIsUsed(lp))
        {
            UnlockReleaseBuffer(buffer);
            relation_close(rel, AccessShareLock);
            PG_RETURN_NULL();
        }
        
        /* Create temporary HeapTuple structure */
        heapTuple.t_len = ItemIdGetLength(lp);
        heapTuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
        heapTuple.t_tableOid = reloid;
        heapTuple.t_self = *tid;
        
        /* Compute logical checksum */
        checksum = pg_tuple_logical_checksum_internal(rel, &heapTuple, include_header);
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
    
    if (checksum == 0)
        PG_RETURN_NULL();
    
    PG_RETURN_INT32((int32)checksum);
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
    Relation    rel;
    ChecksumAccum acc = {0, 0};
    uint32      aggregate;
    uint64      final_checksum;
    uint32      relation_hash;
    uint32      nblocks;
    int         workers;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    include_header = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);

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

            part_checksum = DatumGetInt64(
                DirectFunctionCall2(pg_table_physical_checksum,
                                    ObjectIdGetDatum(partoid),
                                    BoolGetDatum(include_header)));

            checksum_accum64_add(&acc64,
                                 combine_checksums_64(part_checksum,
                                                      (uint64) partoid));
        }

        relation_close(rel, AccessShareLock);

        final_checksum = checksum_accum64_finalize(&acc64);
        final_checksum = combine_checksums_64(final_checksum, (uint64) reloid);

        PG_RETURN_INT64((int64) final_checksum);
    }
    
    /* Include relation metadata in checksum */
    relation_hash = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    relation_hash = combine_checksums(relation_hash, rel->rd_rel->relpages);
    relation_hash = combine_checksums(relation_hash, rel->rd_rel->reltuples);
    
    nblocks = RelationGetNumberOfBlocks(rel);
    workers = pg_checksums_worker_count();
    if (workers > (int) nblocks)
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
    
    if (aggregate == FNV_BASIS_32) {
        /* Guarantee hash for empty tables */
        aggregate = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    }

    if (aggregate == 0) {
        aggregate = 0x7FFFFFFF;
    }
    
    /* Combine aggregate hash (upper 32 bits) with OID (lower 32 bits) */
    final_checksum = ((uint64)aggregate << 32) | reloid;
    
    PG_RETURN_INT64((int64)final_checksum);
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
    Relation    rel;
    ChecksumAccum acc = {0, 0};
    List       *pk_columns;
    uint32      aggregate;
    uint64      final_checksum;
    bool        found_invalid = false;
    uint32      nblocks;
    int         workers;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);

    /*
     * A partitioned table has no storage of its own. Check the primary key
     * on the parent (partitions inherit it), then aggregate the logical
     * checksums of all leaf partitions.
     */
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        List       *parts;
        ChecksumAccum64 acc64 = {0, 0};
        ListCell   *lc;
        bool        any_null = false;

        pk_columns = find_primary_key_columns(reloid);
        if (pk_columns == NIL)
        {
            list_free(pk_columns);
            relation_close(rel, AccessShareLock);
            PG_RETURN_NULL();
        }
        list_free(pk_columns);

        parts = pg_checksums_leaf_partitions(reloid);

        foreach(lc, parts)
        {
            Oid         partoid = lfirst_oid(lc);
            Datum       d = DirectFunctionCall1(pg_table_logical_checksum,
                                                ObjectIdGetDatum(partoid));

            if (DatumGetPointer(d) == NULL)
                any_null = true;
            else
            {
                uint64      part_checksum = DatumGetInt64(d);

                checksum_accum64_add(&acc64,
                                     combine_checksums_64(part_checksum,
                                                          (uint64) partoid));
            }
        }

        relation_close(rel, AccessShareLock);

        if (any_null)
            PG_RETURN_NULL();

        final_checksum = checksum_accum64_finalize(&acc64);
        final_checksum = combine_checksums_64(final_checksum, (uint64) reloid);

        PG_RETURN_INT64((int64) final_checksum);
    }
    
    /* Check for primary key */
    pk_columns = find_primary_key_columns(reloid);
    if (pk_columns == NIL)
    {
        list_free(pk_columns);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }
    list_free(pk_columns);
    
    nblocks = RelationGetNumberOfBlocks(rel);
    workers = pg_checksums_worker_count();
    if (workers > (int) nblocks)
        workers = (int) nblocks;
    
    if (workers < 1)
    {
        Snapshot    snapshot = pg_checksums_get_snapshot();
        
        scan_table_blocks_logical(rel, 0, nblocks, snapshot, &acc,
                                  &found_invalid);
    }
    else
    {
        pg_checksums_parallel_scan_blocks(CHECKSUM_TASK_TABLE_LOGICAL,
                                          reloid, false, nblocks,
                                          workers, &acc, &found_invalid);
    }
    
    relation_close(rel, AccessShareLock);
    
    /* A NULL primary key value makes the logical checksum undefined. */
    if (found_invalid)
        PG_RETURN_NULL();
    
    /* Compute order-independent aggregate */
    aggregate = checksum_accum_finalize(&acc);
    
    /* Combine with relation OID for uniqueness */
    final_checksum = ((uint64)aggregate << 32) | reloid;
    
    PG_RETURN_INT64((int64)final_checksum);
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
 * Supports all major index types. Returns NULL for unsupported
 * index types.
 *
 * For empty indexes, returns a hash of the relation OID.
 */
PG_FUNCTION_INFO_V1(pg_index_physical_checksum);

Datum
pg_index_physical_checksum(PG_FUNCTION_ARGS)
{
    Oid         indexoid;
    Relation    idxRel;
    ChecksumAccum acc = {0, 0};
    uint32      index_checksum = 0;
    uint32      index_type_hash;
    bool        is_brin;
    uint32      nblocks;
    int         workers;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    indexoid = PG_GETARG_OID(0);
    
    /* Open index */
    idxRel = index_open(indexoid, AccessShareLock);
    
    /* Check index type support */
    if (!index_supports_checksum(idxRel->rd_rel->relam))
    {
        index_close(idxRel, AccessShareLock);
        PG_RETURN_NULL();  /* Return NULL without warning for unsupported types */
    }
    
    is_brin = (idxRel->rd_rel->relam == BRIN_AM_OID);
    nblocks = RelationGetNumberOfBlocks(idxRel);
    workers = pg_checksums_worker_count();
    if (workers > (int) nblocks)
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
        index_type_hash = combine_checksums(index_type_hash, idxRel->rd_rel->relnatts);
        index_checksum = combine_checksums(index_checksum, index_type_hash);
    }
    
    /* Close index */
    index_close(idxRel, AccessShareLock);
    
    /* For empty indexes, return hash of relation OID */
    if (index_checksum == FNV_BASIS_32)
    {
        index_checksum = fnv1a_32_hash(&indexoid, sizeof(indexoid), FNV_BASIS_32);
    }
    
    PG_RETURN_INT32((int32)index_checksum);
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
    Relation    idxRel;
    ChecksumAccum acc = {0, 0};
    uint32      index_checksum = 0;
    uint32      nblocks;
    int         workers;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    indexoid = PG_GETARG_OID(0);
    
    /* Open index */
    idxRel = index_open(indexoid, AccessShareLock);
    
    /* Check index type support */
    if (!index_supports_checksum(idxRel->rd_rel->relam))
    {
        index_close(idxRel, AccessShareLock);
        PG_RETURN_NULL(); 
    }
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    workers = pg_checksums_worker_count();
    if (workers > (int) nblocks)
        workers = (int) nblocks;
    
    if (workers < 1)
        scan_index_blocks_logical(idxRel, 0, nblocks, &acc);
    else
        pg_checksums_parallel_scan_blocks(CHECKSUM_TASK_INDEX_LOGICAL,
                                          indexoid, false, nblocks, workers,
                                          &acc, NULL);
    
    /* Close index */
    index_close(idxRel, AccessShareLock);
    
    /* Empty indexes hash the relation OID. */
    if (acc.count == 0)
        index_checksum = fnv1a_32_hash(&indexoid, sizeof(indexoid), FNV_BASIS_32);
    else
        index_checksum = checksum_accum_finalize(&acc);
    
    PG_RETURN_INT32((int32)index_checksum);
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
    *end = Min(first + chunk, state->nitems);
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

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;
        OffsetNumber offnum;

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId      lp = PageGetItemId(page, offnum);
            HeapTupleHeader htup;
            HeapTupleData tuple;
            uint32      tuple_checksum;

            if (!ItemIdIsNormal(lp))
                continue;

            htup = (HeapTupleHeader) PageGetItem(page, lp);
            tuple.t_len = ItemIdGetLength(lp);
            tuple.t_data = htup;
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            if (!HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer))
                continue;

            tuple_checksum = pg_tuple_physical_checksum_internal(page, offnum,
                                                                 blkno,
                                                                 include_header);
            if (tuple_checksum != 0)
                checksum_accum_add(acc, tuple_checksum);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
}

/*
 * scan_table_blocks_logical - logical checksum over a range of heap blocks
 */
static void
scan_table_blocks_logical(Relation rel, BlockNumber start, BlockNumber end,
                          Snapshot snapshot, ChecksumAccum *acc,
                          bool *found_invalid)
{
    BlockNumber blkno;

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;
        OffsetNumber offnum;

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId      lp = PageGetItemId(page, offnum);
            HeapTupleHeader htup;
            HeapTupleData tuple;
            uint32      tuple_checksum;

            if (!ItemIdIsNormal(lp))
                continue;

            htup = (HeapTupleHeader) PageGetItem(page, lp);
            tuple.t_len = ItemIdGetLength(lp);
            tuple.t_data = htup;
            tuple.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&tuple.t_self, blkno, offnum);

            if (!HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer))
                continue;

            tuple_checksum = pg_tuple_logical_checksum_internal(rel, &tuple, false);
            if (tuple_checksum == 0)
            {
                if (found_invalid != NULL)
                    *found_invalid = true;
                continue;
            }
            checksum_accum_add(acc, tuple_checksum);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
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
        page_size = PageGetPageSize(page);

        if (!PageIsNew(page))
        {
            PageHeader phdr = (PageHeader) page;

            page_hash = fnv1a_32_hash((char *) page, page_size, 0);

            if (is_brin)
            {
                uint8      *special_space;
                Size        special_size;

                page_hash = combine_checksums(page_hash, (uint32) page_size);

                special_space = (uint8 *) PageGetSpecialPointer(page);
                special_size = PageGetSpecialSize(page);
                if (special_size >= 4)
                {
                    uint32      brin_info = 0;

                    memcpy(&brin_info, special_space, 4);
                    page_hash = combine_checksums(page_hash, brin_info);
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
            page_hash = combine_checksums(page_hash, (uint32) page_size);
            checksum_accum_add(acc, page_hash);
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*
 * scan_index_blocks_logical - logical checksum over a range of index blocks
 */
static void
scan_index_blocks_logical(Relation idxRel, BlockNumber start, BlockNumber end,
                          ChecksumAccum *acc)
{
    BlockNumber blkno;
    TupleDesc   idx_tupdesc = RelationGetDescr(idxRel);
    BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);

    for (blkno = start; blkno < end; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;

        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buffer);

        if (!PageIsNew(page))
        {
            maxoff = PageGetMaxOffsetNumber(page);

            for (OffsetNumber offnum = FirstOffsetNumber;
                 offnum <= maxoff;
                 offnum = OffsetNumberNext(offnum))
            {
                ItemId      itemId = PageGetItemId(page, offnum);

                if (ItemIdIsUsed(itemId) && !ItemIdIsDead(itemId))
                {
                    IndexTuple  itup;
                    Datum      *values;
                    bool       *isnull;
                    int         i;
                    uint32      entry_hash = FNV_BASIS_32;
                    ItemPointerData tid;
                    uint32      tid_hash;

                    itup = (IndexTuple) PageGetItem(page, itemId);

                    values = (Datum *) palloc(idx_tupdesc->natts * sizeof(Datum));
                    isnull = (bool *) palloc(idx_tupdesc->natts * sizeof(bool));

                    index_deform_tuple(itup, idx_tupdesc, values, isnull);

                    for (i = 0; i < idx_tupdesc->natts; i++)
                    {
                        if (isnull[i])
                        {
                            entry_hash = combine_checksums(entry_hash, CHECKSUM_NULL);
                        }
                        else
                        {
                            Form_pg_attribute attr = TupleDescAttr(idx_tupdesc, i);
                            uint32      col_hash = pg_cell_checksum_internal(values[i], false,
                                                                             attr->atttypid,
                                                                             attr->atttypmod,
                                                                             i + 1);

                            entry_hash = combine_checksums(entry_hash, col_hash);
                        }
                    }

                    tid = itup->t_tid;
                    tid_hash = fnv1a_32_hash(&tid, sizeof(tid), 0);
                    entry_hash = combine_checksums(entry_hash, tid_hash);

                    pfree(values);
                    pfree(isnull);

                    checksum_accum_add(acc, entry_hash);
                }
            }
        }

        UnlockReleaseBuffer(buffer);

        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }

    FreeAccessStrategy(bstrategy);
}

/*
 * compute_database_items - database checksum over a range of relations
 */
static void
compute_database_items(ChecksumRelationItem *items, uint32 start, uint32 end,
                       bool physical, ChecksumAccum64 *acc)
{
    uint32      i;

    for (i = start; i < end; i++)
    {
        Oid         relid = items[i].reloid;
        char        relkind = items[i].relkind;
        uint64      rel_checksum = 0;
        bool        skip = false;

        PG_TRY();
        {
            if (relkind == RELKIND_INDEX)
            {
                if (physical)
                {
                    rel_checksum = (uint64) DatumGetInt32(
                        DirectFunctionCall1(pg_index_physical_checksum,
                                            ObjectIdGetDatum(relid)));
                }
                else
                {
                    Datum       d = DirectFunctionCall1(pg_index_logical_checksum,
                                                        ObjectIdGetDatum(relid));

                    if (DatumGetPointer(d) == NULL)
                        skip = true;
                    else
                        rel_checksum = (uint64) DatumGetInt32(d);
                }
            }
            else
            {
                if (physical)
                {
                    Datum       d = DirectFunctionCall2(pg_table_physical_checksum,
                                                        ObjectIdGetDatum(relid),
                                                        BoolGetDatum(false));

                    rel_checksum = DatumGetInt64(d);
                }
                else
                {
                    Datum       d = DirectFunctionCall1(pg_table_logical_checksum,
                                                        ObjectIdGetDatum(relid));

                    if (DatumGetPointer(d) == NULL)
                        skip = true;
                    else
                        rel_checksum = DatumGetInt64(d);
                }
            }

            if (!skip)
                checksum_accum64_add(acc,
                                     combine_checksums_64(rel_checksum,
                                                          (uint64) relid));
        }
        PG_CATCH();
        {
            /* Skip relations that can't be processed. */
            FlushErrorState();
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

    myresult->partial = 0;
    myresult->count = 0;
    myresult->found_invalid = false;

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
                                               state->include_header, snapshot, &acc);

                relation_close(rel, AccessShareLock);
                myresult->partial = acc.partial;
                myresult->count = acc.count;
                break;
            }
        case CHECKSUM_TASK_TABLE_LOGICAL:
            {
                Relation    rel = relation_open(state->reloid, AccessShareLock);
                ChecksumAccum acc = {0, 0};
                bool        found_invalid = false;

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_table_blocks_logical(rel, start, end, snapshot, &acc,
                                              &found_invalid);

                relation_close(rel, AccessShareLock);
                myresult->partial = acc.partial;
                myresult->count = acc.count;
                myresult->found_invalid = found_invalid;
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
        case CHECKSUM_TASK_INDEX_LOGICAL:
            {
                Relation    idxRel = index_open(state->reloid, AccessShareLock);
                ChecksumAccum acc = {0, 0};

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_index_blocks_logical(idxRel, start, end, &acc);

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
                    compute_database_items(items, start, end, physical, &acc64);

                if (pushed)
                    PopActiveSnapshot();

                myresult->partial = acc64.partial;
                myresult->count = acc64.count;
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
 * caller's accumulator. found_invalid (when non-NULL) is set if any logical
 * tuple had a NULL primary key value.
 */
static void
pg_checksums_parallel_scan_blocks(ChecksumTaskType task_type, Oid reloid,
                                  bool include_header, uint32 nblocks,
                                  int workers, ChecksumAccum *acc,
                                  bool *found_invalid)
{
    ParallelContext *pcxt;
    shm_toc_estimator e;
    ChecksumParallelState *state;
    ChecksumWorkerResult *results;
    Snapshot    snapshot = NULL;
    uint32      snapshot_size = 0;
    bool        is_table = (task_type == CHECKSUM_TASK_TABLE_PHYSICAL ||
                            task_type == CHECKSUM_TASK_TABLE_LOGICAL);
    uint32      start;
    uint32      end;
    bool        local_found_invalid = false;
    int         i;

    if (is_table)
    {
        snapshot = pg_checksums_get_snapshot();
        snapshot_size = (uint32) EstimateSnapshotSpace(snapshot);
    }

    pcxt = CreateParallelContext("pg_checksums",
                                 "pg_checksums_parallel_worker_main",
                                 workers);

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
                    scan_table_blocks_logical(rel, start, end, snapshot, acc,
                                              &local_found_invalid);

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
        case CHECKSUM_TASK_INDEX_LOGICAL:
            {
                Relation    idxRel = index_open(reloid, AccessShareLock);

                while (pg_checksums_consume(state, PG_CHECKSUMS_BLOCK_CHUNK,
                                            &start, &end))
                    scan_index_blocks_logical(idxRel, start, end, acc);

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
            checksum_accum_merge(acc, results[i].partial, results[i].count);
            if (results[i].found_invalid)
                local_found_invalid = true;
        }
    }

    DestroyParallelContext(pcxt);

    if (found_invalid != NULL)
        *found_invalid = local_found_invalid;
}

/*
 * pg_checksums_parallel_scan_database - run a database scan in parallel
 */
static void
pg_checksums_parallel_scan_database(bool physical, bool include_system,
                                    bool include_toast,
                                    ChecksumRelationItem *items, uint32 nitems,
                                    int workers, ChecksumAccum64 *acc)
{
    ParallelContext *pcxt;
    shm_toc_estimator e;
    ChecksumParallelState *state;
    ChecksumWorkerResult *results;
    Snapshot    snapshot;
    uint32      snapshot_size;
    uint32      start;
    uint32      end;
    int         i;

    /*
     * Propagate a consistent snapshot to the workers so that every relation
     * is checksummed as of the same point in time, exactly like the serial
     * path.
     */
    snapshot = pg_checksums_get_snapshot();
    snapshot_size = (uint32) EstimateSnapshotSpace(snapshot);

    pcxt = CreateParallelContext("pg_checksums",
                                 "pg_checksums_parallel_worker_main",
                                 workers);

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
        compute_database_items(items, start, end, physical, acc);

    if (pcxt->nworkers_launched > 0)
        WaitForParallelWorkersToFinish(pcxt);

    for (i = 0; i < workers; i++)
        if (pcxt->worker[i].bgwhandle != NULL)
            checksum_accum64_merge(acc, results[i].partial, results[i].count);

    DestroyParallelContext(pcxt);
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
compute_database_checksum_internal(bool physical, bool include_system, bool include_toast)
{
    Relation    pg_class_rel;
    TableScanDesc scan;
    HeapTuple   classTuple;
    Snapshot    snapshot;
    ChecksumRelationItem *items;
    uint32      nitems = 0;
    uint32      capacity = 64;
    ChecksumAccum64 acc = {0, 0};
    int         workers;
    
    /* Use a consistent snapshot */
    snapshot = GetActiveSnapshot();

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
        
        /* Apply inclusion filters */
        if (!include_system && 
            (relnamespace == PG_CATALOG_NAMESPACE ||
             relnamespace == PG_TOAST_NAMESPACE))
            continue;

        if (!include_toast && relkind == RELKIND_TOASTVALUE)
            continue;

        /* Skip non-table/index relations */
        if (relkind != RELKIND_RELATION && 
            relkind != RELKIND_INDEX &&
            relkind != RELKIND_MATVIEW)
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
    if (workers > (int) nitems)
        workers = (int) nitems;

    if (workers < 1)
        compute_database_items(items, 0, nitems, physical, &acc);
    else
        pg_checksums_parallel_scan_database(physical, include_system,
                                            include_toast, items, nitems,
                                            workers, &acc);

    pfree(items);

    return checksum_accum64_finalize(&acc);
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
    db_checksum = compute_database_checksum_internal(true, include_system, include_toast);

    PG_RETURN_INT64((int64)db_checksum);
}

/*
 * pg_database_logical_checksum - SQL function for logical database checksums
 *
 * Public interface for computing logical database checksums.
 * Requires superuser privileges and skips tables without primary keys.
 */
PG_FUNCTION_INFO_V1(pg_database_logical_checksum);

Datum
pg_database_logical_checksum(PG_FUNCTION_ARGS)
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

    /* Compute logical database checksum */
    db_checksum = compute_database_checksum_internal(false, include_system, include_toast);

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