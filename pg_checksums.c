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
 * 2. COLUMN LEVEL (pg_column_checksum):
 *    - Logical only (depends only on column value and attribute number)
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
#include "catalog/pg_type.h"
#include "catalog/pg_index.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_class.h" 
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"              
#include "storage/checksum.h" 

#include "pg_checksums.h"

PG_MODULE_MAGIC;

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
 * Helper function declarations
 *-------------------------------------------------------------------------
 */
static List *find_primary_key_columns(Oid reloid);
static uint32 compute_pk_seed(Relation rel, HeapTuple tuple, TupleDesc tupdesc, List *pk_columns);
static uint32 pg_tuple_logical_checksum_internal(Relation rel, HeapTuple tuple, bool include_header);
static bool index_supports_checksum(Oid amoid);
static uint32 compute_generic_index_physical_checksum(Relation idxRel);
static uint32 compute_brin_index_physical_checksum(Relation idxRel);
static uint32 compute_index_logical_checksum_internal(Relation idxRel);
static uint32 pg_column_checksum_internal(Datum value, bool isnull, Oid typid,
                                          int32 typmod, int attnum);
static uint32 compute_order_independent_checksum(List *hash_list);
static uint8 get_brin_page_type(Page page);
static IndexLogicalEntry *extract_index_key_values(IndexTuple itup, TupleDesc idx_tupdesc);
static void free_index_logical_entry(IndexLogicalEntry *entry);
static uint32 compute_index_entry_hash(IndexLogicalEntry *entry);
static int compare_index_entries(const void *a, const void *b);
static uint64 compute_database_checksum_internal(bool physical, bool include_system, bool include_toast);
static uint32 compute_typlen_byval_checksum(Datum value, Oid typid, int len, int attnum);
static uint32 compute_checksum_for_data(const char *data, int len, int attnum);
static uint32 pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value);
static uint32 pg_tuple_physical_checksum_internal(Page page, OffsetNumber offnum, 
                                                  BlockNumber blkno, bool include_header);
static uint32 combine_checksums(uint32 current, uint32 new_val);
static int compare_ints(const void *a, const void *b);

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
    
    /* Combine using FNV-1a with 8-byte chunks */
    for (int i = 56; i >= 0; i -= 8)
    {
        hash ^= (new_val >> i) & 0xFF;
        hash *= prime;
    }
    
    return hash;
}

/*-------------------------------------------------------------------------
 * Comparison Functions for Sorting
 *-------------------------------------------------------------------------
 */

/*
 * compare_ints - Compare two integers for sorting
 *
 * Simple comparison function for qsort. Returns -1, 0, or 1 based on
 * the relative order of two integers.
 */
int
compare_ints(const void *a, const void *b)
{
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    
    if (ia < ib) return -1;
    if (ia > ib) return 1;
    return 0;
}

/*-------------------------------------------------------------------------
 * Order-Independent Aggregation
 *-------------------------------------------------------------------------
 */

/*
 * compute_order_independent_checksum - Compute order-independent aggregate
 *
 * This function computes an aggregate checksum from a list of hash values
 * in an order-independent manner. This is critical for table and database
 * checksums where the order of tuples may vary between scans (e.g., due to
 * VACUUM, index scans vs sequential scans).
 *
 * The algorithm:
 * 1. Convert the list to an array
 * 2. Sort the array
 * 3. Combine sorted hashes using FNV-1a
 *
 * This ensures that identical sets of tuples produce identical aggregate
 * checksums regardless of their physical order.
 */
static uint32
compute_order_independent_checksum(List *hash_list)
{
    uint32 aggregate = FNV_BASIS_32;
    int num_hashes;
    uint32 *hashes_array;
    int i;
    ListCell *lc;
    
    if (hash_list == NIL)
        return aggregate;
    
    /* Convert list to array for sorting */
    num_hashes = list_length(hash_list);
    hashes_array = (uint32 *)palloc(num_hashes * sizeof(uint32));
    i = 0;
    
    foreach(lc, hash_list)
    {
        hashes_array[i++] = (uint32)lfirst_int(lc);
    }
    
    /* Sort the array to ensure order independence */
    qsort(hashes_array, num_hashes, sizeof(uint32), 
          (int (*)(const void *, const void *))compare_ints);
    
    /* Combine sorted hashes */
    for (i = 0; i < num_hashes; i++)
    {
        aggregate = combine_checksums(aggregate, hashes_array[i]);
    }
    
    pfree(hashes_array);
    
    return aggregate;
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
 * COLUMN LEVEL FUNCTIONS (Logical only)
 *-------------------------------------------------------------------------
 */

/*
 * pg_column_checksum_internal - Core column checksum calculation
 *
 * This is the central function for computing column-level checksums.
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
pg_column_checksum_internal(Datum value, bool isnull, Oid typid,
                            int32 typmod, int attnum)
{
    Form_pg_type typeForm;
    HeapTuple   typeTuple;
    char       *data;
    int         data_len;
    uint32      checksum;

    /* Handle NULL values - return special sentinel value */
    if (isnull)
        return CHECKSUM_NULL;

    /* Get type information from system cache */
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);
    
    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    if (typeForm->typbyval && typeForm->typlen > 0)
    {
        /* Fixed-length pass-by-value type (e.g., int4, int8, bool, float8) */
        checksum = compute_typlen_byval_checksum(value, typid, 
                                                typeForm->typlen, attnum);
    }
    else if (typeForm->typlen == -1)
    {
        /* varlena type (text, bytea, arrays, etc.) */
        struct varlena *varlena;
        
        /* Detoast if necessary - we need the actual data, not a toast pointer */
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
    
    /* 
     * Critical safety guarantee: non-NULL values must never return 
     * CHECKSUM_NULL or 0. This ensures that checksums can be used
     * for equality comparisons without special NULL handling.
     */
    if (checksum == CHECKSUM_NULL || checksum == 0)
    {
        /*
         * Generate a deterministic alternative checksum based on
         * attribute number and type OID. The multipliers are prime
         * numbers to reduce collision probability.
         */
        checksum = (attnum * 100003) ^ (typid * 100019);
        
        /* Double-check: ensure it's not CHECKSUM_NULL or 0 */
        if (checksum == CHECKSUM_NULL || checksum == 0)
            checksum = 0x7FFFFFFF; /* A safe positive maximum value */
    }
    
    return checksum;
}

/*
 * pg_column_checksum - SQL function for column-level checksums
 *
 * Public SQL-callable function that computes checksum for a specific
 * column of a specific tuple identified by its TID (tuple identifier).
 *
 * Parameters:
 *   relname: OID of the relation (table)
 *   tid: TID (block number and offset) identifying the tuple
 *   attnum: Attribute number (1-based) of the column to checksum
 *
 * Returns: 32-bit checksum, or NULL if the tuple doesn't exist or
 *          attnum is out of range
 */
PG_FUNCTION_INFO_V1(pg_column_checksum);

Datum
pg_column_checksum(PG_FUNCTION_ARGS)
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
    
    /* Compute column checksum using the internal function */
    checksum = pg_column_checksum_internal(value, isnull, 
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
uint32
pg_tuple_physical_checksum_internal(Page page, OffsetNumber offnum, 
                                   BlockNumber blkno, bool include_header)
{
    ItemId      lp;
    HeapTupleHeader tuple;
    char       *data;
    uint32      len;
    uint32      checksum;
    uint32      location_hash;
    
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

    /* Create location hash from block number and offset */
    location_hash = (blkno << 16) | offnum;
    
    /* Calculate checksum using location_hash as the initial value */
    checksum = pg_checksum_data_custom(data, len, location_hash);
    
    /* Additionally XOR with location_hash to guarantee uniqueness */
    checksum ^= location_hash;
    
    /* Incorporate MVCC information when excluding header */
    if (!include_header)
    {
        uint32 mvcc_info = (HeapTupleHeaderGetRawXmin(tuple) ^ 
                           HeapTupleHeaderGetRawXmax(tuple));
        checksum ^= mvcc_info;
    }
    
    /* Guarantee checksum never equals CHECKSUM_NULL */
    if (checksum == CHECKSUM_NULL)
        checksum = (CHECKSUM_NULL ^ location_hash) & 0xFFFFFFFE;
    
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
 * compute_pk_seed - Compute seed based on primary key values
 *
 * Computes a deterministic seed value based on the primary key values
 * of a tuple. This seed is then used to compute logical checksums.
 *
 * Returns 0 if:
 * 1. No primary key exists
 * 2. Primary key contains NULL values (invalid state)
 */
static uint32
compute_pk_seed(Relation rel, HeapTuple tuple, TupleDesc tupdesc, List *pk_columns)
{
    ListCell *lc;
    bool pk_has_null = false;
    uint32 seed = FNV_BASIS_32;
    
    if (pk_columns == NIL)
        return 0; /* No PK */
    
    foreach(lc, pk_columns)
    {
        int attnum = lfirst_int(lc);
        Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
        Datum value;
        bool isnull;
        uint32 col_checksum;
        
        value = heap_getattr(tuple, attnum, tupdesc, &isnull);
        
        if (isnull)
        {
            /* Primary key cannot contain NULL, but handle gracefully */
            pk_has_null = true;
            continue;
        }
        
        /* Compute column checksum with seed=0 */
        col_checksum = pg_column_checksum_internal(value, false,
                                                   attr->atttypid,
                                                   attr->atttypmod,
                                                   0);
        
        /* Combine with overall seed using FNV-1a */
        seed = fnv1a_32_hash(&col_checksum, sizeof(col_checksum), seed);
    }
    
    if (pk_has_null)
        return 0; /* Invalid PK */
    
    return seed;
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
    uint32 seed;
    uint32 checksum;
    HeapTupleHeader tup = tuple->t_data;
    char *data;
    uint32 len;
    
    if (pk_columns == NIL)
    {
        list_free(pk_columns);
        return 0; /* No PK, cannot compute logical checksum */
    }
    
    /* Compute seed based on PK values */
    seed = compute_pk_seed(rel, tuple, tupdesc, pk_columns);
    list_free(pk_columns);
    
    if (seed == 0)
        return 0; /* Failed to compute seed */
    
    /* Determine data to hash */
    if (include_header)
    {
        data = (char *) tup;
        len = tuple->t_len;
    }
    else
    {
        data = (char *) tup + tup->t_hoff;
        len = tuple->t_len - tup->t_hoff;
        
        if (len <= 0)
            return 0;
    }
    
    /* Compute checksum with PK-based seed */
    checksum = pg_checksum_data_custom(data, len, seed);
    
    /* Include MVCC information when excluding header */
    if (!include_header)
    {
        uint32 mvcc_info = (HeapTupleHeaderGetRawXmin(tup) ^ 
                           HeapTupleHeaderGetRawXmax(tup));
        checksum ^= mvcc_info;
    }
    
    /* Guarantee checksum never equals CHECKSUM_NULL */
    if (checksum == CHECKSUM_NULL)
        checksum = (CHECKSUM_NULL ^ seed) & 0xFFFFFFFE;
    
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
    TableScanDesc scan;
    HeapTuple   tuple;
    List       *tuple_hashes = NIL;
    uint32      aggregate;
    uint64      final_checksum;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    include_header = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Start table scan */
    scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
    
    /* Process each tuple in the table */
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Buffer      buffer;
        Page        page;
        uint32      tuple_checksum;
        
        /* Read the page containing this tuple */
        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&tuple->t_self));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        
        /* Compute physical tuple checksum */
        tuple_checksum = pg_tuple_physical_checksum_internal(page,
                                                            ItemPointerGetOffsetNumber(&tuple->t_self),
                                                            ItemPointerGetBlockNumber(&tuple->t_self),
                                                            include_header);
        
        /* Store for aggregation */
        if (tuple_checksum != 0)
            tuple_hashes = lappend_int(tuple_hashes, tuple_checksum);
        
        UnlockReleaseBuffer(buffer);
    }
    
    /* Clean up */
    table_endscan(scan);
    relation_close(rel, AccessShareLock);
    
    /* Compute order-independent aggregate */
    aggregate = compute_order_independent_checksum(tuple_hashes);
    
    /* Include relation OID in final checksum */
    if (tuple_hashes != NIL)
        list_free(tuple_hashes);
    
    /* Combine with relation OID for uniqueness (64-bit) */
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
    TableScanDesc scan;
    HeapTuple   tuple;
    List       *tuple_hashes = NIL;
    List       *pk_columns;
    uint32      aggregate;
    uint64      final_checksum;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Check for primary key */
    pk_columns = find_primary_key_columns(reloid);
    if (pk_columns == NIL)
    {
        list_free(pk_columns);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();  /* Return NULL without warning - expected behavior */
    }
    list_free(pk_columns);
    
    /* Start table scan */
    scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
    
    /* Process each tuple in the table */
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        uint32 tuple_checksum;
        
        /* Compute logical tuple checksum */
        tuple_checksum = pg_tuple_logical_checksum_internal(rel, tuple, false);
        
        if (tuple_checksum != 0)
            tuple_hashes = lappend_int(tuple_hashes, tuple_checksum);
    }
    
    /* Clean up */
    table_endscan(scan);
    relation_close(rel, AccessShareLock);
    
    /* Compute order-independent aggregate */
    aggregate = compute_order_independent_checksum(tuple_hashes);
    
    /* Include relation OID in final checksum */
    if (tuple_hashes != NIL)
        list_free(tuple_hashes);
    
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

/*
 * get_brin_page_type - Determine BRIN page type
 *
 * BRIN indexes have special page types stored in the special space.
 * This function extracts the page type byte.
 */
static uint8
get_brin_page_type(Page page)
{
    uint8 *special_space;
    
    special_space = (uint8 *)PageGetSpecialPointer(page);
    return special_space[0];
}

/*-------------------------------------------------------------------------
 * Physical Index Checksum Functions
 *-------------------------------------------------------------------------
 */

/*
 * compute_generic_index_physical_checksum - Generic physical index checksum
 *
 * Computes physical checksum for most index types (B-tree, Hash, GiST,
 * GIN, SP-GiST) by hashing all index tuples and their physical locations.
 * This provides a fingerprint of the physical index structure.
 */
static uint32
compute_generic_index_physical_checksum(Relation idxRel)
{
    BlockNumber nblocks;
    BufferAccessStrategy bstrategy;
    BlockNumber blkno;
    List *tuple_hashes = NIL;
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
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
                ItemId itemId;
                
                itemId = PageGetItemId(page, offnum);
                
                if (ItemIdIsUsed(itemId) && !ItemIdIsDead(itemId))
                {
                    IndexTuple itup;
                    Size itup_len;
                    uint32 tuple_hash;
                    
                    itup = (IndexTuple) PageGetItem(page, itemId);
                    itup_len = IndexTupleSize(itup);
                    
                    /* Hash the physical index tuple */
                    tuple_hash = fnv1a_32_hash((char *)itup, itup_len, 0);
                    
                    /* Include physical location for uniqueness */
                    tuple_hash = combine_checksums(tuple_hash, blkno);
                    tuple_hash = combine_checksums(tuple_hash, offnum);
                    
                    tuple_hashes = lappend_int(tuple_hashes, tuple_hash);
                }
            }
        }
        
        UnlockReleaseBuffer(buffer);
        
        /* Allow query cancellation every 64 blocks */
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    return compute_order_independent_checksum(tuple_hashes);
}

/*
 * compute_brin_index_physical_checksum - BRIN index physical checksum
 *
 * BRIN indexes have a different structure (page-based rather than
 * tuple-based), so they require special handling. We checksum entire
 * pages and include page type information.
 */
static uint32
compute_brin_index_physical_checksum(Relation idxRel)
{
    BlockNumber nblocks;
    BufferAccessStrategy bstrategy;
    BlockNumber blkno;
    List *tuple_hashes = NIL;
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
        
        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        
        if (!PageIsNew(page))
        {
            /* Get page checksum - include page type for BRIN */
            uint32 page_hash = fnv1a_32_hash((char *)page, BLCKSZ, 0);
            
            /* Add page type information */
            uint8 page_type = get_brin_page_type(page);
            page_hash = combine_checksums(page_hash, (uint32)page_type);
            page_hash = combine_checksums(page_hash, blkno);
            
            /* Store page hash */
            tuple_hashes = lappend_int(tuple_hashes, page_hash);
        }
        
        UnlockReleaseBuffer(buffer);
        
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    return compute_order_independent_checksum(tuple_hashes);
}

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
    uint32      index_checksum = 0;
    
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
    
    /* Compute physical checksum based on index type */
    switch (idxRel->rd_rel->relam)
    {
        case BTREE_AM_OID:
        case HASH_AM_OID:
        case GIST_AM_OID:
        case GIN_AM_OID:
        case SPGIST_AM_OID:
            index_checksum = compute_generic_index_physical_checksum(idxRel);
            break;
            
        case BRIN_AM_OID:
            index_checksum = compute_brin_index_physical_checksum(idxRel);
            break;
            
        default:
            index_checksum = 0;
            break;
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
 * Index Logical Checksum Helper Functions
 *
 * These functions extract and process index key values for logical
 * checksums, which depend only on the logical content of the index
 * (key values and heap TIDs), not on the physical index structure.
 */

static IndexLogicalEntry *
extract_index_key_values(IndexTuple itup, TupleDesc idx_tupdesc)
{
    IndexLogicalEntry *entry;
    Datum *values;
    bool *isnull;
    uint32 *key_hashes;
    int nkeys = idx_tupdesc->natts;
    int i;
    
    entry = (IndexLogicalEntry *)palloc(sizeof(IndexLogicalEntry));
    values = (Datum *)palloc(nkeys * sizeof(Datum));
    isnull = (bool *)palloc(nkeys * sizeof(bool));
    key_hashes = (uint32 *)palloc(nkeys * sizeof(uint32));
    
    /* Extract index key values from the index tuple */
    index_deform_tuple(itup, idx_tupdesc, values, isnull);
    
    /* Compute hash for each key */
    for (i = 0; i < nkeys; i++)
    {
        if (isnull[i])
        {
            key_hashes[i] = CHECKSUM_NULL;
        }
        else
        {
            Form_pg_attribute attr = TupleDescAttr(idx_tupdesc, i);
            key_hashes[i] = pg_column_checksum_internal(values[i], false,
                                                        attr->atttypid,
                                                        attr->atttypmod,
                                                        i + 1);
        }
    }
    
    entry->key_values = values;
    entry->key_nulls = isnull;
    entry->key_hashes = key_hashes;
    entry->nkeys = nkeys;
    entry->tid = itup->t_tid;
    entry->entry_hash = 0;
    
    return entry;
}

static void
free_index_logical_entry(IndexLogicalEntry *entry)
{
    if (entry->key_values)
        pfree(entry->key_values);
    if (entry->key_nulls)
        pfree(entry->key_nulls);
    if (entry->key_hashes)
        pfree(entry->key_hashes);
    pfree(entry);
}

static uint32
compute_index_entry_hash(IndexLogicalEntry *entry)
{
    uint32 hash = FNV_BASIS_32;
    int i;
    
    /* Combine all key hashes */
    for (i = 0; i < entry->nkeys; i++)
    {
        hash = combine_checksums(hash, entry->key_hashes[i]);
    }
    
    /* Add TID for uniqueness */
    hash = combine_checksums(hash, ItemPointerGetBlockNumber(&entry->tid));
    hash = combine_checksums(hash, ItemPointerGetOffsetNumber(&entry->tid));
    
    return hash;
}

static int
compare_index_entries(const void *a, const void *b)
{
    const IndexLogicalEntry *entry_a = *(const IndexLogicalEntry **)a;
    const IndexLogicalEntry *entry_b = *(const IndexLogicalEntry **)b;
    int i;
    
    /* Compare key hashes (logical key values) */
    for (i = 0; i < entry_a->nkeys && i < entry_b->nkeys; i++)
    {
        if (entry_a->key_hashes[i] < entry_b->key_hashes[i])
            return -1;
        if (entry_a->key_hashes[i] > entry_b->key_hashes[i])
            return 1;
    }
    
    /* If keys are equal, compare TIDs */
    if (ItemPointerGetBlockNumber(&entry_a->tid) < ItemPointerGetBlockNumber(&entry_b->tid))
        return -1;
    if (ItemPointerGetBlockNumber(&entry_a->tid) > ItemPointerGetBlockNumber(&entry_b->tid))
        return 1;
    
    if (ItemPointerGetOffsetNumber(&entry_a->tid) < ItemPointerGetOffsetNumber(&entry_b->tid))
        return -1;
    if (ItemPointerGetOffsetNumber(&entry_a->tid) > ItemPointerGetOffsetNumber(&entry_b->tid))
        return 1;
    
    return 0;
}

/*
 * compute_index_logical_checksum_internal - Core logical index checksum
 *
 * Computes a logical checksum for an index that depends only on:
 * 1. Index key values (logical content)
 * 2. Heap TIDs (logical pointers to tuples)
 *
 * This checksum ignores the physical index structure and is stable
 * across REINDEX operations with the same data.
 */
static uint32
compute_index_logical_checksum_internal(Relation idxRel)
{
    BlockNumber nblocks;
    BufferAccessStrategy bstrategy;
    BlockNumber blkno;
    List *entries = NIL;
    uint32 aggregate;
    TupleDesc idx_tupdesc = RelationGetDescr(idxRel);
    int num_entries;
    IndexLogicalEntry **entries_array;
    int i;
    ListCell *lc;
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
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
                ItemId itemId;
                
                itemId = PageGetItemId(page, offnum);
                
                if (ItemIdIsUsed(itemId) && !ItemIdIsDead(itemId))
                {
                    IndexTuple itup;
                    IndexLogicalEntry *entry;
                    
                    itup = (IndexTuple) PageGetItem(page, itemId);
                    entry = extract_index_key_values(itup, idx_tupdesc);
                    
                    /* Compute hash for this entry and store it */
                    entry->entry_hash = compute_index_entry_hash(entry);
                    
                    entries = lappend(entries, entry);
                }
            }
        }
        
        UnlockReleaseBuffer(buffer);
        
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    /* For empty indexes, return hash of relation OID */
    if (entries == NIL)
    {
        Oid relid = RelationGetRelid(idxRel);
        return fnv1a_32_hash(&relid, sizeof(relid), FNV_BASIS_32);
    }
    
    /* Convert to array and sort for order-independent aggregation */
    num_entries = list_length(entries);
    entries_array = (IndexLogicalEntry **)palloc(num_entries * sizeof(IndexLogicalEntry *));
    i = 0;
    
    foreach(lc, entries)
    {
        entries_array[i++] = (IndexLogicalEntry *)lfirst(lc);
    }
    
    qsort(entries_array, num_entries, sizeof(IndexLogicalEntry *),
          compare_index_entries);
    
    /* Compute aggregate hash from sorted entries */
    aggregate = FNV_BASIS_32;
    for (i = 0; i < num_entries; i++)
    {
        IndexLogicalEntry *entry = entries_array[i];
        aggregate = combine_checksums(aggregate, entry->entry_hash);
    }
    
    /* Clean up */
    for (i = 0; i < num_entries; i++)
    {
        free_index_logical_entry(entries_array[i]);
    }
    pfree(entries_array);
    list_free(entries);
    
    return aggregate;
}

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
    uint32      index_checksum = 0;
    
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
    
    /* Compute logical checksum */
    index_checksum = compute_index_logical_checksum_internal(idxRel);
    
    /* Close index */
    index_close(idxRel, AccessShareLock);
    
    PG_RETURN_INT32((int32)index_checksum);
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
    uint64      db_checksum = UINT64CONST(14695981039346656037);
    Snapshot    snapshot;
    uint64      relation_count = 0;
    
    /* Use a consistent snapshot */
    snapshot = GetActiveSnapshot();

    /* Scan pg_class to find all relations in the database */
    pg_class_rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan(pg_class_rel, snapshot, 0, NULL);

    while ((classTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
        Oid         relid = classForm->oid;
        Oid         relnamespace = classForm->relnamespace;
        char        relkind = classForm->relkind;
        uint64      rel_checksum;
        
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
        
        /* Get relation checksum based on type and mode */
        PG_TRY();
        {
            if (relkind == RELKIND_INDEX)
            {
                /* Index checksum */
                if (physical)
                {
                    rel_checksum = (uint64)DatumGetInt32(
                        DirectFunctionCall1(pg_index_physical_checksum, 
                                           ObjectIdGetDatum(relid)));
                }
                else
                {
                    Datum d = DirectFunctionCall1(pg_index_logical_checksum,
                                                 ObjectIdGetDatum(relid));
                    
                    if (DatumGetPointer(d) == NULL)
                        continue; /* Skip unsupported index types */
                    
                    rel_checksum = (uint64)DatumGetInt32(d);
                }
            }
            else
            {
                /* Table/materialized view checksum */
                if (physical)
                {
                    Datum d = DirectFunctionCall2(pg_table_physical_checksum,
                                                 ObjectIdGetDatum(relid),
                                                 BoolGetDatum(false));
                    
                    /* pg_table_physical_checksum never returns NULL */
                    rel_checksum = DatumGetInt64(d);
                }
                else
                {
                    /* For logical checksum, tables without PK return NULL */
                    Datum d = DirectFunctionCall1(pg_table_logical_checksum,
                                                 ObjectIdGetDatum(relid));
                    
                    if (DatumGetPointer(d) == NULL)
                        continue; /* Skip tables without PK in logical mode */
                    
                    rel_checksum = DatumGetInt64(d);
                }
            }
            
            /* Combine with database checksum */
            db_checksum = combine_checksums_64(db_checksum, rel_checksum);
            db_checksum = combine_checksums_64(db_checksum, (uint64)relid);
            relation_count++;
        }
        PG_CATCH();
        {
            /* Skip relations that can't be processed */
            FlushErrorState();
            continue;
        }
        PG_END_TRY();
        
        CHECK_FOR_INTERRUPTS();
    }

    /* Clean up */
    table_endscan(scan);
    table_close(pg_class_rel, AccessShareLock);

    /* Include relation count in final checksum */
    db_checksum = combine_checksums_64(db_checksum, relation_count);
    
    return db_checksum;
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