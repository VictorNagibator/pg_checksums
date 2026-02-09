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
#include "access/xlogdefs.h"
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
static uint32 pg_tuple_logical_checksum_internal(Relation rel, HeapTuple tuple, bool include_header);
static bool index_supports_checksum(Oid amoid);
static uint32 compute_generic_index_physical_checksum(Relation idxRel);
static uint32 compute_brin_index_physical_checksum(Relation idxRel);
static uint32 compute_index_logical_checksum_internal(Relation idxRel);
static uint32 pg_column_checksum_internal(Datum value, bool isnull, Oid typid,
                                          int32 typmod, int attnum);
static uint32 compute_order_independent_checksum(List *hash_list);
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
        
        col_hash = pg_column_checksum_internal(value, false,
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
        
        col_hash = pg_column_checksum_internal(value, isnull,
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
    uint32      relation_hash;
    
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    reloid = PG_GETARG_OID(0);
    include_header = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Include relation metadata in checksum */
    relation_hash = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    relation_hash = combine_checksums(relation_hash, rel->rd_rel->relpages);
    relation_hash = combine_checksums(relation_hash, rel->rd_rel->reltuples);
    
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
    
    /* Combine with relation metadata */
    aggregate = combine_checksums(aggregate, relation_hash);
    
    if (aggregate == FNV_BASIS_32) {
        /* Guarantee hash for empty tables */
        aggregate = fnv1a_32_hash(&reloid, sizeof(reloid), FNV_BASIS_32);
    }

    if (aggregate == 0) {
        aggregate = 0x7FFFFFFF;
    }
    
    /* Include relation OID in final checksum (64-bit) */
    if (tuple_hashes != NIL)
        list_free(tuple_hashes);
    
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
    TableScanDesc scan;
    HeapTuple   tuple;
    List       *tuple_hashes = NIL;
    List       *pk_columns;
    uint32      aggregate;
    uint64      final_checksum;
    bool        has_valid_pk = true;
    
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
        PG_RETURN_NULL();
    }
    
    /* Verify PK doesn't contain NULLs in any tuple */
    scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        uint32 tuple_checksum = pg_tuple_logical_checksum_internal(rel, tuple, false);
        if (tuple_checksum == 0)
        {
            has_valid_pk = false;
            break;
        }
        tuple_hashes = lappend_int(tuple_hashes, tuple_checksum);
    }
    table_endscan(scan);
    
    if (!has_valid_pk)
    {
        list_free(pk_columns);
        list_free(tuple_hashes);
        relation_close(rel, AccessShareLock);
        PG_RETURN_NULL();
    }
    
    relation_close(rel, AccessShareLock);
    list_free(pk_columns);
    
    /* Compute order-independent aggregate */
    aggregate = compute_order_independent_checksum(tuple_hashes);
    
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
    List *page_hashes = NIL;
    uint32 index_type_hash;
    uint32 aggregate;
    Size page_size;
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    /* Include index metadata in checksum */
    index_type_hash = fnv1a_32_hash(&idxRel->rd_rel->relam, sizeof(Oid), FNV_BASIS_32);
    index_type_hash = combine_checksums(index_type_hash, idxRel->rd_rel->relnatts);
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
        uint32 page_hash;
        PageHeader phdr;
        
        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        page_size = PageGetPageSize(page);
        
        if (!PageIsNew(page))
        {
            /* Hash the entire page using actual page size */
            page_hash = fnv1a_32_hash((char *)page, page_size, 0);
            
            /* Include page header details */
            phdr = (PageHeader)page;
            page_hash = combine_checksums(page_hash, phdr->pd_lower);
            page_hash = combine_checksums(page_hash, phdr->pd_upper);
            page_hash = combine_checksums(page_hash, phdr->pd_special);
            
            /* Include block number for uniqueness */
            page_hash = combine_checksums(page_hash, blkno);
            
            /* Include page flags */
            page_hash = combine_checksums(page_hash, phdr->pd_flags);
            
            /* Include page size for additional uniqueness */
            page_hash = combine_checksums(page_hash, (uint32)page_size);
            
            page_hashes = lappend_int(page_hashes, page_hash);
        }
        else
        {
            /* New pages contribute to checksum too */
            page_hash = fnv1a_32_hash("NEW_PAGE", 8, blkno);
            /* Include page size even for new pages */
            page_hash = combine_checksums(page_hash, (uint32)page_size);
            page_hashes = lappend_int(page_hashes, page_hash);
        }
        
        UnlockReleaseBuffer(buffer);
        
        /* Allow query cancellation every 64 blocks */
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    /* Combine page hashes with index metadata */
    aggregate = compute_order_independent_checksum(page_hashes);
    aggregate = combine_checksums(aggregate, index_type_hash);
    
    /* Free the list */
    if (page_hashes != NIL)
        list_free(page_hashes);
    
    return aggregate;
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
    List *page_hashes = NIL;
    Size page_size;
    
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer buffer;
        Page page;
        uint32 page_hash;
        
        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                   RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        page_size = PageGetPageSize(page);
        
        if (!PageIsNew(page))
        {
            uint8 *special_space;
            Size special_size;
            PageHeader phdr;

            /* Hash the page using actual page size */
            page_hash = fnv1a_32_hash((char *)page, page_size, 0);
            
            /* Include page size */
            page_hash = combine_checksums(page_hash, (uint32)page_size);
            
            /* Get BRIN special space */
            special_space = (uint8 *)PageGetSpecialPointer(page);
            special_size = PageGetSpecialSize(page);
            
            /* Include BRIN-specific information from special space */
            if (special_size >= 4)
            {
                /* First 4 bytes of BRIN special space typically contain type and metadata */
                uint32 brin_info = 0;
                memcpy(&brin_info, special_space, 4);
                page_hash = combine_checksums(page_hash, brin_info);
            }
            
            /* Include block number */
            page_hash = combine_checksums(page_hash, blkno);
            
            /* Include page header info */
            phdr = (PageHeader)page;
            page_hash = combine_checksums(page_hash, phdr->pd_lower);
            page_hash = combine_checksums(page_hash, phdr->pd_upper);
            page_hash = combine_checksums(page_hash, phdr->pd_flags);
            
            page_hashes = lappend_int(page_hashes, page_hash);
        }
        else
        {
            page_hash = fnv1a_32_hash("BRIN_NEW", 8, blkno);
            page_hash = combine_checksums(page_hash, (uint32)page_size);
            page_hashes = lappend_int(page_hashes, page_hash);
        }
        
        UnlockReleaseBuffer(buffer);
        
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    if (page_hashes != NIL)
    {
        uint32 result = compute_order_independent_checksum(page_hashes);
        list_free(page_hashes);
        return result;
    }
    
    return FNV_BASIS_32;
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
    List *entry_hashes = NIL;
    TupleDesc idx_tupdesc = RelationGetDescr(idxRel);
    Oid relid = RelationGetRelid(idxRel);
    uint32 result;
    
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
                    Datum *values;
                    bool *isnull;
                    int i;
                    uint32 entry_hash = FNV_BASIS_32;
                    ItemPointerData tid;
                    uint32 tid_hash;
                    
                    itup = (IndexTuple) PageGetItem(page, itemId);
                    
                    /* Extract index key values */
                    values = (Datum *)palloc(idx_tupdesc->natts * sizeof(Datum));
                    isnull = (bool *)palloc(idx_tupdesc->natts * sizeof(bool));
                    
                    index_deform_tuple(itup, idx_tupdesc, values, isnull);
                    
                    /* Hash each key value */
                    for (i = 0; i < idx_tupdesc->natts; i++)
                    {
                        if (isnull[i])
                        {
                            entry_hash = combine_checksums(entry_hash, CHECKSUM_NULL);
                        }
                        else
                        {
                            Form_pg_attribute attr = TupleDescAttr(idx_tupdesc, i);
                            uint32 col_hash = pg_column_checksum_internal(values[i], false,
                                                                         attr->atttypid,
                                                                         attr->atttypmod,
                                                                         i + 1);
                            entry_hash = combine_checksums(entry_hash, col_hash);
                        }
                    }
                    
                    /* Hash the heap TID */
                    tid = itup->t_tid;
                    tid_hash = fnv1a_32_hash(&tid, sizeof(tid), 0);
                    entry_hash = combine_checksums(entry_hash, tid_hash);
                    
                    pfree(values);
                    pfree(isnull);
                    
                    entry_hashes = lappend_int(entry_hashes, entry_hash);
                }
            }
        }
        
        UnlockReleaseBuffer(buffer);
        
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
    
    /* For empty indexes, return hash of relation OID */
    if (entry_hashes == NIL)
    {
        return fnv1a_32_hash(&relid, sizeof(relid), FNV_BASIS_32);
    }
    
    /* Sort hashes for order-independent aggregation */
    result = compute_order_independent_checksum(entry_hashes);
    
    list_free(entry_hashes);
    
    return result;
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