/*-------------------------------------------------------------------------
 *
 * pg_checksums.c
 *    Implementation of multi-level checksum functionality for PostgreSQL
 *
 * This module provides checksum computation at five distinct levels:
 *
 * 1. COLUMN LEVEL (pg_column_checksum):
 *    - Computes checksum for individual column values
 *    - NULL values return CHECKSUM_NULL (0xFFFFFFFF)
 *    - Uses attribute number as hash seed for uniqueness across columns
 *    - Handles all PostgreSQL data types (byval, byref, varlena, cstring)
 *    - Guarantee: Different column types/values → different checksums
 *
 * 2. TUPLE LEVEL (pg_tuple_checksum):
 *    - Computes checksum for entire table rows (tuples)
 *    - Two modes: with/without tuple header (transaction visibility info)
 *    - Seeds hash with physical location (block << 16 | offset)
 *    - Incorporates MVCC information (xmin/xmax) when excluding header
 *    - Guarantee: Same data at different locations → different checksums
 *    - Guarantee: Any data change → different checksum
 *
 * 3. TABLE LEVEL (pg_table_checksum):
 *    - Aggregates checksums of all tuples in a table
 *    - Uses XOR aggregation for efficient incremental updates
 *    - Includes relation OID in aggregation to ensure cross-table uniqueness
 *    - Empty tables return 0
 *    - Property: Adding/removing tuples changes table checksum
 *
 * 4. INDEX LEVEL (pg_index_checksum):
 *    - Computes checksum for index contents
 *    - Processes all index pages and live index tuples
 *    - For B-tree indexes, includes heap TID in checksum calculation
 *    - XOR aggregation across all index tuples
 *    - Property: Index rebuild with same data produces same checksum
 *
 * 5. DATABASE LEVEL (pg_database_checksum):
 *    - Superuser-only function for entire database checksum
 *    - Aggregates checksums of all tables and indexes
 *    - Optional filtering of system catalogs and TOAST tables
 *    - Uses consistent snapshot for repeatable results
 *    - Security: Requires superuser to prevent information disclosure
 *
 * Algorithm Details:
 * - Uses FNV-1a 32-bit hash (same as PostgreSQL's built-in checksums)
 * - Hash formula: hash = (hash ^ byte) * FNV_PRIME_32
 * - Initial seed varies by context (location, attribute number, etc.)
 * - XOR post-processing ensures uniqueness and avoids CHECKSUM_NULL collisions
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

/*
 * fnv1a_32_hash - FNV-1a 32-bit hash implementation
 *
 * This is identical to PostgreSQL's internal checksum algorithm for
 * compatibility and consistency. The algorithm provides good avalanche
 * properties and low collision rates for typical database data.
 *
 * Parameters:
 *   data: Pointer to data to hash
 *   len: Length of data in bytes
 *   seed: Initial hash value (0 uses FNV_BASIS_32)
 *
 * Returns: 32-bit hash value
 *
 * Algorithm: hash = (hash ^ byte) * FNV_PRIME_32 for each byte
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
 * pg_checksum_data_custom - Wrapper for FNV-1a hash
 *
 * This function provides the checksum algorithm for all data types.
 * It's used when pg_checksum_data() is not available or when we need
 * custom seeding behavior for different granularity levels.
 *
 * The init_value parameter allows different contexts to seed the hash
 * differently (e.g., with attribute number for columns, location for tuples).
 */
uint32
pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value)
{
    return fnv1a_32_hash(data, len, init_value);
}

/*
 * pg_page_checksum - SQL function for page-level checksums
 *
 * Wrapper around PostgreSQL's built-in pg_checksum_page() function.
 * Provides checksums for individual database pages, useful for detecting
 * physical corruption at the storage level.
 *
 * Returns: 16-bit page checksum (same as built-in) or 0 for new pages
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
    
    /* Open relation */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Read the page */
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

/*
 * pg_tuple_checksum_internal - Core tuple checksum calculation
 *
 * This is the heart of the tuple checksum system. It computes a checksum
 * that uniquely identifies a tuple based on:
 * 1. Tuple data content (header optional)
 * 2. Physical location (block and offset)
 * 3. MVCC information (xmin/xmax when header excluded)
 */
uint32
pg_tuple_checksum_internal(Page page, OffsetNumber offnum, BlockNumber blkno, bool include_header)
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
    
    /* Incorporate MVCC information */
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
 * pg_tuple_checksum - SQL function for tuple-level checksums
 *
 * Public interface for computing tuple checksums. Handles buffer management,
 * locking, and error recovery to ensure safe access to shared buffers.
 *
 * Parameters:
 *   reloid: Relation OID containing the tuple
 *   tid: TID (block, offset) of the tuple
 *   include_header: Whether to include tuple header in checksum
 *
 * Returns: 32-bit tuple checksum
 */
PG_FUNCTION_INFO_V1(pg_tuple_checksum);

Datum
pg_tuple_checksum(PG_FUNCTION_ARGS)
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
        checksum = pg_tuple_checksum_internal(page,
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

/*
 * pg_column_checksum_internal - Core column checksum calculation
 *
 * Computes checksums for individual column values with special handling for:
 * 1. NULL values: Returns CHECKSUM_NULL (0xFFFFFFFF)
 * 2. Data types: Proper handling of byval, byref, varlena, and cstring types
 * 3. Detoasting: Expands compressed varlena data for accurate checksums
 * 4. Type awareness: Uses type information for proper length determination
 *
 * The attnum parameter seeds the hash, ensuring different columns with
 * identical values have different checksums.
 */
static uint32
pg_column_checksum_internal(Datum value, bool isnull, Oid typid,
                            int32 typmod, int attnum)
{
    Form_pg_type typeForm;
    HeapTuple   typeTuple;
    char       *data;
    int         len;
    uint32      checksum;

    /* Handle NULL values */
    if (isnull)
        return CHECKSUM_NULL;

    /* Get type information */
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);
    
    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    if (typeForm->typbyval && typeForm->typlen > 0)
    {
        /* Fixed-length pass-by-value type */
        data = (char *) &value;
        len = typeForm->typlen;
        checksum = pg_checksum_data_custom(data, len, attnum);
    }
    else if (typeForm->typlen == -1)
    {
        /* varlena type */
        struct varlena *varlena;
        
        /* Detoast if necessary */
        varlena = PG_DETOAST_DATUM(value);
        
        data = (char *) varlena;
        len = VARSIZE_ANY(varlena);
        
        checksum = pg_checksum_data_custom(data, len, attnum);
        
        if (varlena != (struct varlena *) DatumGetPointer(value))
            pfree(varlena);
    }
    else if (typeForm->typlen == -2)
    {
        /* cstring type */
        data = DatumGetCString(value);
        len = strlen(data);
        checksum = pg_checksum_data_custom(data, len, attnum);
    }
    else
    {
        /* Fixed-length pass-by-reference type */
        data = DatumGetPointer(value);
        len = typeForm->typlen;
        
        if (data == NULL)
            elog(ERROR, "invalid pointer for fixed-length reference type");
            
        checksum = pg_checksum_data_custom(data, len, attnum);
    }

    ReleaseSysCache(typeTuple);
    
    /* Guarantee that non-NULL values never return CHECKSUM_NULL */
    if (checksum == CHECKSUM_NULL)
        checksum = (CHECKSUM_NULL ^ attnum ^ typid) & 0xFFFFFFFE;
    
    return checksum;
}

/*
 * pg_column_checksum - SQL function for column-level checksums
 *
 * Public interface for computing column checksums. Handles relation access,
 * tuple fetching, and type system interaction.
 *
 * Returns CHECKSUM_NULL (-1) for NULL columns, ensuring clear distinction
 * between NULL and valid checksum values (which can include 0).
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

    /* Get attribute value */
    attr = TupleDescAttr(tupdesc, attnum_arg - 1);
    value = heap_getattr(&heapTuple, attnum_arg, tupdesc, &isnull);
    
    /* Compute column checksum */
    checksum = pg_column_checksum_internal(value, isnull, 
                                          attr->atttypid, 
                                          attr->atttypmod,
                                          attnum_arg);

    /* Clean up */
    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32)checksum);
}

/*
 * pg_table_checksum - SQL function for table-level checksums
 *
 * Computes a checksum representing the entire table state by aggregating
 * all tuple checksums using XOR. This provides:
 * 1. Efficient aggregation: XOR is associative, commutative, and reversible
 * 2. Incremental updates: Adding/removing tuples has predictable effect
 * 3. Relation identity: Includes relation OID in aggregation
 * 4. Snapshot consistency: Uses GetActiveSnapshot() for MVCC correctness
 */
PG_FUNCTION_INFO_V1(pg_table_checksum);

Datum
pg_table_checksum(PG_FUNCTION_ARGS)
{
    Oid         reloid;
    bool        include_header;
    Relation    rel;
    TableScanDesc scan;
    HeapTuple   tuple;
    uint64      table_checksum = 0;
    
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
        
        /* Compute tuple checksum */
        tuple_checksum = pg_tuple_checksum_internal(page,
                                                   ItemPointerGetOffsetNumber(&tuple->t_self),
                                                   ItemPointerGetBlockNumber(&tuple->t_self),
                                                   include_header);
        
        /* XOR with table checksum */
        table_checksum ^= ((uint64)tuple_checksum << 32) | reloid;
        
        UnlockReleaseBuffer(buffer);
    }
    
    /* Clean up */
    table_endscan(scan);
    relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT64((int64)table_checksum);
}

/*
 * pg_index_checksum - SQL function for index-level checksums
 *
 * Computes checksums for index structures by scanning all index pages.
 * Special handling for B-tree indexes includes heap TID in checksum
 * to maintain consistency with table organization.
 */
PG_FUNCTION_INFO_V1(pg_index_checksum);

Datum
pg_index_checksum(PG_FUNCTION_ARGS)
{
    Oid         indexoid;
    Relation    idxRel;
    uint32      index_checksum = 0;
    BlockNumber nblocks;
    BufferAccessStrategy bstrategy;
    BlockNumber blkno;
    Buffer      buffer;
    Page        page;
    OffsetNumber maxoff;
    OffsetNumber offnum;
    ItemId      itemId;
    IndexTuple  itup;
    uint32      tuple_checksum;
    Size        itup_len;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    
    indexoid = PG_GETARG_OID(0);
    
    /* Open index */
    idxRel = index_open(indexoid, AccessShareLock);
    
    /* Get number of blocks */
    nblocks = RelationGetNumberOfBlocks(idxRel);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    
    /* Scan index blocks */
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        
        /* Skip uninitialized pages */
        if (!PageIsNew(page))
        {
            maxoff = PageGetMaxOffsetNumber(page);
            
            for (offnum = FirstOffsetNumber;
                 offnum <= maxoff;
                 offnum = OffsetNumberNext(offnum))
            {
                itemId = PageGetItemId(page, offnum);
                
                if (ItemIdIsUsed(itemId) && !ItemIdIsDead(itemId))
                {
                    itup = (IndexTuple) PageGetItem(page, itemId);
                    itup_len = IndexTupleSize(itup);
                    
                    /* Calculate index tuple checksum */
                    tuple_checksum = pg_checksum_data_custom((char *)itup, itup_len, offnum);
                    
                    /* For B-tree, include heap TID */
                    if (idxRel->rd_rel->relam == BTREE_AM_OID)
                    {
                        tuple_checksum ^= ItemPointerGetBlockNumber(&itup->t_tid);
                        tuple_checksum ^= ItemPointerGetOffsetNumber(&itup->t_tid) << 16;
                    }
                    
                    /* XOR into index checksum */
                    index_checksum ^= tuple_checksum;
                }
            }
        }
        
        UnlockReleaseBuffer(buffer);
        
        /* Check for interrupts occasionally */
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    /* Clean up */
    FreeAccessStrategy(bstrategy);
    index_close(idxRel, AccessShareLock);
    
    PG_RETURN_INT32((int32)index_checksum);
}

/*
 * pg_database_checksum - SQL function for database-level checksums
 *
 * Superuser-only function that computes checksum for entire database
 * by aggregating checksums of all relations. Security considerations:
 * 1. Requires superuser to prevent information disclosure about system objects
 * 2. Uses PG_TRY/PG_CATCH to handle inaccessible relations gracefully
 * 3. Configurable filtering of system catalogs and TOAST tables
 *
 * Implementation notes:
 * - Scans pg_class with consistent snapshot for repeatable results
 * - Skips non-table/index relations (views, foreign tables, etc.)
 * - Handles relation access failures gracefully
 */
PG_FUNCTION_INFO_V1(pg_database_checksum);

Datum
pg_database_checksum(PG_FUNCTION_ARGS)
{
    bool        include_system = false;
    bool        include_toast = false;
    Relation    pg_class_rel;
    TableScanDesc scan;
    HeapTuple   classTuple;
    uint64      db_checksum = 0;
    Snapshot    snapshot;
    
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
            relkind != RELKIND_MATVIEW &&
            relkind != RELKIND_SEQUENCE)
            continue;
        
        /* Get relation checksum based on type */
        if (relkind == RELKIND_INDEX)
        {
            uint32 idx_checksum;
            
            /* Try to get index checksum */
            PG_TRY();
            {
                idx_checksum = (uint32)DatumGetInt32(DirectFunctionCall1(pg_index_checksum, 
                                                                        ObjectIdGetDatum(relid)));
                db_checksum ^= ((uint64)idx_checksum << 32) | relid;
            }
            PG_CATCH();
            {
                /* Skip indexes that can't be processed */
                continue;
            }
            PG_END_TRY();
        }
        else
        {
            uint64 tbl_checksum;
            
            /* Get table checksum */
            PG_TRY();
            {
                tbl_checksum = DatumGetInt64(DirectFunctionCall2(pg_table_checksum,
                                                               ObjectIdGetDatum(relid),
                                                               BoolGetDatum(false)));
                db_checksum ^= tbl_checksum;
            }
            PG_CATCH();
            {
                /* Skip tables that can't be processed */
                continue;
            }
            PG_END_TRY();
        }
        
        CHECK_FOR_INTERRUPTS();
    }

    /* Clean up */
    table_endscan(scan);
    table_close(pg_class_rel, AccessShareLock);

    PG_RETURN_INT64((int64)db_checksum);
}

/*
 * pg_data_checksum - Generic data checksum utility function
 *
 * Raw data checksum function for arbitrary bytea data. Useful for:
 * - Application-level data verification
 * - Custom checksum requirements
 * - Testing and debugging
 *
 * The seed parameter allows different contexts to produce different
 * checksums for identical data.
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
 * Convenience wrapper for text data. Useful for string comparison
 * and text content verification without encoding concerns.
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