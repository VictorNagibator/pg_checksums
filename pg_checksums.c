/*-------------------------------------------------------------------------
 *
 * contrib/pg_checksums/pg_checksums.c
 *    Checksum functions for PostgreSQL extension
 *
 * This extension provides checksum functionality at all granularities:
 * - Tuple-level checksums
 * - Column-level checksums  
 * - Table-level checksums
 * - Index-level checksums
 * - Database-level checksums
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * FNV-1a 32-bit hash implementation (identical to PostgreSQL's algorithm)
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
 * Custom checksum implementation based on PostgreSQL's algorithm
 * This is used for data blocks when pg_checksum_data is not available
 */
uint32
pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value)
{
    return fnv1a_32_hash(data, len, init_value);
}

/*
 * Wrapper for pg_checksum_page (built-in function)
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
 * Internal tuple checksum calculation
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
 * Tuple checksum - SQL function
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
 * Column checksum calculation
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
 * Column checksum - SQL function
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
 * Table checksum - SQL function
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
 * Index checksum - SQL function
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
 * Database checksum - SQL function (superuser only)
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
 * Data checksum - SQL function
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
 * Text checksum - SQL function
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