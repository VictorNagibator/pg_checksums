/*-------------------------------------------------------------------------
 *
 * pg_checksums.h
 *    Header file for checksum extension for PostgreSQL
 *
 * This extension provides a comprehensive checksum framework for PostgreSQL
 * at various granularity levels: column, tuple, table, index, and database.
 * The implementation uses FNV-1a 32-bit hash algorithm for consistency with
 * PostgreSQL's internal checksum implementation.
 *
 * Checksum Calculation Hierarchy:
 * 1. Column Level: Hash of column value with attribute number as seed
 * 2. Tuple Level: Hash of tuple data with physical location (ctid) as seed
 * 3. Table Level: XOR aggregation of all tuple checksums in the table
 * 4. Index Level: XOR aggregation of all index tuple checksums
 * 5. Database Level: XOR aggregation of all relation checksums in the database
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CHECKSUMS_H
#define PG_CHECKSUMS_H

#include "postgres.h"
#include "fmgr.h"

/* FNV-1a 32-bit hash algorithm constants */
#define FNV_PRIME_32 16777619U
#define FNV_BASIS_32 2166136261U

/* Special checksum value for NULL columns (0xFFFFFFFF = -1 in signed int32) */
#define CHECKSUM_NULL 0xFFFFFFFF

/* Function declarations - Public SQL-callable functions */
extern Datum pg_page_checksum(PG_FUNCTION_ARGS);
extern Datum pg_tuple_checksum(PG_FUNCTION_ARGS);
extern Datum pg_column_checksum(PG_FUNCTION_ARGS);
extern Datum pg_table_checksum(PG_FUNCTION_ARGS);
extern Datum pg_index_checksum(PG_FUNCTION_ARGS);
extern Datum pg_database_checksum(PG_FUNCTION_ARGS);
extern Datum pg_data_checksum(PG_FUNCTION_ARGS);
extern Datum pg_text_checksum(PG_FUNCTION_ARGS);

/* Internal utility functions */
extern uint32 pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value);
extern uint32 pg_tuple_checksum_internal(Page page, OffsetNumber offnum, BlockNumber blkno, bool include_header);

#endif /* PG_CHECKSUMS_H */