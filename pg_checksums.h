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
 * CHECKSUM TYPES:
 * 1. Physical Checksums: Depend on physical storage layout
 *    - Change after VACUUM FULL, REINDEX, or physical reorganization
 *    - Useful for detecting physical corruption
 *    - Include: physical location, page headers, etc.
 *
 * 2. Logical Checksums: Depend only on logical data content
 *    - Stable across physical reorganization
 *    - Useful for logical consistency checks, replication, migration
 *    - Based on: column values, primary keys, etc.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CHECKSUMS_H
#define PG_CHECKSUMS_H

#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_operator.h"
#include "nodes/pg_list.h"

/* FNV-1a 32-bit hash algorithm constants */
#define FNV_PRIME_32 16777619U
#define FNV_BASIS_32 2166136261U

/* Special checksum value for NULL columns (0xFFFFFFFF = -1 in signed int32) */
#define CHECKSUM_NULL 0xFFFFFFFF

/* Function declarations - Public SQL-callable functions */
extern Datum pg_page_checksum(PG_FUNCTION_ARGS);
extern Datum pg_column_checksum(PG_FUNCTION_ARGS);
extern Datum pg_tuple_physical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_tuple_logical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_table_physical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_table_logical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_index_physical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_index_logical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_database_physical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_database_logical_checksum(PG_FUNCTION_ARGS);
extern Datum pg_data_checksum(PG_FUNCTION_ARGS);
extern Datum pg_text_checksum(PG_FUNCTION_ARGS);

#endif /* PG_CHECKSUMS_H */