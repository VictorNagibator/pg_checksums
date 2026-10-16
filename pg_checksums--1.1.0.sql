-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_checksums" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums - Multi-Level Checksum Framework for PostgreSQL
 *
 * This extension provides checksum computation at six granularity levels,
 * with both physical and logical variants (except where noted):
 *
 * 1. PAGE LEVEL: Physical only - detects storage corruption
 * 2. CELL LEVEL: Logical only - based on cell values
 * 3. TUPLE LEVEL: Physical & Logical - row-level integrity
 * 4. TABLE LEVEL: Physical & Logical - table-wide consistency
 * 5. INDEX LEVEL: Physical & Logical - index structure validation
 * 6. DATABASE LEVEL: Physical & Logical - database-wide integrity
 *
 * Physical checksums depend on storage layout and change after:
 *   - VACUUM FULL, CLUSTER, REINDEX
 *   - Physical data movement
 *   - Storage reorganization
 *
 * Logical checksums depend only on data content and are stable across:
 *   - Physical reorganization
 *   - VACUUM FULL, CLUSTER, REINDEX (with same data)
 *   - Storage migration
 *
 * All functions use the FNV-1a hash algorithm for consistency with
 * PostgreSQL's internal checksum implementation.
 *-------------------------------------------------------------------------
 */

/*-----------------------------------------------------------
 * PAGE LEVEL FUNCTIONS (Physical only)
 *-----------------------------------------------------------
 */

-- Page-level checksum (wraps PostgreSQL's built-in page checksums)
CREATE OR REPLACE FUNCTION pg_page_checksum(relname regclass, blkno integer)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_page_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_page_checksum(regclass, integer) IS 
'Physical checksum of a database page. Uses PostgreSQL''s built-in page checksum algorithm.
 Returns 0 for new (uninitialized) pages. Detects physical storage corruption.';

/*-----------------------------------------------------------
 * CELL LEVEL FUNCTIONS (Logical only)
 *-----------------------------------------------------------
 */

-- Cell-level checksum (logical - depends on Cell value only)
CREATE OR REPLACE FUNCTION pg_cell_checksum(relname regclass, tid tid, attnum integer)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_cell_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_cell_checksum(regclass, tid, integer) IS 
'Logical checksum of a cell value. Returns CHECKSUM_NULL (4294967295) for NULL values.
 Same value in same column always produces same checksum. Independent of physical storage.';

/*-----------------------------------------------------------
 * TUPLE LEVEL FUNCTIONS (Physical and Logical)
 *-----------------------------------------------------------
 */

-- Physical tuple checksum (depends on physical location)
CREATE OR REPLACE FUNCTION pg_tuple_physical_checksum(relname regclass, tid tid, include_header boolean DEFAULT false)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_tuple_physical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_tuple_physical_checksum(regclass, tid, boolean) IS 
'Physical checksum of a tuple (row). Depends on physical location (block, offset).
 Changes after VACUUM, CLUSTER, or physical data movement. Useful for detecting physical corruption.';

-- Logical tuple checksum (depends on primary key values)
CREATE OR REPLACE FUNCTION pg_tuple_logical_checksum(relname regclass, tid tid, include_header boolean DEFAULT false)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_tuple_logical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_tuple_logical_checksum(regclass, tid, boolean) IS 
'Logical checksum of a tuple (row). Uses primary key values instead of physical location.
 Returns NULL if table has no primary key. Stable across physical reorganization.';

/*-----------------------------------------------------------
 * TABLE LEVEL FUNCTIONS (Physical and Logical)
 *-----------------------------------------------------------
 */

-- Physical table checksum (aggregates physical tuple checksums)
CREATE OR REPLACE FUNCTION pg_table_physical_checksum(relname regclass, include_header boolean DEFAULT false)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_table_physical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_table_physical_checksum(regclass, boolean) IS 
'Physical checksum of entire table. Aggregates physical checksums of all tuples.
 Order-independent aggregation. Changes after physical reorganization. 64-bit return value.';

-- Logical table checksum (aggregates logical tuple checksums)
CREATE OR REPLACE FUNCTION pg_table_logical_checksum(relname regclass)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_table_logical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_table_logical_checksum(regclass) IS 
'Logical checksum of entire table. Aggregates logical checksums of all tuples.
 Requires primary key. Stable across VACUUM FULL, CLUSTER. Returns NULL if no primary key.';

/*-----------------------------------------------------------
 * INDEX LEVEL FUNCTIONS (Physical and Logical)
 *-----------------------------------------------------------
 */

-- Physical index checksum (depends on physical index structure)
CREATE OR REPLACE FUNCTION pg_index_physical_checksum(relname regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_index_physical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_index_physical_checksum(regclass) IS 
'Physical checksum of an index. Depends on physical index structure.
 Supports B-tree, Hash, GiST, GIN, SP-GiST, and BRIN indexes.
 Changes after REINDEX or physical reorganization.';

-- Logical index checksum (depends on index key values)
CREATE OR REPLACE FUNCTION pg_index_logical_checksum(relname regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_index_logical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_index_logical_checksum(regclass) IS 
'Logical checksum of an index. Depends only on index key values and heap TIDs.
 Stable across REINDEX with same data. Supports same index types as physical checksum.';

/*-----------------------------------------------------------
 * DATABASE LEVEL FUNCTIONS (Physical and Logical)
 *-----------------------------------------------------------
 */

-- Physical database checksum (aggregates physical relation checksums)
CREATE OR REPLACE FUNCTION pg_database_physical_checksum(include_system boolean DEFAULT false, include_toast boolean DEFAULT false)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_database_physical_checksum'
LANGUAGE C STRICT SECURITY DEFINER;

COMMENT ON FUNCTION pg_database_physical_checksum(boolean, boolean) IS 
'Physical checksum of entire database. Aggregates physical checksums of all relations.
 Superuser-only. Optional filtering of system catalogs and TOAST tables.
 Changes after any physical reorganization. 64-bit return value.';

-- Logical database checksum (aggregates logical relation checksums)
CREATE OR REPLACE FUNCTION pg_database_logical_checksum(include_system boolean DEFAULT false, include_toast boolean DEFAULT false)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_database_logical_checksum'
LANGUAGE C STRICT SECURITY DEFINER;

COMMENT ON FUNCTION pg_database_logical_checksum(boolean, boolean) IS 
'Logical checksum of entire database. Aggregates logical checksums of all relations.
 Superuser-only. Skips tables without primary keys. Stable across physical reorganization
 of entire database. 64-bit return value.';

/*-----------------------------------------------------------
 * UTILITY FUNCTIONS
 *-----------------------------------------------------------
 */

-- Generic binary data checksum
CREATE OR REPLACE FUNCTION pg_data_checksum(data bytea, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_data_checksum'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION pg_data_checksum(bytea, integer) IS 
'Generic checksum for arbitrary binary data. Uses FNV-1a 32-bit hash.
 Seed parameter allows different contexts to produce different checksums for same data.';

-- Text data checksum
CREATE OR REPLACE FUNCTION pg_text_checksum(text text, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_text_checksum'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION pg_text_checksum(text, integer) IS 
'Convenience wrapper for text data checksum. Uses FNV-1a 32-bit hash.
 Useful for string comparison and text content verification.';