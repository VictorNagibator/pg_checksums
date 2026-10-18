-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_checksums UPDATE TO '1.2.0'" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums 1.1.1 -> 1.2.0
 *
 * 1.2.0 adds pg_table_content_checksum() and makes partitioned indexes
 * aggregate their leaf indexes.  These changes are implemented in C; this
 * script only adds the new SQL function and refreshes the index comments.
 *
 * The pg_checksums.strict GUC is registered by the C module and needs no
 * SQL changes.
 *-------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION pg_table_content_checksum(relname regclass)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_table_content_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_table_content_checksum(regclass) IS
'Content-only checksum of a table. Does not require a primary key and does not
 include relation OIDs. Uses a sum-plus-count aggregate so duplicate rows are
 preserved. Partitioned tables aggregate all leaf rows.';

COMMENT ON FUNCTION pg_index_physical_checksum(regclass) IS
'Physical checksum of an index. Depends on physical index structure.
 Supports B-tree, Hash, GiST, GIN, SP-GiST, and BRIN indexes.
 A partitioned index aggregates the checksums of its leaf indexes.
 Changes after REINDEX or physical reorganization.';

COMMENT ON FUNCTION pg_index_logical_checksum(regclass) IS
'Logical checksum of an index. Depends only on index key values and heap TIDs.
 Stable across REINDEX with same data. B-tree only; returns NULL for Hash, GiST,
 GIN, SP-GiST and BRIN, whose pages do not contain ordinary index tuples.
 A partitioned B-tree index aggregates the checksums of its leaf indexes.';
