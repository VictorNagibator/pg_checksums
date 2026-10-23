-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_checksums UPDATE TO '1.3.0'" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums 1.2.0 -> 1.3.0
 *
 * This is an intentionally breaking release.  Logical checksums are now
 * canonical and no longer contain relation OIDs, heap TIDs or type OIDs.
 * The old content checksum and the three-argument logical tuple function are
 * removed; the database logical function keeps its signature but its second
 * argument now means include_indexes.
 *-------------------------------------------------------------------------
 */

-- Removed in 1.3: table logical checksum now handles tables without a PK.
DROP FUNCTION IF EXISTS pg_table_content_checksum(regclass);

-- Removed in 1.3: logical tuple checksums no longer take include_header.
DROP FUNCTION IF EXISTS pg_tuple_logical_checksum(regclass, tid, boolean);

CREATE OR REPLACE FUNCTION pg_tuple_logical_checksum(relname regclass, tid tid)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_tuple_logical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_tuple_logical_checksum(regclass, tid) IS
'Logical checksum of a tuple (row). Hashes canonical column values, without
 relation OIDs, heap TIDs or the tuple header. No primary key is required.
 Stable across physical reorganization.';

COMMENT ON FUNCTION pg_table_logical_checksum(regclass) IS
'Logical checksum of an entire table. No primary key is required. Aggregates
 canonical row hashes with a sum-plus-count aggregate, preserving duplicate
 rows. Relation OIDs and heap TIDs are excluded. Stable across VACUUM FULL,
 CLUSTER and REINDEX. Partitioned tables aggregate all leaf rows.';

COMMENT ON FUNCTION pg_index_logical_checksum(regclass) IS
'Logical checksum of an index. Derived from the visible table rows and the
 index definition, not from index pages: works for every index access method,
 including expression, partial and INCLUDE indexes. No heap TIDs or relation
 OIDs are included. Stable across VACUUM, CLUSTER and REINDEX. Physical
 checksums remain the tool for detecting missing or corrupt index entries.';

DROP FUNCTION IF EXISTS pg_database_logical_checksum(boolean, boolean);

CREATE FUNCTION pg_database_logical_checksum(include_system boolean DEFAULT false, include_indexes boolean DEFAULT true)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_database_logical_checksum'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_database_logical_checksum(boolean, boolean) IS
'Logical checksum of an entire database. Aggregates canonical table checksums;
no primary key is required. include_indexes controls whether logical index
checksums are included. TOAST content is covered through the parent table.
Superuser-only (enforced by the C implementation).';

REVOKE EXECUTE ON FUNCTION pg_database_logical_checksum(boolean, boolean) FROM PUBLIC;
