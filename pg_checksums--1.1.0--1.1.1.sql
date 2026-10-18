-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_checksums UPDATE TO '1.1.1'" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums 1.1.0 -> 1.1.1
 *
 * 1.1.1 is a bug-fix release.  The C implementation now validates relation
 * kinds, block numbers, line pointers and page headers; enforces SELECT
 * privileges and rejects relations protected by row-level security; runs
 * database scans in subtransactions so that cancellation and resource
 * cleanup work correctly; and enters parallel mode before creating a
 * parallel context.
 *
 * The only SQL-visible change is the security fix for the two
 * superuser-only database functions: they are no longer SECURITY DEFINER
 * (which made superuser() see the owner and let any user pass the check),
 * and EXECUTE is revoked from PUBLIC as defence in depth.
 *-------------------------------------------------------------------------
 */

ALTER FUNCTION pg_database_physical_checksum(boolean, boolean) SECURITY INVOKER;
ALTER FUNCTION pg_database_logical_checksum(boolean, boolean) SECURITY INVOKER;

REVOKE EXECUTE ON FUNCTION pg_database_physical_checksum(boolean, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_database_logical_checksum(boolean, boolean) FROM PUBLIC;

COMMENT ON FUNCTION pg_page_checksum(regclass, integer) IS
'Physical checksum of a database page. Uses PostgreSQL''s built-in page checksum algorithm.
 Returns 0 for new (uninitialized) pages. The block number is validated; an
 out-of-range block raises an error instead of extending the relation.';

COMMENT ON FUNCTION pg_index_logical_checksum(regclass) IS
'Logical checksum of an index. Depends only on index key values and heap TIDs.
 Stable across REINDEX with same data. B-tree only; returns NULL for Hash, GiST,
 GIN, SP-GiST and BRIN, whose pages do not contain ordinary index tuples.';

COMMENT ON FUNCTION pg_database_physical_checksum(boolean, boolean) IS
'Physical checksum of entire database. Aggregates physical checksums of all relations.
 Superuser-only (enforced by the C implementation). Optional filtering of system
 catalogs and TOAST tables. Relations protected by row-level security are rejected.
 Changes after any physical reorganization. 64-bit return value.';

COMMENT ON FUNCTION pg_database_logical_checksum(boolean, boolean) IS
'Logical checksum of entire database. Aggregates logical checksums of all relations.
 Superuser-only (enforced by the C implementation). Skips tables without primary keys
 and TOAST relations; toast content is covered through the parent table.
 64-bit return value.';
