-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_checksums UPDATE TO '1.3.1'" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums 1.3.0 -> 1.3.1
 *
 * 1.3.1 only changes the C implementation: logical index scans now use
 * parallel workers.  There are no SQL object changes.
 *-------------------------------------------------------------------------
 */
