-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_checksums UPDATE TO '1.1.0'" to load this file. \quit

/*-------------------------------------------------------------------------
 * pg_checksums 1.0.0 -> 1.1.0
 *
 * Version 1.1.0 only changes the C implementation (performance of the logical
 * checksum paths and a relcache reference leak in the parallel database
 * scan). The SQL objects are unchanged, so the upgrade is a no-op.
 *-------------------------------------------------------------------------
 */
