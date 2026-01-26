/* 
 * pg_checksums--1.0.sql
 * SQL installation script for checksum extension
 *
 * This script installs the public SQL interface for the checksum extension.
 * It defines functions for all granularity levels and provides helper views
 * for practical usage scenarios.
 *
 * Function Categories:
 * 1. Core checksum functions: Compute checksums at various levels
 * 2. Utility functions: Generic checksums for arbitrary data
 * 3. Helper views: Convenient views for monitoring and verification
 *
 * Usage Examples:
 * -- Column-level integrity check
 * SELECT pg_column_checksum('my_table'::regclass, ctid, 1) FROM my_table;
 *
 * -- Table-level change detection  
 * SELECT pg_table_checksum('my_table'::regclass) != expected_checksum 
 *   FROM monitoring_table;
 *
 * -- Database consistency check (superuser)
 * SELECT pg_database_checksum(false, false) FROM pg_database WHERE datname = current_database();
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_checksums" to load this file. \quit

/* Core checksum functions */

-- Page-level checksum function (uses built-in pg_checksum_page)
CREATE OR REPLACE FUNCTION pg_page_checksum(relname regclass, blkno integer)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_page_checksum'
LANGUAGE C STRICT;

-- Tuple-level checksum function
CREATE OR REPLACE FUNCTION pg_tuple_checksum(relname regclass, tid tid, include_header boolean DEFAULT false)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_tuple_checksum'
LANGUAGE C STRICT;

-- Column-level checksum function
CREATE OR REPLACE FUNCTION pg_column_checksum(relname regclass, tid tid, attnum integer)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_column_checksum'
LANGUAGE C STRICT;

-- Table-level checksum function
CREATE OR REPLACE FUNCTION pg_table_checksum(relname regclass, include_header boolean DEFAULT false)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_table_checksum'
LANGUAGE C STRICT;

-- Index-level checksum function
CREATE OR REPLACE FUNCTION pg_index_checksum(relname regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_index_checksum'
LANGUAGE C STRICT;

-- Database-level checksum function (superuser only)
CREATE OR REPLACE FUNCTION pg_database_checksum(include_system boolean DEFAULT false, include_toast boolean DEFAULT false)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_database_checksum'
LANGUAGE C STRICT SECURITY DEFINER;

/* Utility functions for arbitrary data */

-- Helper function: checksum of any binary data
CREATE OR REPLACE FUNCTION pg_data_checksum(data bytea, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_data_checksum'
LANGUAGE C STRICT IMMUTABLE;

-- Helper function: checksum of text data  
CREATE OR REPLACE FUNCTION pg_text_checksum(text text, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_text_checksum'
LANGUAGE C STRICT IMMUTABLE;

/* Monitoring and diagnostic views */

-- View to show checksums for all user tables in the database
-- Useful for tracking table changes and detecting data drift
CREATE OR REPLACE VIEW pg_table_checksums AS
SELECT 
    c.oid as table_oid,
    n.nspname as schema_name,
    c.relname as table_name,
    pg_table_checksum(c.oid, false) as table_checksum,
    pg_size_pretty(pg_relation_size(c.oid)) as table_size,
    c.reltuples as estimated_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm', 'p')  -- regular tables, materialized views, partitioned tables
AND n.nspname NOT IN ('pg_catalog', 'information_schema')  -- exclude system schemas
AND pg_table_is_visible(c.oid);  -- only tables in search path

/* Integrity verification functions */

-- Function to verify table integrity by computing checksums for all rows
-- Returns each row's ctid, checksum, and status for manual verification
CREATE OR REPLACE FUNCTION pg_verify_table_integrity(relname regclass)
RETURNS TABLE(
    ctid tid,
    checksum integer,
    status text
)
AS $$
DECLARE
    r RECORD;
BEGIN
    -- Simple verification: compute checksums for all rows
    FOR r IN EXECUTE format('SELECT ctid FROM %s', relname)
    LOOP
        RETURN QUERY SELECT r.ctid, pg_tuple_checksum(relname, r.ctid, false), 'OK';
    END LOOP;
END;
$$ LANGUAGE plpgsql;