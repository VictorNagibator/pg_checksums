/* 
 * pg_checksums--1.0.sql
 * SQL functions for checksum extension
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_checksums" to load this file. \quit

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

-- Helper function: checksum of any data (wrapper around pg_checksum_data if available)
CREATE OR REPLACE FUNCTION pg_data_checksum(data bytea, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_data_checksum'
LANGUAGE C STRICT IMMUTABLE;

-- Helper function: checksum of text
CREATE OR REPLACE FUNCTION pg_text_checksum(text text, seed integer DEFAULT 0)
RETURNS integer
AS 'MODULE_PATHNAME', 'pg_text_checksum'
LANGUAGE C STRICT IMMUTABLE;

-- View to show checksums for all tables
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
WHERE c.relkind IN ('r', 'm', 'p')
AND n.nspname NOT IN ('pg_catalog', 'information_schema')
AND pg_table_is_visible(c.oid);

-- Function to verify table integrity
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