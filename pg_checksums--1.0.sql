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