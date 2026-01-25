-- Database-level checksum tests

-- Create a test schema for isolated testing
CREATE SCHEMA IF NOT EXISTS test_checksum_schema;

-- Create test tables
CREATE TABLE test_checksum_schema.table1 (
    id integer PRIMARY KEY,
    data text
);

CREATE TABLE test_checksum_schema.table2 (
    id integer PRIMARY KEY,
    value integer,
    description text
);

-- Insert deterministic test data
INSERT INTO test_checksum_schema.table1 (id, data)
SELECT gs, 'data_' || gs FROM generate_series(1, 50) gs;

INSERT INTO test_checksum_schema.table2 (id, value, description)
SELECT gs, gs * 10, 'desc_' || gs FROM generate_series(1, 100) gs;

-- Create index
CREATE INDEX idx_table2_value ON test_checksum_schema.table2 (value);

-- Test A: Table-level checksum aggregation matches expectation
WITH table1_checksum AS (
    SELECT BIT_XOR(pg_tuple_checksum('test_checksum_schema.table1'::regclass, ctid, false)) AS agg_checksum
    FROM test_checksum_schema.table1
)
SELECT 
    agg_checksum != 0 AS table_has_non_zero_aggregate_checksum
FROM table1_checksum;

-- Test B: Different tables have different aggregate checksums
WITH table_checksums AS (
    SELECT 
        'table1' as table_name,
        BIT_XOR(pg_tuple_checksum('test_checksum_schema.table1'::regclass, ctid, false)) AS checksum
    FROM test_checksum_schema.table1
    UNION ALL
    SELECT 
        'table2' as table_name,
        BIT_XOR(pg_tuple_checksum('test_checksum_schema.table2'::regclass, ctid, false)) AS checksum
    FROM test_checksum_schema.table2
)
SELECT 
    COUNT(DISTINCT checksum) = 2 AS different_tables_have_different_checksums
FROM table_checksums;

-- Clean up
DROP SCHEMA test_checksum_schema CASCADE;