# Copyright (c) 2026, PostgreSQL Global Development Group
#
# TAP tests for checksum functionality at all granularities
# Tests: tuple, column, table, index, and database checksums

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# Set up test node
my $node = PostgreSQL::Test::Cluster->new('checksum_tests');
$node->init;
$node->start;

# Create extension
$node->safe_psql('postgres', 'CREATE EXTENSION pg_checksums;');

# Helper function to validate checksums
sub checksum_is_valid {
    my ($checksum) = @_;
    return defined($checksum) && $checksum != 0xFFFFFFFF;
}

# Test 1: Basic tuple checksum functionality
sub test_tuple_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_tuple (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_tuple VALUES (1, 'test data'), (2, 'more data')");
    
    # Get checksums for all tuples
    my $result = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple ORDER BY id");
    
    my @checksums = split(/\n/, $result);
    
    is(scalar(@checksums), 2, 'checksum computed for both tuples');
    ok(checksum_is_valid($checksums[0]), 'first tuple checksum is valid');
    ok(checksum_is_valid($checksums[1]), 'second tuple checksum is valid');
    isnt($checksums[0], $checksums[1], 'different tuples have different checksums');
    
    # Test with and without header
    my $checksum_without = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple WHERE id = 1");
    my $checksum_with = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_tuple'::regclass, ctid, true) FROM test_tuple WHERE id = 1");
    
    isnt($checksum_without, $checksum_with, 'checksum with header differs from without header');
    
    $node->safe_psql('postgres', 'DROP TABLE test_tuple');
}

# Test 2: Column checksum functionality
sub test_column_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_column (id int, name text, value float, nullable int)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_column VALUES (1, 'Alice', 100.5, NULL), (2, 'Bob', 200.75, 42)");
    
    # Test non-NULL columns
    my $result1 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_column'::regclass, ctid, 1) FROM test_column WHERE id = 1");
    my $result2 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_column'::regclass, ctid, 2) FROM test_column WHERE id = 1");
    
    ok(checksum_is_valid($result1), 'non-NULL column 1 checksum valid');
    ok(checksum_is_valid($result2), 'non-NULL column 2 checksum valid');
    isnt($result1, $result2, 'different columns have different checksums');
    
    # Test NULL column returns -1
    my $null_checksum = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_column'::regclass, ctid, 4) FROM test_column WHERE id = 1");
    is($null_checksum, -1, 'NULL column returns CHECKSUM_NULL (-1)');
    
    # Test non-NULL column doesn't return -1
    my $non_null_checksum = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_column'::regclass, ctid, 4) FROM test_column WHERE id = 2");
    ok($non_null_checksum != -1, 'non-NULL column does not return CHECKSUM_NULL');
    
    $node->safe_psql('postgres', 'DROP TABLE test_column');
}

# Test 3: Table checksum functionality
sub test_table_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_table (id int, group_id int, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_table SELECT gs, gs % 3, 'data_' || gs FROM generate_series(1, 10) gs");
    
    # Get table checksums with and without header
    my $checksum_without = $node->safe_psql('postgres',
        "SELECT pg_table_checksum('test_table'::regclass, false)");
    my $checksum_with = $node->safe_psql('postgres',
        "SELECT pg_table_checksum('test_table'::regclass, true)");
    
    ok(checksum_is_valid($checksum_without), 'table checksum without header is valid');
    ok(checksum_is_valid($checksum_with), 'table checksum with header is valid');
    isnt($checksum_without, $checksum_with, 'table checksums with/without header differ');
    
    # Empty table should have zero checksum
    $node->safe_psql('postgres', 'CREATE TABLE empty_table (id int)');
    my $empty_checksum = $node->safe_psql('postgres',
        "SELECT pg_table_checksum('empty_table'::regclass, false)");
    is($empty_checksum, 0, 'empty table returns zero checksum');
    
    $node->safe_psql('postgres', 'DROP TABLE test_table, empty_table');
}

# Test 4: Index checksum functionality
sub test_index_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_index (id int PRIMARY KEY, key1 int, key2 text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_index SELECT gs, gs * 10, 'key_' || gs FROM generate_series(1, 5) gs");
    
    # Create various index types
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_btree ON test_index (key1)');
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_btree_multi ON test_index (key1, key2)');
    $node->safe_psql('postgres',
        'CREATE UNIQUE INDEX idx_test_unique ON test_index (key2)');
    
    # Get index checksums
    my $checksum_btree = $node->safe_psql('postgres',
        "SELECT pg_index_checksum('idx_test_btree'::regclass)");
    my $checksum_multi = $node->safe_psql('postgres',
        "SELECT pg_index_checksum('idx_test_btree_multi'::regclass)");
    my $checksum_unique = $node->safe_psql('postgres',
        "SELECT pg_index_checksum('idx_test_unique'::regclass)");
    
    ok(checksum_is_valid($checksum_btree), 'btree index checksum valid');
    ok(checksum_is_valid($checksum_multi), 'multi-column btree index checksum valid');
    ok(checksum_is_valid($checksum_unique), 'unique index checksum valid');
    
    # Different indexes should have different checksums
    isnt($checksum_btree, $checksum_multi, 'different indexes have different checksums');
    isnt($checksum_btree, $checksum_unique, 'btree and unique indexes have different checksums');
    
    # Index checksum should change when data changes
    $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 999 WHERE id = 1');
    my $new_checksum_btree = $node->safe_psql('postgres',
        "SELECT pg_index_checksum('idx_test_btree'::regclass)");
    $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 10 WHERE id = 1'); # Restore
    
    isnt($checksum_btree, $new_checksum_btree, 'index checksum changes after data modification');
    
    $node->safe_psql('postgres', 'DROP TABLE test_index CASCADE');
}

# Test 5: Database checksum functionality (requires superuser)
sub test_database_checksum_basic {
    my $node = shift;
    
    # Create test schema and tables
    $node->safe_psql('postgres',
        'CREATE SCHEMA test_checksum_schema');
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_checksum_schema.table1 (id int PRIMARY KEY, data text)');
    $node->safe_psql('postgres',
        'CREATE TABLE test_checksum_schema.table2 (id int PRIMARY KEY, value int, descr text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_checksum_schema.table1 SELECT gs, 'data_' || gs FROM generate_series(1, 10) gs");
    $node->safe_psql('postgres',
        "INSERT INTO test_checksum_schema.table2 SELECT gs, gs * 100, 'desc_' || gs FROM generate_series(1, 15) gs");
    
    # Create index
    $node->safe_psql('postgres',
        'CREATE INDEX idx_table2_value ON test_checksum_schema.table2 (value)');
    
    # Get database checksum excluding system tables
    my $db_checksum = $node->safe_psql('postgres',
        "SELECT pg_database_checksum(false, false)");
    
    ok(checksum_is_valid($db_checksum), 'database checksum is valid and non-zero');
    
    # Database checksum should change when data changes
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table1 SET data = 'modified' WHERE id = 1");
    my $new_db_checksum = $node->safe_psql('postgres',
        "SELECT pg_database_checksum(false, false)");
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table1 SET data = 'data_1' WHERE id = 1"); # Restore
    
    isnt($db_checksum, $new_db_checksum, 'database checksum changes after data modification');
    
    # Clean up
    $node->safe_psql('postgres', 'DROP SCHEMA test_checksum_schema CASCADE');
}

# Test 6: Checksum detects data corruption
sub test_checksum_detects_corruption {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_corrupt (id int PRIMARY KEY, original text, modified text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_corrupt VALUES (1, 'original data', 'original data')");
    
    # Get original checksums
    my $orig_tuple = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $orig_col1 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_corrupt'::regclass, ctid, 2) FROM test_corrupt");
    my $orig_col2 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_corrupt'::regclass, ctid, 3) FROM test_corrupt");
    
    # Simulate corruption by modifying data
    $node->safe_psql('postgres',
        "UPDATE test_corrupt SET modified = 'CORRUPTED DATA' WHERE id = 1");
    
    # Get new checksums
    my $new_tuple = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $new_col1 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_corrupt'::regclass, ctid, 2) FROM test_corrupt");
    my $new_col2 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_corrupt'::regclass, ctid, 3) FROM test_corrupt");
    
    # Tuple checksum should change
    isnt($orig_tuple, $new_tuple, 'tuple checksum detects data modification');
    
    # Column checksums: unchanged column should be same, modified column should differ
    is($orig_col1, $new_col1, 'unmodified column checksum remains same');
    isnt($orig_col2, $new_col2, 'modified column checksum changes');
    
    $node->safe_psql('postgres', 'DROP TABLE test_corrupt');
}

# Test 7: Identical data at different locations
sub test_identical_data_different_location {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_identical (id int, data text)');
    
    # Insert two identical rows
    $node->safe_psql('postgres',
        "INSERT INTO test_identical VALUES (1, 'identical data'), (2, 'identical data')");
    
    # Get checksums
    my $result = $node->safe_psql('postgres',
        "SELECT id, pg_tuple_checksum('test_identical'::regclass, ctid, false) FROM test_identical ORDER BY id");
    
    my @lines = split(/\n/, $result);
    my ($id1, $checksum1) = split(/\|/, $lines[0]);
    my ($id2, $checksum2) = split(/\|/, $lines[1]);
    
    isnt($checksum1, $checksum2, 'identical data at different ctid has different checksums');
    
    # Column checksums should be identical
    my $col_checksum1 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_identical'::regclass, ctid, 2) FROM test_identical WHERE id = 1");
    my $col_checksum2 = $node->safe_psql('postgres',
        "SELECT pg_column_checksum('test_identical'::regclass, ctid, 2) FROM test_identical WHERE id = 2");
    
    is($col_checksum1, $col_checksum2, 'identical column data has identical checksums');
    
    $node->safe_psql('postgres', 'DROP TABLE test_identical');
}

# Test 8: MVCC behavior with checksums
sub test_mvcc_checksum_behavior {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_mvcc (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_mvcc VALUES (1, 'initial data')");
    
    # Get initial checksum
    my $initial_checksum = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    # Update creates new row version
    $node->safe_psql('postgres',
        "UPDATE test_mvcc SET data = 'updated data' WHERE id = 1");
    
    # Get checksum after update
    my $updated_checksum = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    isnt($initial_checksum, $updated_checksum, 'checksum changes after UPDATE (new row version)');
    
    # Even if we restore original data, MVCC info differs
    $node->safe_psql('postgres',
        "UPDATE test_mvcc SET data = 'initial data' WHERE id = 1");
    
    my $restored_checksum = $node->safe_psql('postgres',
        "SELECT pg_tuple_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    isnt($initial_checksum, $restored_checksum, 'checksum differs even with same data due to MVCC');
    
    $node->safe_psql('postgres', 'DROP TABLE test_mvcc');
}

# Test 9: Various data types
sub test_various_data_types {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_types (
            id int,
            t_text text,
            t_int int,
            t_float float8,
            t_bool bool,
            t_timestamp timestamptz,
            t_array int[],
            t_json jsonb,
            t_bytea bytea
        )');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_types VALUES (
            1,
            'text value',
            123456,
            3.14159,
            true,
            '2024-01-01 12:00:00 UTC',
            ARRAY[1,2,3,4,5],
            '{\"key\": \"value\"}',
            E'\\\\xDEADBEEF'
        )");
    
    # Test checksums for each column type
    for my $col (1..9) {
        my $checksum = $node->safe_psql('postgres',
            "SELECT pg_column_checksum('test_types'::regclass, ctid, $col) FROM test_types");
        
        ok(checksum_is_valid($checksum), "checksum valid for column $col (various types)");
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_types');
}

# Run all tests
test_tuple_checksum_basic($node);
test_column_checksum_basic($node);
test_table_checksum_basic($node);
test_index_checksum_basic($node);
test_database_checksum_basic($node);
test_checksum_detects_corruption($node);
test_identical_data_different_location($node);
test_mvcc_checksum_behavior($node);
test_various_data_types($node);

# Clean up
$node->stop;

done_testing();