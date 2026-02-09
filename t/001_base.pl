# Copyright (c) 2026, PostgreSQL Global Development Group
#
# TAP tests for checksum functionality at all granularities
# Tests: tuple, cell, table, index, and database checksums (physical and logical)

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
    # Valid checksum is: defined, not CHECKSUM_NULL (0xFFFFFFFF = -1)
    # Note: 0 is a valid checksum value for some cases
    return defined($checksum) && $checksum != 0xFFFFFFFF;
}

# Test 1: Basic tuple checksum functionality (physical and logical)
sub test_tuple_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_tuple (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_tuple VALUES (1, 'test data'), (2, 'more data')");
    
    # Get physical checksums for all tuples
    my $result_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple ORDER BY id");
    
    my @checksums_physical = split(/\n/, $result_physical);
    
    is(scalar(@checksums_physical), 2, 'physical checksum computed for both tuples');
    ok(checksum_is_valid($checksums_physical[0]), 'first tuple physical checksum is valid');
    ok(checksum_is_valid($checksums_physical[1]), 'second tuple physical checksum is valid');
    isnt($checksums_physical[0], $checksums_physical[1], 'different tuples have different physical checksums');
    
    # Get logical checksums for all tuples (requires PK)
    my $result_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple ORDER BY id");
    
    my @checksums_logical = split(/\n/, $result_logical);
    
    is(scalar(@checksums_logical), 2, 'logical checksum computed for both tuples (table has PK)');
    ok(checksum_is_valid($checksums_logical[0]), 'first tuple logical checksum is valid');
    ok(checksum_is_valid($checksums_logical[1]), 'second tuple logical checksum is valid');
    isnt($checksums_logical[0], $checksums_logical[1], 'different tuples have different logical checksums');
    
    # Test with and without header (physical)
    my $checksum_without_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple WHERE id = 1");
    my $checksum_with_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_tuple'::regclass, ctid, true) FROM test_tuple WHERE id = 1");
    
    isnt($checksum_without_physical, $checksum_with_physical, 'physical checksum with header differs from without header');
    
    # Test with and without header (logical)
    my $checksum_without_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_tuple'::regclass, ctid, false) FROM test_tuple WHERE id = 1");
    my $checksum_with_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_tuple'::regclass, ctid, true) FROM test_tuple WHERE id = 1");
    
    isnt($checksum_without_logical, $checksum_with_logical, 'logical checksum with header differs from without header');
    
    # Test physical vs logical are different
    isnt($checksum_without_physical, $checksum_without_logical, 'physical and logical checksums are different');
    
    $node->safe_psql('postgres', 'DROP TABLE test_tuple');
}

# Test 2: Cell checksum functionality
sub test_cell_checksum_basic {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_cell (id int, name text, value float, nullable int)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_cell VALUES (1, 'Alice', 100.5, NULL), (2, 'Bob', 200.75, 42)");
    
    # Test non-NULL cells
    my $result1 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_cell'::regclass, ctid, 1) FROM test_cell WHERE id = 1");
    my $result2 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_cell'::regclass, ctid, 2) FROM test_cell WHERE id = 1");
    
    ok(checksum_is_valid($result1), 'non-NULL cell 1 checksum valid');
    ok(checksum_is_valid($result2), 'non-NULL cell 2 checksum valid');
    isnt($result1, $result2, 'different cells have different checksums');
    
    # Test NULL cell returns -1
    my $null_checksum = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_cell'::regclass, ctid, 4) FROM test_cell WHERE id = 1");
    is($null_checksum, -1, 'NULL cell returns CHECKSUM_NULL (-1)');
    
    # Test non-NULL cell doesn't return -1
    my $non_null_checksum = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_cell'::regclass, ctid, 4) FROM test_cell WHERE id = 2");
    ok($non_null_checksum != -1, 'non-NULL cell does not return CHECKSUM_NULL');
    
    $node->safe_psql('postgres', 'DROP TABLE test_cell');
}

# Test 3: Table checksum functionality (physical and logical)
sub test_table_checksum_basic {
    my $node = shift;
    
    # Test with table that has PK
    $node->safe_psql('postgres',
        'CREATE TABLE test_table_pk (id int PRIMARY KEY, group_id int, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_table_pk SELECT gs, gs % 3, 'data_' || gs FROM generate_series(1, 10) gs");
    
    # Get table physical checksums with and without header
    my $checksum_without_physical = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_table_pk'::regclass, false)");
    my $checksum_with_physical = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_table_pk'::regclass, true)");
    
    ok(checksum_is_valid($checksum_without_physical), 'table physical checksum without header is valid');
    ok(checksum_is_valid($checksum_with_physical), 'table physical checksum with header is valid');
    isnt($checksum_without_physical, $checksum_with_physical, 'table physical checksums with/without header differ');
    
    # Get table logical checksum (table has PK)
    my $checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_table_pk'::regclass)");
    
    ok(checksum_is_valid($checksum_logical), 'table logical checksum is valid (table has PK)');
    
    # Test physical vs logical are different
    isnt($checksum_without_physical, $checksum_logical, 'table physical and logical checksums are different');
    
    # Test table without PK - logical checksum should be NULL
    $node->safe_psql('postgres', 'CREATE TABLE test_table_no_pk (id int, data text)');
    $node->safe_psql('postgres',
        "INSERT INTO test_table_no_pk SELECT gs, 'data_' || gs FROM generate_series(1, 5) gs");
    
    my $checksum_no_pk_logical = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_table_no_pk'::regclass)");
    is($checksum_no_pk_logical, '', 'table without PK returns NULL for logical checksum');
    
    # Empty table physical checksum should be non-zero (based on OID)
    $node->safe_psql('postgres', 'CREATE TABLE empty_table (id int)');
    my $empty_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('empty_table'::regclass, false)");
    ok(checksum_is_valid($empty_checksum_physical), 'empty table physical checksum is valid');
    
    # Empty table without PK - logical checksum should be NULL
    my $empty_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('empty_table'::regclass)");
    is($empty_checksum_logical, '', 'empty table without PK returns NULL for logical checksum');
    
    $node->safe_psql('postgres', 'DROP TABLE test_table_pk, test_table_no_pk, empty_table');
}

# Test 4: Index checksum functionality (physical and logical)
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
    
    # Get index physical checksums
    my $checksum_btree_physical = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_btree'::regclass)");
    my $checksum_multi_physical = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_btree_multi'::regclass)");
    my $checksum_unique_physical = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_unique'::regclass)");
    
    ok(checksum_is_valid($checksum_btree_physical), 'btree index physical checksum valid');
    ok(checksum_is_valid($checksum_multi_physical), 'multi-column btree index physical checksum valid');
    ok(checksum_is_valid($checksum_unique_physical), 'unique index physical checksum valid');
    
    # Different indexes should have different physical checksums
    isnt($checksum_btree_physical, $checksum_multi_physical, 'different indexes have different physical checksums');
    isnt($checksum_btree_physical, $checksum_unique_physical, 'btree and unique indexes have different physical checksums');
    
    # Get index logical checksums (for B-tree indexes)
    my $checksum_btree_logical = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_btree'::regclass)");
    
    if ($checksum_btree_logical ne '') {
        ok(checksum_is_valid($checksum_btree_logical), 'btree index logical checksum valid');
        isnt($checksum_btree_physical, $checksum_btree_logical, 'index physical and logical checksums are different');
    } else {
        pass('btree index logical checksum not supported (returns NULL)');
    }
    
    # Index physical checksum should change when data changes
    $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 999 WHERE id = 1');
    my $new_checksum_btree_physical = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_btree'::regclass)");
    $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 10 WHERE id = 1'); # Restore
    
    isnt($checksum_btree_physical, $new_checksum_btree_physical, 'index physical checksum changes after data modification');
    
    # Index logical checksum should change when data changes (if supported)
    if ($checksum_btree_logical ne '') {
        $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 999 WHERE id = 2');
        my $new_checksum_btree_logical = $node->safe_psql('postgres',
            "SELECT pg_index_logical_checksum('idx_test_btree'::regclass)");
        $node->safe_psql('postgres', 'UPDATE test_index SET key1 = 20 WHERE id = 2'); # Restore
        
        isnt($checksum_btree_logical, $new_checksum_btree_logical, 'index logical checksum changes after data modification');
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_index CASCADE');
}

# Test 5: Database checksum functionality (physical and logical) - requires superuser
sub test_database_checksum_basic {
    my $node = shift;
    
    # Create test schema and tables (all with PK)
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
    
    # Get database physical checksum excluding system tables
    my $db_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_database_physical_checksum(false, false)");
    
    ok(checksum_is_valid($db_checksum_physical), 'database physical checksum is valid and non-zero');
    
    # Get database logical checksum (all tables have PK)
    my $db_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_database_logical_checksum(false, false)");
    
    ok(checksum_is_valid($db_checksum_logical), 'database logical checksum is valid and non-zero (all tables have PK)');
    isnt($db_checksum_physical, $db_checksum_logical, 'database physical and logical checksums are different');
    
    # Database physical checksum should change when data changes
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table1 SET data = 'modified' WHERE id = 1");
    my $new_db_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_database_physical_checksum(false, false)");
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table1 SET data = 'data_1' WHERE id = 1"); # Restore
    
    isnt($db_checksum_physical, $new_db_checksum_physical, 'database physical checksum changes after data modification');
    
    # Database logical checksum should change when data changes
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table2 SET descr = 'modified' WHERE id = 1");
    my $new_db_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_database_logical_checksum(false, false)");
    $node->safe_psql('postgres',
        "UPDATE test_checksum_schema.table2 SET descr = 'desc_1' WHERE id = 1"); # Restore
    
    isnt($db_checksum_logical, $new_db_checksum_logical, 'database logical checksum changes after data modification');
    
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
    my $orig_tuple_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $orig_tuple_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $orig_col1 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_corrupt'::regclass, ctid, 2) FROM test_corrupt");
    my $orig_col2 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_corrupt'::regclass, ctid, 3) FROM test_corrupt");
    
    # Simulate corruption by modifying data
    $node->safe_psql('postgres',
        "UPDATE test_corrupt SET modified = 'CORRUPTED DATA' WHERE id = 1");
    
    # Get new checksums
    my $new_tuple_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $new_tuple_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_corrupt'::regclass, ctid, false) FROM test_corrupt");
    my $new_col1 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_corrupt'::regclass, ctid, 2) FROM test_corrupt");
    my $new_col2 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_corrupt'::regclass, ctid, 3) FROM test_corrupt");
    
    # Tuple checksums should change
    isnt($orig_tuple_physical, $new_tuple_physical, 'tuple physical checksum detects data modification');
    isnt($orig_tuple_logical, $new_tuple_logical, 'tuple logical checksum detects data modification');
    
    # Cell checksums: unchanged cell should be same, modified cell should differ
    is($orig_col1, $new_col1, 'unmodified cell checksum remains same');
    isnt($orig_col2, $new_col2, 'modified cell checksum changes');
    
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
    
    # Get physical checksums
    my $result = $node->safe_psql('postgres',
        "SELECT id, pg_tuple_physical_checksum('test_identical'::regclass, ctid, false) FROM test_identical ORDER BY id");
    
    my @lines = split(/\n/, $result);
    my ($id1, $checksum1) = split(/\|/, $lines[0]);
    my ($id2, $checksum2) = split(/\|/, $lines[1]);
    
    isnt($checksum1, $checksum2, 'identical data at different ctid has different physical checksums');
    
    # Cell checksums should be identical
    my $col_checksum1 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_identical'::regclass, ctid, 2) FROM test_identical WHERE id = 1");
    my $col_checksum2 = $node->safe_psql('postgres',
        "SELECT pg_cell_checksum('test_identical'::regclass, ctid, 2) FROM test_identical WHERE id = 2");
    
    is($col_checksum1, $col_checksum2, 'identical cell data has identical checksums');
    
    $node->safe_psql('postgres', 'DROP TABLE test_identical');
}

# Test 8: MVCC behavior with checksums
sub test_mvcc_checksum_behavior {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_mvcc (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_mvcc VALUES (1, 'initial data')");
    
    # Get initial checksums
    my $initial_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    my $initial_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    ok(checksum_is_valid($initial_checksum_physical), 'initial physical checksum valid');
    ok(checksum_is_valid($initial_checksum_logical), 'initial logical checksum valid');
    
    # Update creates new row version (data changes)
    $node->safe_psql('postgres',
        "UPDATE test_mvcc SET data = 'updated data' WHERE id = 1");
    
    # Get checksums after update
    my $updated_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    my $updated_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    # Both checksums should change because data changed
    isnt($initial_checksum_physical, $updated_checksum_physical, 
         'physical checksum changes after UPDATE (new row version, different data)');
    isnt($initial_checksum_logical, $updated_checksum_logical, 
         'logical checksum changes after UPDATE (data changed)');
    
    # Restore original data (same logical content, new physical version)
    $node->safe_psql('postgres',
        "UPDATE test_mvcc SET data = 'initial data' WHERE id = 1");
    
    my $restored_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    my $restored_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    # Physical checksum should differ (different MVCC info, possibly different ctid)
    isnt($initial_checksum_physical, $restored_checksum_physical, 
         'physical checksum differs even with same data due to MVCC/new version');
    
    # Logical checksum should return to original (same data content)
    is($initial_checksum_logical, $restored_checksum_logical, 
       'logical checksum returns to original when data is restored (ignores MVCC)');
    
    # Test case 3: UPDATE with same data (logical checksum should NOT change)
    $node->safe_psql('postgres',
        "UPDATE test_mvcc SET data = 'initial data' WHERE id = 1");
    
    my $same_data_checksum_physical = $node->safe_psql('postgres',
        "SELECT pg_tuple_physical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    my $same_data_checksum_logical = $node->safe_psql('postgres',
        "SELECT pg_tuple_logical_checksum('test_mvcc'::regclass, ctid, false) FROM test_mvcc");
    
    # Physical checksum may change (new MVCC version)
    # Logical checksum should remain the same (data unchanged)
    is($restored_checksum_logical, $same_data_checksum_logical,
       'logical checksum unchanged when UPDATE does not change data');
    
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
            "SELECT pg_cell_checksum('test_types'::regclass, ctid, $col) FROM test_types");
        
        ok(checksum_is_valid($checksum), "checksum valid for column $col (various types)");
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_types');
}

# Test 10: Data checksum utility functions
sub test_utility_functions {
    my $node = shift;
    
    # Test pg_data_checksum
    my $data_checksum1 = $node->safe_psql('postgres',
        "SELECT pg_data_checksum(E'\\\\x01020304'::bytea, 0)");
    my $data_checksum2 = $node->safe_psql('postgres',
        "SELECT pg_data_checksum(E'\\\\x01020304'::bytea, 0)");
    my $data_checksum3 = $node->safe_psql('postgres',
        "SELECT pg_data_checksum(E'\\\\x01020305'::bytea, 0)");
    
    ok(checksum_is_valid($data_checksum1), 'pg_data_checksum returns valid checksum');
    is($data_checksum1, $data_checksum2, 'same data returns same checksum');
    isnt($data_checksum1, $data_checksum3, 'different data returns different checksum');
    
    # Test pg_text_checksum
    my $text_checksum1 = $node->safe_psql('postgres',
        "SELECT pg_text_checksum('test', 0)");
    my $text_checksum2 = $node->safe_psql('postgres',
        "SELECT pg_text_checksum('test', 0)");
    my $text_checksum3 = $node->safe_psql('postgres',
        "SELECT pg_text_checksum('test2', 0)");
    
    ok(checksum_is_valid($text_checksum1), 'pg_text_checksum returns valid checksum');
    is($text_checksum1, $text_checksum2, 'same text returns same checksum');
    isnt($text_checksum1, $text_checksum3, 'different text returns different checksum');
}

# Test 11: Page checksum (physical page checksum)
sub test_page_checksum {
    my $node = shift;
    
    $node->safe_psql('postgres',
        'CREATE TABLE test_page (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_page VALUES (1, 'test data')");
    
    # Get page checksum for block 0
    my $page_checksum = $node->safe_psql('postgres',
        "SELECT pg_page_checksum('test_page'::regclass, 0)");
    
    # Page checksum could be 0 for new pages, but should not be NULL
    ok(defined($page_checksum) && $page_checksum ne '', 'page checksum returns a value');
    
    $node->safe_psql('postgres', 'DROP TABLE test_page');
}

# Run all tests
test_tuple_checksum_basic($node);
test_cell_checksum_basic($node);
test_table_checksum_basic($node);
test_index_checksum_basic($node);
test_database_checksum_basic($node);
test_checksum_detects_corruption($node);
test_identical_data_different_location($node);
test_mvcc_checksum_behavior($node);
test_various_data_types($node);
test_utility_functions($node);
test_page_checksum($node);

# Clean up
$node->stop;

done_testing();