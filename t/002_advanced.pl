# Copyright (c) 2026, PostgreSQL Global Development Group
# 
# This test verifies that logical checksums remain stable while physical
# checksums change after VACUUM, CLUSTER, and REINDEX operations.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

# Set up test node
my $node = PostgreSQL::Test::Cluster->new('checksum_stability');
$node->init;
$node->start;

# Create extension
$node->safe_psql('postgres', 'CREATE EXTENSION pg_checksums;');

# Helper function to validate checksums
sub checksum_is_valid {
    my ($checksum) = @_;
    # Valid checksum is: defined, not CHECKSUM_NULL (0xFFFFFFFF), not zero
    return defined($checksum) && $checksum != 0xFFFFFFFF && $checksum != 0;
}

#----------------------------------------------------------------------------
# Test 1: Logical checksum stability after VACUUM and VACUUM FULL
#----------------------------------------------------------------------------
sub test_vacuum_stability {
    my $node = shift;
    
    note("Testing checksum stability after VACUUM and VACUUM FULL");
    
    # Create test table with primary key
    $node->safe_psql('postgres',
        'CREATE TABLE test_vacuum_stability (id int PRIMARY KEY, data text, value int)');
    
    # Insert deterministic test data
    $node->safe_psql('postgres',
        "INSERT INTO test_vacuum_stability SELECT gs, 'data_' || gs, gs * 100 FROM generate_series(1, 1000) gs");
    
    # Create an index to test index checksums
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_vacuum ON test_vacuum_stability (value)');
    
    # Capture original checksums before any operations
    my $orig_tuples = $node->safe_psql('postgres',
        "SELECT id, ctid,
                pg_tuple_physical_checksum('test_vacuum_stability'::regclass, ctid, false) as phys,
                pg_tuple_logical_checksum('test_vacuum_stability'::regclass, ctid, false) as logic
         FROM test_vacuum_stability WHERE id IN (1, 500, 1000) ORDER BY id");
    
    my $orig_table_phys = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_vacuum_stability'::regclass, false)");
    my $orig_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_vacuum_stability'::regclass)");
    
    my $orig_idx_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_vacuum'::regclass)");
    my $orig_idx_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_vacuum'::regclass)");
    
    #-------------------------------------------------------------
    # Regular VACUUM test
    # Regular VACUUM removes dead tuples but doesn't move live tuples
    # Logical checksums should remain unchanged
    # Physical checksums may or may not change (depending on MVCC cleanup)
    #-------------------------------------------------------------
    note("Performing VACUUM...");
    $node->safe_psql('postgres', 'VACUUM test_vacuum_stability');
    
    # Get checksums after VACUUM
    my $after_vacuum_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_vacuum_stability'::regclass)");
    
    cmp_ok($orig_table_logic, '==', $after_vacuum_table_logic,
           'Table logical checksum unchanged after VACUUM');
    
    #-------------------------------------------------------------
    # VACUUM FULL test
    # VACUUM FULL moves tuples to new physical locations
    # Logical checksums must remain unchanged (same data)
    # Physical checksums must change (different ctids or metadata)
    #-------------------------------------------------------------
    note("Performing VACUUM FULL...");
    $node->safe_psql('postgres', 'VACUUM FULL test_vacuum_stability');
    
    # Get checksums after VACUUM FULL
    my $after_vacuum_full_tuples = $node->safe_psql('postgres',
        "SELECT id, ctid,
                pg_tuple_physical_checksum('test_vacuum_stability'::regclass, ctid, false) as phys,
                pg_tuple_logical_checksum('test_vacuum_stability'::regclass, ctid, false) as logic
         FROM test_vacuum_stability WHERE id IN (1, 500, 1000) ORDER BY id");
    
    my $after_vacuum_full_table_phys = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_vacuum_stability'::regclass, false)");
    my $after_vacuum_full_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_vacuum_stability'::regclass)");
    
    my $after_vacuum_full_idx_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_vacuum'::regclass)");
    my $after_vacuum_full_idx_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_vacuum'::regclass)");
    
    # Verify logical checksum stability
    cmp_ok($orig_table_logic, '==', $after_vacuum_full_table_logic,
           'Table logical checksum unchanged after VACUUM FULL');
    
    # Verify physical checksum change
    cmp_ok($orig_table_phys, '!=', $after_vacuum_full_table_phys,
           'Table physical checksum changed after VACUUM FULL');
    
    if ($orig_idx_logic ne '' && $after_vacuum_full_idx_logic ne '') {
        cmp_ok($orig_idx_logic, '==', $after_vacuum_full_idx_logic,
               'Index logical checksum unchanged after VACUUM FULL');
        cmp_ok($orig_idx_phys, '!=', $after_vacuum_full_idx_phys,
               'Index physical checksum changed after VACUUM FULL');
    }
    
    # Verify tuple-level changes
    my @orig_tuples_lines = split(/\n/, $orig_tuples);
    my @after_vacuum_full_tuples_lines = split(/\n/, $after_vacuum_full_tuples);
    
    for my $i (0..$#orig_tuples_lines) {
        my ($orig_id, $orig_ctid, $orig_phys, $orig_logic) = split(/\|/, $orig_tuples_lines[$i]);
        my ($after_id, $after_ctid, $after_phys, $after_logic) = split(/\|/, $after_vacuum_full_tuples_lines[$i]);
        
        is($orig_id, $after_id, "Row ID $orig_id matches after VACUUM FULL");
        
        # Logical checksum must be identical
        cmp_ok($orig_logic, '==', $after_logic,
               "Row $orig_id logical checksum unchanged after VACUUM FULL");
        
        # Physical checksum must change (tuple moved or metadata changed)
        cmp_ok($orig_phys, '!=', $after_phys,
               "Row $orig_id physical checksum changed after VACUUM FULL");
        
        # ctid MAY change (physical location changed) - but not required
        # Note: ctid might not change for all rows after VACUUM FULL
        # We only log if it changed, but don't fail the test if it didn't
        if ($orig_ctid ne $after_ctid) {
            note("Row $orig_id ctid changed from $orig_ctid to $after_ctid after VACUUM FULL");
        } else {
            note("Row $orig_id ctid unchanged after VACUUM FULL (but physical checksum changed due to metadata)");
        }
    }
    
    # Clean up
    $node->safe_psql('postgres', 'DROP TABLE test_vacuum_stability CASCADE');
}

#----------------------------------------------------------------------------
# Test 2: Logical checksum stability after CLUSTER
# CLUSTER physically reorders rows based on index
# Logical checksums must remain stable
#----------------------------------------------------------------------------
sub test_cluster_stability {
    my $node = shift;
    
    note("Testing checksum stability after CLUSTER");
    
    # Create table with data inserted in reverse order
    $node->safe_psql('postgres',
        'CREATE TABLE test_cluster_stability (id int PRIMARY KEY, data text)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_cluster_stability SELECT gs, 'data_' || (1000 - gs) FROM generate_series(1, 1000) gs");
    
    # Create index for clustering
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_cluster ON test_cluster_stability (id)');
    
    # Get original checksums
    my $orig_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_cluster_stability'::regclass)");
    my $orig_table_phys = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_cluster_stability'::regclass, false)");
    
    # Capture sample rows before CLUSTER
    my @orig_rows = ();
    for my $id (1, 100, 500, 1000) {
        my $row = $node->safe_psql('postgres',
            "SELECT ctid,
                    pg_tuple_physical_checksum('test_cluster_stability'::regclass, ctid, false) as phys,
                    pg_tuple_logical_checksum('test_cluster_stability'::regclass, ctid, false) as logic
             FROM test_cluster_stability WHERE id = $id");
        push @orig_rows, { id => $id, row => $row };
    }
    
    # Perform CLUSTER (physically reorders rows)
    note("Performing CLUSTER...");
    $node->safe_psql('postgres', 'CLUSTER test_cluster_stability USING idx_test_cluster');
    
    # Get checksums after CLUSTER
    my $after_cluster_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_cluster_stability'::regclass)");
    my $after_cluster_table_phys = $node->safe_psql('postgres',
        "SELECT pg_table_physical_checksum('test_cluster_stability'::regclass, false)");
    
    # Verify table-level stability
    cmp_ok($orig_table_logic, '==', $after_cluster_table_logic,
           'Table logical checksum unchanged after CLUSTER');
    
    cmp_ok($orig_table_phys, '!=', $after_cluster_table_phys,
           'Table physical checksum changed after CLUSTER');
    
    # Verify row-level stability
    for my $orig_row (@orig_rows) {
        my $id = $orig_row->{id};
        my ($orig_ctid, $orig_phys, $orig_logic) = split(/\|/, $orig_row->{row});
        
        my $after_row = $node->safe_psql('postgres',
            "SELECT ctid,
                    pg_tuple_physical_checksum('test_cluster_stability'::regclass, ctid, false) as phys,
                    pg_tuple_logical_checksum('test_cluster_stability'::regclass, ctid, false) as logic
             FROM test_cluster_stability WHERE id = $id");
        
        my ($after_ctid, $after_phys, $after_logic) = split(/\|/, $after_row);
        
        # Logical checksum must be identical
        cmp_ok($orig_logic, '==', $after_logic,
               "Row $id logical checksum unchanged after CLUSTER");
        
        # Physical checksum must change (row reordered or metadata changed)
        cmp_ok($orig_phys, '!=', $after_phys,
               "Row $id physical checksum changed after CLUSTER");
        
        # ctid MAY change (physical location changed) - but not required
        # Note: after CLUSTER, ctid might not change for all rows
        if ($orig_ctid ne $after_ctid) {
            note("Row $id ctid changed from $orig_ctid to $after_ctid after CLUSTER");
        } else {
            note("Row $id ctid unchanged after CLUSTER (but physical checksum changed due to metadata)");
        }
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_cluster_stability CASCADE');
}

#----------------------------------------------------------------------------
# Test 3: Logical checksum stability after REINDEX
# REINDEX rebuilds index structure but preserves logical content
# Index logical checksums must remain stable
#----------------------------------------------------------------------------
sub test_reindex_stability {
    my $node = shift;
    
    note("Testing checksum stability after REINDEX");
    
    # Create test table
    $node->safe_psql('postgres',
        'CREATE TABLE test_reindex_stability (id int PRIMARY KEY, data text, value int)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_reindex_stability SELECT gs, 'data_' || gs, gs * 10 FROM generate_series(1, 500) gs");
    
    # Create multiple index types
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_reindex_btree ON test_reindex_stability (value)');
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_reindex_multi ON test_reindex_stability (value, data)');
    $node->safe_psql('postgres',
        'CREATE UNIQUE INDEX idx_test_reindex_unique ON test_reindex_stability (data)');
    
    # Get original index checksums
    my $orig_idx_btree_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_btree'::regclass)");
    my $orig_idx_btree_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_reindex_btree'::regclass)");
    
    my $orig_idx_multi_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_multi'::regclass)");
    my $orig_idx_multi_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_reindex_multi'::regclass)");
    
    # Get table logical checksum for baseline
    my $orig_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_reindex_stability'::regclass)");
    
    #-----------------------------------------------------------------
    # Test REINDEX on single index
    #-----------------------------------------------------------------
    note("Performing REINDEX on single index...");
    $node->safe_psql('postgres', 'REINDEX INDEX idx_test_reindex_btree');
    
    # Get checksums after REINDEX
    my $after_reidx_btree_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_btree'::regclass)");
    my $after_reidx_btree_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_reindex_btree'::regclass)");
    
    # Physical checksum must change (index rebuilt)
    if ($orig_idx_btree_phys ne '' && $after_reidx_btree_phys ne '') {
        cmp_ok($orig_idx_btree_phys, '!=', $after_reidx_btree_phys,
               'B-tree index physical checksum changed after REINDEX');
    }
    
    # Logical checksum must remain same (same index content)
    if ($orig_idx_btree_logic ne '' && $after_reidx_btree_logic ne '') {
        cmp_ok($orig_idx_btree_logic, '==', $after_reidx_btree_logic,
               'B-tree index logical checksum unchanged after REINDEX');
    }
    
    # Verify other indexes unchanged (not reindexed)
    my $after_reidx_multi_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_multi'::regclass)");
    
    if ($orig_idx_multi_phys ne '' && $after_reidx_multi_phys ne '') {
        cmp_ok($orig_idx_multi_phys, '==', $after_reidx_multi_phys,
               'Multi-column index physical checksum unchanged (not reindexed)');
    }
    
    # Table logical checksum must remain same
    my $after_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_reindex_stability'::regclass)");
    
    cmp_ok($orig_table_logic, '==', $after_table_logic,
           'Table logical checksum unchanged after REINDEX');
    
    #-----------------------------------------------------------------
    # Test REINDEX TABLE (all indexes)
    #-----------------------------------------------------------------
    note("Performing REINDEX TABLE...");
    $node->safe_psql('postgres', 'REINDEX TABLE test_reindex_stability');
    
    # Get checksums after REINDEX TABLE
    my $after_table_reidx_btree_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_btree'::regclass)");
    my $after_table_reidx_multi_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_multi'::regclass)");
    my $after_table_reidx_unique_phys = $node->safe_psql('postgres',
        "SELECT pg_index_physical_checksum('idx_test_reindex_unique'::regclass)");
    
    # All physical checksums must change
    if ($orig_idx_btree_phys ne '' && $after_table_reidx_btree_phys ne '') {
        cmp_ok($orig_idx_btree_phys, '!=', $after_table_reidx_btree_phys,
               'B-tree index physical checksum changed after REINDEX TABLE');
    }
    
    if ($orig_idx_multi_phys ne '' && $after_table_reidx_multi_phys ne '') {
        cmp_ok($orig_idx_multi_phys, '!=', $after_table_reidx_multi_phys,
               'Multi-column index physical checksum changed after REINDEX TABLE');
    }
    
    # Logical checksums must remain same
    my $after_table_reidx_btree_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_reindex_btree'::regclass)");
    
    if ($orig_idx_btree_logic ne '' && $after_table_reidx_btree_logic ne '') {
        cmp_ok($orig_idx_btree_logic, '==', $after_table_reidx_btree_logic,
               'B-tree index logical checksum unchanged after REINDEX TABLE');
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_reindex_stability CASCADE');
}

#----------------------------------------------------------------------------
# Test 4: Comprehensive stability across multiple operations
# Tests that logical checksums only change when data changes,
# not when physical organization changes
#----------------------------------------------------------------------------
sub test_comprehensive_stability {
    my $node = shift;
    
    note("Testing comprehensive checksum stability across multiple operations");
    
    # Create test table
    $node->safe_psql('postgres',
        'CREATE TABLE test_comprehensive (id int PRIMARY KEY, data text, category int)');
    
    $node->safe_psql('postgres',
        "INSERT INTO test_comprehensive SELECT gs, 'data_' || gs, gs % 10 FROM generate_series(1, 2000) gs");
    
    $node->safe_psql('postgres',
        'CREATE INDEX idx_test_comprehensive ON test_comprehensive (category)');
    
    # Step 1: Get baseline checksums
    my $orig_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_comprehensive'::regclass)");
    my $orig_idx_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_comprehensive'::regclass)");
    
    # Store checksums of specific rows
    my %orig_row_checksums = ();
    for my $id (1, 100, 500, 1500, 2000) {
        my $row = $node->safe_psql('postgres',
            "SELECT ctid,
                    pg_tuple_physical_checksum('test_comprehensive'::regclass, ctid, false) as phys,
                    pg_tuple_logical_checksum('test_comprehensive'::regclass, ctid, false) as logic
             FROM test_comprehensive WHERE id = $id");
        
        $orig_row_checksums{$id} = $row;
    }
    
    #-----------------------------------------------------------------
    # Step 2: UPDATE data (logical checksums MUST change)
    #-----------------------------------------------------------------
    note("Step 1: Performing UPDATE...");
    $node->safe_psql('postgres',
        "UPDATE test_comprehensive SET data = 'updated_' || id WHERE id <= 100");
    
    my $after_update_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_comprehensive'::regclass)");
    
    cmp_ok($orig_table_logic, '!=', $after_update_table_logic,
           'Table logical checksum changed after UPDATE');
    
    #-----------------------------------------------------------------
    # Step 3: VACUUM FULL (logical checksums MUST remain same)
    #-----------------------------------------------------------------
    note("Step 2: Performing VACUUM FULL...");
    $node->safe_psql('postgres', 'VACUUM FULL test_comprehensive');
    
    my $after_vacuum_full_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_comprehensive'::regclass)");
    
    cmp_ok($after_update_table_logic, '==', $after_vacuum_full_table_logic,
           'Table logical checksum unchanged after VACUUM FULL (after UPDATE)');
    
    #-----------------------------------------------------------------
    # Step 4: DELETE and INSERT (logical checksums MUST change)
    #-----------------------------------------------------------------
    note("Step 3: Performing DELETE and INSERT...");
    $node->safe_psql('postgres', 'DELETE FROM test_comprehensive WHERE id BETWEEN 1500 AND 1600');
    $node->safe_psql('postgres',
        "INSERT INTO test_comprehensive SELECT gs, 'new_data_' || gs, gs % 10 FROM generate_series(1500, 1600) gs");
    
    my $after_delete_insert_table_logic = $node->safe_psql('postgres',
        "SELECT pg_table_logical_checksum('test_comprehensive'::regclass)");
    
    cmp_ok($after_vacuum_full_table_logic, '!=', $after_delete_insert_table_logic,
           'Table logical checksum changed after DELETE and INSERT');
    
    #-----------------------------------------------------------------
    # Step 5: REINDEX (index logical checksums MUST remain same)
    #-----------------------------------------------------------------
    note("Step 4: Performing REINDEX...");
    $node->safe_psql('postgres', 'REINDEX INDEX idx_test_comprehensive');
    
    my $after_reindex_idx_logic = $node->safe_psql('postgres',
        "SELECT pg_index_logical_checksum('idx_test_comprehensive'::regclass)");
    
    if ($orig_idx_logic ne '' && $after_reindex_idx_logic ne '') {
        # Index logical checksum changed because data changed, not because of REINDEX
        cmp_ok($orig_idx_logic, '!=', $after_reindex_idx_logic,
               'Index logical checksum changed (because data changed, not because of REINDEX)');
    }
    
    #-----------------------------------------------------------------
    # Verify row-level stability for unchanged rows
    #-----------------------------------------------------------------
    for my $id (100, 500) {
        my $orig_row = $orig_row_checksums{$id};
        my ($orig_ctid, $orig_phys, $orig_logic) = split(/\|/, $orig_row);
        
        my $current_row = $node->safe_psql('postgres',
            "SELECT ctid,
                    pg_tuple_physical_checksum('test_comprehensive'::regclass, ctid, false) as phys,
                    pg_tuple_logical_checksum('test_comprehensive'::regclass, ctid, false) as logic
             FROM test_comprehensive WHERE id = $id");
        
        if ($current_row ne '') {
            my ($current_ctid, $current_phys, $current_logic) = split(/\|/, $current_row);
            
            # Row 100: data was updated, logical checksum must change
            if ($id == 100) {
                cmp_ok($orig_logic, '!=', $current_logic,
                       "Row $id logical checksum changed after UPDATE");
            }
            # Row 500: data not modified, logical checksum must remain same
            elsif ($id == 500) {
                cmp_ok($orig_logic, '==', $current_logic,
                       "Row $id logical checksum unchanged (data not modified)");
            }
            
            # Physical checksums must differ (VACUUM FULL was performed)
            cmp_ok($orig_phys, '!=', $current_phys,
                   "Row $id physical checksum changed (VACUUM FULL performed)");
        }
    }
    
    $node->safe_psql('postgres', 'DROP TABLE test_comprehensive CASCADE');
}

#----------------------------------------------------------------------------
# Main test execution
#----------------------------------------------------------------------------

note("Starting checksum stability tests");
note("=================================");

test_vacuum_stability($node);
test_cluster_stability($node);
test_reindex_stability($node);
test_comprehensive_stability($node);

note("=================================");
note("All stability tests completed");

# Clean up
$node->stop;
done_testing();