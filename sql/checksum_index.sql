-- Complete index checksum tests
-- Test all index types: B-tree, Hash, GiST, GIN, SP-GiST, and BRIN

-- Create test table with various data types
CREATE TABLE test_index_checksum (
    id integer PRIMARY KEY,
    int_col integer NOT NULL,
    text_col text NOT NULL,
    float_col float NOT NULL,
    tsvector_col tsvector,
    range_col int4range,
    bool_col boolean
);

-- Insert test data
INSERT INTO test_index_checksum (
    id, int_col, text_col, float_col, tsvector_col, range_col, bool_col
)
SELECT 
    gs,
    gs,
    'text_' || gs,
    gs * 1.5,
    to_tsvector('english', 'item ' || gs || ' description'),
    int4range(gs, gs + 100),
    gs % 2 = 0
FROM generate_series(1, 100) gs;

-- Create B-tree indexes
CREATE INDEX idx_btree_int ON test_index_checksum (int_col);
CREATE INDEX idx_btree_text ON test_index_checksum (text_col);
CREATE INDEX idx_btree_multi ON test_index_checksum (int_col, text_col);
CREATE UNIQUE INDEX idx_btree_unique ON test_index_checksum (int_col, text_col, float_col);
CREATE INDEX idx_btree_expression ON test_index_checksum ((int_col * 2));
CREATE INDEX idx_btree_partial ON test_index_checksum (int_col) WHERE float_col > 50;

-- Create Hash index
CREATE INDEX idx_hash_text ON test_index_checksum USING hash (text_col);

-- Create GiST index
CREATE INDEX idx_gist_range ON test_index_checksum USING gist (range_col);

-- Create GIN index
CREATE INDEX idx_gin_tsvector ON test_index_checksum USING gin (tsvector_col);

-- Create SP-GiST index
CREATE INDEX idx_spgist_range ON test_index_checksum USING spgist (range_col);

-- Create BRIN index
CREATE INDEX idx_brin_int ON test_index_checksum USING brin (int_col);

-- Test 1: B-tree index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_btree_int'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_btree_int'::regclass) != 0 
    AS btree_physical_checksum_valid;

-- Test 2: B-tree index logical checksum is non-zero
SELECT 
    pg_index_logical_checksum('idx_btree_int'::regclass) IS NOT NULL AND
    pg_index_logical_checksum('idx_btree_int'::regclass) != 0 
    AS btree_logical_checksum_valid;

-- Test 3: Hash index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_hash_text'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_hash_text'::regclass) != 0 
    AS hash_physical_checksum_valid;

-- Test 4: GiST index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_gist_range'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_gist_range'::regclass) != 0 
    AS gist_physical_checksum_valid;

-- Test 5: GIN index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_gin_tsvector'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_gin_tsvector'::regclass) != 0 
    AS gin_physical_checksum_valid;

-- Test 6: SP-GiST index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_spgist_range'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_spgist_range'::regclass) != 0 
    AS spgist_physical_checksum_valid;

-- Test 7: BRIN index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_brin_int'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_brin_int'::regclass) != 0 
    AS brin_physical_checksum_valid;

-- Test 8: Different B-tree indexes have different physical checksums
WITH checksums AS (
    SELECT pg_index_physical_checksum('idx_btree_int'::regclass) as checksum
    UNION ALL
    SELECT pg_index_physical_checksum('idx_btree_text'::regclass)
    UNION ALL
    SELECT pg_index_physical_checksum('idx_btree_multi'::regclass)
    UNION ALL
    SELECT pg_index_physical_checksum('idx_btree_unique'::regclass)
    WHERE pg_index_physical_checksum('idx_btree_unique'::regclass) IS NOT NULL
)
SELECT 
    COUNT(DISTINCT checksum) = COUNT(*) 
    AS different_btree_indexes_have_different_checksums
FROM checksums
WHERE checksum IS NOT NULL;

-- Test 9: Expression index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_btree_expression'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_btree_expression'::regclass) != 0 
    AS expression_index_physical_checksum_valid;

-- Test 10: Partial index physical checksum is non-zero
SELECT 
    pg_index_physical_checksum('idx_btree_partial'::regclass) IS NOT NULL AND
    pg_index_physical_checksum('idx_btree_partial'::regclass) != 0 
    AS partial_index_physical_checksum_valid;

-- Test 11: Physical and logical checksums are different for B-tree
SELECT 
    pg_index_physical_checksum('idx_btree_int'::regclass) != 
    pg_index_logical_checksum('idx_btree_int'::regclass)
    AS physical_and_logical_checksums_are_different;

-- Test 12: Checksum changes after data modification
DO $$
DECLARE
    old_physical integer;
    new_physical integer;
    old_logical integer;
    new_logical integer;
    physical_changed boolean;
    logical_changed boolean;
BEGIN
    -- Get original checksums
    old_physical := pg_index_physical_checksum('idx_btree_int'::regclass);
    old_logical := pg_index_logical_checksum('idx_btree_int'::regclass);
    
    -- Modify indexed column
    UPDATE test_index_checksum 
    SET int_col = int_col + 1000
    WHERE id = 1;
    
    -- Get new checksums
    new_physical := pg_index_physical_checksum('idx_btree_int'::regclass);
    new_logical := pg_index_logical_checksum('idx_btree_int'::regclass);
    
    -- Restore original value
    UPDATE test_index_checksum 
    SET int_col = int_col - 1000
    WHERE id = 1;
    
    -- Check if checksums changed
    physical_changed := (old_physical IS NOT NULL AND new_physical IS NOT NULL AND 
                        old_physical != new_physical);
    logical_changed := (old_logical IS NOT NULL AND new_logical IS NOT NULL AND 
                       old_logical != new_logical);
    
    -- Output results
    IF physical_changed THEN
        RAISE NOTICE 'checksum_changes_after_data_modification_physical passed';
    ELSE
        RAISE NOTICE 'checksum_changes_after_data_modification_physical failed';
    END IF;
    
    IF logical_changed THEN
        RAISE NOTICE 'checksum_changes_after_data_modification_logical passed';
    ELSE
        RAISE NOTICE 'checksum_changes_after_data_modification_logical failed';
    END IF;
END;
$$;

-- Test 13: Empty index handling
DO $$
DECLARE
    empty_physical_valid boolean;
    empty_logical_valid boolean;
BEGIN
    -- Create empty table with index
    CREATE TABLE test_empty_table (
        id integer PRIMARY KEY,
        data text
    );
    
    CREATE INDEX idx_test_empty ON test_empty_table (data);
    
    -- Check checksums for empty index
    empty_physical_valid := (pg_index_physical_checksum('idx_test_empty'::regclass) IS NOT NULL);
    empty_logical_valid := (pg_index_logical_checksum('idx_test_empty'::regclass) IS NOT NULL);
    
    -- Clean up
    DROP TABLE test_empty_table;
    
    -- Output results
    IF empty_physical_valid THEN
        RAISE NOTICE 'empty_index_physical_checksum_valid passed';
    ELSE
        RAISE NOTICE 'empty_index_physical_checksum_valid failed';
    END IF;
    
    IF empty_logical_valid THEN
        RAISE NOTICE 'empty_index_logical_checksum_valid passed';
    ELSE
        RAISE NOTICE 'empty_index_logical_checksum_valid failed';
    END IF;
END;
$$;

-- Clean up
DROP INDEX idx_btree_int;
DROP INDEX idx_btree_text;
DROP INDEX idx_btree_multi;
DROP INDEX idx_btree_unique;
DROP INDEX idx_btree_expression;
DROP INDEX idx_btree_partial;
DROP INDEX idx_hash_text;
DROP INDEX idx_gist_range;
DROP INDEX idx_gin_tsvector;
DROP INDEX idx_spgist_range;
DROP INDEX idx_brin_int;
DROP TABLE test_index_checksum;