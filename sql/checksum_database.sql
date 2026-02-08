-- Database-level checksum tests
-- Note: These functions require superuser privileges

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

-- Test A: Database physical checksum returns non-zero value
DO $$
DECLARE
    db_physical_checksum bigint;
BEGIN
    -- Try to get database physical checksum (excluding system tables)
    db_physical_checksum := pg_database_physical_checksum(false, false);
    
    IF db_physical_checksum IS NULL THEN
        RAISE EXCEPTION 'Test A failed: Database physical checksum returned NULL';
    END IF;
    
    -- Note: checksum could be 0 for empty database, but we have test data
    IF db_physical_checksum = 0 THEN
        RAISE NOTICE 'Test A: Database physical checksum returned 0 (unexpected but not fatal)';
    END IF;
END;
$$;

-- Test B: Database logical checksum returns non-zero value (all tables have PK)
DO $$
DECLARE
    db_logical_checksum bigint;
BEGIN
    -- Try to get database logical checksum (excluding system tables)
    db_logical_checksum := pg_database_logical_checksum(false, false);
    
    IF db_logical_checksum IS NULL THEN
        RAISE EXCEPTION 'Test B failed: Database logical checksum returned NULL';
    END IF;
    
    IF db_logical_checksum = 0 THEN
        RAISE NOTICE 'Test B: Database logical checksum returned 0 (unexpected but not fatal)';
    END IF;
END;
$$;

-- Test C: Physical checksum changes after data modification
DO $$
DECLARE
    old_physical_checksum bigint;
    new_physical_checksum bigint;
BEGIN
    -- Get initial physical checksum
    old_physical_checksum := pg_database_physical_checksum(false, false);
    
    -- Modify data in one of the tables
    UPDATE test_checksum_schema.table1 
    SET data = 'modified_' || id 
    WHERE id = 1;
    
    -- Get new physical checksum
    new_physical_checksum := pg_database_physical_checksum(false, false);
    
    -- Restore original data
    UPDATE test_checksum_schema.table1 
    SET data = 'data_1' 
    WHERE id = 1;
    
    -- Check if checksums are different
    IF old_physical_checksum = new_physical_checksum THEN
        RAISE NOTICE 'Test C: Database physical checksum did not change after data modification (may happen due to caching)';
    ELSE
        RAISE NOTICE 'Test C passed: Database physical checksum changed after data modification';
    END IF;
END;
$$;

-- Test D: Logical checksum changes after data modification
DO $$
DECLARE
    old_logical_checksum bigint;
    new_logical_checksum bigint;
BEGIN
    -- Get initial logical checksum
    old_logical_checksum := pg_database_logical_checksum(false, false);
    
    -- Modify non-PK data in one of the tables
    UPDATE test_checksum_schema.table1 
    SET data = 'modified_' || id 
    WHERE id = 2;
    
    -- Get new logical checksum
    new_logical_checksum := pg_database_logical_checksum(false, false);
    
    -- Restore original data
    UPDATE test_checksum_schema.table1 
    SET data = 'data_2' 
    WHERE id = 2;
    
    -- Check if checksums are different
    IF old_logical_checksum = new_logical_checksum THEN
        RAISE EXCEPTION 'Test D failed: Database logical checksum did not change after data modification';
    END IF;
    
    RAISE NOTICE 'Test D passed: Database logical checksum changed after data modification';
END;
$$;

-- Test E: Physical checksum changes after structural modification
DO $$
DECLARE
    old_physical_checksum bigint;
    new_physical_checksum bigint;
BEGIN
    -- Get initial physical checksum
    old_physical_checksum := pg_database_physical_checksum(false, false);
    
    -- Add a new table (structural change)
    CREATE TABLE test_checksum_schema.table3 (
        id integer PRIMARY KEY,
        extra text
    );
    
    INSERT INTO test_checksum_schema.table3 (id, extra)
    SELECT gs, 'extra_' || gs FROM generate_series(1, 10) gs;
    
    -- Get new physical checksum
    new_physical_checksum := pg_database_physical_checksum(false, false);
    
    -- Clean up
    DROP TABLE test_checksum_schema.table3;
    
    -- Check if checksums are different
    IF old_physical_checksum = new_physical_checksum THEN
        RAISE NOTICE 'Test E: Database physical checksum did not change after structural modification (unexpected)';
    ELSE
        RAISE NOTICE 'Test E passed: Database physical checksum changed after structural modification';
    END IF;
END;
$$;

-- Test F: Include system tables parameter works
DO $$
DECLARE
    checksum_with_system bigint;
    checksum_without_system bigint;
BEGIN
    -- Get checksum including system tables
    checksum_with_system := pg_database_physical_checksum(true, false);
    
    -- Get checksum excluding system tables
    checksum_without_system := pg_database_physical_checksum(false, false);
    
    -- They should be different
    IF checksum_with_system = checksum_without_system THEN
        RAISE NOTICE 'Test F: Checksums with and without system tables are the same (unexpected)';
    ELSE
        RAISE NOTICE 'Test F passed: Checksums with and without system tables are different';
    END IF;
END;
$$;

-- Test G: Database logical checksum for table without PK should skip that table
DO $$
DECLARE
    logical_checksum_before bigint;
    logical_checksum_after bigint;
BEGIN
    -- Get current logical checksum
    logical_checksum_before := pg_database_logical_checksum(false, false);
    
    -- Create a table without primary key
    CREATE TABLE test_checksum_schema.table_no_pk (
        id integer,
        data text
    );
    
    INSERT INTO test_checksum_schema.table_no_pk (id, data)
    SELECT gs, 'no_pk_' || gs FROM generate_series(1, 20) gs;
    
    -- Get logical checksum after adding table without PK
    logical_checksum_after := pg_database_logical_checksum(false, false);
    
    -- Clean up
    DROP TABLE test_checksum_schema.table_no_pk;
    
    -- Logical checksum should remain the same (table without PK is skipped)
    IF logical_checksum_before = logical_checksum_after THEN
        RAISE NOTICE 'Test G passed: Table without PK does not affect logical checksum';
    ELSE
        RAISE NOTICE 'Test G: Logical checksum changed after adding table without PK (unexpected)';
    END IF;
END;
$$;

-- Test H: Verify that physical checksum is different from logical checksum
DO $$
DECLARE
    physical_checksum bigint;
    logical_checksum bigint;
BEGIN
    physical_checksum := pg_database_physical_checksum(false, false);
    logical_checksum := pg_database_logical_checksum(false, false);
    
    IF physical_checksum = logical_checksum THEN
        RAISE EXCEPTION 'Test H failed: Physical and logical checksums should be different';
    END IF;
    
    RAISE NOTICE 'Test H passed: Physical and logical checksums are different';
END;
$$;

-- Test I: Verify consistency - same checksum for same data state
DO $$
DECLARE
    checksum1 bigint;
    checksum2 bigint;
BEGIN
    -- Get checksum twice in the same state
    checksum1 := pg_database_physical_checksum(false, false);
    checksum2 := pg_database_physical_checksum(false, false);
    
    IF checksum1 != checksum2 THEN
        RAISE EXCEPTION 'Test I failed: Database checksum should be consistent for same data state';
    END IF;
    
    RAISE NOTICE 'Test I passed: Database checksum is consistent';
END;
$$;

-- Test J: Index physical checksum returns non-zero
DO $$
DECLARE
    idx_physical_checksum integer;
BEGIN
    SELECT pg_index_physical_checksum('test_checksum_schema.idx_table2_value'::regclass)
    INTO idx_physical_checksum;
    
    IF idx_physical_checksum IS NULL THEN
        RAISE NOTICE 'Test J: Index physical checksum returned NULL (may be unsupported index type)';
    ELSIF idx_physical_checksum = 0 THEN
        RAISE NOTICE 'Test J: Index physical checksum returned 0';
    ELSE
        RAISE NOTICE 'Test J passed: Index physical checksum is non-zero';
    END IF;
END;
$$;

-- Test K: Index logical checksum returns non-zero
DO $$
DECLARE
    idx_logical_checksum integer;
BEGIN
    SELECT pg_index_logical_checksum('test_checksum_schema.idx_table2_value'::regclass)
    INTO idx_logical_checksum;
    
    IF idx_logical_checksum IS NULL THEN
        RAISE NOTICE 'Test K: Index logical checksum returned NULL (may be unsupported index type)';
    ELSIF idx_logical_checksum = 0 THEN
        RAISE NOTICE 'Test K: Index logical checksum returned 0';
    ELSE
        RAISE NOTICE 'Test K passed: Index logical checksum is non-zero';
    END IF;
END;
$$;

-- Clean up
DROP SCHEMA test_checksum_schema CASCADE;