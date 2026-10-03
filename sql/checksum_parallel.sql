-- Parallel checksum tests (table, index, and database levels)
-- Verifies that parallel computation produces identical, deterministic results.

CREATE TABLE test_parallel (
    id integer PRIMARY KEY,
    group_id integer NOT NULL,
    data text NOT NULL,
    padding char(200) DEFAULT 'x'
);

INSERT INTO test_parallel (id, group_id, data)
SELECT gs, gs % 10, 'data_' || gs FROM generate_series(1, 5000) gs;

CREATE INDEX idx_test_parallel_group ON test_parallel (group_id);
CREATE INDEX idx_test_parallel_data ON test_parallel (data);

-- Test A: parallel table physical checksum equals serial
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_table_physical_checksum('test_parallel'::regclass, false);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_table_physical_checksum('test_parallel'::regclass, false);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test A failed: parallel table physical checksum differs from serial';
    END IF;
END;
$$;

-- Test B: parallel table logical checksum equals serial
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_table_logical_checksum('test_parallel'::regclass);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_table_logical_checksum('test_parallel'::regclass);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test B failed: parallel table logical checksum differs from serial';
    END IF;
END;
$$;

-- Test C: parallel index physical checksum equals serial
DO $$
DECLARE
    serial_val integer;
    parallel_val integer;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_index_physical_checksum('idx_test_parallel_group'::regclass);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_index_physical_checksum('idx_test_parallel_group'::regclass);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test C failed: parallel index physical checksum differs from serial';
    END IF;
END;
$$;

-- Test D: parallel index logical checksum equals serial
DO $$
DECLARE
    serial_val integer;
    parallel_val integer;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_index_logical_checksum('idx_test_parallel_group'::regclass);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_index_logical_checksum('idx_test_parallel_group'::regclass);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test D failed: parallel index logical checksum differs from serial';
    END IF;
END;
$$;

-- Test E: parallel database physical checksum equals serial
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_database_physical_checksum(false, false);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_database_physical_checksum(false, false);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test E failed: parallel database physical checksum differs from serial';
    END IF;
END;
$$;

-- Test F: parallel database logical checksum equals serial
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_database_logical_checksum(false, false);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_database_logical_checksum(false, false);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test F failed: parallel database logical checksum differs from serial';
    END IF;
END;
$$;

-- Test G: parallel results are deterministic across repeated runs
DO $$
DECLARE
    first_val bigint;
    second_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '4', true);
    first_val := pg_table_physical_checksum('test_parallel'::regclass, false);
    second_val := pg_table_physical_checksum('test_parallel'::regclass, false);
    IF first_val != second_val THEN
        RAISE EXCEPTION 'Test G failed: parallel checksum is not deterministic';
    END IF;
END;
$$;

-- Test H: parallel physical and logical checksums are different
DO $$
DECLARE
    physical_val bigint;
    logical_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '4', true);
    physical_val := pg_table_physical_checksum('test_parallel'::regclass, false);
    logical_val := pg_table_logical_checksum('test_parallel'::regclass);
    IF physical_val = logical_val THEN
        RAISE EXCEPTION 'Test H failed: parallel physical and logical checksums should differ';
    END IF;
END;
$$;

-- Test I: parallel checksum changes after data modification
DO $$
DECLARE
    before_val bigint;
    after_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '4', true);
    before_val := pg_table_logical_checksum('test_parallel'::regclass);

    UPDATE test_parallel SET data = 'modified' WHERE id = 1;
    after_val := pg_table_logical_checksum('test_parallel'::regclass);
    UPDATE test_parallel SET data = 'data_1' WHERE id = 1;

    IF before_val = after_val THEN
        RAISE EXCEPTION 'Test I failed: parallel checksum should change after data modification';
    END IF;
END;
$$;

-- Clean up
DROP INDEX idx_test_parallel_group;
DROP INDEX idx_test_parallel_data;
DROP TABLE test_parallel;
