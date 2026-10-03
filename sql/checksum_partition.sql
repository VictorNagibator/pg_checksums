-- Partitioned table checksum tests (physical and logical)

-- Range-partitioned table with a composite primary key
CREATE TABLE test_partitioned (
    id integer,
    region text NOT NULL,
    data text NOT NULL,
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE test_partitioned_west PARTITION OF test_partitioned FOR VALUES IN ('west');
CREATE TABLE test_partitioned_east PARTITION OF test_partitioned FOR VALUES IN ('east');

INSERT INTO test_partitioned (id, region, data)
SELECT gs, CASE WHEN gs % 2 = 0 THEN 'east' ELSE 'west' END, 'data_' || gs
FROM generate_series(1, 100) gs;

-- Test A: partitioned table checksums are non-zero / non-null
SELECT
    pg_table_physical_checksum('test_partitioned'::regclass, false) != 0 AS partitioned_physical_non_zero,
    pg_table_physical_checksum('test_partitioned'::regclass, true) != 0 AS partitioned_physical_header_non_zero,
    pg_table_logical_checksum('test_partitioned'::regclass) IS NOT NULL AS partitioned_logical_not_null,
    pg_table_logical_checksum('test_partitioned'::regclass) != 0 AS partitioned_logical_non_zero;

-- Test B: physical checksum differs when the header is included
SELECT
    pg_table_physical_checksum('test_partitioned'::regclass, false) !=
    pg_table_physical_checksum('test_partitioned'::regclass, true)
    AS header_changes_partitioned_physical;

-- Test C: physical and logical checksums differ
SELECT
    pg_table_physical_checksum('test_partitioned'::regclass, false) !=
    pg_table_logical_checksum('test_partitioned'::regclass)
    AS partitioned_physical_logical_differ;

-- Test D: physical checksum changes after data modification
DO $$
DECLARE
    old bigint;
    new bigint;
BEGIN
    old := pg_table_physical_checksum('test_partitioned'::regclass, false);
    UPDATE test_partitioned SET data = 'modified' WHERE id = 1;
    new := pg_table_physical_checksum('test_partitioned'::regclass, false);
    UPDATE test_partitioned SET data = 'data_1' WHERE id = 1;
    IF old = new THEN
        RAISE EXCEPTION 'Test D failed: partitioned physical checksum should change after data modification';
    END IF;
END;
$$;

-- Test E: logical checksum changes after data modification
DO $$
DECLARE
    old bigint;
    new bigint;
BEGIN
    old := pg_table_logical_checksum('test_partitioned'::regclass);
    UPDATE test_partitioned SET data = 'modified' WHERE id = 2;
    new := pg_table_logical_checksum('test_partitioned'::regclass);
    UPDATE test_partitioned SET data = 'data_2' WHERE id = 2;
    IF old = new THEN
        RAISE EXCEPTION 'Test E failed: partitioned logical checksum should change after data modification';
    END IF;
END;
$$;

-- Test F: logical checksum is deterministic for the same data state
DO $$
DECLARE
    a bigint;
    b bigint;
BEGIN
    a := pg_table_logical_checksum('test_partitioned'::regclass);
    b := pg_table_logical_checksum('test_partitioned'::regclass);
    IF a != b THEN
        RAISE EXCEPTION 'Test F failed: partitioned logical checksum should be deterministic';
    END IF;
END;
$$;

-- Test G: partitioned table without a primary key returns NULL logical checksum
CREATE TABLE test_partitioned_nopk (
    id integer,
    data text
) PARTITION BY RANGE (id);

CREATE TABLE test_partitioned_nopk_p1 PARTITION OF test_partitioned_nopk FOR VALUES FROM (1) TO (50);
CREATE TABLE test_partitioned_nopk_p2 PARTITION OF test_partitioned_nopk FOR VALUES FROM (50) TO (100);

INSERT INTO test_partitioned_nopk (id, data)
SELECT gs, 'nopk_' || gs FROM generate_series(1, 99) gs;

SELECT
    pg_table_logical_checksum('test_partitioned_nopk'::regclass) IS NULL AS partitioned_nopk_logical_null,
    pg_table_physical_checksum('test_partitioned_nopk'::regclass, false) != 0 AS partitioned_nopk_physical_non_zero;

-- Test H: empty partitioned tables
CREATE TABLE test_partitioned_empty (
    id integer PRIMARY KEY,
    data text
) PARTITION BY RANGE (id);

CREATE TABLE test_partitioned_empty_nopk (
    id integer,
    data text
) PARTITION BY RANGE (id);

SELECT
    pg_table_physical_checksum('test_partitioned_empty'::regclass, false) != 0 AS empty_partitioned_physical_non_zero,
    pg_table_logical_checksum('test_partitioned_empty'::regclass) IS NOT NULL AS empty_partitioned_logical_not_null,
    pg_table_logical_checksum('test_partitioned_empty_nopk'::regclass) IS NULL AS empty_partitioned_nopk_logical_null;

-- Test I: multi-level partitioning
CREATE TABLE test_partitioned_multi (
    id integer,
    region text NOT NULL,
    subregion text NOT NULL,
    PRIMARY KEY (id, region, subregion)
) PARTITION BY LIST (region);

CREATE TABLE test_partitioned_multi_west PARTITION OF test_partitioned_multi
    FOR VALUES IN ('west') PARTITION BY LIST (subregion);
CREATE TABLE test_partitioned_multi_west_n PARTITION OF test_partitioned_multi_west
    FOR VALUES IN ('north');
CREATE TABLE test_partitioned_multi_west_s PARTITION OF test_partitioned_multi_west
    FOR VALUES IN ('south');
CREATE TABLE test_partitioned_multi_east PARTITION OF test_partitioned_multi
    FOR VALUES IN ('east');

INSERT INTO test_partitioned_multi (id, region, subregion)
VALUES (1, 'west', 'north'), (2, 'west', 'south'), (3, 'east', 'east');

SELECT
    pg_table_physical_checksum('test_partitioned_multi'::regclass, false) != 0 AS multilevel_physical_non_zero,
    pg_table_logical_checksum('test_partitioned_multi'::regclass) IS NOT NULL AS multilevel_logical_not_null;

-- Test J: parallel and serial physical checksums agree for partitioned tables
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_table_physical_checksum('test_partitioned'::regclass, false);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_table_physical_checksum('test_partitioned'::regclass, false);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test J failed: parallel partitioned physical checksum differs from serial';
    END IF;
END;
$$;

-- Test K: parallel and serial logical checksums agree for partitioned tables
DO $$
DECLARE
    serial_val bigint;
    parallel_val bigint;
BEGIN
    PERFORM set_config('pg_checksums.workers', '0', true);
    serial_val := pg_table_logical_checksum('test_partitioned'::regclass);
    PERFORM set_config('pg_checksums.workers', '4', true);
    parallel_val := pg_table_logical_checksum('test_partitioned'::regclass);
    IF serial_val IS NULL OR serial_val != parallel_val THEN
        RAISE EXCEPTION 'Test K failed: parallel partitioned logical checksum differs from serial';
    END IF;
END;
$$;

-- Clean up
DROP TABLE test_partitioned;
DROP TABLE test_partitioned_nopk;
DROP TABLE test_partitioned_empty;
DROP TABLE test_partitioned_empty_nopk;
DROP TABLE test_partitioned_multi;
