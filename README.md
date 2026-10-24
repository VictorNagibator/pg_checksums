# pg_checksums - Multi-Level Data Integrity Extension for PostgreSQL

[![CI](https://github.com/VictorNagibator/pg_checksums/actions/workflows/ci.yml/badge.svg)](https://github.com/VictorNagibator/pg_checksums/actions/workflows/ci.yml)
[![PostgreSQL 12-18](https://img.shields.io/badge/postgres-12--18-336791)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

`pg_checksums` is a PostgreSQL extension that provides comprehensive checksum functionality at six granularity levels, with both physical and logical variants (except for cell level). It enables data integrity verification, change detection, consistency checking, and corruption detection across the entire database hierarchy.

---

## Key Features

### Multi-Granularity & Dual-Mode Support

* **Six Granularity Levels**: Page, Cell, Tuple, Table, Index, and Database
* **Dual Checksum Types**: Physical (storage-dependent) and Logical (content-dependent)
* **Type-Aware**: Full support for all PostgreSQL data types
* **NULL-Aware**: Special `CHECKSUM_NULL` constant for NULL values
* **MVCC-Aware**: Incorporates transaction visibility in physical checksums
* **No Primary Key Required**: Logical table/database checksums hash row
  content directly and preserve duplicate rows with a sum-plus-count aggregate
* **Portable Content**: Logical checksums omit relation OIDs, heap TIDs and
  type OIDs; enum labels, numeric scale, arrays, composites and ranges are
  canonicalized
* **All Index Access Methods**: Logical index checksums are derived from table
  data and the index definition, so B-tree, Hash, GiST, GIN, SP-GiST, BRIN and
  custom AMs are supported
* **Parallel Computation**: Table, index and database checksums can be
  computed with PostgreSQL parallel workers for faster scans of large
  relations
* **Partitioned Support**: Table and index checksums transparently aggregate
  the checksums of all leaf partitions/indexes
* **Strict Mode**: `pg_checksums.strict` makes database scans fail instead of
  skipping relations that cannot be checksummed

---

## Physical and Logical Checksums

### Physical Checksums

* **Depend on**: Physical storage layout (block numbers, offsets, page headers)
* **Change when**: `VACUUM FULL`, `CLUSTER`, `REINDEX`, physical data movement
* **Useful for**: Physical corruption detection, storage integrity
* **Includes**: Location encoding, MVCC information, page structure

### Logical Checksums

* **Depend on**: Only logical data content.  Relation OIDs, heap TIDs and
  type OIDs are excluded.
* **Stable across**: `VACUUM`, `VACUUM FULL`, `CLUSTER` and `REINDEX`.
* **Useful for**: Logical consistency, replication validation, migration and
  cross-database content comparison.
* **No primary key required**: tables without a PK are covered.
* **Index checksums**: derived from visible table rows and the index
  definition, not from physical index pages.  This works for every index AM
  and is stable across `VACUUM`/`REINDEX`.  Physical index checksums remain
  the tool for detecting missing or corrupt index entries.

---

## Installation

### Prerequisites

* PostgreSQL 12 or later (tested on 12, 13, 14, 15, 16, 17, and 18)
* C compiler and PostgreSQL development headers
* Superuser access to install the extension and to run the database-level
  checksum functions

> **Privileges:** the table, page, cell, tuple and index checksum functions
> read relation storage directly.  They require `SELECT` on the relation (or
> on the base table for an index) and reject relations protected by row-level
> security, because a direct page scan cannot apply RLS policies.  The
> database-level functions are superuser-only; this is enforced in C, not just
> in SQL.

> **Note:** PostgreSQL 12 and 13 are past end-of-life upstream, but they are
> still fully supported by this extension. The build deliberately fails with a
> clear error on PostgreSQL 11 and older, which lack the table-AM API the
> extension relies on.

> **Note:** the distribution is named `pg_checksums`, the same name as the
> `pg_checksums` utility shipped with PostgreSQL core. They are unrelated: the
> core utility verifies data page checksums on disk, while this extension
> provides SQL functions for computing checksums.

### Build and Install

The extension builds with PGXS, so `pg_config` must be on your `PATH`, or you
can point `make` at a specific installation with `PG_CONFIG=/path/to/pg_config`.

```bash
# Clone or extract the extension
git clone https://github.com/VictorNagibator/pg_checksums

# Go into extension directory
cd pg_checksums

# Build
make

# Install
make install

# Create extension in your database
psql -d your_database -c "CREATE EXTENSION pg_checksums;"
```

Or install it straight from [PGXN](https://pgxn.org/dist/pg_checksums/) with the
[pgxn client](https://github.com/pgxn/pgxnclient):

```bash
pgxn install pg_checksums
```

---

## Function Reference

### Page Level Functions (Physical Only)

**`pg_page_checksum(regclass, integer) -> integer`**

Computes PostgreSQL's built-in page checksum for physical page corruption detection.
The block number is validated: an out-of-range block (including `-1`, which
would otherwise be interpreted as `P_NEW`) raises an error instead of reading
or extending the relation.  The page is copied before the checksum is
computed, so the shared buffer is never modified.

```sql
-- Get checksum for page 5 of a table
SELECT pg_page_checksum('my_table'::regclass, 5);
```

### Cell Level Functions (Logical Only)

**`pg_cell_checksum(regclass, tid, integer) -> integer`**

Computes logical checksum of a cell value. NULL values return `CHECKSUM_NULL` (-1).
Requires `SELECT` on the relation; an out-of-range TID or attribute number
returns NULL.  Relations protected by row-level security are rejected.

```sql
-- Get checksum for cell (column) 3 of a specific tuple
SELECT pg_cell_checksum('employees'::regclass, '(5,2)'::tid, 3);
```

**Note**: Cell checksums are computed for individual values within specific rows, not for entire columns. To compute checksums for all values in a column, you would need to aggregate individual checksums.

### Tuple Level Functions

**`pg_tuple_physical_checksum(regclass, tid, boolean) -> integer`**

Computes physical tuple checksum based on physical location (block, offset).
Requires `SELECT` on the relation.  An invalid TID returns NULL; a corrupted
line pointer or tuple header raises a clean `DATA_CORRUPTED` error.

```sql
-- Physical checksum without header
SELECT pg_tuple_physical_checksum('orders'::regclass, ctid, false) 
FROM orders WHERE order_id = 123;

-- Physical checksum with header (includes MVCC info)
SELECT pg_tuple_physical_checksum('orders'::regclass, ctid, true) 
FROM orders WHERE order_id = 123;
```

**`pg_tuple_logical_checksum(regclass, tid) -> integer`**

Computes a canonical logical checksum of one row.  All non-dropped columns are
hashed; relation OIDs, heap TIDs and the tuple header are excluded.  No primary
key is required.  Requires `SELECT`; relations protected by row-level security
are rejected.

```sql
-- Logical checksum of one row
SELECT pg_tuple_logical_checksum('customers'::regclass, ctid)
FROM customers WHERE customer_id = 456;
```

### Table Level Functions

**`pg_table_physical_checksum(regclass, boolean) -> bigint`**

Aggregates physical tuple checksums for entire table (order-independent).
Requires `SELECT` on the table.  The function accepts ordinary tables,
materialized views, partitioned tables and (for physical checksums) TOAST
relations.  A TOAST relation is checksummed as raw pages, because its tuples
are not heap tuples.  A corrupted page produces a clean error rather than
crashing the backend.

For an ordinary table, the physical checksum covers the main heap only: a
TOAST value is represented by its pointer in the main tuple, so corruption
inside the TOAST data is not detected by this function.  Use
`pg_table_logical_checksum` (which detoasts values) or a database physical
checksum with `include_toast = true` to cover TOAST content.

```sql
-- Table physical checksum
SELECT pg_table_physical_checksum('inventory'::regclass, false);

-- With tuple headers included
SELECT pg_table_physical_checksum('inventory'::regclass, true);
```

**`pg_table_logical_checksum(regclass) -> bigint`**

Aggregates canonical logical row checksums for an entire table.  It does not
require a primary key, and relation OIDs are not included.  Duplicate rows are
preserved by a sum-plus-count aggregate, so two identical rows contribute
twice.  Requires `SELECT` on the table; returns NULL for a TOAST relation
(toast content is covered when the parent table detoasts its values).

```sql
-- Logical checksum (no primary key required)
SELECT pg_table_logical_checksum('products'::regclass);
```

**Partitioned tables**: `pg_table_physical_checksum` and
`pg_table_logical_checksum` accept partitioned tables. Since a partitioned
table has no storage of its own, the extension recurses into every leaf
partition.  Logical checksums accumulate all leaf rows into the same
accumulator, so the result does not depend on the partitioning layout.
Nested (multi-level) partitioning is supported as well.

```sql
-- Checksum a partitioned table (recurses into all partitions)
SELECT pg_table_physical_checksum('sales'::regclass, false);
SELECT pg_table_logical_checksum('sales'::regclass);
```

### Index Level Functions

**`pg_index_physical_checksum(regclass) -> integer`**

Computes physical checksum of index structure. Supports B-tree, Hash, GiST, GIN, SP-GiST, BRIN.
Requires `SELECT` on the underlying table.  Returns NULL for an unsupported
access method.  A partitioned index aggregates the physical checksums of its
leaf indexes.

```sql
-- Physical checksum of a B-tree index
SELECT pg_index_physical_checksum('idx_product_name'::regclass);
```

**`pg_index_logical_checksum(regclass) -> integer`**

Computes the logical content of an index from the visible table rows and the
index definition.  Plain columns, expressions, partial-index predicates and
INCLUDE columns are evaluated for each row; no heap TIDs, relation OIDs or dead
entries are included.  It works for every index access method (B-tree, Hash,
GiST, GIN, SP-GiST, BRIN and custom AMs), and for partitioned indexes.

```sql
-- Logical content of any index
SELECT pg_index_logical_checksum('idx_product_name'::regclass);
```

**Logical vs physical index checksums.**  The logical checksum describes what
the index should contain from the table data; it does not inspect physical
index pages.  A missing or corrupt index entry is therefore detected by
`pg_index_physical_checksum`, not by the logical value.

### Database Level Functions (Superuser Only)

**`pg_database_physical_checksum(boolean, boolean) -> bigint`**

Aggregates physical relation checksums for entire database.  Superuser-only;
the check is enforced in C (the SQL functions are security invoker, so
`superuser()` sees the calling session user).

```sql
-- Database physical checksum (excluding system tables)
SELECT pg_database_physical_checksum(false, false);

-- Include system catalogs and TOAST tables
SELECT pg_database_physical_checksum(true, true);
```

**TOAST:** `include_toast` is independent of `include_system`.  With
`include_toast = true`, TOAST relations are hashed as raw pages.  With
`include_toast = false` (the default), changes inside toasted values are not
covered by the physical database checksum; table-level physical checksums
store only the toast pointer in the main tuple.

**Note about shared system tables**: The database checksum only includes
relations from the current database.  Cluster-wide shared catalogs (e.g.,
`pg_authid`, `pg_database`) are excluded even when `include_system` is true.

**Skipped relations:** if a relation cannot be processed (dropped
concurrently, corrupt, unsupported, ...), the scan rolls back an internal
subtransaction, emits a warning and continues.  A summary warning is emitted
at the end.  Cancellation and `statement_timeout` are propagated, so a
canceled scan never returns a partial result as if it were complete.

**PostgreSQL 12-16 and parallel database scans:** those releases prohibit
internal subtransactions while in parallel mode.  A parallel database scan
therefore cannot isolate relation errors: if a relation fails, the scan is
aborted.  Run with `pg_checksums.workers = 0` (the default) to get the
skip-with-warning behavior on PostgreSQL 12-16.  Serial scans and parallel
scans on PostgreSQL 17+ isolate errors as described above.

**`pg_database_logical_checksum(include_system boolean, include_indexes boolean) -> bigint`**

Aggregates canonical logical checksums for the database.  Tables without a
primary key are included.  TOAST relations are skipped: their content is
covered when the parent table detoasts the values.  `include_indexes`
(default true) controls whether logical index checksums are included; because
they are derived from table data, they do not change when dead index entries
are vacuumed.

Superuser-only (enforced in C).

```sql
-- Database logical checksum, including indexes
SELECT pg_database_logical_checksum(false, true);

-- Tables only
SELECT pg_database_logical_checksum(false, false);
```

### Utility Functions

**`pg_data_checksum(bytea, integer) -> integer`**

Generic checksum for arbitrary binary data using FNV-1a.

```sql
SELECT pg_data_checksum(E'\x01020304'::bytea, 0);
```

**`pg_text_checksum(text, integer) -> integer`**

Wrapper for text data checksum.

```sql
SELECT pg_text_checksum('sample text', 0);
```

---

## Algorithm Details

### FNV-1a 32-bit Hash Algorithm

The extension uses the FNV-1a (Fowler-Noll-Vo) 32-bit hash algorithm, identical to PostgreSQL's internal checksum implementation.

**Algorithm:**

```text
hash = FNV_BASIS_32;
for each byte in data:
    hash = hash ^ byte;
    hash = hash * FNV_PRIME_32;
```

**Constants:**

```
FNV_BASIS_32 = 2166136261U
FNV_PRIME_32 = 16777619U
CHECKSUM_NULL = 0xFFFFFFFF (signed -1)
```

### Order-Independent Aggregation

Physical and index aggregates use XOR, which is commutative and associative and
therefore independent of scan order and partition boundaries.

Logical table/database aggregates use a **sum plus count** instead.  Addition
is still order-independent, but the count preserves duplicate rows, which a
pure XOR would cancel.  Parallel workers combine their partial sums and counts
before the final 32-bit mix, so serial and parallel results are identical.

### Type-Specific Handling

Logical values use a canonical type-aware encoding:

* **Scalar types** use their binary send function (network byte order), or the
  output function when no send function exists.
* **Enums** are hashed by label, not by type OID.
* **numeric** is normalized with `numeric_normalize()`, so `1.0` and `1.00`
  compare equal.
* **Arrays, composites, domains and ranges** are traversed recursively, so
  embedded type OIDs are not part of the hash.
* **Floating point** canonicalizes NaN and negative zero.

---

## Limitations and Portability

These limitations are documented rather than silently ignored.

* **Portability is scoped.** Logical checksums exclude relation OIDs, heap
  TIDs and type OIDs.  Built-in types, enums, numeric, arrays, composites,
  domains and ranges use a canonical encoding.  Cross-database comparison is
  intended for the same PostgreSQL major version and compatible schema/type
  names; custom types whose send/output representation changes between
  versions may still produce different values.
* **Custom types are type-defined.** A user-defined type is hashed through its
  binary send function, or its output function when no send function exists.
  If that representation differs between databases, the checksum differs.
* **Physical checksums are not replica fingerprints.** Tuple/table physical
  checksums depend on tuple locations, MVCC fields, tuple bytes and the
  relation's relfilenode/reltablespace; index physical checksums hash raw
  index pages, including page LSNs.  Use physical checksums for on-disk
  corruption within one cluster, not for comparing replicas.
* **Logical index checksums describe expected content, not stored pages.**
  They are derived from visible table rows and the index definition, so a
  missing or corrupt index entry is not detected by this value.  Use
  `pg_index_physical_checksum` for actual index storage.
* **Coverage gaps.** The database physical checksum does not include TOAST
  content by default, and never includes sequences, large objects (unless
  `include_system = true`), FSM/VM forks or relation free space.  Foreign
  table partitions are skipped by partitioned table checksums because they
  have no local storage in this database.
* **32-bit hashes.** All logical checksums are 32-bit.  This is adequate for
  accidental corruption detection but not for cryptographic tamper evidence;
  FNV-1a is not a cryptographic hash.
* **RLS and column privileges.** Relations protected by row-level security are
  rejected because a direct heap scan cannot apply policies.  Table-level
  `SELECT` is required; column-only privileges are not sufficient.
* **Foreign tables.** Foreign partitions/tables have no local storage and are
  skipped by partitioned checksums.

---

## Parallel Computation

Table, index and database checksums can be computed in parallel using
PostgreSQL's parallel worker infrastructure. Each worker scans a subset of the
relation's blocks (or a subset of the database's relations) using a shared,
self-balancing atomic counter, and the partial results are combined by the
leader.  Logical index checksums parallelize the heap scan and evaluate the
index definition in each worker.

### Configuration

The degree of parallelism is controlled by the `pg_checksums.workers` GUC:

| Setting                          | Behavior                                   |
| -------------------------------- | ------------------------------------------ |
| `pg_checksums.workers = 0`       | Disabled (default, single-backend)         |
| `pg_checksums.workers = N`       | Use up to `N` parallel workers             |

The actual number of workers is clamped by `max_parallel_workers` and by the
number of blocks (or relations) being scanned. Parallelism is also skipped
when the function is already running inside a parallel worker, since
parallel workers cannot be nested.

```sql
-- Compute a table checksum with 4 parallel workers
SET pg_checksums.workers = 4;
SELECT pg_table_physical_checksum('large_table'::regclass, false);

-- Disable parallelism (restore the default)
RESET pg_checksums.workers;
```

### Strict mode

Database checksum functions normally skip a relation that cannot be
checksummed (for example an invalid index, a foreign partition, or a relation
that fails during the scan).  Tables without a primary key and all index
access methods are now supported, so they are not skipped.  Set
`pg_checksums.strict = on` to turn an actual skip into an error instead:

```sql
SET pg_checksums.strict = on;
SELECT pg_database_logical_checksum(false, true);
-- ERROR: relation "..." cannot be checksummed
-- DETAIL: The relation has no usable checksum.
```

The default is `off`.  In strict mode a skipped relation aborts the whole
database checksum, which is useful for monitoring where a partial result
would be worse than no result.

### Correctness

Because aggregation is order- and partition-independent, the parallel result
is always identical to the serial result for the same data state, regardless
of the number of workers. The same consistent snapshot is propagated to all
workers, so every tuple is checksummed as of the same point in time.

---

## Practical Applications

### 1. Data Integrity Monitoring

```sql
-- Create checksum monitoring system
CREATE TABLE checksum_audit_log (
    audit_time timestamptz DEFAULT now(),
    table_name text,
    checksum_type text,  -- 'physical' or 'logical'
    checksum_value bigint,
    PRIMARY KEY (table_name, checksum_type, audit_time)
);

-- Periodic monitoring of critical tables
INSERT INTO checksum_audit_log (table_name, checksum_type, checksum_value)
SELECT 
    'transactions',
    'physical',
    pg_table_physical_checksum('transactions'::regclass, false)
UNION ALL
SELECT 
    'transactions',
    'logical',
    pg_table_logical_checksum('transactions'::regclass);
```

### 2. Replication Consistency Validation

```sql
-- Compare source and replica consistency.
-- IMPORTANT: this only works when the replica has the same relation OIDs and
-- heap TIDs (for example a physical standby that has not been vacuumed
-- differently).  Physical checksums include LSNs and are usually not equal on
-- a standby; logical checksums include OIDs.  See Limitations.
-- Run on source:
SELECT 
    c.relname,
    pg_table_logical_checksum(c.oid) as logical_cs,
    pg_table_physical_checksum(c.oid, false) as physical_cs
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p', 'm')
ORDER BY c.relname;

-- Run the same query on the replica and compare results.
```

### 3. Data Migration Verification

```sql
-- Verify migration integrity.  Logical checksums omit relation OIDs and heap
-- TIDs, so values are comparable between source and target databases.
-- Run this query on both databases and compare the two result sets.
WITH table_checksums AS (
    SELECT
        c.relname,
        pg_table_logical_checksum(c.oid) AS checksum
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p', 'm')
)
SELECT * FROM table_checksums ORDER BY relname;
```

### 4. Corruption Detection and Repair

```sql
-- Find potentially corrupted tuples
SELECT ctid, * 
FROM my_table
WHERE pg_tuple_physical_checksum('my_table'::regclass, ctid, false) != 
      expected_checksum;

-- Validate index integrity
SELECT 
    indexname,
    pg_index_physical_checksum(indexname::regclass) as current,
    expected_checksum,
    CASE WHEN pg_index_physical_checksum(indexname::regclass) != expected_checksum 
         THEN 'CORRUPT' 
         ELSE 'OK' 
    END as status
FROM index_monitoring;
```

### 5. Change Detection for Auditing

```sql
-- Trigger-based change detection
CREATE FUNCTION audit_table_changes()
RETURNS trigger AS $$
DECLARE
    current_logical bigint;
    current_physical bigint;
BEGIN
    current_logical := pg_table_logical_checksum(TG_TABLE_NAME::regclass);
    current_physical := pg_table_physical_checksum(TG_TABLE_NAME::regclass, false);
    
    -- Compare with last known good checksums
    IF EXISTS (
        SELECT 1 FROM checksum_baseline 
        WHERE table_name = TG_TABLE_NAME 
        AND (logical_cs != current_logical OR physical_cs != current_physical)
    ) THEN
        INSERT INTO unauthorized_changes 
        VALUES (TG_TABLE_NAME, current_logical, current_physical, now());
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE EVENT TRIGGER audit_changes 
ON ddl_command_end 
EXECUTE FUNCTION audit_table_changes();
```

### 6. Point-in-Time Recovery Validation

```sql
-- Verify restored database matches expected state
SELECT 
    'Restored DB' as source,
    pg_database_logical_checksum(false, true) as checksum
UNION ALL
SELECT 
    'Backup timestamp ' || backup_time,
    expected_checksum
FROM backup_catalog 
WHERE backup_time = '2024-01-15 03:00:00';
```

---

## Performance Characteristics

### Computational Complexity

| Function                 |       Complexity | Notes                           |
| ------------------------ | ---------------: | ------------------------------- |
| `pg_cell_checksum`     |             O(1) | Single cell, minimal overhead |
| `pg_tuple_*_checksum`    |    O(tuple size) | Requires buffer lock            |
| `pg_table_*_checksum`    |   O(rows * columns) | Order-independent aggregation   |
| `pg_index_*_checksum`    |    O(index size) | Bulk read strategy              |
| `pg_database_*_checksum` | O(total DB size) | Periodic interrupt checks       |

Table, index, and database scans can be parallelized with
`pg_checksums.workers`; the scan work is divided among the workers with
linear speedup up to the point where the scan becomes I/O-bound.

---

## Test Suite

The pg_checksums extension includes a comprehensive test suite that validates functionality at all granularity levels and ensures algorithm correctness. The tests are divided into two categories: regression tests (SQL-based) and TAP tests (Perl-based).

```bash
# Run the regression tests
make installcheck

# Run the regression and TAP tests (TAP needs the IPC::Run Perl module and the
# PostgreSQL test modules shipped with the matching server release)
make installcheck enable_tap_tests=yes PROVE=prove
```

CI runs both suites on every supported major version (PostgreSQL 12 through 18)
on Linux, macOS, and Windows.

**Tests cover:**

1. Basic functionality at all granularity levels
2. Physical and logical checksum differences
3. NULL handling and type support
4. MVCC behavior and concurrency
5. Stability of logical checksums across physical reorganizations (VACUUM, CLUSTER, REINDEX)
6. Error conditions and edge cases
7. Parallel vs serial checksum consistency at table, index and database levels
8. Partitioned tables and indexes (single- and multi-level, with and without
   primary keys)
9. Safety and validation: logical index checksums work for every access
   method; invalid block numbers/TIDs are rejected; views/sequences/indexes
   are rejected by table functions; SELECT privileges and RLS are enforced;
   and TOAST physical checksums are computed from raw pages
10. Corrupt page handling: unsafe page headers and line pointers produce clean
    errors; database scans skip the affected relation with a warning
11. Partitioned index aggregation, including deterministic physical and
    logical checksums for partitioned indexes
12. Canonical logical checksums: no-PK tables, duplicate rows, empty and
    partitioned tables, enum/numeric/array/composite/range values, all index
    AMs, expression/partial/INCLUDE indexes, and serial/parallel database
    consistency

---

## Automated Testing with `run_tests.sh`

To streamline the development workflow, the extension includes a helper script `run_tests.sh` that automates the complete build–install–test cycle. The script performs the following steps:

- Cleans previous builds (`make clean`)
- Builds the extension with parallel jobs (`make -j$JOBS`, `JOBS` defaults to 4)
- Installs it into the target PostgreSQL installation (using the provided `pg_config`)
- Starts the PostgreSQL server only if it is not already running
- Runs the regression tests (`make installcheck`); TAP tests are run only when
  `RUN_TAP=yes`
- Stops the server only if the script started it, and leaves an already
  running server alone

```bash
# Run it from the extension’s root directory
./run_tests.sh

# Also run TAP tests
RUN_TAP=yes ./run_tests.sh
```
### Customization

The script uses the same environment settings as your manual commands. You can adjust the following variables in the environment:

* `PG_CONFIG` – path to `pg_config` of the target PostgreSQL installation

* `PGCTL` – path to `pg_ctl`

* `PGDATA` – data directory of the test cluster

* `JOBS` – number of parallel build jobs (passed to `make -j`)

* `SUDO` – command used for `make install` (defaults to `sudo`; empty as root)

* `RUN_TAP` – set to `yes` to run the TAP suite

* `CLEAN` – set to `yes` to remove build artifacts on exit

---

## Building a Release

The `dist` target packs the current `HEAD` into the archive that PGXN expects
(`pg_checksums-<version>.zip`, with everything under a `pg_checksums-<version>/`
directory):

```bash
make dist
```

### Automated releases

Pushing a `v*` tag runs the [Release workflow](.github/workflows/release.yml),
which:

1. checks that the tag, `pg_checksums.control` and `META.json` all carry the
   same version;
2. builds the archive with `make dist` and verifies its contents;
3. takes the release notes from the matching `Changes` entry (the release fails
   if there is none, rather than publishing an empty note);
4. creates the GitHub Release and attaches the archive (tags containing
   `-rc`, `-beta` or `-alpha` are marked as pre-releases);
5. uploads the archive to [PGXN](https://manager.pgxn.org/) when the
   `PGXN_USERNAME` and `PGXN_PASSWORD` repository secrets are configured.

So bump the version in `pg_checksums.control`, `META.json` and `Changes`, add
the new SQL script (and an upgrade script if needed), and only then tag:

```bash
git tag -a v1.3.1 -m "pg_checksums 1.3.1"
git push origin v1.3.1
```

---

## License

Released under the [MIT License](LICENSE).

## Support

* Source code and issue tracker: <https://github.com/VictorNagibator/pg_checksums>
* Distribution page on PGXN: <https://pgxn.org/dist/pg_checksums/>
