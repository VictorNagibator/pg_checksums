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
* **Primary Key Aware**: Logical checksums use primary keys for row identity
* **Content Checksums**: `pg_table_content_checksum()` works without a primary
  key, omits relation OIDs, and preserves duplicate rows with a sum-plus-count
  aggregate
* **Parallel Computation**: Table, index, and database checksums can be computed
  with PostgreSQL parallel workers for faster scans of large relations (the
  content-only checksum is currently serial)
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

* **Depend on**: Only logical data content (cell values, primary keys)
* **Stable across**: Physical reorganization for table-level checksums
  (`VACUUM FULL`, `CLUSTER`, `REINDEX`)
* **Useful for**: Logical consistency, replication validation, migration verification
* **Requires**: Primary keys for tuple/table/database logical checksums
* **Index caveat**: logical checksums are available for B-tree only.  They
  include heap TIDs, and database-level logical checksums include indexes, so
  the database checksum can change when dead index entries are removed by
  `VACUUM`.  See [Limitations](#limitations-and-portability).

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

**`pg_tuple_logical_checksum(regclass, tid, boolean) -> integer`**

Computes logical tuple checksum based on primary key values and data content. Returns NULL if no PK.
Requires `SELECT`; relations protected by row-level security are rejected.
Note that `include_header = true` includes the tuple header (xmin/xmax/ctid),
so that variant is not stable across physical reorganization.

```sql
-- Logical checksum (requires primary key)
SELECT pg_tuple_logical_checksum('customers'::regclass, ctid, false) 
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

Aggregates logical tuple checksums for entire table. Requires primary key.
Requires `SELECT` on the table; returns NULL for a TOAST relation (toast
content is covered when the parent table detoasts its values).

```sql
-- Table logical checksum (requires PK)
SELECT pg_table_logical_checksum('products'::regclass);
```

**`pg_table_content_checksum(regclass) -> bigint`**

Computes a content-only checksum without requiring a primary key.  It hashes
all column values of every visible row and combines them with a
sum-plus-count aggregate, so duplicate rows change the result (pure XOR would
cancel them).  The accumulator is 64-bit, while each row's content hash is
32-bit.  It does **not** include relation OIDs, which makes it suitable for
comparing a table's content across databases; partitioned tables accumulate
all leaf rows into the same accumulator, so the result does not depend on the
partitioning layout.

```sql
-- Works for tables without a primary key
SELECT pg_table_content_checksum('event_log'::regclass);
```

The content checksum is currently computed serially; it does not use
`pg_checksums.workers`.

**Partitioned tables**: `pg_table_physical_checksum`,
`pg_table_logical_checksum` and `pg_table_content_checksum` accept
partitioned tables. Since a partitioned table has no storage of its own, the
extension recurses into every leaf partition. Logical checksums require the
primary key to be defined on the partitioned table (partitions inherit it);
the content checksum has no such requirement. Nested (multi-level)
partitioning is supported as well.

```sql
-- Checksum a partitioned table (recurses into all partitions)
SELECT pg_table_physical_checksum('sales'::regclass, false);
SELECT pg_table_logical_checksum('sales'::regclass);
SELECT pg_table_content_checksum('sales'::regclass);
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

Computes logical checksum of index key values. Stable across REINDEX with the
same data.

**B-tree only.**  Hash, GiST, GIN, SP-GiST and BRIN pages do not contain
ordinary index tuples; trying to interpret them produced meaningless values
and, for SP-GiST, a server crash.  Those access methods now return NULL for
the logical checksum (physical checksums remain available).  The checksum
includes heap TIDs, so it changes when heap tuples move (for example after
`CLUSTER` or `VACUUM FULL`).  A partitioned B-tree index aggregates the
logical checksums of its leaf indexes.

```sql
-- Logical checksum of a B-tree index
SELECT pg_index_logical_checksum('idx_product_name'::regclass);
```

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

**`pg_database_logical_checksum(boolean, boolean) -> bigint`**

Aggregates logical relation checksums for entire database. Skips tables without PK.
Superuser-only (enforced in C).  TOAST relations are skipped: their content is
covered when the parent table detoasts the values.

Because the database logical checksum includes indexes, and logical index
checksums include heap TIDs and currently visible index entries, the database
checksum can change when dead index entries are removed by `VACUUM`.  Use
table-level logical checksums when you need a pure content fingerprint; see
[Limitations](#limitations-and-portability).

```sql
-- Database logical checksum
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

For table, index, and database checksums, the extension uses an order-independent aggregate so that the result does not depend on scan order:

1. Collect all individual checksums
2. Fold each through a 32-bit (or 64-bit) avalanche finalizer
3. Combine them with XOR

XOR is both commutative and associative, which makes the aggregate not only order-independent but also partition-independent. This is what allows a single backend and any number of parallel workers to produce byte-for-byte identical results without extra coordination.

### Type-Specific Handling

The extension properly handles all PostgreSQL type categories:

* **Pass-by-value types** (int4, float8, bool): Binary representation hashed
* **Varlena types** (text, bytea): Detoasted and entire structure hashed
* **C-string types**: Null-terminated string hashed
* **Pass-by-reference types**: Pointer dereferenced and data hashed

---

## Limitations and Portability

These limitations are documented rather than silently ignored; several of them
will be addressed by the planned checksum-format (pg_checksums v2).

* **Not fully portable between databases.** Table, partition and database
  logical checksums mix relation OIDs into the result, and enum/composite/array
  values include type OIDs.  `pg_table_content_checksum()` omits relation
  OIDs and is a better fit for cross-database content comparison, but the
  type-level caveats below still apply.
* **Not canonical for every type.** `numeric` values with different display
  scale (for example `1.0` and `1.00`) are SQL-equal but hash differently;
  composite and array values include type OIDs; pass-by-value hashes use the
  native byte order.
* **Physical checksums are not replica fingerprints.** Tuple/table physical
  checksums depend on tuple locations, MVCC fields, tuple bytes and the
  relation's relfilenode/reltablespace; index physical checksums hash raw
  index pages, including page LSNs.  A standby, a restored backup or a table
  re-created with the same data usually produces different physical
  checksums.  Planner statistics are not part of the checksum, so `ANALYZE`
  does not change it; use physical checksums to detect on-disk corruption
  within one cluster, not to compare replicas.
* **Logical index checksums are B-tree only.**  For B-tree they include heap
  TIDs, and they currently include index entries that are not yet removed by
  `VACUUM` (including entries from aborted transactions).  Database logical
  checksums include indexes, so they can change after `VACUUM` even though the
  logical table contents did not.
* **Tables without a primary key are skipped** by logical table and database
  checksums.  Their indexes are still included, which makes the database
  checksum change on vacuum and TID movement without covering the table data.
  Use `pg_table_content_checksum()` for those tables; the content checksum
  needs no primary key.
* **Coverage gaps.**  The database physical checksum does not include TOAST
  content by default, and never includes sequences, large objects (unless
  `include_system = true`), FSM/VM forks or relation free space.  Foreign
  table partitions are skipped by partitioned table checksums because they
  have no local storage in this database.
* **Content checksums are serial.**  `pg_table_content_checksum()` does not
  use `pg_checksums.workers` yet; it scans the table in the calling backend.
* **32-bit content hashes.**  The existing table and index logical functions
  return a 64-bit value whose content component is 32 bits.
  `pg_table_content_checksum()` returns a 64-bit aggregate, but each row's
  content hash is still 32 bits.  This is adequate for accidental corruption
  detection but not for cryptographic tamper evidence; FNV-1a is not a
  cryptographic hash.
* **The existing logical aggregates are XOR-based.**  XOR is order-independent
  and parallel-safe, but it can mask duplicate entries.
  `pg_table_content_checksum()` uses a sum-plus-count aggregate instead; a
  similar v2 algorithm for the existing table/index functions is planned.

---

## Parallel Computation

Table, index, and database checksums can be computed in parallel using
PostgreSQL's parallel worker infrastructure. Each worker scans a subset of
the relation's blocks (or a subset of the database's relations) using a
shared, self-balancing atomic counter, and the partial results are combined
by the leader.  `pg_table_content_checksum()` is the exception: it currently
runs serially.

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

Database checksum functions normally skip relations that cannot be
checksummed: tables without a primary key in logical mode, unsupported index
access methods, or relations that fail during the scan.  Set
`pg_checksums.strict = on` to turn those cases into an error instead:

```sql
SET pg_checksums.strict = on;
SELECT pg_database_logical_checksum(false, false);
-- ERROR: relation "event_log" cannot be checksummed
-- DETAIL: The relation has no usable checksum (missing primary key,
-- unsupported index access method, or a scan error).
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
-- Verify migration integrity.  pg_table_content_checksum() omits relation OIDs,
-- so values are comparable between the source and target databases.
-- Run this query on both databases and compare the two result sets.
WITH table_checksums AS (
    SELECT
        c.relname,
        pg_table_content_checksum(c.oid) AS checksum
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
    pg_database_logical_checksum(false, false) as checksum
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
7. Parallel vs serial checksum consistency at the table, index, and database levels
8. Partitioned tables (single- and multi-level, with and without primary keys)
9. Safety and validation: non-B-tree logical index checksums return NULL,
   invalid block numbers/TIDs are rejected, views/sequences/indexes are
   rejected by table functions, SELECT privileges and RLS are enforced, and
   TOAST physical checksums are computed from raw pages
10. Corrupt page handling: unsafe page headers and line pointers produce clean
    errors; database scans skip the affected relation with a warning
11. Partitioned index aggregation, including deterministic physical and
    logical checksums for partitioned B-tree indexes
12. `pg_table_content_checksum()`: no-PK tables, duplicate rows, empty and
    partitioned tables, TOAST parents, and `pg_checksums.strict`

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
git tag -a v1.2.0 -m "pg_checksums 1.2.0"
git push origin v1.2.0
```

---

## License

Released under the [MIT License](LICENSE).

## Support

* Source code and issue tracker: <https://github.com/VictorNagibator/pg_checksums>
* Distribution page on PGXN: <https://pgxn.org/dist/pg_checksums/>
