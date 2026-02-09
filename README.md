# pg_checksums - Multi-Level Data Integrity Extension for PostgreSQL

## Overview

`pg_checksums` is a PostgreSQL extension that provides comprehensive checksum functionality at six granularity levels, with both physical and logical variants (except for column level). It enables data integrity verification, change detection, consistency checking, and corruption detection across the entire database hierarchy.

---

## Key Features

### Multi-Granularity & Dual-Mode Support

* **Six Granularity Levels**: Page, Column, Tuple, Table, Index, and Database
* **Dual Checksum Types**: Physical (storage-dependent) and Logical (content-dependent)
* **Type-Aware**: Full support for all PostgreSQL data types
* **NULL-Aware**: Special `CHECKSUM_NULL` constant for NULL values
* **MVCC-Aware**: Incorporates transaction visibility in physical checksums
* **Primary Key Aware**: Logical checksums require and use primary keys

---

## Physical and Logical Checksums

### Physical Checksums

* **Depend on**: Physical storage layout (block numbers, offsets, page headers)
* **Change when**: `VACUUM FULL`, `CLUSTER`, `REINDEX`, physical data movement
* **Useful for**: Physical corruption detection, storage integrity
* **Includes**: Location encoding, MVCC information, page structure

### Logical Checksums

* **Depend on**: Only logical data content (column values, primary keys)
* **Stable across**: Physical reorganization, `VACUUM FULL`, `CLUSTER`, `REINDEX`
* **Useful for**: Logical consistency, replication validation, migration verification
* **Requires**: Primary keys for tuple/table/database logical checksums

---

## Installation

### Prerequisites

* PostgreSQL 18 or later
* C compiler and PostgreSQL development headers

### Build and Install

```bash
# Clone or extract the extension
git clone https://github.com/VictorNagibator/pg_checksums

# Go into extension directory
cd pg_checksums

# Build
make USE_PGXS=1

# Install
make USE_PGXS=1 install

# Create extension in your database
psql -d your_database -c "CREATE EXTENSION pg_checksums;"
```

---

## Function Reference

### Page Level Functions (Physical Only)

**`pg_page_checksum(regclass, integer) -> integer`**

Computes PostgreSQL's built-in page checksum for physical page corruption detection.

```sql
-- Get checksum for page 5 of a table
SELECT pg_page_checksum('my_table'::regclass, 5);
```

### Column Level Functions (Logical Only)

**`pg_column_checksum(regclass, tid, integer) -> integer`**

Computes logical checksum of a column (cell) value. NULL values return `CHECKSUM_NULL` (-1).

```sql
-- Get checksum for column 3 of a specific tuple
SELECT pg_column_checksum('employees'::regclass, '(5,2)'::tid, 3);
```

**Note**: Column checksums are computed for individual values within specific rows, not for entire columns. To compute checksums for all values in a column, you would need to aggregate individual checksums.

### Tuple Level Functions

**`pg_tuple_physical_checksum(regclass, tid, boolean) -> integer`**

Computes physical tuple checksum based on physical location (block, offset).

```sql
-- Physical checksum without header
SELECT pg_tuple_physical_checksum('orders'::regclass, ctid, false) 
FROM orders WHERE order_id = 123;

-- Physical checksum with header (includes MVCC info)
SELECT pg_tuple_physical_checksum('orders'::regclass, ctid, true) 
FROM orders WHERE order_id = 123;
```

**`pg_tuple_logical_checksum(regclass, tid, boolean) -> integer`**

Computes logical tuple checksum based on primary key values. Returns NULL if no PK.

```sql
-- Logical checksum (requires primary key)
SELECT pg_tuple_logical_checksum('customers'::regclass, ctid, false) 
FROM customers WHERE customer_id = 456;
```

### Table Level Functions

**`pg_table_physical_checksum(regclass, boolean) -> bigint`**

Aggregates physical tuple checksums for entire table (order-independent).

```sql
-- Table physical checksum
SELECT pg_table_physical_checksum('inventory'::regclass, false);

-- With tuple headers included
SELECT pg_table_physical_checksum('inventory'::regclass, true);
```

**`pg_table_logical_checksum(regclass) -> bigint`**

Aggregates logical tuple checksums for entire table. Requires primary key.

```sql
-- Table logical checksum (requires PK)
SELECT pg_table_logical_checksum('products'::regclass);
```

### Index Level Functions

**`pg_index_physical_checksum(regclass) -> integer`**

Computes physical checksum of index structure. Supports B-tree, Hash, GiST, GIN, SP-GiST, BRIN.

```sql
-- Physical checksum of a B-tree index
SELECT pg_index_physical_checksum('idx_product_name'::regclass);
```

**`pg_index_logical_checksum(regclass) -> integer`**

Computes logical checksum of index key values. Stable across REINDEX.

```sql
-- Logical checksum of index (key values only)
SELECT pg_index_logical_checksum('idx_product_name'::regclass);
```

### Database Level Functions (Superuser Only)

**`pg_database_physical_checksum(boolean, boolean) -> bigint`**

Aggregates physical relation checksums for entire database.

```sql
-- Database physical checksum (excluding system tables)
SELECT pg_database_physical_checksum(false, false);

-- Include system catalogs and TOAST tables
SELECT pg_database_physical_checksum(true, true);
```

**Note about shared system tables**: The pg_database_checksum function only includes tables from the current database. Cluster-wide shared system tables (e.g., pg_authid, pg_database) are not included in the checksum calculation as they belong to the entire PostgreSQL cluster, not to individual databases.

**`pg_database_logical_checksum(boolean, boolean) -> bigint`**

Aggregates logical relation checksums for entire database. Skips tables without PK.

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

Convenience wrapper for text data checksum.

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

For table and database checksums, extension uses order-independent aggregation to ensure consistent results regardless of scan order:

1. Collect all individual checksums
2. Sort them (ensures order independence)
3. Combine using FNV-1a

### Type-Specific Handling

The extension properly handles all PostgreSQL type categories:

* **Pass-by-value types** (int4, float8, bool): Binary representation hashed
* **Varlena types** (text, bytea): Detoasted and entire structure hashed
* **C-string types**: Null-terminated string hashed
* **Pass-by-reference types**: Pointer dereferenced and data hashed

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
-- Compare source and replica consistency
-- Run on source:
SELECT 
    table_name,
    pg_table_logical_checksum(table_name::regclass) as logical_cs,
    pg_table_physical_checksum(table_name::regclass, false) as physical_cs
FROM information_schema.tables 
WHERE table_schema = 'public';

-- Run same query on replica and compare results
```

### 3. Data Migration Verification

```sql
-- Verify migration integrity
WITH source_checksums AS (
    SELECT 
        table_name,
        pg_table_logical_checksum(table_name::regclass) as checksum
    FROM information_schema.tables
    WHERE table_schema = 'public'
),
target_checksums AS (
    -- Same query on target database
)
SELECT 
    s.table_name,
    s.checksum = t.checksum as consistent
FROM source_checksums s
JOIN target_checksums t USING (table_name);
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
| `pg_column_checksum`     |             O(1) | Single column, minimal overhead |
| `pg_tuple_*_checksum`    |    O(tuple size) | Requires buffer lock            |
| `pg_table_*_checksum`    |   O(N) full scan | Order-independent aggregation   |
| `pg_index_*_checksum`    |    O(index size) | Bulk read strategy              |
| `pg_database_*_checksum` | O(total DB size) | Periodic interrupt checks       |

### Memory Usage

* **Column/Tuple level:** Constant (minimal)
* **Table level:** O(N) for checksum collection, but streaming aggregation
* **Index level:** O(index entries) for logical checksums
* **Database level:** Streaming aggregation across relations

---

## Test Suite

The pg_checksums extension includes a comprehensive test suite that validates functionality at all granularity levels and ensures algorithm correctness. The tests are divided into two categories: regression tests (SQL-based) and TAP tests (Perl-based).

```bash
# Run regression (and TAP if enabled) tests
make USE_PGXS=1 installcheck
```

**Tests cover:**

1. Basic functionality at all granularity levels
2. Physical and logical checksum differences
3. NULL handling and type support
4. MVCC behavior and concurrency
5. Error conditions and edge cases
