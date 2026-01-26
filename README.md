# pg_checksums - Multi-Level Data Integrity Extension for PostgreSQL

## Overview

`pg_checksums` is a PostgreSQL extension that provides comprehensive checksum functionality at multiple granularity levels: column, tuple (row), table, index, and database. It enables data integrity verification, change detection, and consistency checking across the entire database hierarchy.

---

## Key Features

* **Multi-Granularity Support**: Checksums at column, tuple, table, index, and database levels
* **NULL-Aware**: Special handling for NULL values with dedicated `CHECKSUM_NULL` constant
* **MVCC-Aware**: Incorporates transaction visibility information in tuple checksums
* **Location-Sensitive**: Same data at different physical locations produces different checksums
* **Deterministic & Sensitive**: Any data change produces different checksum values
* **Efficient Aggregation**: XOR-based aggregation for table and database-level checksums

---

## Installation

### Prerequisites

* PostgreSQL 18 or later
* C compiler and PostgreSQL development headers

### Build and Install

```bash
# Clone or extract the extension
cd pg_checksums

# Build
make USE_PGXS=1

# Install
make USE_PGXS=1 install

# Create extension in your database
psql -d your_database -c "CREATE EXTENSION pg_checksums;"
```

---

## Usage Examples

### Column-Level Integrity Check

```sql
-- Get checksum for a specific column value
SELECT pg_column_checksum('employees'::regclass, ctid, 3) 
FROM employees WHERE employee_id = 123;

-- Check for NULL values (returns -1 for NULL)
SELECT pg_column_checksum('table'::regclass, ctid, 2) = -1 
FROM table WHERE column IS NULL;
```

### Tuple-Level Change Detection

```sql
-- Monitor specific rows for changes
SELECT ctid, pg_tuple_checksum('orders'::regclass, ctid, false)
FROM orders WHERE order_date = CURRENT_DATE;

-- Compare with previously stored checksums
SELECT ctid FROM orders 
WHERE pg_tuple_checksum('orders'::regclass, ctid, false) != stored_checksum;
```

### Table-Level Consistency

```sql
-- Get current table checksum
SELECT pg_table_checksum('inventory'::regclass, false);

-- Monitor table changes over time
INSERT INTO table_checksum_log 
SELECT now(), 'inventory', pg_table_checksum('inventory'::regclass, false);
```

### Database-Level Verification (Superuser)

```sql
-- Get database checksum (excluding system tables)
SELECT pg_database_checksum(false, false);

-- Include system tables and TOAST tables
SELECT pg_database_checksum(true, true);
```

### Index Consistency Check

```sql
-- Verify index integrity
SELECT pg_index_checksum('idx_employee_name'::regclass);

-- Compare index checksums before/after maintenance
SELECT pg_index_checksum('idx_large_table'::regclass) != previous_checksum 
FROM index_monitoring WHERE index_name = 'idx_large_table';
```

---

## Checksum Algorithm

### Core Algorithm: FNV-1a 32-bit Hash

The extension uses the FNV-1a (Fowler-Noll-Vo) 32-bit hash algorithm, identical to PostgreSQL's internal checksum implementation for consistency and compatibility.

**Algorithm Formula:**

```text
hash = (hash ^ byte) * FNV_PRIME_32  // For each byte
```

**Constants:**

```
FNV_BASIS_32 = 2166136261U

FNV_PRIME_32 = 16777619U
```

---

## Granularity-Specific Implementations

### 1. Column-Level Checksums

* **Seed:** Attribute number (column position)
* **NULL Handling:** Returns CHECKSUM_NULL (0xFFFFFFFF = -1)
* **Type Awareness:** Handles all PostgreSQL types (byval, byref, varlena, cstring)
* **Detoasting:** Automatically expands compressed varlena data
* **Guarantee:** Different columns with same values → different checksums

```c
if (isnull) return CHECKSUM_NULL;
hash = fnv1a_32_hash(column_data, data_length, attribute_number);
```

### 2. Tuple-Level Checksums

* **Seed:** Physical location (block_number << 16) | offset_number
* **Options:** Include/exclude tuple header (MVCC information)
* **MVCC Integration:** XOR with (xmin ^ xmax) when header excluded
* **Location Encoding:** Ensures same data at different ctids → different checksums
* **Collision Prevention:** XOR with location hash after initial hash

```c
location_hash = (block << 16) | offset;
checksum = fnv1a_32_hash(tuple_data, data_length, location_hash);
checksum ^= location_hash;  // Additional uniqueness guarantee
if (!include_header) checksum ^= (xmin ^ xmax);
```

### 3. Table-Level Checksums

* **Aggregation:** XOR of all tuple checksums in the table
* **Relation Identity:** Includes relation OID in aggregation
* **Empty Tables:** Return 0
* **Properties:** XOR is associative, commutative, and reversible
* **Efficiency:** Constant memory usage regardless of table size

```c
table_checksum = 0;
foreach tuple in table:
    tuple_checksum = compute_tuple_checksum(tuple);
    table_checksum ^= ((uint64)tuple_checksum << 32) | relation_oid;
```

### 4. Index-Level Checksums

* **Scope:** All live index tuples across all index pages
* **B-Tree Specific:** Includes heap TID in checksum calculation
* **Optimization:** Bulk read strategy for sequential access
* **Skipping:** Dead index items and uninitialized pages are ignored

### 5. Database-Level Checksums

* **Security:** Superuser-only to prevent information disclosure
* **Scope:** All tables and indexes in the database
* **Filtering:** Optional exclusion of system catalogs and TOAST tables
* **Snapshot:** Uses consistent snapshot for repeatable results
* **Error Handling:** Graceful skipping of inaccessible relations

---

## Practical Applications

### 1. Data Integrity Monitoring

```sql
-- Create monitoring table
CREATE TABLE checksum_monitoring (
    table_name text,
    checksum bigint,
    checked_at timestamptz,
    PRIMARY KEY (table_name, checked_at)
);

-- Periodic checksum collection
INSERT INTO checksum_monitoring
SELECT 
    relname,
    pg_table_checksum(oid, false),
    now()
FROM pg_class 
WHERE relkind = 'r';
```

### 2. Replication Consistency Check

```sql
-- Compare checksums between primary and replica
-- On primary:
SELECT pg_table_checksum('critical_table'::regclass, false) AS primary_checksum;

-- On replica (if extension is installed):
SELECT pg_table_checksum('critical_table'::regclass, false) AS replica_checksum;
```

### 3. Data Migration Validation

```sql
-- Verify data consistency after migration
SELECT 
    'source_db' as db,
    pg_table_checksum('migrated_table'::regclass, false) as source_checksum
UNION ALL
SELECT 
    'target_db' as db,
    pg_table_checksum('migrated_table'::regclass, false) as target_checksum;
```

### 4. Change Detection for Auditing

```sql
-- Detect unauthorized changes
CREATE FUNCTION audit_table_changes()
RETURNS trigger AS $$
BEGIN
    IF pg_table_checksum(TG_TABLE_NAME::regclass, false) != expected_checksum THEN
        RAISE WARNING 'Table % checksum changed unexpectedly', TG_TABLE_NAME;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER audit_changes ON ddl_command_end
EXECUTE FUNCTION audit_table_changes();
```

---

## Performance Considerations

### Lightweight Operations

* **Column Checksums:** O(1) per column, minimal overhead
* **Tuple Checksums:** O(tuple size), requires buffer locking
* **Table Checksums:** O(N) full table scan, efficient with XOR aggregation
* **Index Checksums:** O(index size), uses bulk read strategy
* **Database Checksums:** O(total database size), periodic interrupt checks

---

## Test Suite

```bash
# Run regression (and TAP if enabled) tests
make USE_PGXS=1 installcheck
```
