#ifndef PG_CHECKSUMS_H
#define PG_CHECKSUMS_H

#include "postgres.h"
#include "fmgr.h"

/* FNV-1a constants */
#define FNV_PRIME_32 16777619U
#define FNV_BASIS_32 2166136261U

/* Special checksum value for NULL */
#define CHECKSUM_NULL 0xFFFFFFFF

/* Function declarations */
extern Datum pg_page_checksum(PG_FUNCTION_ARGS);
extern Datum pg_tuple_checksum(PG_FUNCTION_ARGS);
extern Datum pg_column_checksum(PG_FUNCTION_ARGS);
extern Datum pg_table_checksum(PG_FUNCTION_ARGS);
extern Datum pg_index_checksum(PG_FUNCTION_ARGS);
extern Datum pg_database_checksum(PG_FUNCTION_ARGS);
extern Datum pg_data_checksum(PG_FUNCTION_ARGS);
extern Datum pg_text_checksum(PG_FUNCTION_ARGS);

/* Internal functions */
extern uint32 pg_checksum_data_custom(const char *data, uint32 len, uint32 init_value);
extern uint32 pg_tuple_checksum_internal(Page page, OffsetNumber offnum, BlockNumber blkno, bool include_header);

#endif /* PG_CHECKSUMS_H */