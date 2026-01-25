# contrib/pg_checksums/Makefile

MODULE_big = pg_checksums
OBJS = pg_checksums.o

EXTENSION = pg_checksums
DATA = pg_checksums--1.0.sql

REGRESS = checksum_basic checksum_table checksum_index checksum_database
REGRESS_OPTS = \
    --inputdir=./ \
    --load-extension=pg_checksums

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_checksums
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif