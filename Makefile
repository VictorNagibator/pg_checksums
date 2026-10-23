# Makefile
#
# The extension is built with PGXS. That is what the PGXN client relies on:
# it runs `make` and `make install` with nothing but PG_CONFIG in the
# environment, so PGXS must be the default (there is no in-tree contrib build
# path any more).

EXTENSION = pg_checksums
MODULE_big = pg_checksums
OBJS = pg_checksums.o

# The control file is the single source of truth for the extension version;
# the distribution archive name derives from it.
EXTVERSION = $(shell sed -n "s/^default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/p" $(EXTENSION).control)

# Every versioned install script and every upgrade script between them.
DATA = $(wildcard $(EXTENSION)--*.sql)

REGRESS = checksum_basic checksum_database checksum_index checksum_table checksum_stability checksum_parallel checksum_partition checksum_safety checksum_logical
REGRESS_OPTS = \
    --inputdir=./ \
    --load-extension=pg_checksums

# TAP tests are opt-in: they need the PostgreSQL test modules and IPC::Run.
# Run "make installcheck enable_tap_tests=yes PROVE=prove" to include them.
ifdef enable_tap_tests
TAP_TESTS = 1
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Build the PGXN distribution archive. `git archive` packs HEAD, so commit
# (and ideally tag) the release before running this.
dist:
	git archive --format zip --prefix=$(EXTENSION)-$(EXTVERSION)/ \
		-o $(EXTENSION)-$(EXTVERSION).zip HEAD

.PHONY: dist
