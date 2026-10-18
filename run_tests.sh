#!/bin/bash
# run_tests.sh - build, install and test pg_checksums
#
# Environment overrides:
#   PG_CONFIG   path to pg_config (default: $HOME/postgres-custom/bin/pg_config)
#   PGCTL       path to pg_ctl   (default: next to pg_config)
#   PGDATA      data directory  (default: $HOME/pgdata-custom)
#   JOBS        build parallelism (default: 4)
#   SUDO        command used for "make install" (default: sudo, empty as root)
#   RUN_TAP     set to "yes" to run the TAP tests as well (default: no)
#   CLEAN       set to "yes" to run "make clean" on exit (default: no)

set -e

PG_CONFIG="${PG_CONFIG:-$HOME/postgres-custom/bin/pg_config}"
PGCTL="${PGCTL:-$(dirname "$PG_CONFIG")/pg_ctl}"
PGDATA="${PGDATA:-$HOME/pgdata-custom}"
JOBS="${JOBS:-4}"
RUN_TAP="${RUN_TAP:-no}"
CLEAN="${CLEAN:-no}"

if [ "$(id -u)" -eq 0 ]; then
    SUDO="${SUDO:-}"
else
    SUDO="${SUDO-sudo}"
fi

cd "$(dirname "$0")"

STARTED=0

cleanup() {
    if [ "$STARTED" -eq 1 ]; then
        echo ">>> Stopping PostgreSQL server"
        "$PGCTL" stop -D "$PGDATA" -m fast 2>/dev/null || true
    fi
    if [ "$CLEAN" = "yes" ]; then
        echo ">>> Cleaning build artifacts"
        make clean 2>/dev/null || true
    fi
}
trap cleanup EXIT

# 1. Clean
echo ">>> Cleaning previous build"
make clean

# 2. Build
echo ">>> Building with -j$JOBS"
make PG_CONFIG="$PG_CONFIG" -j"$JOBS"

# 3. Install
echo ">>> Installing extension"
$SUDO make PG_CONFIG="$PG_CONFIG" install

# 4. Start the server only if it is not already running.  If it was already
# running, leave it alone on exit.
echo ">>> Checking PostgreSQL server"
if "$PGCTL" status -D "$PGDATA" >/dev/null 2>&1; then
    echo ">>> Server is already running; it will be left running"
else
    echo ">>> Starting PostgreSQL server"
    "$PGCTL" start -D "$PGDATA" -l "$PGDATA/server.log" -w
    STARTED=1
fi

# 5. Run tests.  TAP is opt-in.
TAP_OPTS=""
if [ "$RUN_TAP" = "yes" ]; then
    TAP_OPTS="enable_tap_tests=yes PROVE=prove"
fi

echo ">>> Running tests (make installcheck $TAP_OPTS)"
set +e
make PG_CONFIG="$PG_CONFIG" installcheck $TAP_OPTS
TEST_RESULT=$?
set -e

if [ "$TEST_RESULT" -ne 0 ]; then
    echo ">>> WARNING: tests failed with exit code $TEST_RESULT"
else
    echo ">>> All tests passed successfully"
fi

exit "$TEST_RESULT"
