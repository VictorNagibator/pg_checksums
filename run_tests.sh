#!/bin/bash
# run_tests.sh - Automate building and testing the pg_checksums extension

set -e  # Exit on any error (except specially handled ones)

# Settings (all overridable from the environment)
PG_CONFIG="${PG_CONFIG:-$HOME/postgres-custom/bin/pg_config}"
PGCTL="${PGCTL:-$(dirname "$PG_CONFIG")/pg_ctl}"
PGDATA="${PGDATA:-$HOME/pgdata-custom}"
JOBS="${JOBS:-4}"

# Function to stop the PostgreSQL server and clean up build artifacts
# (called on exit)
cleanup() {
    echo "Stopping PostgreSQL server..."
    "$PGCTL" stop -D "$PGDATA" -m fast 2>/dev/null || true
    echo "Cleaning up build artifacts..."
    make clean 2>/dev/null || true
}

# Trap EXIT to ensure cleanup is called on normal or abnormal exit
trap cleanup EXIT

# Change to the script's directory (assumed to be the extension root)
cd "$(dirname "$0")"

# 1. Clean
echo ">>> Cleaning previous build"
make clean

# 2. Build
echo ">>> Building with -j$JOBS"
make PG_CONFIG="$PG_CONFIG" -j"$JOBS"

# 3. Install into the target PostgreSQL
echo ">>> Installing extension"
sudo make PG_CONFIG="$PG_CONFIG" install

# 4. Start the server (ignore errors if already running)
echo ">>> Starting PostgreSQL server"
"$PGCTL" start -D "$PGDATA" -l "$PGDATA/server.log" || {
    echo "Failed to start server, maybe it's already running. Checking status..."
    "$PGCTL" status -D "$PGDATA"
}

# Give the server a moment to fully start
sleep 2

# 5. Run regression tests (including TAP if enabled)
echo ">>> Running tests (make installcheck)"
# We don't want set -e to abort on test failure, so we handle exit code manually
set +e
make PG_CONFIG="$PG_CONFIG" installcheck
TEST_RESULT=$?
set -e

if [ $TEST_RESULT -ne 0 ]; then
    echo ">>> WARNING: tests failed with exit code $TEST_RESULT"
    # Server will still be stopped via trap cleanup
else
    echo ">>> All tests passed successfully"
fi

# Server will be stopped automatically on script exit (trap cleanup)

exit $TEST_RESULT
