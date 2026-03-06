#!/bin/bash
# run_tests.sh - Automate building and testing the pg_checksums extension

set -e  # Exit on any error (except specially handled ones)

# Settings
PG_CONFIG="$HOME/postgres-custom/bin/pg_config"
PGCTL="$HOME/postgres-custom/bin/pg_ctl"
PGDATA="$HOME/pgdata-custom"

# Function to stop the PostgreSQL server (called on exit)
cleanup() {
    echo "Stopping PostgreSQL server..."
    "$PGCTL" stop -D "$PGDATA" -m fast 2>/dev/null || true
}

# Trap EXIT to ensure cleanup is called on normal or abnormal exit
trap cleanup EXIT

# Change to the script's directory (assumed to be the extension root)
cd "$(dirname "$0")"

# 1. Clean
echo ">>> Cleaning previous build"
make USE_PGXS=1 clean

# 2. Build
echo ">>> Building with -j4"
make USE_PGXS=1 -j4

# 3. Install into the target PostgreSQL
echo ">>> Installing extension"
sudo make USE_PGXS=1 PG_CONFIG="$PG_CONFIG" install

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
make USE_PGXS=1 installcheck
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