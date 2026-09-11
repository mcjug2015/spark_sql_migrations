#!/usr/bin/env bash
# The quality gates, in the order ci.yml runs them.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RUN_FMT=0
for arg in "$@"; do
  case "$arg" in
    --fmt) RUN_FMT=1 ;;
    *) echo "Usage: $0 [--fmt]" >&2; exit 1 ;;
  esac
done

# Pants overrides TMPDIR per test process; conftest.py reads this instead.
mkdir -p "$HOME/.cache/pytest-tmp"
export PYTEST_TMP_BASE="$HOME/.cache/pytest-tmp"

if [ "$RUN_FMT" -eq 1 ]; then
  pants fmt spark_sql_migrations:: spark_sql_migrations_test:: scripts::
fi

# `dir::` and not `dir/`: a bare directory argument covers only the targets in that
# directory, which silently skipped the spark_sql/ and integration/ subtrees.
pants lint check spark_sql_migrations:: spark_sql_migrations_test:: scripts::

# Unit tests run first, with coverage. Integration tests run afterward, as their own
# invocation, never in parallel with the unit run, and are excluded from --use-coverage --
# coverage should come only from unit tests (see CLAUDE.md "Laws of integration testing").
pants test --output=all --test-force --use-coverage --report spark_sql_migrations_test/:: -spark_sql_migrations_test/integration::
pants test --output=all --test-force --report spark_sql_migrations_test/integration::
