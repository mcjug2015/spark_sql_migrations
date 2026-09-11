# spark_sql_migrations

[![CI](https://github.com/mcjug2015/spark_sql_migrations/actions/workflows/ci.yml/badge.svg)](https://github.com/mcjug2015/spark_sql_migrations/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Chained, idempotent SQL migrations for Spark and Databricks.

Alembic-shaped schema migrations for a Spark/Delta catalog: your migrations are plain
`.sql` files linked into a chain by revision headers, the applied revision is recorded in
a version table inside your own schema, and each run applies only the tail that has not
been applied yet.

## Why

Spark and Databricks have no migration story of their own. Delta gives you `CREATE TABLE
IF NOT EXISTS`, but nothing that tracks which DDL a given catalog has seen, and nothing
that lets you write a change once and have it apply on a laptop and on Databricks alike.
spark_sql_migrations is the small amount of machinery that closes that gap, and nothing
more: no ORM, no autogeneration, no Python migration scripts. You write SQL.

## Installing

`spark_sql_migrations` declares **no Spark of its own** — the extras decide which one you get:

```bash
pip install "spark_sql_migrations[local]"        # pyspark + delta-spark, for laptops and CI
pip install "spark_sql_migrations[databricks]"   # databricks-connect
pip install spark_sql_migrations                 # no Spark at all
```

Pick exactly one flavour. `databricks-connect` ships its own top-level `pyspark/` and
`delta/` packages, so installing it alongside `pyspark`/`delta-spark` silently clobbers
both.

Unreleased changes can be consumed as the wheel built by CI (the `dist` artifact on any
green run) or built locally with `pants package //:dist`.

## The two kinds of migration

Migrations come from two different owners, and they behave differently:

| | ships in the wheel | owned by you |
|---|---|---|
| **what** | catalog, schema, `_spark_migrations_version` table | your tables and columns |
| **where** | `spark_sql_migrations/migrations_initial/` | `all_spark_migrations/`, `dbr_only_migrations/` |
| **selected by** | filename suffix — `_all.sql` everywhere, `_dbr_only.sql` on Databricks | the `prev_revision_id` chain |
| **when applied** | every run | once, then recorded |

The bootstrap chain is re-applied on **every** run, so it must be idempotent. Your own
chains are applied once each and their head revision is written to the version table.

## Laying out your chains

Point spark_sql_migrations at a directory holding one or both chains:

```
your_project/
└── migrations/                 <- this is --migrations-dir
    ├── all_spark_migrations/   <- runs everywhere
    └── dbr_only_migrations/    <- runs only when is_dbr()
```

Choose deliberately. SQL that only Databricks understands must not live in
`all_spark_migrations/`, or your local runs will break.

## Running migrations

```bash
python -m spark_sql_migrations.spark_sql.spark_sql run \
    --cat spark_catalog \
    --schema default \
    --migrations-dir path/to/migrations
```

Or from Python, which is what a consuming project usually wraps:

```python
import os

from spark_sql_migrations.spark_sql.spark_sql import main


def get_migrations_dir():
    """the parent of this project's own migration chains."""
    return os.path.dirname(__file__)


main(cat="spark_catalog", schema="default", migrations_dir=get_migrations_dir())
```

`main` builds its own session via `get_spark()`. To supply your own — a session already
configured by your job, say — call `run_migrations(spark, cat, schema, output_folder,
migrations_root)` directly.

Every rendered statement is written to a timestamped folder under `migrations_out/`
before it is executed, so you can always read the exact SQL a run applied.

## Writing a migration

Generate one rather than hand-rolling the header:

```bash
python -m spark_sql_migrations.spark_sql.spark_sql create_new_migration \
    --message "add batch id to metrics" \
    --output-path path/to/migrations/all_spark_migrations
```

That writes `<yymmdd>_<slug>_<revision_id>.sql` from the template shipped in the package
(override with `--template-path`), with `revision_id` filled in:

```sql
-- revision_id:e5ce0039b32b;
-- prev_revision_id:;
begin
create table if not exists {{cat}}.{{schema}}.test_table(int_id bigint, stuff string);
end;
```

Then set `prev_revision_id` to the revision this one follows. Ordering comes from that
chain, **not** from the filename — the date prefix is for humans. A chain must have
exactly one root (empty `prev_revision_id`) and no forks.

`{{cat}}` and `{{schema}}` are Jinja placeholders rendered at apply time; never hardcode a
catalog or schema.

### Idempotency

Prefer `IF NOT EXISTS`. Where Databricks doesn't offer it — notably
`ALTER TABLE ... ADD COLUMN` — guard with a SQLSTATE exit handler:

```sql
-- revision_id:57037b6c19b4;
-- prev_revision_id:07990e2a101e;
begin
  -- idempotent add: swallow FIELD_ALREADY_EXISTS (SQLSTATE 42710) if the column
  -- is already present. Databricks has no ADD COLUMN IF NOT EXISTS, so use a
  -- SQL-scripting EXIT handler (CONTINUE handlers are unsupported).
  declare exit handler for sqlstate '42710'
  begin end;
  alter table {{cat}}.{{schema}}.metrics add column metric_batch_id string;
end;
```

## Local vs Databricks

`get_spark()` returns a `DatabricksSession` when `is_dbr()` is true and a Delta-configured
local `SparkSession` otherwise, so the same code runs in both places. Local behaviour is
tuned by environment variables:

| Variable | Effect |
|---|---|
| `SPARK_WAREHOUSE_DIR` | warehouse location (default: `./spark-warehouse`) |
| `SPARK_METASTORE_DIR` | Derby metastore location; unset means Spark's default |
| `SPARK_REMOTE` | connect to an existing Spark Connect server instead of starting one |

## Development

Built with [Pants](https://www.pantsbuild.org/). Two resolves: `python-default` for the
library, `py-reqs-dev` for tests.

```bash
# black, isort, flake8, mypy. `::` and not `/`, or the subtrees go unchecked.
pants fmt lint check spark_sql_migrations:: spark_sql_migrations_test:: scripts::

# unit, then integration as its own invocation
pants test --use-coverage spark_sql_migrations_test/:: -spark_sql_migrations_test/integration::
pants test spark_sql_migrations_test/integration::

# wheel + sdist
pants package //:dist
```

`scripts/run_local.sh` runs everything CI runs, including building the distribution and
verifying all three install flavours. Branch coverage over `spark_sql_migrations/` is
gated at 94%.

Dependencies are locked. Edit `spark_sql_migrations/requirements.txt` or
`spark_sql_migrations_test/requirements-dev.txt`, then `pants generate-lockfiles` — never
hand-edit a
lockfile.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
