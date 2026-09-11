"""Chained, idempotent SQL migrations for Spark and Databricks.

Two kinds of migration exist, and they are owned by different parties:

* the *initial* chain and the new-migration template ship inside this package.
  They bootstrap the catalog, the schema and the version table, are selected by
  filename suffix (``_all.sql`` / ``_dbr_only.sql``) rather than by revision,
  and are re-applied on every run -- so they must be idempotent.
* the ``all_spark_migrations/`` and ``dbr_only_migrations/`` chains belong to
  the consuming project. The caller passes their parent directory as
  ``migrations_root``; each chain is walked from its root via
  ``prev_revision_id`` and only the tail not yet recorded in the version table
  is applied.
"""

import argparse
import datetime
import logging
import os
import re
import uuid
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape
from pyspark.sql.functions import col

from spark_sql_migrations.spark_utils import get_spark, is_dbr

logger = logging.getLogger(__name__)


ALL_SPARK = "all_spark"
DBR_ONLY = "dbr_only"

VERSION_TABLE = "_spark_migrations_version"

PACKAGE_NAME = "spark_sql_migrations"
INITIAL_MIGRATIONS_PACKAGE_PATH = "migrations_initial"
TEMPLATES_PACKAGE_PATH = "migration_templates"
DEFAULT_TEMPLATE_NAME = "default_migration_template.sql"

_HEADER_RE = re.compile(r"^--\s*(revision_id|prev_revision_id)\s*:\s*(\w*)\s*;$")


@dataclass(frozen=True)
class Migration:
    revision_id: str
    prev_revision_id: str | None
    template_name: str


def get_migrations_dir(migrations_root, migration_type):
    """the client-owned directory holding one chain."""
    return os.path.join(migrations_root, f"{migration_type}_migrations")


def get_default_template_path():
    """the template this package ships; callers may pass their own instead."""
    return os.path.join(os.path.dirname(__file__), "..", TEMPLATES_PACKAGE_PATH, DEFAULT_TEMPLATE_NAME)


def _parse_migration(migrations_dir, template_name):
    headers = {}
    migration_file_path = os.path.join(migrations_dir, template_name)
    if os.path.getsize(migration_file_path) == 0:
        raise ValueError(f"{template_name} is empty;")
    with open(migration_file_path) as migration_file:
        for line in migration_file:
            line = line.strip()
            match = _HEADER_RE.match(line)
            if match:
                headers[match.group(1)] = match.group(2) or None
            elif line and not line.startswith("--"):
                break

    if not headers.get("revision_id"):
        raise ValueError(f"{template_name} has no revision_id header;")

    return Migration(
        revision_id=headers["revision_id"],
        prev_revision_id=headers.get("prev_revision_id"),
        template_name=template_name,
    )


def get_ordered_migration_objs(migration_objs):
    by_revision = {}
    for migration in migration_objs:
        clash = by_revision.get(migration.revision_id)
        if clash:
            raise ValueError(
                f"revision_id {migration.revision_id} is claimed by both "
                f"{clash.template_name} and {migration.template_name};"
            )
        by_revision[migration.revision_id] = migration

    roots = [migration for migration in migration_objs if not migration.prev_revision_id]
    if len(roots) != 1:
        raise ValueError(
            f"expected exactly one migration with an empty prev_revision_id, "
            f"found {[m.template_name for m in roots]};"
        )

    next_by_revision = {}
    for migration in migration_objs:
        if not migration.prev_revision_id:
            continue
        if migration.prev_revision_id not in by_revision:
            raise ValueError(
                f"{migration.template_name} points at unknown prev_revision_id {migration.prev_revision_id};"
            )
        sibling = next_by_revision.get(migration.prev_revision_id)
        if sibling:
            raise ValueError(
                f"revision_id {migration.prev_revision_id} is the parent of both "
                f"{sibling.template_name} and {migration.template_name};"
            )
        next_by_revision[migration.prev_revision_id] = migration

    ordered = []
    current = roots[0]
    while current:
        ordered.append(current)
        current = next_by_revision.get(current.revision_id)

    if len(ordered) != len(migration_objs):
        walked = {migration.template_name for migration in ordered}
        raise ValueError(
            f"Migrations are not reachable from the root: "
            f"{sorted(m.template_name for m in migration_objs if m.template_name not in walked)};"
        )

    return ordered


def get_migrations_list(migrations_dir):
    """every migration in one directory, walked from its root to its head."""
    if not os.path.isdir(migrations_dir):
        raise ValueError(f"no migrations dir at {migrations_dir};")

    migrations = [
        _parse_migration(migrations_dir, template_name)
        for template_name in sorted(os.listdir(migrations_dir))
        if template_name.endswith(".sql")
    ]
    if not migrations:
        return []

    return get_ordered_migration_objs(migrations)


def get_unapplied_migrations_list(spark, migrations_dir, migration_type, cat: str, schema: str):
    """the tail of one chain after the revision VERSION_TABLE records for it.

    the whole chain when the table is missing or holds no row for this type.
    """
    ordered = get_migrations_list(migrations_dir)

    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    if not spark.catalog.tableExists(version_table):
        return ordered

    current = (
        spark.read.table(version_table).where(col("migration_type") == migration_type).select("version_num").head()
    )
    if not current:
        return ordered

    current_revision_id = current["version_num"]

    for index, migration in enumerate(ordered):
        if migration.revision_id == current_revision_id:
            return ordered[index + 1 :]

    raise ValueError(
        f"{VERSION_TABLE} has {migration_type} at {current_revision_id}, "
        f"which matches no migration in {migrations_dir};"
    )


def create_new_migration(message: str, template_path: str, output_path: str, truncate_slug_length: int = 40):
    """
    TODO XXX start looking up prev revision id from db or files and including it
    """
    rev_id = uuid.uuid4().hex[-12:]

    date_prefix = datetime.datetime.today().strftime("%y%m%d")

    slug = "_".join(re.split(r"\W+", message.lower())).strip("_")
    if len(slug) > truncate_slug_length:
        slug = slug[:truncate_slug_length].rsplit("_", 1)[0]

    migration_filename = f"{date_prefix}_{slug}_{rev_id}.sql"

    migration_path = os.path.join(output_path, migration_filename)

    default_migration_content = open(template_path).read()
    default_migration_content = default_migration_content.replace("{{revision_id}}", rev_id)

    with open(migration_path, "w") as migration_file:
        migration_file.write(default_migration_content)


def apply_template(output_dir, template, cat: str, schema: str):
    result_sql = template.render(cat=cat, schema=schema)
    with open(os.path.join(output_dir, template.name.replace(".sql", "_primed.sql")), "w") as file_handle:
        file_handle.write(result_sql)
    return result_sql


def get_ascending_letters_within_minute():
    micros_since_minute = datetime.datetime.now() - datetime.datetime.now().replace(second=0, microsecond=0)
    result = str(micros_since_minute.microseconds).translate(str.maketrans("0123456789", "ABCDEFGHIJ"))
    return result


def get_output_folder(output_parent_path):
    folder_name = f"{datetime.datetime.today().strftime('%Y%m%d_%H%M')}_{get_ascending_letters_within_minute()}"
    return os.path.join(output_parent_path, folder_name)


def use_migration_file(fname):
    if fname.endswith("all.sql"):
        return True
    elif is_dbr() and fname.endswith("dbr_only.sql"):
        return True
    return False


def migrate_initial(spark, output_folder, cat: str, schema: str):
    """apply the package's own bootstrap chain; selected by suffix, not revision."""
    env = Environment(
        loader=PackageLoader(package_name=PACKAGE_NAME, package_path=INITIAL_MIGRATIONS_PACKAGE_PATH),
        autoescape=select_autoescape(),
    )
    all_templates = env.list_templates(filter_func=use_migration_file)

    logger.info(f"found {len(all_templates)} migrations; first five are {all_templates[:5]};")
    for template_name in all_templates:
        result_sql = apply_template(output_folder, env.get_template(template_name), cat=cat, schema=schema)
        spark.sql(result_sql)
    logger.info(f"invoked spark on {len(all_templates)} migrations;")


def record_revision(spark, cat: str, schema: str, migration_type: str, revision_id: str):
    """point VERSION_TABLE's row for one migration type at revision_id.

    delete + insert rather than merge: there is one row per type, and a replay
    after a crash between the two statements is harmless because migrations are
    idempotent. the values are inlined rather than passed as `args` because
    delta's DELETE does not bind named parameters.
    """
    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    type_literal = migration_type.replace("'", "''")
    revision_literal = revision_id.replace("'", "''")
    spark.sql(f"delete from {version_table} where migration_type = '{type_literal}'")
    spark.sql(
        f"insert into {version_table} (migration_type, version_num) " f"values ('{type_literal}', '{revision_literal}')"
    )


def migrate_w_rev(spark, output_folder, migrations_root, cat: str, schema: str, migration_type: str):
    """apply the migrations of one client chain that VERSION_TABLE hasn't recorded yet."""
    migrations_dir = get_migrations_dir(migrations_root, migration_type)
    unapplied = get_unapplied_migrations_list(spark, migrations_dir, migration_type, cat=cat, schema=schema)

    logger.info(
        f"{migration_type} has {len(unapplied)} unapplied migrations; "
        f"first five are {[migration.template_name for migration in unapplied[:5]]};"
    )

    env = Environment(loader=FileSystemLoader(migrations_dir), autoescape=select_autoescape())
    for migration in unapplied:
        result_sql = apply_template(output_folder, env.get_template(migration.template_name), cat=cat, schema=schema)
        spark.sql(result_sql)
        record_revision(spark, cat, schema, migration_type, migration.revision_id)
        logger.info(f"applied {migration.template_name}; {migration_type} is now at {migration.revision_id};")


def run_migrations(spark, cat, schema, output_folder, migrations_root):
    initial_output_folder = os.path.join(output_folder, INITIAL_MIGRATIONS_PACKAGE_PATH)
    dbr_ouput_folder = os.path.join(output_folder, f"{DBR_ONLY}_migrations")
    all_spark_ouput_folder = os.path.join(output_folder, f"{ALL_SPARK}_migrations")
    os.makedirs(initial_output_folder, exist_ok=True)
    os.makedirs(dbr_ouput_folder, exist_ok=True)
    os.makedirs(all_spark_ouput_folder, exist_ok=True)
    migrate_initial(spark, initial_output_folder, cat, schema)
    if is_dbr():
        migrate_w_rev(spark, dbr_ouput_folder, migrations_root, cat, schema, DBR_ONLY)
    migrate_w_rev(spark, all_spark_ouput_folder, migrations_root, cat, schema, ALL_SPARK)


def main(cat, schema, migrations_dir, output_parent_path=None):
    output_folder = get_output_folder(output_parent_path or os.path.join(os.getcwd(), "migrations_out"))
    os.makedirs(output_folder)
    run_migrations(get_spark(), cat, schema, output_folder, migrations_dir)


def _cli_main(cat, schema, migrations_dir):  # pragma: no cover
    main(cat, schema, migrations_dir)


def _cli_create_new_migration(message, output_path, template_path):  # pragma: no cover
    create_new_migration(message, template_path or get_default_template_path(), output_path)


def build_parser():  # pragma: no cover
    parser = argparse.ArgumentParser(description="spark_sql_migrations migration utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--cat", default="spark_catalog")
    p_run.add_argument("--schema", default="default")
    p_run.add_argument(
        "--migrations-dir", required=True, help="parent of all_spark_migrations/ and dbr_only_migrations/"
    )
    p_run.set_defaults(func=_cli_main)

    p_create = subparsers.add_parser("create_new_migration", help="create a new migration from the template")
    p_create.add_argument("--message", required=True)
    p_create.add_argument("--output-path", required=True, help="the chain directory to write the new migration into")
    p_create.add_argument("--template-path", default=None, help="defaults to the template shipped in this package")
    p_create.set_defaults(func=_cli_create_new_migration)

    return parser


if __name__ == "__main__":  # pragma: no cover
    args = build_parser().parse_args()
    kwargs = vars(args)
    func = kwargs.pop("func")
    kwargs.pop("command")
    func(**kwargs)
