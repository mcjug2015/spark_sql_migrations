"""Chained, idempotent SQL migrations for Spark and Databricks.

Two kinds of migration exist, and they are owned by different parties:

* the *initial* chain and the new-migration template ship inside this package.
  They bootstrap the catalog, the schema and the version table, carry no
  revision of their own, and are applied in filename order on every run until
  the version table exists -- so a run that failed partway through replays
  them, and they must be idempotent.
* the client chain belongs to the consuming project. The caller passes the
  directory holding it as ``migrations_dir``; the chain is walked from its root
  via ``prev_revision_id`` and only the tail not yet recorded in the version
  table is applied.

Which engine a migration targets is decided inside the migration, not by where it
sits: every one of them is rendered through jinja before it is executed, with
``is_dbr`` in the render context, so a chain that runs everywhere can still gate
the statements one engine would reject. A migration left holding no statements is
recorded and not executed, which is what lets a project keep one chain and no
second head.
"""

import argparse
import datetime
import logging
import os
import re
import uuid
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader, PackageLoader, select_autoescape

from spark_sql_migrations.spark_utils import get_spark, is_dbr

logger = logging.getLogger(__name__)


VERSION_TABLE = "_spark_migrations_version"

PACKAGE_NAME = "spark_sql_migrations"
INITIAL_MIGRATIONS_PACKAGE_PATH = "migrations_initial"
# where a run's rendered client migrations land, under its output folder
CLIENT_OUTPUT_DIRNAME = "migrations"
TEMPLATES_PACKAGE_PATH = "migration_templates"
DEFAULT_TEMPLATE_NAME = "default_migration_template.sql"

_HEADER_RE = re.compile(r"^--\s*(revision_id|prev_revision_id)\s*:\s*(\w*)\s*;$")
_COMMENT_OR_BLANK_RE = re.compile(r"^\s*(--.*)?$")


@dataclass(frozen=True)
class Migration:
    revision_id: str
    prev_revision_id: str | None
    template_name: str


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


def get_table_version(spark, cat: str, schema: str):
    """the revision VERSION_TABLE records, or None if it holds no row yet."""
    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    if not spark.catalog.tableExists(version_table):
        return None
    return spark.read.table(version_table).select("version_num").head()


def get_unapplied_migrations_list(spark, migrations_dir, cat: str, schema: str):
    ordered = get_migrations_list(migrations_dir)
    current = get_table_version(spark, cat, schema)
    if not current:
        return ordered

    current_revision_id = current["version_num"]

    if not ordered:
        logger.warning(
            f"{VERSION_TABLE} is at {current_revision_id}, but {migrations_dir} holds no migrations; "
            f"treating the chain as retired;"
        )
        return ordered

    for index, migration in enumerate(ordered):
        if migration.revision_id == current_revision_id:
            return ordered[index + 1 :]

    raise ValueError(f"{VERSION_TABLE} is at {current_revision_id}, which matches no migration in {migrations_dir};")


def gate_on_is_dbr(migration_content):
    """wrap a migration's body in the jinja conditional, below its headers.

    the revision headers stay outside it: they are read off the file as comments,
    and the first line that is neither comment nor blank ends that read -- so an
    opening tag above them would hide them. a body-less template gets an empty
    gate at the end, which renders to nothing either way.
    """
    lines = migration_content.splitlines()
    body_start = len(lines)
    for index, line in enumerate(lines):
        if line.strip() and not line.startswith("--"):
            body_start = index
            break
    return "\n".join(lines[:body_start] + ["{% if is_dbr %}"] + lines[body_start:] + ["{% endif %}"]) + "\n"


def create_new_migration(
    message: str, template_path: str, output_path: str, truncate_slug_length: int = 40, add_is_dbr: bool = False
):
    rev_id = uuid.uuid4().hex[-12:]

    date_prefix = datetime.datetime.today().strftime("%y%m%d")

    slug = "_".join(re.split(r"\W+", message.lower())).strip("_")
    if len(slug) > truncate_slug_length:
        slug = slug[:truncate_slug_length].rsplit("_", 1)[0]

    migration_filename = f"{date_prefix}_{slug}_{rev_id}.sql"

    migration_path = os.path.join(output_path, migration_filename)

    try:
        ordered_migrations = get_migrations_list(output_path)
        last_migration_rev_id = ordered_migrations[-1].revision_id if len(ordered_migrations) > 0 else ""
    except ValueError:
        last_migration_rev_id = ""

    default_migration_content = open(template_path).read()
    default_migration_content = default_migration_content.replace("{{revision_id}}", rev_id)
    default_migration_content = default_migration_content.replace("{{prev_revision_id}}", last_migration_rev_id)
    if add_is_dbr:
        default_migration_content = gate_on_is_dbr(default_migration_content)

    with open(migration_path, "w") as migration_file:
        migration_file.write(default_migration_content)


def apply_template(output_dir, template, cat: str, schema: str):
    result_sql = template.render(cat=cat, schema=schema, is_dbr=is_dbr())
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


def has_no_statements(rendered_sql):
    for line in rendered_sql.splitlines():
        if not _COMMENT_OR_BLANK_RE.match(line):
            return False
    return True


def render_and_apply(spark, output_folder, template, cat: str, schema: str):
    """render one migration and run it, unless this engine was gated out of it."""
    result_sql = apply_template(output_folder, template, cat=cat, schema=schema)
    if has_no_statements(result_sql):
        logger.info(f"{template.name} rendered no statements; nothing to apply here;")
        return
    spark.sql(result_sql)


def migrate_initial(spark, output_folder, cat: str, schema: str):
    """apply the package's own bootstrap chain, in filename order rather than by revision."""
    env = Environment(
        loader=PackageLoader(package_name=PACKAGE_NAME, package_path=INITIAL_MIGRATIONS_PACKAGE_PATH),
        autoescape=select_autoescape(),
    )
    all_templates = env.list_templates(extensions=["sql"])

    logger.info(f"found {len(all_templates)} migrations; first five are {all_templates[:5]};")
    for template_name in all_templates:
        render_and_apply(spark, output_folder, env.get_template(template_name), cat=cat, schema=schema)
    logger.info(f"rendered {len(all_templates)} initial migrations;")


def record_revision(spark, cat: str, schema: str, revision_id: str):
    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    revision_literal = revision_id.replace("'", "''")
    spark.sql(f"delete from {version_table}")
    spark.sql(f"insert into {version_table} (version_num) values ('{revision_literal}')")


def migrate_w_rev(spark, output_folder, migrations_dir, cat: str, schema: str):
    """
    TODO XXX permit applying up to a revision below the head revision
    TODO XXX add backwards migrations, permit migrating backwards
    """
    if not os.path.isdir(migrations_dir):
        logger.info(f"no chain at {migrations_dir}; skipping it;")
        return
    unapplied = get_unapplied_migrations_list(spark, migrations_dir, cat=cat, schema=schema)

    logger.info(
        f"{len(unapplied)} unapplied migrations; "
        f"first five are {[migration.template_name for migration in unapplied[:5]]};"
    )

    env = Environment(loader=FileSystemLoader(migrations_dir), autoescape=select_autoescape())
    for migration in unapplied:
        render_and_apply(spark, output_folder, env.get_template(migration.template_name), cat=cat, schema=schema)
        record_revision(spark, cat, schema, migration.revision_id)
        logger.info(f"{migration.template_name} done; now at {migration.revision_id};")


def run_migrations(spark, cat, schema, output_folder, migrations_dir):
    initial_output_folder = os.path.join(output_folder, INITIAL_MIGRATIONS_PACKAGE_PATH)
    client_output_folder = os.path.join(output_folder, CLIENT_OUTPUT_DIRNAME)
    os.makedirs(initial_output_folder, exist_ok=True)
    os.makedirs(client_output_folder, exist_ok=True)
    version_table = f"{cat}.{schema}.{VERSION_TABLE}"
    if spark.catalog.tableExists(version_table):
        logger.info(f"{version_table} already exists; skipping the initial migrations;")
    else:
        migrate_initial(spark, initial_output_folder, cat, schema)
    migrate_w_rev(spark, client_output_folder, migrations_dir, cat, schema)


def main(cat, schema, migrations_dir, output_parent_path=None):
    output_folder = get_output_folder(output_parent_path or os.path.join(os.getcwd(), "migrations_out"))
    os.makedirs(output_folder)
    run_migrations(get_spark(), cat, schema, output_folder, migrations_dir)


def _cli_main(cat, schema, migrations_dir):  # pragma: no cover
    main(cat, schema, migrations_dir)


def _cli_create_new_migration(message, output_path, template_path, add_is_dbr):  # pragma: no cover
    create_new_migration(message, template_path or get_default_template_path(), output_path, add_is_dbr=add_is_dbr)


def build_parser():  # pragma: no cover
    parser = argparse.ArgumentParser(description="spark_sql_migrations migration utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--cat", default="spark_catalog")
    p_run.add_argument("--schema", default="default")
    p_run.add_argument("--migrations-dir", required=True, help="the directory holding the migration chain")
    p_run.set_defaults(func=_cli_main)

    p_create = subparsers.add_parser("create_new_migration", help="create a new migration from the template")
    p_create.add_argument("--message", required=True)
    p_create.add_argument(
        "--output-path",
        required=True,
        help="the chain directory to write the new migration into. if sql files are in there, the last sql's rev_id"
        "will be set as the new migrations prev_rev_id.",
    )
    p_create.add_argument("--template-path", default=None, help="defaults to the template shipped in this package")
    # the %% are argparse's: it interpolates help strings
    p_create.add_argument(
        "--add-is-dbr",
        action="store_true",
        help="wrap the new migration's body in {%% if is_dbr %%}, for statements only databricks understands",
    )
    p_create.set_defaults(func=_cli_create_new_migration)

    return parser


def cli(argv=None):  # pragma: no cover
    """the `spark-sql-migrations` console command.

    an application entry point, so unlike the rest of this package it configures
    logging. basicConfig is a no-op once the root logger has handlers, which keeps
    this from clobbering a host application that got there first.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(asctime)s %(name)s : %(message)s",
        datefmt="%y/%m/%d %H:%M:%S",
    )
    logging.getLogger("py4j").setLevel(logging.ERROR)
    kwargs = vars(build_parser().parse_args(argv))
    func = kwargs.pop("func")
    kwargs.pop("command")
    func(**kwargs)


if __name__ == "__main__":  # pragma: no cover
    cli()
