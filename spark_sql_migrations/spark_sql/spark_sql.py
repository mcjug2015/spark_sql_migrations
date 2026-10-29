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
from typing import Any, Dict, List, Optional, Set

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row

from spark_sql_migrations.spark_utils import get_spark, is_dbr

logger = logging.getLogger(__name__)


# value returned by get_version_table_name from start of run should have been passed in to you
__DO_NOT_USE_VERSION_TABLE = "_spark_migrations_version"
# what `history` prints in place of the revision a root migration follows
BASE_LABEL = "<base>"

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
    prev_revision_id: Optional[str]
    template_name: str


@dataclass(frozen=True)
class ChainSummary:
    """a directory's chain as `history` found it, forks and all.

    ``ordered`` is oldest first and holds every migration in the directory, so a
    listing built from it never silently drops a file. Everything else is what
    makes the chain unapplicable: more than one ``heads`` entry is a split head,
    a ``branch_points`` entry is where it split, more than one ``roots`` entry is
    a second chain in the same directory, and ``orphans`` are files no walk from
    any root reaches -- a cycle, in practice.
    """

    ordered: List[Migration]
    heads: List[Migration]
    roots: List[Migration]
    branch_points: Dict[str, List[Migration]]
    orphans: List[Migration]


def get_version_table_name(spark: SparkSession) -> str:
    # only correct ar start of run
    return f"{spark.catalog.currentCatalog()}.{spark.catalog.currentDatabase()}._spark_migrations_version"


def get_default_template_path() -> str:
    """the template this package ships; callers may pass their own instead."""
    return os.path.join(os.path.dirname(__file__), "..", TEMPLATES_PACKAGE_PATH, DEFAULT_TEMPLATE_NAME)


def _parse_migration(migrations_dir: str, template_name: str) -> Migration:
    headers: Dict[str, Any | str] = {}
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


def get_ordered_migration_objs(migration_objs: List[Migration]) -> List[Migration]:
    by_revision: Dict[str, Migration] = {}
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

    next_by_revision: Dict[str, Migration] = {}
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
    current: Optional[Migration] = roots[0]
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


def read_migrations_dir(migrations_dir: str) -> List[Migration]:
    """every .sql file in one directory, parsed, in filename order and unwalked.

    the headers are all this reads, so a chain that forks or points at nothing
    comes back whole -- it is `get_ordered_migration_objs` that insists on a
    single walkable chain, and `history` that reports one which is not.
    """
    if not os.path.isdir(migrations_dir):
        raise ValueError(f"no migrations dir at {migrations_dir};")

    return [
        _parse_migration(migrations_dir, template_name)
        for template_name in sorted(os.listdir(migrations_dir))
        if template_name.endswith(".sql")
    ]


def get_migrations_list(migrations_dir: str, max_revision_id: Optional[str] = None) -> List[Migration]:
    """every migration in one directory, walked from its root to its head.

    ``revision_id`` cuts the walk short: the chain is returned up to and including
    the migration carrying that revision. A revision no migration in the directory
    carries is not an error -- the whole chain comes back, same as for no revision
    at all.
    """
    migrations = read_migrations_dir(migrations_dir)
    if not migrations:
        return []

    ordered = get_ordered_migration_objs(migrations)
    if max_revision_id is None:
        return ordered

    for index, migration in enumerate(ordered):
        if migration.revision_id == max_revision_id:
            return ordered[: index + 1]

    return ordered


def get_children_by_revision(migrations: List[Migration]) -> Dict[Optional[str], List[Migration]]:
    """each revision's followers, keyed by the revision they follow.

    the root's followers sit under None. A revision with two followers is a fork,
    which is why this returns a list and not one migration.
    """
    children: Dict[Optional[str], List[Migration]] = {}
    for migration in sorted(migrations, key=lambda migration: migration.template_name):
        children.setdefault(migration.prev_revision_id, []).append(migration)
    return children


def summarize_chain(migrations: List[Migration]) -> ChainSummary:
    """walk a chain for reading rather than for applying.

    this raises nothing: a fork, a second root or a revision following one no file
    carries is what the caller asked to be shown, not a reason to refuse. The walk
    is depth first from each root, so one branch of a fork is listed to its head
    before the next branch starts, and a revision already listed is not listed
    again -- which is what keeps a cycle from looping here.
    """
    children = get_children_by_revision(migrations)
    known_revisions = {migration.revision_id for migration in migrations}
    roots = [
        migration
        for migration in sorted(migrations, key=lambda migration: migration.template_name)
        if not migration.prev_revision_id or migration.prev_revision_id not in known_revisions
    ]

    ordered: List[Migration] = []
    seen: Set[Migration] = set()
    stack = list(reversed(roots))
    while stack:
        migration = stack.pop()
        if migration in seen:
            continue
        seen.add(migration)
        ordered.append(migration)
        stack.extend(reversed(children.get(migration.revision_id, [])))

    # orphan - somehow comes from no root
    orphans = [migration for migration in migrations if migration not in seen]
    ordered.extend(sorted(orphans, key=lambda migration: migration.template_name))

    return ChainSummary(
        ordered=ordered,
        heads=[migration for migration in ordered if not children.get(migration.revision_id)],
        roots=roots,
        branch_points={
            revision_id: followers for revision_id, followers in children.items() if revision_id and len(followers) > 1
        },
        orphans=orphans,
    )


def format_history(summary: ChainSummary, limit: Optional[int] = None, oldest_first: bool = False) -> List[str]:
    """the listing, newest first like alembic's unless asked for the other end.

    ``limit`` cuts from the far end, so it keeps the newest with the default order
    and the oldest with ``oldest_first`` -- either way, the end the listing starts
    at is the end that survives. The notes are appended whole, even to a listing
    cut short, because a split head is the thing worth seeing most.
    """
    if limit is not None and limit < 1:
        raise ValueError(f"a history limit lists at least one migration, got {limit};")

    ordered = summary.ordered if oldest_first else list(reversed(summary.ordered))
    if limit is not None:
        ordered = ordered[:limit]

    head_revisions = {migration.revision_id for migration in summary.heads}
    branch_point_revisions = set(summary.branch_points)
    lines = [format_history_line(migration, head_revisions, branch_point_revisions) for migration in ordered]
    return lines + format_chain_notes(summary)


def history(migrations_dir: str, limit: Optional[int] = None, oldest_first: bool = False) -> List[str]:
    """the chain in one directory, as lines ready to be printed.

    the files on disk are the whole of it: no catalog is read and no session is
    built, so a chain can be read anywhere, whatever any catalog has been migrated
    to. `get_table_version` is what says where a catalog actually stands.
    """
    return format_history(summarize_chain(read_migrations_dir(migrations_dir)), limit=limit, oldest_first=oldest_first)


def get_table_version(spark: SparkSession, version_table_name: str) -> Optional[Row]:
    """the revision VERSION_TABLE records, or None if it holds no row yet."""
    version_table = f"{version_table_name}"
    if not spark.catalog.tableExists(version_table):
        return None
    return spark.read.table(version_table).select("version_num").head()


def get_unapplied_migrations_list(
    spark: SparkSession, version_table_name, migrations_dir: str, max_revision_id: Optional[str] = None
) -> List[Migration]:
    ordered = get_migrations_list(migrations_dir, max_revision_id)
    current = get_table_version(spark, version_table_name)
    if not current:
        return ordered

    current_revision_id = current["version_num"]

    if not ordered:
        logger.warning(
            f"{version_table_name} is at {current_revision_id}, but {migrations_dir} holds no migrations; "
            f"treating the chain as retired;"
        )
        return ordered

    for index, migration in enumerate(ordered):
        if migration.revision_id == current_revision_id:
            return ordered[index + 1 :]

    raise ValueError(
        f"{version_table_name} is at {current_revision_id}, which matches no migration in {migrations_dir};"
    )


def gate_on_is_dbr(migration_content: str) -> str:
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
) -> None:
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


def apply_template(
    output_dir: str,
    template,
    template_vals: Optional[Dict[str, str]] = None,
) -> str:
    prep: Dict[str, str | bool] = {}
    prep.update(template_vals or {})
    prep["is_dbr"] = is_dbr()
    result_sql = template.render(**prep)
    with open(os.path.join(output_dir, template.name.replace(".sql", "_primed.sql")), "w") as file_handle:
        file_handle.write(result_sql)
    return result_sql


def get_ascending_letters_within_minute() -> str:
    """a letter stamp that sorts in the order it was taken, within one minute.

    the full microseconds since the minute began, not timedelta.microseconds -- that
    holds only the sub-second part and so starts over every second. zero-padded to a
    fixed width, because these are compared as text once they are folder names.
    """
    now = datetime.datetime.now()
    micros_since_minute = (now - now.replace(second=0, microsecond=0)) // datetime.timedelta(microseconds=1)
    return f"{micros_since_minute:08d}".translate(str.maketrans("0123456789", "ABCDEFGHIJ"))


def get_output_folder(output_parent_path: str) -> str:
    folder_name = f"{datetime.datetime.today().strftime('%Y%m%d_%H%M')}_{get_ascending_letters_within_minute()}"
    return os.path.join(output_parent_path, folder_name)


def has_no_statements(rendered_sql: str) -> bool:
    for line in rendered_sql.splitlines():
        if not _COMMENT_OR_BLANK_RE.match(line):
            return False
    return True


def render_and_apply(
    spark: SparkSession,
    output_folder: str,
    template,
    template_vals: Optional[Dict[str, str]] = None,
) -> None:
    """render one migration and run it, unless this engine was gated out of it."""
    result_sql = apply_template(output_folder, template, template_vals)
    if has_no_statements(result_sql):
        logger.info(f"{template.name} rendered no statements; nothing to apply here;")
        return
    spark.sql(result_sql)


def record_revision(spark: SparkSession, version_table_name: str, revision_id: str) -> None:
    revision_literal = revision_id.replace("'", "''")
    spark.sql(f"delete from {version_table_name}")
    spark.sql(f"insert into {version_table_name} (version_num) values ('{revision_literal}')")


def upgrade(
    spark: SparkSession,
    version_table_name: str,
    output_folder: str,
    migrations_dir: str,
    max_revision_id: Optional[str] = None,
    template_vals: Optional[Dict[str, str]] = None,
) -> None:
    """
    TODO XXX permit applying up to a revision below the head revision
    TODO XXX add backwards migrations, permit migrating backwards
    """
    if not os.path.isdir(migrations_dir):
        logger.info(f"no chain at {migrations_dir}; skipping it;")
        return
    unapplied = get_unapplied_migrations_list(
        spark, version_table_name, migrations_dir, max_revision_id=max_revision_id
    )

    logger.info(
        f"{len(unapplied)} unapplied migrations; "
        f"first five are {[migration.template_name for migration in unapplied[:5]]};"
    )

    env = Environment(loader=FileSystemLoader(migrations_dir), autoescape=select_autoescape())
    for migration in unapplied:
        render_and_apply(spark, output_folder, env.get_template(migration.template_name), template_vals)
        record_revision(spark, version_table_name, migration.revision_id)
        logger.info(f"{migration.template_name} done; now at {migration.revision_id};")


def run_upgrade(
    spark: SparkSession,
    output_folder: str,
    migrations_dir: str,
    max_revision_id: Optional[str] = None,
    template_vals: Optional[Dict[str, str]] = None,
) -> None:
    initial_output_folder = os.path.join(output_folder, INITIAL_MIGRATIONS_PACKAGE_PATH)
    client_output_folder = os.path.join(output_folder, CLIENT_OUTPUT_DIRNAME)
    os.makedirs(initial_output_folder, exist_ok=True)
    os.makedirs(client_output_folder, exist_ok=True)
    version_table_name = get_version_table_name(spark)
    if spark.catalog.tableExists(version_table_name):
        logger.info(f"{version_table_name} already exists; skipping the initial migrations;")
    else:
        spark.sql(f"create table if not exists {version_table_name}(version_num string not null);")
    upgrade(spark, version_table_name, client_output_folder, migrations_dir, max_revision_id, template_vals)


def format_history_line(
    migration: Migration, head_revisions: Set[str], branch_point_revisions: Set[str]
) -> str:  # pragma: no cover
    """one alembic-shaped line: `prev -> rev (tags), filename`."""
    tags = []
    if migration.revision_id in head_revisions:
        tags.append("head")
    if migration.revision_id in branch_point_revisions:
        tags.append("branchpoint")
    tag_text = f" ({', '.join(tags)})" if tags else ""
    return f"{migration.prev_revision_id or BASE_LABEL} -> {migration.revision_id}{tag_text}, {migration.template_name}"


def format_chain_notes(summary: ChainSummary) -> List[str]:  # pragma: no cover
    """what is wrong with this chain, below the listing, or nothing if it is sound."""
    notes = []
    if len(summary.heads) > 1:
        notes.append(
            f"split heads: {', '.join(migration.revision_id for migration in summary.heads)}; "
            f"a chain with more than one head cannot be applied until it is rejoined;"
        )
    for revision_id, followers in sorted(summary.branch_points.items()):
        notes.append(
            f"{revision_id} is the prev_revision_id of "
            f"{', '.join(migration.template_name for migration in followers)};"
        )
    if len(summary.roots) > 1:
        notes.append(
            f"more than one root: {', '.join(migration.template_name for migration in summary.roots)}; "
            f"a chain has exactly one migration following no other;"
        )
    if summary.orphans:
        notes.append(
            f"no root reaches {', '.join(migration.template_name for migration in summary.orphans)}; "
            f"they are listed together at the tail end of the chain order, having none of their own;"
        )
    return notes


def main(
    migrations_dir: str,
    output_parent_path: Optional[str] = None,
    max_revision_id: Optional[str] = None,
    template_vals: Optional[Dict[str, str]] = None,
) -> None:
    output_folder = get_output_folder(output_parent_path or os.path.join(os.getcwd(), "migrations_out"))
    os.makedirs(output_folder)
    spark = get_spark()
    run_upgrade(spark, output_folder, migrations_dir, max_revision_id, template_vals)


def _cli_main(
    migrations_dir: str,
    max_revision_id: Optional[str] = None,
    template_vals: Optional[Dict[str, str]] = None,
) -> None:  # pragma: no cover
    main(migrations_dir, max_revision_id=max_revision_id, template_vals=template_vals)


def _cli_create_new_migration(
    message: str, output_path: str, template_path: str, add_is_dbr: bool
) -> None:  # pragma: no cover
    create_new_migration(message, template_path or get_default_template_path(), output_path, add_is_dbr=add_is_dbr)


def _cli_history(migrations_dir: str, limit: Optional[int], oldest_first: bool) -> None:  # pragma: no cover
    """the listing goes to stdout, so it pipes; the log records go to stderr as always."""
    for line in history(migrations_dir, limit=limit, oldest_first=oldest_first):
        print(line)


def parse_template_vals(text: str) -> Dict[str, str]:
    """`foo=bar,gee=wiz` -> {"foo": "bar", "gee": "wiz"}; the argparse `type` of --template-vals.

    a key has to be usable as a jinja variable, so an identifier: one that is not would
    render as nothing at all and the migration would go out silently missing it.
    """
    template_vals: Dict[str, str] = {}
    for pair in text.split(","):
        if not pair.strip():
            continue
        key, separator, value = pair.partition("=")
        key = key.strip()
        if not separator or not key.isidentifier():
            raise argparse.ArgumentTypeError(f"expected key=value with an identifier key, got {pair!r};")
        template_vals[key] = value.strip()
    return template_vals


def build_parser() -> argparse.ArgumentParser:  # pragma: no cover
    parser = argparse.ArgumentParser(description="spark_sql_migrations migration utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="upgrade to head/apply pending migrations")
    p_run.add_argument("--migrations-dir", type=str, required=True, help="the directory holding the migration chain")
    p_run.add_argument(
        "--max-revision-id",
        type=str,
        default=None,
        help="apply the migration carrying this revision_id and stop; defaults to applying the chain to its head",
    )
    p_run.add_argument(
        "--template-vals",
        type=parse_template_vals,
        default=None,
        metavar="KEY=VALUE[,KEY=VALUE...]",
        help="jinja render vars handed to every migration. is_dbr is always set from the engine and cannot be "
        "overridden here. the whole thing is one argument, equal signs and all: "
        "--template-vals=foo=bar,gee=wiz,pp=qq",
    )
    p_run.set_defaults(func=_cli_main)

    p_create = subparsers.add_parser("create_new_migration", help="create a new migration from the template")
    p_create.add_argument("--message", type=str, required=True)
    p_create.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="the chain directory to write the new migration into. if sql files are in there, the last sql's rev_id"
        "will be set as the new migrations prev_rev_id.",
    )
    p_create.add_argument(
        "--template-path", type=str, default=None, help="defaults to the template shipped in this package"
    )
    # the %% are argparse's: it interpolates help strings
    p_create.add_argument(
        "--add-is-dbr",
        action="store_true",
        help="wrap the new migration's body in {%% if is_dbr %%}, for statements only databricks understands",
    )
    p_create.set_defaults(func=_cli_create_new_migration)

    p_history = subparsers.add_parser("history", help="list the migration chain on disk, newest first")
    p_history.add_argument("--migrations-dir", type=str, required=True, help="the directory holding the chain")
    p_history.add_argument(
        "-n",
        "--limit",
        type=int,
        default=None,
        help="list at most this many migrations, counted from the end the listing starts at",
    )
    p_history.add_argument(
        "--oldest-first",
        action="store_true",
        help="list from the root up instead of from the head down",
    )
    p_history.set_defaults(func=_cli_history)

    return parser


def cli(argv: Optional[list[str]] = None) -> None:  # pragma: no cover
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
