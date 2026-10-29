import logging
import os
import shutil

from freezegun import freeze_time

from spark_sql_migrations.spark_sql import spark_sql
from spark_sql_migrations.spark_sql.spark_sql import (
    CLIENT_OUTPUT_DIRNAME,
    get_table_version,
    get_version_table_name,
)

logger = logging.getLogger(__name__)

# stands in for a consuming project: the library owns migrations_initial/ and the
# template, the client owns the chain this points at.
CLIENT_MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "res", "migrations")

# the client chain as `history` lists it, root up. For a sound chain this is the whole
# of the output: format_chain_notes appends nothing when there is nothing wrong with it.
CHAIN_LISTING_OLDEST_FIRST = [
    "<base> -> a1b2c3d4e5f6, 260901_01_create_widget_table_a1b2c3d4e5f6.sql",
    "a1b2c3d4e5f6 -> b2c3d4e5f6a1, 260901_02_batch_id_for_widget_table_b2c3d4e5f6a1.sql",
    "b2c3d4e5f6a1 -> c3d4e5f6a1b2, 260901_03_create_widget_kvp_table_c3d4e5f6a1b2.sql",
    "c3d4e5f6a1b2 -> e5f6a1b2c3d4 (head), 260901_04_widget_vol_gated_e5f6a1b2c3d4.sql",
]


def _listed(capsys, *flags: str) -> list[str]:
    """one history run through the console entry point, as the lines it printed."""
    spark_sql.cli(["history", "--migrations-dir", CLIENT_MIGRATIONS_DIR, *flags])
    return capsys.readouterr().out.splitlines()


def test_cli_history(capsys):
    """the history subcommand over the client chain: argparse wiring, order, limit, stdout.

    no session is built and no catalog is read, so this says nothing about where any
    catalog stands -- only what the files on disk chain up to. The two capped runs are
    what show the limit cutting from the far end, so the listing keeps the end it
    started at rather than always the oldest.
    """
    assert _listed(capsys) == list(reversed(CHAIN_LISTING_OLDEST_FIRST))
    assert _listed(capsys, "--oldest-first") == CHAIN_LISTING_OLDEST_FIRST
    assert _listed(capsys, "-n", "2") == list(reversed(CHAIN_LISTING_OLDEST_FIRST))[:2]
    assert _listed(capsys, "--oldest-first", "-n", "2") == CHAIN_LISTING_OLDEST_FIRST[:2]


@freeze_time("2007-07-07")
def test_create_new_migration(tmp_path, request):
    logger.info(f"TEST: {request.node.name}; will be writing migrations under {tmp_path};")
    output_path = tmp_path / "migrations"
    shutil.copytree(CLIENT_MIGRATIONS_DIR, output_path)
    template_path = spark_sql.get_default_template_path()

    spark_sql.create_new_migration("integration test migration", template_path, str(output_path), add_is_dbr=True)

    migrations = spark_sql.get_migrations_list(str(output_path))
    with open(os.path.join(str(output_path), migrations[4].template_name)) as dbr_migration:
        contents = dbr_migration.read()
        assert "is_dbr" in contents
    assert (len(migrations)) == 5
    assert migrations[4].prev_revision_id == "e5f6a1b2c3d4"
    assert migrations[4].revision_id != ""


# its own schema, so the version table this drives is not the one other tests advance
CLI_SCHEMA = "cli_run"


def _primed(run_folder) -> list[str]:
    return sorted(os.listdir(run_folder / CLIENT_OUTPUT_DIRNAME))


def test_cli_run_capped_then_to_head(test_spark, tmp_path, monkeypatch):
    """the console entry point, twice: held to the chain's first revision, then let to head.

    nothing is mocked here. --max-revision-id does the capping that otherwise needs
    get_unapplied_migrations_list patched, so the real walk of the chain, the version
    table and the is_dbr gate all stay in the test.
    """
    # _cli_main passes no output_parent_path, so a run lands under cwd/migrations_out
    monkeypatch.chdir(tmp_path)
    argv = ["run", "--migrations-dir", CLIENT_MIGRATIONS_DIR]

    out_parent = tmp_path / "migrations_out"

    spark_sql.cli(argv + ["--max-revision-id", "a1b2c3d4e5f6", "--template-vals", "cat=spark_catalog,schema=default"])
    assert get_table_version(test_spark, get_version_table_name(test_spark))["version_num"] == "a1b2c3d4e5f6"
    # which folder a run wrote is taken from when it appeared, never from its name:
    # get_ascending_letters_within_minute does not in fact order two folders
    (first,) = out_parent.iterdir()

    spark_sql.cli(argv + ["--template-vals", "cat=spark_catalog,schema=default"])
    assert get_table_version(test_spark, get_version_table_name(test_spark))["version_num"] == "e5f6a1b2c3d4"
    (second,) = [run for run in out_parent.iterdir() if run != first]

    assert _primed(first) == ["260901_01_create_widget_table_a1b2c3d4e5f6_primed.sql"]
    assert _primed(second) == [
        "260901_02_batch_id_for_widget_table_b2c3d4e5f6a1_primed.sql",
        "260901_03_create_widget_kvp_table_c3d4e5f6a1b2_primed.sql",
        "260901_04_widget_vol_gated_e5f6a1b2c3d4_primed.sql",
    ]

    # the gated migration reached head without its CREATE VOLUME ever being run:
    # this session has no such statement, so it would have failed.
    gated = second / CLIENT_OUTPUT_DIRNAME / "260901_04_widget_vol_gated_e5f6a1b2c3d4_primed.sql"
    statements = [line for line in gated.read_text().splitlines() if line.strip() and not line.startswith("--")]
    assert statements == []
