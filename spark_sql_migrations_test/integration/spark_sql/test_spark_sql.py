import logging
import os
import shutil
from unittest import mock

from freezegun import freeze_time

from spark_sql_migrations.spark_sql import spark_sql
from spark_sql_migrations.spark_sql.spark_sql import CLIENT_OUTPUT_DIRNAME, VERSION_TABLE, Migration

logger = logging.getLogger(__name__)

# stands in for a consuming project: the library owns migrations_initial/ and the
# template, the client owns the chain this points at.
CLIENT_MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "res", "migrations")


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


def test_run_migrations(test_spark, tmp_path):
    first_out = tmp_path / "migrations_out_first"
    with mock.patch.object(
        spark_sql,
        "get_unapplied_migrations_list",
        return_value=[
            Migration(
                revision_id="a1b2c3d4e5f6",
                prev_revision_id=None,
                template_name="260901_01_create_widget_table_a1b2c3d4e5f6.sql",
            )
        ],
    ):
        # run migrations w. all_spark capped to a single one
        spark_sql.run_upgrade(test_spark, "spark_catalog", "default", first_out, CLIENT_MIGRATIONS_DIR)

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "a1b2c3d4e5f6"

    first_client_out = first_out / CLIENT_OUTPUT_DIRNAME
    assert sorted(os.listdir(first_client_out)) == ["260901_01_create_widget_table_a1b2c3d4e5f6_primed.sql"]

    second_out = tmp_path / "migrations_out_second"
    with mock.patch.object(
        spark_sql,
        "get_unapplied_migrations_list",
        return_value=[
            Migration(
                revision_id="b2c3d4e5f6a1",
                prev_revision_id="a1b2c3d4e5f6",
                template_name="260901_02_batch_id_for_widget_table_b2c3d4e5f6a1.sql",
            ),
            Migration(
                revision_id="c3d4e5f6a1b2",
                prev_revision_id="b2c3d4e5f6a1",
                template_name="260901_03_create_widget_kvp_table_c3d4e5f6a1b2.sql",
            ),
            Migration(
                revision_id="e5f6a1b2c3d4",
                prev_revision_id="c3d4e5f6a1b2",
                template_name="260901_04_widget_vol_gated_e5f6a1b2c3d4.sql",
            ),
        ],
    ):
        # run migrations w. more all spark let through, the last of them gated to dbr
        spark_sql.run_upgrade(test_spark, "spark_catalog", "default", second_out, CLIENT_MIGRATIONS_DIR)

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "e5f6a1b2c3d4"

    second_client_out = second_out / CLIENT_OUTPUT_DIRNAME
    assert sorted(os.listdir(second_client_out)) == [
        "260901_02_batch_id_for_widget_table_b2c3d4e5f6a1_primed.sql",
        "260901_03_create_widget_kvp_table_c3d4e5f6a1b2_primed.sql",
        "260901_04_widget_vol_gated_e5f6a1b2c3d4_primed.sql",
    ]

    # the gated migration reached the head revision above without its CREATE VOLUME
    # ever being run: this session has no such statement, so it would have failed.
    with open(second_client_out / "260901_04_widget_vol_gated_e5f6a1b2c3d4_primed.sql") as primed_file:
        statements = [line for line in primed_file.read().splitlines() if line.strip() and not line.startswith("--")]
    assert statements == []


# its own schema, so the version table this drives is not the one other tests advance
CLI_SCHEMA = "cli_run"


def _recorded_revision(spark) -> str:
    version_table = f"spark_catalog.{CLI_SCHEMA}.{VERSION_TABLE}"
    return spark.sql(f"select version_num from {version_table}").head()["version_num"]


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
    argv = ["run", "--schema", CLI_SCHEMA, "--migrations-dir", CLIENT_MIGRATIONS_DIR]

    out_parent = tmp_path / "migrations_out"

    spark_sql.cli(argv + ["--max-revision-id", "a1b2c3d4e5f6"])
    assert _recorded_revision(test_spark) == "a1b2c3d4e5f6"
    # which folder a run wrote is taken from when it appeared, never from its name:
    # get_ascending_letters_within_minute does not in fact order two folders
    (first,) = out_parent.iterdir()

    spark_sql.cli(argv)
    assert _recorded_revision(test_spark) == "e5f6a1b2c3d4"
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
