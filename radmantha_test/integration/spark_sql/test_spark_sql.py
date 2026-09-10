import os
from unittest import mock

from freezegun import freeze_time

from radmantha import custom_logging
from radmantha.spark_sql import spark_sql
from radmantha.spark_sql.spark_sql import ALL_SPARK, Migration

logger = custom_logging.setup_logging().getLogger(__name__)

# stands in for a consuming project: the library owns migrations_initial/ and the
# template, the client owns all_spark_migrations/ and dbr_only_migrations/.
CLIENT_MIGRATIONS_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "res")


@freeze_time("2007-07-07")
def test_create_new_migration(tmp_path, request):
    logger.info(f"TEST: {request.node.name}; will be writing migrations under {tmp_path};")
    output_path = tmp_path / f"{spark_sql.ALL_SPARK}_migrations"
    os.makedirs(output_path)
    template_path = spark_sql.get_default_template_path()

    spark_sql.create_new_migration("integration test migration", template_path, str(output_path))

    migrations = spark_sql.get_migrations_list(str(output_path))
    assert (len(migrations)) == 1
    assert migrations[0].prev_revision_id is None
    assert migrations[0].revision_id != ""


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
        spark_sql.run_migrations(test_spark, "spark_catalog", "default", first_out, CLIENT_MIGRATIONS_ROOT)

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version "
            f"where migration_type = '{ALL_SPARK}'"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "a1b2c3d4e5f6"

    first_all_spark_out = first_out / f"{ALL_SPARK}_migrations"
    assert sorted(os.listdir(first_all_spark_out)) == ["260901_01_create_widget_table_a1b2c3d4e5f6_primed.sql"]

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
        ],
    ):
        # run migrations w. more all spark let through
        spark_sql.run_migrations(test_spark, "spark_catalog", "default", second_out, CLIENT_MIGRATIONS_ROOT)

    results = [
        x.asDict()
        for x in test_spark.sql(
            "select version_num from spark_catalog.default._spark_migrations_version "
            f"where migration_type = '{ALL_SPARK}'"
        ).toLocalIterator()
    ]
    assert results[0]["version_num"] == "c3d4e5f6a1b2"

    second_all_spark_out = second_out / f"{ALL_SPARK}_migrations"
    assert sorted(os.listdir(second_all_spark_out)) == [
        "260901_02_batch_id_for_widget_table_b2c3d4e5f6a1_primed.sql",
        "260901_03_create_widget_kvp_table_c3d4e5f6a1b2_primed.sql",
    ]
