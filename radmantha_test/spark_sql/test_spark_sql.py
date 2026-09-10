import os
from unittest import mock
from uuid import UUID

import pytest
from freezegun import freeze_time
from jinja2.environment import Environment
from jinja2.loaders import FileSystemLoader
from jinja2.utils import select_autoescape

from radmantha.spark_sql.spark_sql import (
    ALL_SPARK,
    DBR_ONLY,
    Migration,
    _parse_migration,
    apply_template,
    create_new_migration,
    get_ascending_letters_within_minute,
    get_default_template_path,
    get_migrations_dir,
    get_migrations_list,
    get_ordered_migration_objs,
    get_output_folder,
    get_unapplied_migrations_list,
    main,
    migrate_initial,
    migrate_w_rev,
    record_revision,
    run_migrations,
    use_migration_file,
)

VERSION_TABLE_DDL = """
    begin
    drop table if exists spark_catalog.default._spark_migrations_version;
    create table spark_catalog.default._spark_migrations_version (
        migration_type string not null,
        version_num string not null
    );
    end;
"""


def test_get_migrations_dir():
    assert get_migrations_dir("/i/am/a/fake/root", "qqq") == "/i/am/a/fake/root/qqq_migrations"


def test_get_default_template_path():
    """the shipped template resolves relative to the installed package."""
    assert os.path.isfile(get_default_template_path())


def test_parse_migration_reads_headers(tmp_path):
    with open(tmp_path / "01_headed.sql", "w") as handle:
        handle.write("-- revision_id:aaa;\n-- prev_revision_id:;\nselect 1;\n")

    assert _parse_migration(str(tmp_path), "01_headed.sql") == Migration("aaa", None, "01_headed.sql")


def test_parse_migration_no_rev_id(tmp_path):
    with open(tmp_path / "01_headerless.sql", "w") as handle:
        handle.write("-- a comment, not a header\nbegin\nselect 1;\nend;\n")

    with pytest.raises(ValueError, match="has no revision_id header"):
        _parse_migration(str(tmp_path), "01_headerless.sql")


def test_parse_migration_empty_file(tmp_path):
    (tmp_path / "01_empty.sql").touch()

    with pytest.raises(ValueError, match="is empty"):
        _parse_migration(str(tmp_path), "01_empty.sql")


def test_parse_migration_happy(tmp_path):
    with open(tmp_path / "01_w_revision.sql", "w") as handle:
        handle.write("-- revision_id:TESTREV;\n-- prev_revision_id:TESTPREVREV;\nbegin\nselect 1;\nend;\n")

    result = _parse_migration(str(tmp_path), "01_w_revision.sql")

    assert result.revision_id == "TESTREV"
    assert result.prev_revision_id == "TESTPREVREV"
    assert result.template_name == "01_w_revision.sql"


def test_get_ordered_migration_objs_clash():
    with pytest.raises(ValueError, match="is claimed by both"):
        get_ordered_migration_objs(
            [
                Migration(revision_id="q", prev_revision_id="test10", template_name="test1"),
                Migration(revision_id="q", prev_revision_id="test20", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_too_many_roots():
    with pytest.raises(ValueError, match="expected exactly one migration with an empty"):
        get_ordered_migration_objs(
            [
                Migration(revision_id="q", prev_revision_id="", template_name="test1"),
                Migration(revision_id="p", prev_revision_id="", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_unknown_prev():
    with pytest.raises(ValueError, match="points at unknown prev_revision_id"):
        get_ordered_migration_objs(
            [
                Migration(revision_id="q", prev_revision_id="unknown", template_name="test1"),
                Migration(revision_id="p", prev_revision_id="", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_parent_of_both():
    with pytest.raises(ValueError, match="is the parent of both"):
        get_ordered_migration_objs(
            [
                Migration(revision_id="head", prev_revision_id="q", template_name="test500"),
                Migration(revision_id="another head", prev_revision_id="q", template_name="test700"),
                Migration(revision_id="q", prev_revision_id="", template_name="test1"),
            ]
        )


def test_get_ordered_migration_objs_not_reachable_from_root():
    with pytest.raises(ValueError, match="Migrations are not reachable from the root"):
        get_ordered_migration_objs(
            [
                Migration(revision_id="aaa", prev_revision_id=None, template_name="test500"),
                Migration(revision_id="q", prev_revision_id="p", template_name="test1"),
                Migration(revision_id="p", prev_revision_id="q", template_name="test2"),
            ]
        )


def test_get_ordered_migration_objs_happy():
    result = get_ordered_migration_objs(
        [
            Migration(revision_id="aaa", prev_revision_id=None, template_name="test500"),
            Migration(revision_id="q", prev_revision_id="aaa", template_name="test1"),
            Migration(revision_id="p", prev_revision_id="q", template_name="test2"),
        ]
    )

    assert [migration.template_name for migration in result] == ["test500", "test1", "test2"]


def test_get_migrations_list_not_isdir():
    with pytest.raises(ValueError, match="no migrations dir at"):
        get_migrations_list("/i/am/a/fake/migrations/dir")


def test_get_migrations_list_no_migrations(tmp_path):
    assert get_migrations_list(str(tmp_path)) == []


@mock.patch("radmantha.spark_sql.spark_sql.get_ordered_migration_objs", return_value=["testing"])
@mock.patch("radmantha.spark_sql.spark_sql._parse_migration", return_value="testing")
def test_get_migrations_list_ignores_non_sql_files(parse_migration, get_ordered, tmp_path):
    (tmp_path / "01_i_am_test.sql").touch()
    (tmp_path / "02_i_am_non_sql_test.txt").touch()

    assert get_migrations_list(str(tmp_path)) == ["testing"]
    parse_migration.assert_called_once_with(str(tmp_path), "01_i_am_test.sql")
    get_ordered.assert_called_once_with(["testing"])


@freeze_time("2007-07-07")
@mock.patch("radmantha.spark_sql.spark_sql.uuid.uuid4", return_value=UUID(bytes=b"1111222233334444", version=4))
def test_create_new_migration_long_slug(_uuid, tmp_path):
    template_path = tmp_path / "template.sql"
    with open(template_path, "w") as handle:
        handle.write("-- revision_id:{{revision_id}};\n-- prev_revision_id:;\n")
    output_path = tmp_path / f"{ALL_SPARK}_migrations"
    os.makedirs(output_path)

    create_new_migration(
        "a very long message that certainly runs past the slug truncation limit",
        template_path=str(template_path),
        output_path=str(output_path),
    )

    expected = output_path / "070707_a_very_long_message_that_certainly_runs_333334343434.sql"
    with open(expected) as result_file:
        assert "-- revision_id:333334343434;" in result_file.read()


def test_apply_template(tmp_path):
    with open(tmp_path / "01_template.sql", "w") as handle:
        handle.write("\nbegin\nselect x from {{cat}}.{{schema}}.fake_table;\nend;\n")
    env = Environment(loader=FileSystemLoader(str(tmp_path)), autoescape=select_autoescape())

    result = apply_template(str(tmp_path), env.get_template("01_template.sql"), "FAKETESTCAT", "FAKETESTSCHEMA")

    assert "FAKETESTCAT.FAKETESTSCHEMA" in result
    with open(tmp_path / "01_template_primed.sql") as result_file:
        assert "FAKETESTCAT.FAKETESTSCHEMA" in result_file.read()


@freeze_time("2007-07-07 01:02:03.123456")
def test_get_ascending_letters_within_minute():
    assert get_ascending_letters_within_minute() == "BCDEFG"


@freeze_time("2007-07-07 01:02:03")
@mock.patch("radmantha.spark_sql.spark_sql.get_ascending_letters_within_minute", return_value="QQPP")
def test_get_output_folder(get_ascending_letters):
    assert get_output_folder("/i/am/a/fake/parent") == "/i/am/a/fake/parent/20070707_0102_QQPP"
    get_ascending_letters.assert_called_once()


def test_use_migration_file_all():
    assert use_migration_file("TESTING_all.sql")


@mock.patch("radmantha.spark_sql.spark_sql.is_dbr", return_value=True)
def test_use_migration_file_dbr(is_dbr):
    assert use_migration_file("TESTING_dbr_only.sql")
    is_dbr.assert_called_once()


@mock.patch("radmantha.spark_sql.spark_sql.is_dbr", return_value=False)
def test_use_migration_file_false(is_dbr):
    assert use_migration_file("TESTING.sql") is False
    is_dbr.assert_called_once()


@mock.patch("radmantha.spark_sql.spark_sql.migrate_w_rev")
@mock.patch("radmantha.spark_sql.spark_sql.migrate_initial")
@mock.patch("radmantha.spark_sql.spark_sql.is_dbr", return_value=True)
def test_run_migrations(is_dbr, migrate_initial_mock, migrate_w_rev_mock, tmp_path):
    output_folder = str(tmp_path / "28818989_8182_BCDEQQ")
    spark = mock.MagicMock()

    run_migrations(spark, "spark_catalog", "default", output_folder, "/i/am/a/fake/root")

    assert os.path.isdir(output_folder)
    migrate_initial_mock.assert_called_once()
    is_dbr.assert_called_once()
    assert migrate_w_rev_mock.call_args_list == [
        mock.call(spark, mock.ANY, "/i/am/a/fake/root", "spark_catalog", "default", DBR_ONLY),
        mock.call(spark, mock.ANY, "/i/am/a/fake/root", "spark_catalog", "default", ALL_SPARK),
    ]


@mock.patch("radmantha.spark_sql.spark_sql.run_migrations")
@mock.patch("radmantha.spark_sql.spark_sql.get_spark")
@mock.patch("radmantha.spark_sql.spark_sql.get_output_folder")
def test_main(get_output_folder_mock, get_spark_mock, run_migrations_mock, tmp_path):
    get_output_folder_mock.return_value = str(tmp_path / "20070707_0102_QQPP")

    main("spark_catalog", "default", "/i/am/a/fake/root", output_parent_path=str(tmp_path))

    assert os.path.isdir(tmp_path / "20070707_0102_QQPP")
    get_output_folder_mock.assert_called_once_with(str(tmp_path))
    get_spark_mock.assert_called_once()
    run_migrations_mock.assert_called_once_with(
        get_spark_mock.return_value,
        "spark_catalog",
        "default",
        str(tmp_path / "20070707_0102_QQPP"),
        "/i/am/a/fake/root",
    )


@mock.patch("radmantha.spark_sql.spark_sql.get_output_folder", return_value="/i/am/a/fake/out")
@mock.patch("radmantha.spark_sql.spark_sql.run_migrations")
@mock.patch("radmantha.spark_sql.spark_sql.get_spark")
@mock.patch("radmantha.spark_sql.spark_sql.os.makedirs")
@mock.patch("radmantha.spark_sql.spark_sql.os.getcwd", return_value="/fake/cwd")
def test_main_defaults_output_under_cwd(getcwd, makedirs, get_spark_mock, run_migrations_mock, get_output_folder_mock):
    main("spark_catalog", "default", "/i/am/a/fake/root")

    get_output_folder_mock.assert_called_once_with("/fake/cwd/migrations_out")
    getcwd.assert_called_once()
    makedirs.assert_called_once_with("/i/am/a/fake/out")
    get_spark_mock.assert_called_once()
    run_migrations_mock.assert_called_once()


@mock.patch("radmantha.spark_sql.spark_sql.get_migrations_list", return_value=["testing"])
def test_get_unapplied_migrations_list_no_table(get_migrations_list_mock, test_spark):
    test_spark.sql("drop table if exists spark_catalog.default._spark_migrations_version")

    result = get_unapplied_migrations_list(test_spark, "/i/am/a/fake/dir", "fake", "spark_catalog", "default")

    assert result == ["testing"]
    get_migrations_list_mock.assert_called_once_with("/i/am/a/fake/dir")


@mock.patch("radmantha.spark_sql.spark_sql.get_migrations_list", return_value=["testing"])
def test_get_unapplied_migrations_list_no_rows(get_migrations_list_mock, test_spark):
    test_spark.sql(VERSION_TABLE_DDL)

    result = get_unapplied_migrations_list(test_spark, "/i/am/a/fake/dir", "fake", "spark_catalog", "default")

    assert result == ["testing"]
    get_migrations_list_mock.assert_called_once()


@mock.patch(
    "radmantha.spark_sql.spark_sql.get_migrations_list",
    return_value=[Migration(revision_id="wont match", prev_revision_id=None, template_name="test1")],
)
def test_get_unapplied_migrations_list_no_match(get_migrations_list_mock, test_spark):
    test_spark.sql(VERSION_TABLE_DDL)
    test_spark.sql(
        "insert into spark_catalog.default._spark_migrations_version(migration_type, version_num) "
        "values('fake', 'non matching')"
    )

    with pytest.raises(ValueError, match="which matches no migration in"):
        get_unapplied_migrations_list(test_spark, "/i/am/a/fake/dir", "fake", "spark_catalog", "default")
    get_migrations_list_mock.assert_called_once()


@mock.patch(
    "radmantha.spark_sql.spark_sql.get_migrations_list",
    return_value=[
        Migration(revision_id="55", prev_revision_id=None, template_name="test1"),
        Migration(revision_id="unapplied", prev_revision_id="55", template_name="test2"),
    ],
)
def test_get_unapplied_migrations_list_happy(get_migrations_list_mock, test_spark):
    test_spark.sql(VERSION_TABLE_DDL)
    test_spark.sql(
        "insert into spark_catalog.default._spark_migrations_version(migration_type, version_num) "
        "values('fake', '55')"
    )

    result = get_unapplied_migrations_list(test_spark, "/i/am/a/fake/dir", "fake", "spark_catalog", "default")

    assert [migration.template_name for migration in result] == ["test2"]
    get_migrations_list_mock.assert_called_once()


@mock.patch("radmantha.spark_sql.spark_sql.apply_template", return_value="select 1")
@mock.patch("radmantha.spark_sql.spark_sql.Environment")
def test_migrate_initial_happy(environment, apply_template_mock, test_spark):
    environment.return_value.list_templates.return_value = ["20010909_1_all.sql"]

    migrate_initial(test_spark, "/i/am/a/fake/output/folder", "spark_catalog", "default")

    environment.return_value.list_templates.assert_called_once_with(filter_func=use_migration_file)
    environment.return_value.get_template.assert_called_once_with("20010909_1_all.sql")
    apply_template_mock.assert_called_once_with(
        "/i/am/a/fake/output/folder",
        environment.return_value.get_template.return_value,
        cat="spark_catalog",
        schema="default",
    )


def test_record_revision(test_spark):
    test_spark.sql(VERSION_TABLE_DDL)
    test_spark.sql(
        "insert into spark_catalog.default._spark_migrations_version(migration_type, version_num) "
        "values('testing', '000111')"
    )

    record_revision(test_spark, "spark_catalog", "default", "testing", "revIdTesting")

    rows = test_spark.sql("select migration_type, version_num from spark_catalog.default._spark_migrations_version")
    assert [row.asDict() for row in rows.toLocalIterator()] == [
        {"migration_type": "testing", "version_num": "revIdTesting"}
    ]


@mock.patch("radmantha.spark_sql.spark_sql.record_revision")
@mock.patch("radmantha.spark_sql.spark_sql.apply_template", return_value="select 1")
@mock.patch("radmantha.spark_sql.spark_sql.Environment")
@mock.patch(
    "radmantha.spark_sql.spark_sql.get_unapplied_migrations_list",
    return_value=[Migration(revision_id="55", prev_revision_id=None, template_name="test1")],
)
@mock.patch("radmantha.spark_sql.spark_sql.get_migrations_dir", return_value="/i/am/a/fake/root/all_spark_migrations")
def test_migrate_w_rev(
    get_migrations_dir_mock,
    get_unapplied_migrations_list_mock,
    environment,
    apply_template_mock,
    record_revision_mock,
    test_spark,
):
    migrate_w_rev(test_spark, "/i/am/a/fake/out", "/i/am/a/fake/root", "spark_catalog", "default", ALL_SPARK)

    get_migrations_dir_mock.assert_called_once_with("/i/am/a/fake/root", ALL_SPARK)
    get_unapplied_migrations_list_mock.assert_called_once_with(
        test_spark, "/i/am/a/fake/root/all_spark_migrations", ALL_SPARK, cat="spark_catalog", schema="default"
    )
    environment.return_value.get_template.assert_called_once_with("test1")
    apply_template_mock.assert_called_once()
    record_revision_mock.assert_called_once_with(test_spark, "spark_catalog", "default", ALL_SPARK, "55")
