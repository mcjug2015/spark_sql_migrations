import os
from unittest import mock
from uuid import UUID

import pytest
from freezegun import freeze_time
from jinja2.environment import Environment
from jinja2.loaders import FileSystemLoader
from jinja2.utils import select_autoescape

from spark_sql_migrations.spark_sql.spark_sql import (
    CLIENT_OUTPUT_DIRNAME,
    Migration,
    _parse_migration,
    apply_template,
    create_new_migration,
    gate_on_is_dbr,
    get_ascending_letters_within_minute,
    get_default_template_path,
    get_migrations_list,
    get_ordered_migration_objs,
    get_output_folder,
    get_table_version,
    get_unapplied_migrations_list,
    has_no_statements,
    main,
    migrate_initial,
    migrate_w_rev,
    record_revision,
    render_and_apply,
    run_migrations,
)

VERSION_TABLE_DDL = """
    begin
    drop table if exists spark_catalog.default._spark_migrations_version;
    create table spark_catalog.default._spark_migrations_version (
        version_num string not null
    );
    end;
"""


def test_get_default_template_path():
    """the shipped template resolves relative to the installed package."""
    assert os.path.isfile(get_default_template_path())


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


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_ordered_migration_objs", return_value=["testing"])
@mock.patch("spark_sql_migrations.spark_sql.spark_sql._parse_migration", return_value="testing")
def test_get_migrations_list_ignores_non_sql_files(parse_migration, get_ordered, tmp_path):
    (tmp_path / "01_i_am_test.sql").touch()
    (tmp_path / "02_i_am_non_sql_test.txt").touch()

    assert get_migrations_list(str(tmp_path)) == ["testing"]
    parse_migration.assert_called_once_with(str(tmp_path), "01_i_am_test.sql")
    get_ordered.assert_called_once_with(["testing"])


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_migrations_list", return_value=["testing"])
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_table_version", return_value=None)
def test_get_unapplied_migrations_list_no_table(get_table_version, get_migrations_list):
    result = get_unapplied_migrations_list(None, "/i/am/a/fake/dir", "spark_catalog", "default")

    assert result == ["testing"]
    get_migrations_list.assert_called_once_with("/i/am/a/fake/dir")
    get_table_version.assert_called_once()


@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.get_migrations_list",
    return_value=[
        Migration(revision_id="55", prev_revision_id=None, template_name="test1"),
        Migration(revision_id="unapplied", prev_revision_id="55", template_name="test2"),
    ],
)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_table_version", return_value={"version_num": "55"})
def test_get_unapplied_migrations_list_no_rows(get_table_version, get_migrations_list):
    result = get_unapplied_migrations_list(None, "/i/am/a/fake/dir", "spark_catalog", "default")

    assert [migration.template_name for migration in result] == ["test2"]
    get_migrations_list.assert_called_once()
    get_table_version.assert_called_once()


@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.get_migrations_list",
    return_value=[Migration(revision_id="wont match", prev_revision_id=None, template_name="test1")],
)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_table_version", return_value={"version_num": "55"})
def test_get_unapplied_migrations_list_no_match(get_table_version, get_migrations_list):
    with pytest.raises(ValueError, match="which matches no migration in"):
        get_unapplied_migrations_list(None, "/i/am/a/fake/dir", "spark_catalog", "default")
    get_migrations_list.assert_called_once()
    get_table_version.assert_called_once()


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_migrations_list", return_value=[])
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_table_version", return_value={"version_num": "55"})
def test_get_unapplied_migrations_list_retired_chain(get_table_version, get_migrations_list):
    """the chain is gone but its row survives -- what collapsing two chains leaves behind."""
    assert get_unapplied_migrations_list(None, "/i/am/a/fake/dir", "spark_catalog", "default") == []
    get_migrations_list.assert_called_once()
    get_table_version.assert_called_once()


def test_gate_on_is_dbr():
    """the gate opens below the headers, so they stay readable as comments."""
    gated = gate_on_is_dbr("-- revision_id:aaa;\n-- prev_revision_id:;\nbegin\nselect 1;\nend;\n")

    assert gated == (
        "-- revision_id:aaa;\n-- prev_revision_id:;\n{% if is_dbr %}\nbegin\nselect 1;\nend;\n{% endif %}\n"
    )


def test_gate_on_is_dbr_no_body():
    """a template with nothing to gate still comes back parseable."""
    assert gate_on_is_dbr("-- revision_id:aaa;\n") == "-- revision_id:aaa;\n{% if is_dbr %}\n{% endif %}\n"


@freeze_time("2007-07-07")
@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.uuid.uuid4", return_value=UUID(bytes=b"1111222233334444", version=4)
)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_migrations_list", side_effect=ValueError("testing"))
def test_create_new_migration_long_slug(get_migrations_list, _uuid, tmp_path):
    template_path = tmp_path / "template.sql"
    with open(template_path, "w") as handle:
        handle.write("-- revision_id:{{revision_id}};\n-- prev_revision_id:{{prev_revision_id}};\n")
    output_path = tmp_path / "migrations"
    os.makedirs(output_path)

    create_new_migration(
        "a very long message that certainly runs past the slug truncation limit",
        template_path=str(template_path),
        output_path=str(output_path),
    )

    expected = output_path / "070707_a_very_long_message_that_certainly_runs_333334343434.sql"
    with open(expected) as result_file:
        assert "-- revision_id:333334343434;" in result_file.read()
        result_file.seek(0)
        assert "-- prev_revision_id:;" in result_file.read()
    get_migrations_list.assert_called_once()


@freeze_time("2007-07-07")
@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.uuid.uuid4", return_value=UUID(bytes=b"1111222233334444", version=4)
)
@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.get_migrations_list",
    return_value=[Migration(revision_id="7", prev_revision_id="", template_name="test")],
)
def test_create_new_migration_prev_rev(get_migrations_list, _uuid, tmp_path):
    template_path = tmp_path / "template.sql"
    with open(template_path, "w") as handle:
        handle.write("-- revision_id:{{revision_id}};\n-- prev_revision_id:{{prev_revision_id}};\n")
    output_path = tmp_path / "migrations"
    os.makedirs(output_path)

    create_new_migration(
        "a very long message that certainly runs past the slug truncation limit",
        template_path=str(template_path),
        output_path=str(output_path),
    )

    expected = output_path / "070707_a_very_long_message_that_certainly_runs_333334343434.sql"
    with open(expected) as result_file:
        assert "-- revision_id:333334343434;" in result_file.read()
        result_file.seek(0)
        assert "-- prev_revision_id:7;" in result_file.read()
    get_migrations_list.assert_called_once()


@freeze_time("2007-07-07")
@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.uuid.uuid4", return_value=UUID(bytes=b"1111222233334444", version=4)
)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.gate_on_is_dbr", return_value="-- gated;\n")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_migrations_list", return_value=[])
def test_create_new_migration_add_is_dbr(get_migrations_list, gate_on_is_dbr_mock, _uuid, tmp_path):
    template_path = tmp_path / "template.sql"
    with open(template_path, "w") as handle:
        handle.write("-- revision_id:{{revision_id}};\n-- prev_revision_id:{{prev_revision_id}};\nbegin\nend;\n")
    output_path = tmp_path / "migrations"
    os.makedirs(output_path)

    create_new_migration(
        "add a volume",
        template_path=str(template_path),
        output_path=str(output_path),
        add_is_dbr=True,
    )

    expected = output_path / "070707_add_a_volume_333334343434.sql"
    with open(expected) as result_file:
        assert result_file.read() == "-- gated;\n"
    gate_on_is_dbr_mock.assert_called_once_with("-- revision_id:333334343434;\n-- prev_revision_id:;\nbegin\nend;\n")
    get_migrations_list.assert_called_once()


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.is_dbr", return_value=False)
def test_apply_template(is_dbr, tmp_path):
    with open(tmp_path / "01_template.sql", "w") as handle:
        handle.write(
            "\nbegin\n"
            "{% if is_dbr %}grant ALL PRIVILEGES on catalog {{cat}} to `somebody`;"
            "{% else %}select x from {{cat}}.{{schema}}.fake_table;{% endif %}\n"
            "end;\n"
        )
    env = Environment(loader=FileSystemLoader(str(tmp_path)), autoescape=select_autoescape())

    result = apply_template(str(tmp_path), env.get_template("01_template.sql"), "FAKETESTCAT", "FAKETESTSCHEMA")

    assert "select x from FAKETESTCAT.FAKETESTSCHEMA.fake_table;" in result
    with open(tmp_path / "01_template_primed.sql") as result_file:
        assert "select x from FAKETESTCAT.FAKETESTSCHEMA.fake_table;" in result_file.read()
    is_dbr.assert_called_once()


@freeze_time("2007-07-07 01:02:03.123456")
def test_get_ascending_letters_within_minute():
    assert get_ascending_letters_within_minute() == "BCDEFG"


@freeze_time("2007-07-07 01:02:03")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_ascending_letters_within_minute", return_value="QQPP")
def test_get_output_folder(get_ascending_letters):
    assert get_output_folder("/i/am/a/fake/parent") == "/i/am/a/fake/parent/20070707_0102_QQPP"
    get_ascending_letters.assert_called_once()


def test_has_no_statements_only_comments_and_blanks():
    """what a fully gated migration renders to: its headers, and nothing else."""
    assert has_no_statements("-- revision_id:aaa;\n-- prev_revision_id:;\n\n   \n")


def test_has_no_statements_with_a_statement():
    assert has_no_statements("-- revision_id:aaa;\nselect 1;\n") is False


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.has_no_statements", return_value=False)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.apply_template", return_value="select 1")
def test_render_and_apply(apply_template_mock, has_no_statements_mock):
    """the only statement here is one the render mock made up, so spark is a mock too."""
    spark = mock.MagicMock()
    template = mock.MagicMock()

    render_and_apply(spark, "/i/am/a/fake/out", template, "spark_catalog", "default")

    apply_template_mock.assert_called_once_with("/i/am/a/fake/out", template, cat="spark_catalog", schema="default")
    has_no_statements_mock.assert_called_once_with("select 1")
    spark.sql.assert_called_once_with("select 1")


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.has_no_statements", return_value=True)
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.apply_template", return_value="-- gated out;\n")
def test_render_and_apply_gated_out(apply_template_mock, has_no_statements_mock):
    """a render with nothing in it for this engine never reaches spark.

    spark is None on purpose: were the render not skipped, asking it for a session
    method would raise, so reaching the assertions is the proof it was never asked.
    """
    template = mock.MagicMock()
    template.name = "first_01_intial_create_cat.sql"

    assert render_and_apply(None, "/i/am/a/fake/out", template, "spark_catalog", "default") is None

    apply_template_mock.assert_called_once()
    has_no_statements_mock.assert_called_once_with("-- gated out;\n")


def test_migrate_w_rev_no_chain_dir():
    """a chain the project doesn't supply is skipped.

    spark is None and nothing is patched: were the chain not skipped, the walk of
    a nonexistent directory would raise instead.
    """
    assert migrate_w_rev(None, "/i/am/a/fake/out", "/i/am/a/fake/chain", "spark_catalog", "default") is None


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_output_folder", return_value="/i/am/a/fake/out")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.run_migrations")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.get_spark")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.os.makedirs")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.os.getcwd", return_value="/fake/cwd")
def test_main_defaults_output_under_cwd(getcwd, makedirs, get_spark_mock, run_migrations_mock, get_output_folder_mock):
    main("spark_catalog", "default", "/i/am/a/fake/chain")

    get_output_folder_mock.assert_called_once_with("/fake/cwd/migrations_out")
    getcwd.assert_called_once()
    makedirs.assert_called_once_with("/i/am/a/fake/out")
    get_spark_mock.assert_called_once()
    run_migrations_mock.assert_called_once()


def test_get_table_version_no_table(test_spark):
    test_spark.sql("drop table if exists spark_catalog.default._spark_migrations_version")
    assert get_table_version(test_spark, "spark_catalog", "default") is None


def test_get_table_version(test_spark):
    test_spark.sql(VERSION_TABLE_DDL)
    test_spark.sql("insert into spark_catalog.default._spark_migrations_version(version_num) values('77')")
    assert get_table_version(test_spark, "spark_catalog", "default")["version_num"] == "77"


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.render_and_apply")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.Environment")
def test_migrate_initial_happy(environment, render_and_apply_mock, test_spark):
    environment.return_value.list_templates.return_value = ["first_01_intial_create_cat.sql"]

    migrate_initial(test_spark, "/i/am/a/fake/output/folder", "spark_catalog", "default")

    environment.return_value.list_templates.assert_called_once_with(extensions=["sql"])
    environment.return_value.get_template.assert_called_once_with("first_01_intial_create_cat.sql")
    render_and_apply_mock.assert_called_once_with(
        test_spark,
        "/i/am/a/fake/output/folder",
        environment.return_value.get_template.return_value,
        cat="spark_catalog",
        schema="default",
    )


def test_record_revision(test_spark):
    test_spark.sql(VERSION_TABLE_DDL)
    test_spark.sql("insert into spark_catalog.default._spark_migrations_version(version_num) values('000111')")

    record_revision(test_spark, "spark_catalog", "default", "revIdTesting")

    rows = test_spark.sql("select version_num from spark_catalog.default._spark_migrations_version")
    assert [row.asDict() for row in rows.toLocalIterator()] == [{"version_num": "revIdTesting"}]


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.record_revision")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.render_and_apply")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.Environment")
@mock.patch(
    "spark_sql_migrations.spark_sql.spark_sql.get_unapplied_migrations_list",
    return_value=[Migration(revision_id="55", prev_revision_id=None, template_name="test1")],
)
def test_migrate_w_rev(
    get_unapplied_migrations_list_mock,
    environment,
    render_and_apply_mock,
    record_revision_mock,
    test_spark,
    tmp_path,
):
    migrate_w_rev(test_spark, "/i/am/a/fake/out", str(tmp_path), "spark_catalog", "default")

    get_unapplied_migrations_list_mock.assert_called_once_with(
        test_spark, str(tmp_path), cat="spark_catalog", schema="default"
    )
    environment.return_value.get_template.assert_called_once_with("test1")
    render_and_apply_mock.assert_called_once_with(
        test_spark,
        "/i/am/a/fake/out",
        environment.return_value.get_template.return_value,
        cat="spark_catalog",
        schema="default",
    )
    record_revision_mock.assert_called_once_with(test_spark, "spark_catalog", "default", "55")


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.migrate_w_rev")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.migrate_initial")
def test_run_migrations_no_version_table(migrate_initial_mock, migrate_w_rev_mock, test_spark, tmp_path):
    test_spark.sql("drop table if exists spark_catalog.default._spark_migrations_version")
    output_folder = str(tmp_path / "28818989_8182_BCDEQQ")

    run_migrations(test_spark, "spark_catalog", "default", output_folder, "/i/am/a/fake/chain")

    assert os.path.isdir(output_folder)
    migrate_initial_mock.assert_called_once_with(
        test_spark, os.path.join(output_folder, "migrations_initial"), "spark_catalog", "default"
    )
    migrate_w_rev_mock.assert_called_once_with(
        test_spark,
        os.path.join(output_folder, CLIENT_OUTPUT_DIRNAME),
        "/i/am/a/fake/chain",
        "spark_catalog",
        "default",
    )


@mock.patch("spark_sql_migrations.spark_sql.spark_sql.migrate_w_rev")
@mock.patch("spark_sql_migrations.spark_sql.spark_sql.migrate_initial")
def test_run_migrations_version_table_exists(migrate_initial_mock, migrate_w_rev_mock, test_spark, tmp_path):
    test_spark.sql(VERSION_TABLE_DDL)
    output_folder = str(tmp_path / "28818989_8182_BCDEQQ")

    run_migrations(test_spark, "spark_catalog", "default", output_folder, "/i/am/a/fake/chain")

    migrate_initial_mock.assert_not_called()
    migrate_w_rev_mock.assert_called_once()
