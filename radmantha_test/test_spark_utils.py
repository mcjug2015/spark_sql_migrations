import sys
from unittest import mock

from radmantha import spark_utils
from radmantha.spark_utils import get_spark, get_warehouse_dir, is_dbr


def _fake_databricks_modules(databricks_session):
    session_module = mock.MagicMock()
    session_module.DatabricksSession = databricks_session
    return {
        "databricks": mock.MagicMock(),
        "databricks.connect": mock.MagicMock(),
        "databricks.connect.session": session_module,
    }


def test_is_dbr_without_dbutils():
    """off databricks pyspark.dbutils is absent, so the probe import fails."""
    assert is_dbr() is False


def test_get_warehouse_dir_from_env(monkeypatch):
    monkeypatch.setenv(spark_utils.WAREHOUSE_DIR_ENV_VAR, "/fake/from/env")

    assert get_warehouse_dir() == "/fake/from/env"


@mock.patch("radmantha.spark_utils.os.getcwd", return_value="/fake/cwd")
def test_get_warehouse_dir_defaults_under_cwd(getcwd, monkeypatch):
    monkeypatch.delenv(spark_utils.WAREHOUSE_DIR_ENV_VAR, raising=False)

    assert get_warehouse_dir() == "/fake/cwd/spark-warehouse"
    getcwd.assert_called_once()


def test_get_spark_use_dbc(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "placeholder")
    databricks_session = mock.MagicMock()

    with mock.patch.dict(sys.modules, _fake_databricks_modules(databricks_session)):
        result = get_spark(use_dbc=True)

    assert result is databricks_session.builder.getOrCreate.return_value
    databricks_session.builder.getOrCreate.assert_called_once()


@mock.patch("radmantha.spark_utils.is_dbr", return_value=True)
def test_get_spark_on_dbr(is_dbr_mock, monkeypatch):
    """use_dbc left False, so is_dbr() is what routes to DatabricksSession."""
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "placeholder")
    databricks_session = mock.MagicMock()

    with mock.patch.dict(sys.modules, _fake_databricks_modules(databricks_session)):
        result = get_spark()

    assert result is databricks_session.builder.getOrCreate.return_value
    is_dbr_mock.assert_called_once()


@mock.patch("radmantha.spark_utils.SparkSession")
@mock.patch("radmantha.spark_utils.is_dbr", return_value=False)
def test_get_spark_remote(is_dbr_mock, spark_session, monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://fakehost:15002")

    result = get_spark()

    assert result is spark_session.builder.remote.return_value.getOrCreate.return_value
    spark_session.builder.remote.assert_called_once_with("sc://fakehost:15002")
    is_dbr_mock.assert_called_once()


@mock.patch("radmantha.spark_utils.configure_spark_with_delta_pip")
@mock.patch("radmantha.spark_utils.get_warehouse_dir", return_value="/fake/warehouse")
@mock.patch("radmantha.spark_utils.SparkSession")
@mock.patch("radmantha.spark_utils.is_dbr", return_value=False)
def test_get_spark_local(is_dbr_mock, spark_session, get_warehouse_dir_mock, configure, monkeypatch):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    monkeypatch.delenv(spark_utils.METASTORE_DIR_ENV_VAR, raising=False)

    result = get_spark()

    assert result is configure.return_value.getOrCreate.return_value
    spark_session.builder.appName.assert_called_once_with(spark_utils.APP_NAME)
    get_warehouse_dir_mock.assert_called_once()
    is_dbr_mock.assert_called_once()


@mock.patch("radmantha.spark_utils.configure_spark_with_delta_pip")
@mock.patch("radmantha.spark_utils.get_warehouse_dir", return_value="/fake/warehouse")
@mock.patch("radmantha.spark_utils.SparkSession")
@mock.patch("radmantha.spark_utils.is_dbr", return_value=False)
def test_get_spark_local_with_metastore(is_dbr_mock, spark_session, get_warehouse_dir_mock, configure, monkeypatch):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    monkeypatch.setenv(spark_utils.METASTORE_DIR_ENV_VAR, "/fake/metastore")

    get_spark()

    assert any(
        call.args
        == (
            "javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=/fake/metastore;create=true",
        )
        for call in spark_session.mock_calls
    )
    configure.assert_called_once()
