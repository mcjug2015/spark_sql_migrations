import os
import tempfile

import pytest

from spark_sql_migrations.spark_utils import get_spark


def pytest_configure(config):
    # Pants overrides TMPDIR itself for every local test process (to keep temp
    # files inside its own sandbox root), so extra_env_vars can't use it.
    tmp_base = os.environ.get("PYTEST_TMP_BASE")
    if tmp_base:
        os.makedirs(tmp_base, exist_ok=True)
        tempfile.tempdir = tmp_base


@pytest.fixture(scope="session", autouse=True)
def _isolated_spark_storage(tmp_path_factory):
    if os.environ.get("WHICH_SPARK", "local") == "local":
        base = tmp_path_factory.mktemp("spark_store")
        os.environ["SPARK_WAREHOUSE_DIR"] = str(base / "warehouse")
        os.environ["SPARK_METASTORE_DIR"] = str(base / "metastore_db")
    yield


@pytest.fixture(scope="session")
def _spark_connect_remote():
    """Point SPARK_REMOTE at the Spark Connect server on SPARK_CONNECT_PORT.
    Assumes it's already running (started separately, outside of pytest)."""
    if os.environ.get("WHICH_SPARK", "local") == "remote":
        host = os.environ.get("SPARK_CONNECT_HOST", "localhost")
        port = os.environ.get("SPARK_CONNECT_PORT", 15002)
        os.environ["SPARK_REMOTE"] = f"sc://{host}:{port}"
    yield


@pytest.fixture(scope="session")
def test_spark(_spark_connect_remote):
    yield get_spark()
