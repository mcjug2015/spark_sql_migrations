import os
import sys

from delta import configure_spark_with_delta_pip  # type: ignore
from pyspark.sql.session import SparkSession

APP_NAME = "radmantha"

WAREHOUSE_DIR_ENV_VAR = "SPARK_WAREHOUSE_DIR"
METASTORE_DIR_ENV_VAR = "SPARK_METASTORE_DIR"
DEFAULT_WAREHOUSE_DIRNAME = "spark-warehouse"


def is_dbr():
    try:
        from pyspark.dbutils import DBUtils  # type: ignore # noqa: F401

        # This will only succeed if the Databricks environment is available
        return True  # pragma: no cover
    except Exception:
        return False


def get_warehouse_dir():
    """where local (non-DBR) spark keeps its warehouse.

    resolved against the caller's cwd, never against __file__: installed from a
    wheel this module lives in site-packages, so a __file__-relative default
    would write the consumer's tables inside their virtualenv.
    """
    return os.environ.get(
        WAREHOUSE_DIR_ENV_VAR,
        os.path.join(os.getcwd(), DEFAULT_WAREHOUSE_DIRNAME),
    )


def get_spark(use_dbc=False):
    if use_dbc or is_dbr():
        from databricks.connect.session import DatabricksSession  # type: ignore # pants: no-infer-dep

        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
        return DatabricksSession.builder.getOrCreate()

    spark_remote = os.environ.get("SPARK_REMOTE")
    if spark_remote:
        return SparkSession.builder.remote(spark_remote).getOrCreate()

    # Pin the worker interpreter to the one actually running this process. Left
    # unset, Spark resolves "python3" independently for the worker daemon, which
    # can land on a different Python build than the driver's when launched from
    # an IDE (mismatched sys.executable vs. PATH resolution) -- causing worker
    # crashes like "SRE module mismatch" that don't reproduce from the CLI.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.warehouse.dir", get_warehouse_dir())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.default", "delta")
    )
    metastore_dir = os.environ.get(METASTORE_DIR_ENV_VAR)
    if metastore_dir:
        builder = builder.config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
    return configure_spark_with_delta_pip(builder).getOrCreate()
