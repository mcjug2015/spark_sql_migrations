"""Prove one install flavour of the published wheel actually works.

There is a single spark_sql_migrations distribution; `bare`, `local` and `databricks` are
its three install flavours, selected by extras. Run against an environment that
already has the wheel installed:

    python scripts/verify_install.py {bare|local|databricks}
"""

import importlib.resources
import sys


def check_package_data():
    """the .sql files must travel in the wheel or PackageLoader finds nothing."""
    files = importlib.resources.files("spark_sql_migrations")
    for package_path in ("migrations_initial", "migration_templates"):
        if not (files / package_path).is_dir():
            raise AssertionError(f"{package_path} missing from the installed package")
    initial = sorted(p.name for p in (files / "migrations_initial").iterdir())
    print(f"  package data: {len(initial)} initial migrations, first is {initial[0]}")


def check_bare():
    """the base distribution must pull no Spark at all.

    databricks-connect ships its own top-level pyspark/ and delta/, so a base
    dependency on pyspark would clobber the [databricks] flavour on install.
    """
    import jinja2

    print(f"  jinja2 {jinja2.__version__}")
    try:
        import spark_sql_migrations.spark_utils  # noqa: F401
    except ModuleNotFoundError as exc:
        print(f"  no Spark present, as intended ({exc.name} not installed)")
    else:
        raise AssertionError("bare install pulled in a Spark distribution")


def check_local():
    import delta  # noqa: F401
    import pyspark

    from spark_sql_migrations.spark_sql import spark_sql

    print(f"  pyspark {pyspark.__version__}, is_dbr() -> {spark_sql.is_dbr()}")


def check_databricks():
    import databricks.connect  # type: ignore # noqa: F401 # pants: no-infer-dep
    import pyspark

    from spark_sql_migrations.spark_sql import spark_sql

    print(f"  databricks-connect present, pyspark {pyspark.__version__}, is_dbr() -> {spark_sql.is_dbr()}")


CHECKS = {"bare": check_bare, "local": check_local, "databricks": check_databricks}


def main(flavour):
    if flavour not in CHECKS:
        raise SystemExit(f"unknown flavour {flavour!r}; expected one of {sorted(CHECKS)}")
    print(f"verifying the {flavour} install flavour")
    check_package_data()
    CHECKS[flavour]()
    print(f"{flavour}: ok")


if __name__ == "__main__":
    main(sys.argv[1])
