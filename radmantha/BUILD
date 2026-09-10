python_requirements(
    name="reqs",
    source="requirements.txt",
    resolve="python-default",
)

python_sources(
    name="lib",
    dependencies=[
        ":migration_templates",
        ":migrations_initial",
        ":py_typed",
    ],
    # create_new_migration() writes a new .sql file relative to a caller-supplied
    # output path; sandboxed `pants run` would write it into a throwaway chroot
    # copy instead of the real migrations dir.
    run_goal_use_sandbox=False,
)

python_sources(
    name="lib_test",
    dependencies=[
        ":migration_templates",
        ":migrations_initial",
        ":py_typed",
    ],
    resolve="py-reqs-dev",
)

# The initial chain and the default template ship inside the wheel: the library
# owns them, and PackageLoader resolves them out of the installed package.
# PEP 561 marker: without it mypy in a consuming project refuses to read our
# annotations and reports every `import radmantha` as import-untyped.
resources(
    name="py_typed",
    sources=["py.typed"],
)

resources(
    name="migrations_initial",
    sources=["migrations_initial/*.sql"],
)

resources(
    name="migration_templates",
    sources=["migration_templates/*.sql"],
)

python_distribution(
    name="dist",
    dependencies=[
        ":lib",
        "radmantha/spark_sql:lib",
        # Neither Spark flavour is a hard runtime dep. databricks-connect ships
        # its own top-level pyspark/ and delta/, so installing it alongside
        # pyspark/delta-spark silently clobbers both. The caller picks exactly
        # one via the extras below.
        #
        # These must be `!!` (transitive), not `!`. A single `!` only drops a
        # target's *direct* dependencies, and pyspark/delta-spark arrive here
        # transitively via `:lib`'s inferred imports -- with `!` they land in
        # install_requires anyway, silently. Verified by reading METADATA out of
        # the built wheel; re-check it there after changing these.
        "!!:reqs#pyspark",
        "!!:reqs#delta-spark",
    ],
    wheel=True,
    sdist=True,
    # Pants reads the file itself and injects it as long_description, so README.md
    # needs no dependency edge here. Without it the PyPI page renders empty.
    long_description_path="README.md",
    provides=setup_py(
        name="radmantha",
        version="0.0.1",
        description="Chained, idempotent SQL migrations for Spark and Databricks.",
        long_description_content_type="text/markdown",
        author="Victor Semenov",
        # A bare SPDX string, deliberately without a "License :: OSI Approved ::"
        # classifier: setuptools>=77 treats this as a PEP 639 expression and
        # errors out when both are given.
        license="Apache-2.0",
        url="https://github.com/mcjug2015/spark_sql_migrations",
        project_urls={
            "Source": "https://github.com/mcjug2015/spark_sql_migrations",
            "Issues": "https://github.com/mcjug2015/spark_sql_migrations/issues",
        },
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: SQL",
            "Topic :: Database",
            "Typing :: Typed",
        ],
        python_requires=">=3.10",
        extras_require={
            "local": ["pyspark[connect]>=4.0,<5", "delta-spark>=4.0,<5"],
            "databricks": ["databricks-connect>=18,<19"],
        },
    ),
)
