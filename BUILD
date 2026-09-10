# The distribution is defined here at the build root rather than in radmantha/ so
# that it can own LICENSE: Pants requires a python_distribution to sit in or above
# the directory of every target it owns, and the license text has to be at the repo
# root for GitHub to detect it.
resources(
    name="license",
    sources=["LICENSE"],
)


python_distribution(
    name="dist",
    dependencies=[
        "radmantha:lib",
        "radmantha/spark_sql:lib",
        ":license",
        # Neither Spark flavour is a hard runtime dep. databricks-connect ships
        # its own top-level pyspark/ and delta/, so installing it alongside
        # pyspark/delta-spark silently clobbers both. The caller picks exactly
        # one via the extras below.
        #
        # These must be `!!` (transitive), not `!`. A single `!` only drops a
        # target's *direct* dependencies, and pyspark/delta-spark arrive here
        # transitively via `radmantha:lib`'s inferred imports -- with `!` they land in
        # install_requires anyway, silently. Verified by reading METADATA out of
        # the built wheel; re-check it there after changing these.
        "!!radmantha:reqs#pyspark",
        "!!radmantha:reqs#delta-spark",
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
        license_files=["LICENSE"],
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
