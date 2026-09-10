# Client-supplied migration chains, as fixtures. The library never ships these:
# all_spark_migrations/ and dbr_only_migrations/ belong to the consuming project,
# so the tests stand in as that consumer.
resources(
    name="all_spark_migrations",
    sources=["all_spark_migrations/*.sql"],
)

resources(
    name="dbr_only_migrations",
    sources=["dbr_only_migrations/*.sql"],
)
