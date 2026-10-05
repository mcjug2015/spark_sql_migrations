# A client-supplied migration chain, as a fixture. The library never ships one:
# the chain belongs to the consuming project, so the tests stand in as that
# consumer. There is one chain and no second head; the databricks-only statement
# in 260901_04_widget_vol_gated_e5f6a1b2c3d4.sql gates on is_dbr inside it.
resources(
    name="migrations",
    sources=["migrations/*.sql"],
)
