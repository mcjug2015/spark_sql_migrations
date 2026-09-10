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
