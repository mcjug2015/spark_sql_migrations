python_requirements(name="reqs-dev", source="requirements-dev.txt", resolve="py-reqs-dev")

python_sources(
    name="lib_test",
    resolve="py-reqs-dev",
)

python_test_utils(
    name="test_utils",
    resolve="py-reqs-dev",
)

python_tests(
    name="tests",
    dependencies=["radmantha:lib_test"],
    resolve="py-reqs-dev",
)
