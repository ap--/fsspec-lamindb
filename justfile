# List the recipes
default:
    @just --list

# Run the unit tests against the fake LaminHub
test *args:
    uv run --locked pytest {{ args }}

# Run the unit tests and report the branch coverage
coverage *args:
    uv run --locked pytest --cov {{ args }}

# Run the tests and the examples of the docs against the live LaminHub
test-live *args:
    FSSPEC_LAMINDB_LIVE=1 uv run --locked --all-extras pytest tests/test_live.py --doctest-modules src --doctest-glob="*.md" README.md {{ args }}

# Check that the LaminHub defaults match those of the latest lamindb-setup
test-lamindb-setup *args:
    uv run --locked --with lamindb-setup pytest -m lamindb_setup {{ args }}

# Check the lockfile, linting, formatting and types
lint:
    uv lock --check
    uv run --locked ruff check .
    uv run --locked ruff format --check .
    uv run --locked --all-extras ty check --error-on-warning

# Check the types
typecheck:
    uv run --locked --all-extras ty check --error-on-warning

# Format the code and fix what the linter can fix
fmt:
    uv run --locked ruff format .
    uv run --locked ruff check --fix .

# Run all checks
check: lint test
