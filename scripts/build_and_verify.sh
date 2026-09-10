#!/usr/bin/env bash
# Build the single distribution, then prove all three install flavours resolve
# and import: bare (no Spark), [local] (pyspark + delta-spark), [databricks]
# (databricks-connect).
#
# There is one wheel. The flavours are extras in its metadata, not separate
# artifacts -- databricks-connect ships its own top-level pyspark/ and delta/,
# so the two Spark flavours can never be installed together.
#
# The per-flavour assertions live in scripts/verify_install.py, which ci.yml
# runs too, so local and CI check exactly the same things.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# databricks-connect is Requires-Python ==3.12.*, so the verification venvs need 3.12.
VERIFY_PYTHON="${RADMANTHA_VERIFY_PYTHON:-python3.12}"
if ! command -v "$VERIFY_PYTHON" >/dev/null; then
  echo "error: '$VERIFY_PYTHON' not on PATH; set RADMANTHA_VERIFY_PYTHON to a 3.12 interpreter" >&2
  exit 1
fi

rm -f dist/radmantha-*.whl dist/radmantha-*.tar.gz
pants package radmantha:dist

wheel="$(ls dist/radmantha-*.whl)"
echo "built ${wheel}"
python3 - "$wheel" <<'PY'
import sys, zipfile
root = zipfile.Path(zipfile.ZipFile(sys.argv[1]))
info = next(p for p in root.iterdir() if p.name.endswith(".dist-info"))
print("--- metadata ---")
for line in (info / "METADATA").read_text().splitlines():
    if line.startswith(("Name:", "Version:", "Requires-Python:", "Requires-Dist:", "Provides-Extra:")):
        print("  " + line)
PY

verify() {
  local flavour="$1" extra="$2"
  local venv
  venv="$(mktemp -d)"
  trap 'rm -rf "$venv"' RETURN
  echo "=== ${flavour}: pip install radmantha${extra} ==="
  "$VERIFY_PYTHON" -m venv "$venv"
  "$venv/bin/pip" --quiet install --upgrade pip
  "$venv/bin/pip" --quiet install "${wheel}${extra}"
  "$venv/bin/python" scripts/verify_install.py "$flavour"
}

verify bare ""
verify local "[local]"
verify databricks "[databricks]"

echo "all three install flavours verified"
