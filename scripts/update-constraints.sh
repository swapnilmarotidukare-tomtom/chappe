#!/usr/bin/env bash
# Regenerate the vendored Airflow constraints and pyproject.toml's [tool.uv] block for one version.
#
#   scripts/update-constraints.sh VERSION [--from DIR]
#
# Downloads Airflow's official constraints-VERSION/constraints-3.10.txt and -3.12.txt, or reads
# DIR/constraints-3.10.txt and DIR/constraints-3.12.txt with --from (offline), writes
# constraints/airflow-VERSION-py3.10.txt and -py3.12.txt with a "# Source: <url>" header, then
# runs scripts/merge_constraints.py to rewrite the constraint-dependencies block, the dev pin
# apache-airflow==VERSION and its comment. Never edit the block by hand.
set -euo pipefail

usage() {
  echo "usage: $0 VERSION [--from DIR]" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
version=$1
shift
from=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --from)
      [[ $# -ge 2 ]] || usage
      from=$2
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "error: VERSION must look like 3.2.2, got '$version'" >&2
  exit 2
}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
python=${PYTHON:-python3}
# Stage inside the repo so the final moves are renames on one filesystem.
stage=$(mktemp -d "$root/.update-constraints.XXXXXX")
trap 'rm -rf "$stage"' EXIT
mkdir "$stage/constraints"

# 1. Fetch (or copy) both files and build them with their headers, all in the stage.
for py in 3.10 3.12; do
  url="https://raw.githubusercontent.com/apache/airflow/constraints-${version}/constraints-${py}.txt"
  if [[ -n $from ]]; then
    cp "$from/constraints-${py}.txt" "$stage/upstream"
  else
    curl --fail --silent --show-error --location --output "$stage/upstream" "$url"
  fi
  # A file fed back from constraints/ already carries the header; do not stack a second one.
  if [[ $(head -n 1 "$stage/upstream") == "# Source: "* ]]; then
    tail -n +2 "$stage/upstream" >"$stage/body"
  else
    cp "$stage/upstream" "$stage/body"
  fi
  {
    printf '# Source: %s\n' "$url"
    cat "$stage/body"
  } >"$stage/constraints/airflow-${version}-py${py}.txt"
done

# 2. Merge into a staged copy of pyproject.toml.
cp -p "$root/pyproject.toml" "$stage/pyproject.toml"
"$python" "$root/scripts/merge_constraints.py" "$version" --root "$stage"

# 3. Everything succeeded: move the three files into place.
for py in 3.10 3.12; do
  name="airflow-${version}-py${py}.txt"
  mv "$stage/constraints/$name" "$root/constraints/$name"
  echo "wrote constraints/$name"
done
mv "$stage/pyproject.toml" "$root/pyproject.toml"
echo "wrote pyproject.toml"

cat <<EOF
Next:
  uv lock
  uv run pytest
Check [project.optional-dependencies] airflow when the minor version changes, and git rm the
constraints/ files of the previous version.
EOF
