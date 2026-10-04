"""Merge the vendored Airflow constraints into pyproject.toml's [tool.uv] block.

uv reads constraints for `uv lock` and `uv sync` only from `constraint-dependencies`, so the
official files in constraints/ are merged there: a pin identical in both files is unmarked, a pin
present or different only in the Python 3.10 file gets `; python_version < '3.11'`, one only in
the 3.12 file gets `; python_version >= '3.11'`. Sorted case-insensitively by package name.

Usage:
    python scripts/merge_constraints.py [VERSION] [--root DIR] [--check]

VERSION defaults to the dev-group pin `apache-airflow==X.Y.Z`. Without --check the block, the dev
pin and the comment above [tool.uv] are rewritten for VERSION. With --check nothing is written;
the exit code is 1 when pyproject.toml differs from what the merge would produce.

Standard library only; no network.
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

PY_OLD, PY_NEW = "3.10", "3.12"
MARK_OLD = "; python_version < '3.11'"
MARK_NEW = "; python_version >= '3.11'"

_PIN = re.compile(r'^(\s*)"apache-airflow==([^"]+)",(.*)$', re.MULTILINE)
_BLOCK = re.compile(r"^constraint-dependencies = \[\n.*?^\]\n", re.MULTILINE | re.DOTALL)
_COMMENT = re.compile(r"(?:^#[^\n]*\n)*^\[tool\.uv\]\n", re.MULTILINE)

COMMENT = """\
# Dev and CI install under Airflow's official {v} constraints. uv reads constraints only from
# here, so this is the vendored constraints/airflow-{v}-py{old}.txt and -py{new}.txt merged, with
# python_version markers where the two differ. Regenerate it when the pin moves.
[tool.uv]
"""


def read_pins(path: Path) -> dict[str, str]:
    """`name==version` lines of a constraints file; comments and blank lines are skipped."""
    pins: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, sep, version = line.partition("==")
        if not sep or not name or not version:
            raise ValueError(f"{path}: not a 'name==version' line: {raw!r}")
        if name in pins:
            raise ValueError(f"{path}: {name} is pinned twice")
        pins[name] = version
    return pins


def merge(old: dict[str, str], new: dict[str, str]) -> list[str]:
    entries: list[tuple[str, int, str]] = []
    for name in old.keys() | new.keys():
        if name in old and name in new and old[name] == new[name]:
            entries.append((name.lower(), 0, f"{name}=={old[name]}"))
            continue
        if name in old:
            entries.append((name.lower(), 0, f"{name}=={old[name]} {MARK_OLD}"))
        if name in new:
            entries.append((name.lower(), 1, f"{name}=={new[name]} {MARK_NEW}"))
    return [text for _, _, text in sorted(entries)]


def render_block(lines: list[str]) -> str:
    return "constraint-dependencies = [\n" + "".join(f'  "{x}",\n' for x in lines) + "]\n"


def dev_pin(pyproject: str) -> str:
    found = _PIN.findall(pyproject)
    if len(found) != 1:
        raise ValueError('pyproject.toml must hold exactly one "apache-airflow==X.Y.Z" pin')
    return str(found[0][1])


def regenerate(pyproject: str, version: str, block: str) -> str:
    """pyproject.toml text with the block, the dev pin and the [tool.uv] comment for `version`."""
    if len(_BLOCK.findall(pyproject)) != 1:
        raise ValueError("pyproject.toml must hold exactly one constraint-dependencies block")
    if len(_COMMENT.findall(pyproject)) != 1:
        raise ValueError("pyproject.toml must hold exactly one [tool.uv] table")
    text = _BLOCK.sub(lambda _: block, pyproject)
    text = _PIN.sub(lambda m: f'{m.group(1)}"apache-airflow=={version}",{m.group(3)}', text)
    comment = COMMENT.format(v=version, old=PY_OLD, new=PY_NEW)
    return _COMMENT.sub(lambda _: comment, text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", nargs="?", help="Airflow version (default: the dev pin)")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--check", action="store_true", help="compare only; exit 1 on a diff")
    args = parser.parse_args(argv)

    pyproject_path = args.root / "pyproject.toml"
    current = pyproject_path.read_text(encoding="utf-8")
    version = args.version or dev_pin(current)
    folder = args.root / "constraints"
    old = read_pins(folder / f"airflow-{version}-py{PY_OLD}.txt")
    new = read_pins(folder / f"airflow-{version}-py{PY_NEW}.txt")
    wanted = regenerate(current, version, render_block(merge(old, new)))

    if args.check:
        if wanted == current:
            print(f"pyproject.toml matches the Airflow {version} constraints")
            return 0
        diff = difflib.unified_diff(
            current.splitlines(keepends=True),
            wanted.splitlines(keepends=True),
            "pyproject.toml",
            "merged constraints",
        )
        sys.stdout.writelines(diff)
        print(f"pyproject.toml differs from the Airflow {version} constraints", file=sys.stderr)
        return 1
    if wanted != current:
        pyproject_path.write_text(wanted, encoding="utf-8")
    print(f"pyproject.toml: {len(old.keys() | new.keys())} packages for Airflow {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
