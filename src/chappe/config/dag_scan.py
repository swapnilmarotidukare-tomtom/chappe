# src/chappe/config/dag_scan.py
"""Static scan of DAG files for `ChappeNotifier(process=...)` names. Uses `ast`: imports nothing."""

from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

NOTIFIER = "ChappeNotifier"


@dataclass(frozen=True)
class ProcessUse:
    """One `ChappeNotifier(...)` call. `name` is None when the argument is not a string literal."""

    path: Path
    line: int
    name: str | None


def dag_files(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(path.rglob("*.py"))
        else:
            yield path


def _is_notifier(call: ast.Call) -> bool:
    func = call.func
    return (isinstance(func, ast.Name) and func.id == NOTIFIER) or (
        isinstance(func, ast.Attribute) and func.attr == NOTIFIER
    )


def _process_argument(call: ast.Call) -> ast.expr | None:
    for keyword in call.keywords:
        if keyword.arg == "process":
            return keyword.value
    return call.args[0] if call.args else None


def scan_file(path: Path) -> list[ProcessUse]:
    """Every `ChappeNotifier` call in the file that passes a process. Raises OSError/SyntaxError."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    uses: list[ProcessUse] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_notifier(node)):
            continue
        argument = _process_argument(node)
        if argument is None or (isinstance(argument, ast.Constant) and argument.value is None):
            continue  # no process given: the notifier uses the DAG's own process
        literal = argument.value if isinstance(argument, ast.Constant) else None
        uses.append(ProcessUse(path, node.lineno, literal if isinstance(literal, str) else None))
    return sorted(uses, key=lambda use: use.line)
