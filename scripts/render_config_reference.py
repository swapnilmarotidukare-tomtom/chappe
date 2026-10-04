"""Render the Reference section of docs/guides/configuration.md from the config models.

Usage:
    python scripts/render_config_reference.py [GUIDE] [--check]

The block between the BEGIN and END markers in the guide is rewritten from the pydantic models in
`chappe.config.models`: one table per model reachable from `ConfigFile`, in the order the fields
are declared. With --check nothing is written; the exit code is 1 when the committed block differs.

Standard library and pydantic only; no network.
"""

from __future__ import annotations

import argparse
import sys
import types
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin

from annotated_types import Ge, Gt, MinLen
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from chappe.config.models import ConfigFile

ROOT = Path(__file__).resolve().parent.parent
GUIDE = ROOT / "docs" / "guides" / "configuration.md"
BEGIN = "<!-- BEGIN GENERATED REFERENCE -->"
END = "<!-- END GENERATED REFERENCE -->"

_SCALARS: dict[Any, str] = {str: "string", float: "number", int: "integer", bool: "boolean"}


def _nested(annotation: Any) -> Iterator[type[BaseModel]]:
    """The models an annotation refers to, in the order it names them."""
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        yield annotation
    for arg in get_args(annotation):
        yield from _nested(arg)


def models(root: type[BaseModel]) -> list[type[BaseModel]]:
    """`root` and every model reachable from it, each once, depth first in field order."""
    found: list[type[BaseModel]] = []

    def visit(model: type[BaseModel]) -> None:
        if model in found:
            return
        found.append(model)
        for field in model.model_fields.values():
            for inner in _nested(field.annotation):
                visit(inner)

    visit(root)
    return found


def _paths(root: type[BaseModel]) -> dict[type[BaseModel], list[str]]:
    """Where each model sits in the YAML file, for example `chappe.processes.<name>`."""
    where: dict[type[BaseModel], list[str]] = {}

    def visit(model: type[BaseModel], path: str) -> None:
        paths = where.setdefault(model, [])
        if path in paths:
            return
        paths.append(path)
        for name, field in model.model_fields.items():
            below = f"{path}.{name}" if path else name
            if get_origin(field.annotation) is dict:  # a mapping keyed by a user-chosen name
                below += ".<name>"
            for inner in _nested(field.annotation):
                visit(inner, below)

    visit(root, "")
    return where


def _type(annotation: Any) -> str:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Literal:
        return " or ".join(f'`"{value}"`' for value in args)
    if origin in (Union, types.UnionType):
        parts = [_type(a) for a in args if a is not type(None)]
        return " or ".join(parts + (["null"] if type(None) in args else []))
    if origin is list:
        return f"list of {_type(args[0])}"
    if origin is dict:
        if args[1] is Any:
            return "mapping"
        return f"mapping of {_type(args[0])} → {_type(args[1])}"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return f"`{annotation.__name__}`"
    return _SCALARS.get(annotation, str(annotation))


def _limits(metadata: Sequence[Any]) -> str:
    notes = []
    for item in metadata:
        if isinstance(item, Gt):
            notes.append(f"> {item.gt}")
        elif isinstance(item, Ge):
            notes.append(f"≥ {item.ge}")
        elif isinstance(item, MinLen):
            notes.append(f"at least {item.min_length}")
    return f" ({', '.join(notes)})" if notes else ""


def _value(value: Any) -> str:
    if value is None:
        return "`null`"
    if isinstance(value, bool):
        return f"`{str(value).lower()}`"
    if isinstance(value, str):
        return f'`"{value}"`'
    return f"`{value}`"


def _default(field: Any) -> str:
    if field.is_required():
        return "none"
    if field.default is not PydanticUndefined:
        return _value(field.default)
    made = field.default_factory()
    if isinstance(made, BaseModel):
        return f"defaults of `{type(made).__name__}`"
    return "`{}`" if made == {} else _value(made)


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _table(model: type[BaseModel], paths: list[str]) -> str:
    lines = [f"### `{model.__name__}`", ""]
    if model.__doc__:
        lines += [_cell(model.__doc__), ""]
    used = ", ".join(f"`{p}`" for p in paths if p)
    if used:
        lines += [f"Found at {used}.", ""]
    lines += ["| Key | Type | Default | Required | Description |", "|---|---|---|---|---|"]
    for name, field in model.model_fields.items():
        kind = _type(field.annotation) + _limits(field.metadata)
        required = "yes" if field.is_required() else "no"
        row = [f"`{name}`", kind, _default(field), required, field.description or ""]
        lines.append("| " + " | ".join(_cell(c) for c in row) + " |")
    return "\n".join(lines)


def render() -> str:
    """The generated block (without the markers), ending in a newline."""
    paths = _paths(ConfigFile)
    return "\n\n".join(_table(m, paths[m]) for m in models(ConfigFile)) + "\n"


def splice(text: str, block: str) -> str:
    """`text` with what lies between the markers replaced by `block`."""
    start, end = text.find(BEGIN), text.find(END)
    if start < 0 or end < start:
        raise ValueError(f"the guide needs the markers {BEGIN} and {END}, in this order")
    return f"{text[: start + len(BEGIN)]}\n{block}{text[end:]}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("guide", nargs="?", type=Path, default=GUIDE)
    parser.add_argument("--check", action="store_true", help="write nothing; exit 1 if stale")
    args = parser.parse_args(argv)
    committed = args.guide.read_text(encoding="utf-8")
    try:
        fresh = splice(committed, render())
    except ValueError as exc:
        print(f"render_config_reference: {exc}", file=sys.stderr)
        return 1
    if fresh == committed:
        return 0
    if args.check:
        print(
            f"render_config_reference: {args.guide} is out of date with the config models; "
            "regenerate it with: uv run python scripts/render_config_reference.py",
            file=sys.stderr,
        )
        return 1
    args.guide.write_text(fresh, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
