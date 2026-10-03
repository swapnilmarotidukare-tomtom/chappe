"""Text snapshots stored next to the test file in __snapshots__/<name>.txt."""

from __future__ import annotations

from pathlib import Path

UPDATE = "--chappe-update-snapshots"


def check_snapshot(folder: Path, name: str, text: str, *, update: bool) -> None:
    path = folder / f"{name}.txt"
    if update:
        folder.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return
    if not path.exists():
        raise AssertionError(f"snapshot {name} is missing: run pytest {UPDATE} and review {path}")
    expected = path.read_text(encoding="utf-8")
    assert text == expected, f"snapshot {name} changed: review the diff, then run pytest {UPDATE}"
