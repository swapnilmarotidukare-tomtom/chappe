# src/chappe/cli.py
from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone

from chappe.config.loader import load_settings
from chappe.config.models import ChappeSettings
from chappe.core.errors import ChappeConfigError

Handler = Callable[[argparse.Namespace], int]


def _check_theme_names(settings: ChappeSettings) -> None:
    from chappe.themes import THEMES  # lazy: only validate-config needs the themes
    from chappe.themes.tokens import resolve_tokens

    named = [("defaults.theme", settings.defaults.theme)]
    for name, process in settings.processes.items():
        named.append((f"processes.{name}.theme", settings.theme_for(process)))
    for where, theme in named:
        if theme.name not in THEMES:
            available = ", ".join(sorted(THEMES))
            raise ChappeConfigError(
                f"{where}: unknown theme {theme.name!r}; available themes: {available}"
            )
        try:
            resolve_tokens(theme.name, theme.tokens)
        except ChappeConfigError as exc:
            raise ChappeConfigError(f"{where}: {exc}") from exc


def _validate_config(args: argparse.Namespace) -> int:
    try:
        settings = load_settings(args.path)
        _check_theme_names(settings)
    except ChappeConfigError as exc:
        print(f"chappe: {exc}", file=sys.stderr)
        return 1
    print(f"chappe: OK ({len(settings.processes)} processes)")
    return 0


_DURATION = re.compile(r"([1-9][0-9]*)([dh])", re.ASCII)


def parse_duration(text: str) -> timedelta:
    """`7d` -> 7 days, `12h` -> 12 hours."""
    match = _DURATION.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"invalid duration {text!r}: use days or hours, for example 7d or 12h")
    amount = int(match.group(1))
    return timedelta(days=amount) if match.group(2) == "d" else timedelta(hours=amount)


def _cleanup(args: argparse.Namespace) -> int:
    from chappe.stores import airflow_variable as store

    older_than: timedelta = args.older_than
    dry_run: bool = args.dry_run
    try:
        keys = store.cleanup(
            older_than,
            now=datetime.now(timezone.utc),
            list_keys=store.airflow_db_keys,
            get=store.airflow_db_get,
            delete=store.airflow_db_delete,
            dry_run=dry_run,
        )
    except Exception as exc:  # Airflow not installed or its database not reachable
        print(f"chappe: cleanup failed: {exc}", file=sys.stderr)
        return 1
    verb = "would delete" if dry_run else "deleted"
    for key in keys:
        print(f"{verb} {key}")
    print(f"chappe: {verb} {len(keys)} Variable(s) older than {older_than}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chappe")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    validate = commands.add_parser("validate-config", help="check a Chappe config file")
    validate.add_argument("path", nargs="?", help="config file (default: $CHAPPE_CONFIG)")
    validate.set_defaults(handler=_validate_config)

    clean = commands.add_parser(
        "cleanup", help="delete old Chappe Variables (run where the Airflow CLI runs)"
    )
    clean.add_argument(
        "--older-than",
        required=True,
        type=parse_duration,
        metavar="DURATION",
        help="age such as 7d or 12h",
    )
    clean.add_argument("--dry-run", action="store_true", help="only list what would be deleted")
    clean.set_defaults(handler=_cleanup)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handler: Handler = args.handler
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
