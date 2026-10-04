# src/chappe/cli.py
from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

from chappe.config.dag_scan import ProcessUse, dag_files, scan_file
from chappe.config.loader import load_settings
from chappe.core.errors import ChappeConfigError

Handler = Callable[[argparse.Namespace], int]


def _validate_config(args: argparse.Namespace) -> int:
    try:
        settings = load_settings(args.path)  # also checks theme names and tokens
    except ChappeConfigError as exc:
        print(f"chappe: {exc}", file=sys.stderr)
        return 1
    status = _check_dags(args.dags, sorted(settings.processes)) if args.dags else 0
    if status == 0:
        print(f"chappe: OK ({len(settings.processes)} processes)")
    return status


def _check_dags(paths: list[str], configured: list[str]) -> int:
    """Flag literal `ChappeNotifier(process=...)` names that are not configured (1) or 0."""
    uses: list[ProcessUse] = []
    failed = False
    for file in dag_files(Path(path) for path in paths):
        try:
            uses.extend(scan_file(file))
        except (OSError, SyntaxError, ValueError) as exc:
            print(f"chappe: {file}: cannot scan ({exc})", file=sys.stderr)
            failed = True
    for use in uses:
        if use.name is None:
            print(
                f"chappe: {use.path}:{use.line}: ChappeNotifier process cannot be checked "
                "(not a string literal)"
            )
        elif use.name not in configured:
            names = ", ".join(configured) or "none"
            print(
                f"chappe: {use.path}:{use.line}: ChappeNotifier(process={use.name!r}) "
                f"is not a configured process; configured: {names}",
                file=sys.stderr,
            )
            failed = True
    return 1 if failed else 0


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
    validate.add_argument(
        "--dags",
        action="append",
        default=[],
        metavar="PATH",
        help="DAG file or folder to scan for unknown ChappeNotifier process names (repeatable)",
    )
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
