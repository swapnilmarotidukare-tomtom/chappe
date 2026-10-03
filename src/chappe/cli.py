# src/chappe/cli.py
from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chappe")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    validate = commands.add_parser("validate-config", help="check a Chappe config file")
    validate.add_argument("path", nargs="?", help="config file (default: $CHAPPE_CONFIG)")
    validate.set_defaults(handler=_validate_config)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handler: Handler = args.handler
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
