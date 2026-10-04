"""scripts/merge_constraints.py and scripts/update-constraints.sh, offline only.

The download branch of the shell script is never run: every test feeds it files with --from and
puts a `curl` on PATH that records any call and fails.
"""

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MERGE = ROOT / "scripts" / "merge_constraints.py"
UPDATE = ROOT / "scripts" / "update-constraints.sh"
VERSION = "3.2.2"


def merge(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(MERGE), *map(str, args)], capture_output=True, text=True, check=False
    )


def copy_repo_files(dest: Path) -> Path:
    """The files the scripts touch, copied so the real pyproject.toml is never written."""
    shutil.copytree(ROOT / "scripts", dest / "scripts")
    shutil.copytree(ROOT / "constraints", dest / "constraints")
    shutil.copy2(ROOT / "pyproject.toml", dest / "pyproject.toml")
    return dest


def test_the_vendored_block_is_exactly_the_merge_of_the_vendored_files() -> None:
    result = merge("--check")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"matches the Airflow {VERSION} constraints" in result.stdout


def test_check_fails_and_shows_the_diff_when_the_block_drifts(tmp_path: Path) -> None:
    root = copy_repo_files(tmp_path)
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    assert '  "zipp==4.1.0",\n' in text
    pyproject.write_text(text.replace('  "zipp==4.1.0",\n', '  "zipp==4.0.0",\n'), "utf-8")
    result = merge("--check", "--root", root)
    assert result.returncode == 1
    assert '-  "zipp==4.0.0",' in result.stdout and '+  "zipp==4.1.0",' in result.stdout


PYPROJECT = """\
[dependency-groups]
dev = [
  "apache-airflow==1.0.0",  # transitive pins come from the constraints below
  "pytest>=8.0",
]

# old comment
[tool.uv]
constraint-dependencies = [
  "stale==0.1",
]

[tool.ruff]
line-length = 100
"""


def test_merge_marks_pins_by_python_version_and_sorts_case_insensitively(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    folder = tmp_path / "constraints"
    folder.mkdir()
    header = "# Source: https://example.invalid\n\n#\n# comment\n"
    (folder / "airflow-9.9.9-py3.10.txt").write_text(
        header + "Same==1.0\nonly-old==2.0\nb-diff==1.0\nzz==1\n", encoding="utf-8"
    )
    (folder / "airflow-9.9.9-py3.12.txt").write_text(
        header + "b-diff==1.1\nonly_new==3.0\nSame==1.0\nzz==1\n", encoding="utf-8"
    )
    result = merge("9.9.9", "--root", tmp_path)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "pyproject.toml").read_text(encoding="utf-8") == (
        "[dependency-groups]\n"
        "dev = [\n"
        '  "apache-airflow==9.9.9",  # transitive pins come from the constraints below\n'
        '  "pytest>=8.0",\n'
        "]\n"
        "\n"
        "# Dev and CI install under Airflow's official 9.9.9 constraints. uv reads constraints"
        " only from\n"
        "# here, so this is the vendored constraints/airflow-9.9.9-py3.10.txt and -py3.12.txt"
        " merged, with\n"
        "# python_version markers where the two differ. Regenerate it when the pin moves.\n"
        "[tool.uv]\n"
        "constraint-dependencies = [\n"
        "  \"b-diff==1.0 ; python_version < '3.11'\",\n"
        "  \"b-diff==1.1 ; python_version >= '3.11'\",\n"
        "  \"only-old==2.0 ; python_version < '3.11'\",\n"
        "  \"only_new==3.0 ; python_version >= '3.11'\",\n"
        '  "Same==1.0",\n'
        '  "zz==1",\n'
        "]\n"
        "\n"
        "[tool.ruff]\n"
        "line-length = 100\n"
    )
    assert merge("9.9.9", "--root", tmp_path, "--check").returncode == 0


def test_update_script_is_executable_and_strict() -> None:
    assert os.stat(UPDATE).st_mode & stat.S_IXUSR
    assert "set -euo pipefail" in UPDATE.read_text(encoding="utf-8")


def test_update_script_regenerates_the_vendored_files_byte_identically_offline(
    tmp_path: Path,
) -> None:
    root = copy_repo_files(tmp_path / "repo")
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    for py in ("3.10", "3.12"):
        vendored = ROOT / "constraints" / f"airflow-{VERSION}-py{py}.txt"
        upstream = vendored.read_text(encoding="utf-8").split("\n", 1)[1]  # drop "# Source:"
        (downloads / f"constraints-{py}.txt").write_text(upstream, encoding="utf-8")
        (root / "constraints" / vendored.name).unlink()
    pyproject = root / "pyproject.toml"
    original = pyproject.read_text(encoding="utf-8")
    start = original.index("constraint-dependencies = [\n")
    end = original.index("\n]\n", start) + 3
    pyproject.write_text(original[:start] + "constraint-dependencies = [\n]\n" + original[end:])

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    called = tmp_path / "curl-called"
    fake_curl = bin_dir / "curl"
    fake_curl.write_text(f'#!/bin/sh\ntouch "{called}"\nexit 1\n', encoding="utf-8")
    fake_curl.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    env["PYTHON"] = sys.executable

    result = subprocess.run(
        ["bash", str(root / "scripts" / "update-constraints.sh"), VERSION, "--from", downloads],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not called.exists(), "the download branch ran"
    assert pyproject.read_bytes() == (ROOT / "pyproject.toml").read_bytes()
    for py in ("3.10", "3.12"):
        name = f"airflow-{VERSION}-py{py}.txt"
        assert (root / "constraints" / name).read_bytes() == (
            ROOT / "constraints" / name
        ).read_bytes()
    assert "uv lock" in result.stdout


def test_update_script_without_a_version_prints_usage_and_fails() -> None:
    result = subprocess.run(["bash", str(UPDATE)], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert "usage" in result.stderr.lower()
