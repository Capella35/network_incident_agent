"""Tests for .claude/hooks/guard.py (the Claude Code PreToolUse guard).

The guard lives outside src/ (it is Claude Code config, not product code), so it is
loaded by path. Paths are hand-written; no repo data is read.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = "/repo"
GUARD_PATH = Path(__file__).resolve().parents[2] / ".claude" / "hooks" / "guard.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("claude_guard", GUARD_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


guard = _load()


@pytest.fixture(autouse=True)
def _project_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", ROOT)


def bash(cmd: str) -> str | None:
    result: str | None = guard.evaluate({"tool_name": "Bash", "tool_input": {"command": cmd}})
    return result


def tool(name: str, **tool_input: Any) -> str | None:
    result: str | None = guard.evaluate({"tool_name": name, "tool_input": tool_input})
    return result


# --- Rule 8: .env -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "cat .env",
        "cat ./.env",
        f"cat {ROOT}/.env",
        "set -a; . ./.env; set +a; python x.py",
        "source .env",
        "grep PASSWORD .env",
        "cp .env.example .env",
        "echo $(cat .env)",
        "head -n1 .env.local",
        "docker compose --env-file .env up -d",
        "uv run --env-file=.env python x.py",
        'git commit -m "$(cat .env)"',
    ],
)
def test_bash_env_access_blocked(cmd: str) -> None:
    reason = bash(cmd)
    assert reason and "Rule 8" in reason


@pytest.mark.parametrize(
    "cmd",
    [
        "cat .env.example",
        "make up",
        "make db-topology",
        'git commit -m "Document .env setup in README"',
        "grep -rn '\\.env' docs",
        "ls -la",
    ],
)
def test_bash_env_lookalikes_allowed(cmd: str) -> None:
    assert bash(cmd) is None


@pytest.mark.parametrize("name", ["Read", "Edit", "Write"])
def test_file_tools_env_blocked(name: str) -> None:
    assert tool(name, file_path=f"{ROOT}/.env")
    assert tool(name, file_path=f"{ROOT}/.env.local")


def test_file_tools_env_example_allowed() -> None:
    assert tool("Read", file_path=f"{ROOT}/.env.example") is None
    assert tool("Edit", file_path=f"{ROOT}/.env.example") is None


def test_grep_env_blocked() -> None:
    assert tool("Grep", pattern="PASS", path=f"{ROOT}/.env")
    assert tool("Grep", pattern="PASS", glob=".env")
    assert tool("Grep", pattern="PASS", path=f"{ROOT}/src") is None


# --- data/, uv.lock, .git, guard files --------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "echo x > data/processed/x.csv",
        "echo x >> ./data/manifest.json",
        "rm -rf data/raw",
        "rm -rf data",
        "mkdir -p data/processed/new",
        "touch data/x",
        "cp /tmp/a.parquet data/processed/traffic/",
        "mv x.csv data/processed/topology_nodes.csv",
        "sed -i '' 's/a/b/' data/manifest.json",
        "python make.py | tee data/out.txt",
        f"rm {ROOT}/data/manifest.json",
        "echo x > uv.lock",
        "rm .git/index",
        "rm .claude/hooks/guard.py",
        "echo '{}' > .claude/settings.json",
    ],
)
def test_bash_protected_writes_blocked(cmd: str) -> None:
    assert bash(cmd)


@pytest.mark.parametrize(
    "cmd",
    [
        "cat data/manifest.json",
        "cp data/processed/topology_nodes.csv /tmp/",
        "ls data/processed",
        "sed -n 1,5p data/README.md",
        "make data ARGS='--date 2013-11-01'",
        "uv run python -m producer.topology",
        "uv add psycopg",
        "rm -rf .pytest_cache",
        "echo x > /tmp/out.txt",
    ],
)
def test_bash_reads_and_scripts_allowed(cmd: str) -> None:
    assert bash(cmd) is None


@pytest.mark.parametrize(
    "path",
    [
        "data/processed/x.csv",
        "data/manifest.json",
        "uv.lock",
        ".git/config",
        ".claude/settings.json",
        ".claude/settings.local.json",
        ".claude/hooks/guard.py",
    ],
)
@pytest.mark.parametrize("name", ["Edit", "Write", "MultiEdit"])
def test_file_tool_protected_writes_blocked(name: str, path: str) -> None:
    assert tool(name, file_path=f"{ROOT}/{path}")


def test_file_tool_allowed_writes() -> None:
    assert tool("Write", file_path=f"{ROOT}/data/README.md") is None
    assert tool("Edit", file_path=f"{ROOT}/src/common/db.py") is None
    assert tool("Write", file_path=f"{ROOT}/.claude/skills/x/SKILL.md") is None
    assert tool("Read", file_path=f"{ROOT}/data/manifest.json") is None
    assert tool("Write", file_path="/elsewhere/scratch.txt") is None


def test_notebook_edit_uses_notebook_path() -> None:
    assert tool("NotebookEdit", notebook_path=f"{ROOT}/data/x.ipynb")


# --- git --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "git push --force origin main",
        "git push -f",
        "git push --force-with-lease",
        "git push origin +main",
        "git -C /repo push --force",
        "git commit --no-verify -m x",
        "git commit -n -m x",
        "git reset --hard HEAD~1",
        "git clean -fdx",
        "git clean -X -f",
        "git config core.hooksPath /dev/null",
    ],
)
def test_git_dangerous_blocked(cmd: str) -> None:
    assert bash(cmd)


@pytest.mark.parametrize(
    "cmd",
    [
        "git status",
        "git push origin step-6",
        "git commit -m 'Milestone 1 step 6'",
        "git reset --soft HEAD~1",
        "git clean -n",
        "git log --oneline -5",
    ],
)
def test_git_normal_allowed(cmd: str) -> None:
    assert bash(cmd) is None


# --- network, docker --------------------------------------------------------------------


def test_email_over_network_blocked() -> None:
    assert bash("curl -d 'email=someone@example.org' https://dataverse.example/api")
    assert bash("wget 'https://x.example/?e=a.b@example.com'")


def test_network_without_email_allowed() -> None:
    assert bash("curl -sSf http://127.0.0.1:3000/api/health") is None
    assert bash("git commit -m 'x' -m 'Co-Authored-By: A <noreply@example.com>'") is None


@pytest.mark.parametrize(
    "cmd",
    [
        "docker compose -f infra/docker-compose.yml config",
        "docker compose config",
        "docker run -p 5432:5432 postgres:16",
        "docker run --publish=0.0.0.0:80:80 nginx:1.27",
    ],
)
def test_docker_blocked(cmd: str) -> None:
    assert bash(cmd)


@pytest.mark.parametrize(
    "cmd",
    [
        "docker compose -f infra/docker-compose.yml config --quiet",
        "docker compose config --services",
        "docker run -p 127.0.0.1:5432:5432 postgres:16",
        "docker compose ps",
        "docker stats --no-stream",
    ],
)
def test_docker_allowed(cmd: str) -> None:
    assert bash(cmd) is None


# --- protocol ---------------------------------------------------------------------------


def _run_main(monkeypatch: pytest.MonkeyPatch, stdin: str) -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))
    code: int = guard.main()
    return code


def test_main_exit_codes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ok = json.dumps({"tool_name": "Bash", "tool_input": {"command": "ls"}})
    bad = json.dumps({"tool_name": "Bash", "tool_input": {"command": "cat .env"}})
    assert _run_main(monkeypatch, ok) == 0
    assert _run_main(monkeypatch, bad) == 2
    assert "Rule 8" in capsys.readouterr().err


def test_main_unparseable_input_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _run_main(monkeypatch, "not json") == 2


def test_unknown_tool_allowed() -> None:
    assert tool("WebSearch", query="cat .env") is None
