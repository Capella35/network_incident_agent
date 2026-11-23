#!/usr/bin/env python3
"""PreToolUse guard for Claude Code: what the coding agent must never do in this repo.

Scope: this guards Claude Code while it BUILDS the repo. It is not the product's ops-agent
guardrail (Rules 4 and 13 are enforced in src/ and by tests). Rules about what the code
CONTAINS (0.0.0.0 bindings, `latest` tags, default passwords) belong in pre-commit/CI.

This is a tripwire, not a security boundary: shell text can be obfuscated past any
pattern check. It closes the gap left by `.claude/settings.json`, whose Read/Edit deny
rules do not apply to Bash.

Protocol: reads the hook JSON on stdin; exit 0 = allow, exit 2 = block (reason on stderr,
shown to Claude). Stdlib only and Python 3.9-compatible, so the system python3 can run it.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sys
from typing import Any

# --- Policy -----------------------------------------------------------------------------

# Rule 8: secrets live in .env only. Any .env-style file except the committed placeholder.
_SECRET_NAME = re.compile(r"^\.env(\.[\w-]+)?$")
_SECRET_ALLOWED = {".env.example"}
# A .env reference inside shell text (".env", "./.env", ".env.local"; not ".env.example").
_SECRET_IN_SHELL = re.compile(
    r"(?<![\w.\\-])(?:[\w./-]*/)?\.env(?:\.(?!example\b)[\w-]+)?(?![\w.-])"
)

# CLAUDE.md: nothing under data/ except via the data scripts. Two docs files are exempt
# for the file tools (they are committed documentation, not data).
_DATA_DOCS = {"data/README.md", "data/manifest-template.md"}
# Files that only tools may write: uv.lock (use `uv add`), git internals.
_TOOL_OWNED = ("uv.lock",)
# The guardrails themselves: Claude must not be able to switch them off.
_GUARD_FILES = (".claude/settings.json", ".claude/settings.local.json")

_WRITE_TOOLS = {"Edit", "MultiEdit", "Write", "NotebookEdit"}
_READ_TOOLS = {"Read", "Grep"}

# Shell commands whose path arguments are mutated. For copy-like commands only the last
# argument (the destination) is a write target.
_MUTATE_ANY_ARG = {"rm", "rmdir", "touch", "truncate", "mkdir", "chmod", "chown", "shred", "unlink"}
_MUTATE_LAST_ARG = {"cp", "mv", "ln", "rsync", "install"}
_IN_PLACE_EDITORS = {"sed", "perl", "gsed"}
_REDIRECTS = {">", ">>", ">|", "&>", "&>>"}
_SEPARATORS = {";", "&&", "||", "|", "&", "\n", "(", ")"}

_NETWORK = {"curl", "wget", "http", "https", "nc", "ncat", "xh"}
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


class Blocked(Exception):
    """Raised with a user-facing reason when a tool call breaks a rule."""


# --- Path helpers -----------------------------------------------------------------------


def _rel(path: str, root: str) -> str | None:
    """Path relative to the project root, or None if it lies outside it."""
    if not path:
        return None
    full = os.path.normpath(path if os.path.isabs(path) else os.path.join(root, path))
    root_n = os.path.normpath(root)
    if full == root_n:
        return "."
    if not full.startswith(root_n + os.sep):
        return None
    return os.path.relpath(full, root_n).replace(os.sep, "/")


def _is_secret(path: str) -> bool:
    name = os.path.basename(path.rstrip("/"))
    return bool(_SECRET_NAME.match(name)) and name not in _SECRET_ALLOWED


def _is_protected_target(rel: str | None) -> str | None:
    """Rule label if writing to `rel` is forbidden, else None. Shell variant (no exemptions)."""
    if rel is None:
        return None
    if rel == "data" or rel.startswith("data/"):
        return "data/ is written only by the data scripts (make data / topology / calibrate)"
    if rel in _TOOL_OWNED:
        return "uv.lock is changed only by uv (uv add / uv lock), never by hand"
    if rel == ".git" or rel.startswith(".git/"):
        return ".git/ internals are changed only by git itself"
    if rel in _GUARD_FILES or rel.startswith(".claude/hooks/"):
        return "Claude's own guardrails are changed only by the user"
    return None


# --- File tools -------------------------------------------------------------------------


def check_file_tool(tool: str, tool_input: dict[str, Any], root: str) -> None:
    path = str(tool_input.get("file_path") or tool_input.get("notebook_path") or "")
    if tool == "Grep":
        path = str(tool_input.get("path") or "")
        glob = str(tool_input.get("glob") or "")
        if glob and _SECRET_IN_SHELL.search(glob):
            raise Blocked("Rule 8: .env holds secrets; it is never read (Grep glob targets it).")
    if path and _is_secret(path):
        raise Blocked("Rule 8: .env holds secrets; Claude never reads or edits it.")
    if tool in _WRITE_TOOLS:
        rel = _rel(path, root)
        if rel in _DATA_DOCS:
            return
        reason = _is_protected_target(rel)
        if reason:
            raise Blocked(f"Protected path {rel}: {reason}.")


# --- Bash -------------------------------------------------------------------------------


def _tokens(command: str) -> list[str]:
    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>()")
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        return list(lexer)
    except ValueError:  # unbalanced quotes: fall back to a rough split
        return command.split()


def _segments(tokens: list[str]) -> list[list[str]]:
    segs: list[list[str]] = [[]]
    for tok in tokens:
        if tok in _SEPARATORS:
            segs.append([])
        else:
            segs[-1].append(tok)
    return [s for s in segs if s]


def _strip_prefix(seg: list[str]) -> list[str]:
    """Drop leading VAR=value assignments and wrappers like sudo/env/command/xargs."""
    i = 0
    while i < len(seg):
        tok = seg[i]
        if re.match(r"^[A-Za-z_]\w*=", tok) or tok in {"sudo", "env", "command", "xargs", "nohup"}:
            i += 1
            continue
        break
    return seg[i:]


def _check_redirects(seg: list[str], root: str) -> None:
    for i, tok in enumerate(seg[:-1]):
        if tok in _REDIRECTS:
            reason = _is_protected_target(_rel(seg[i + 1], root))
            if reason:
                raise Blocked(f"Shell redirect into {seg[i + 1]}: {reason}.")


def _check_mutation(seg: list[str], root: str) -> None:
    argv = _strip_prefix([t for t in seg if t not in _REDIRECTS])
    if not argv:
        return
    prog = os.path.basename(argv[0])
    args = [a for a in argv[1:] if not a.startswith("-")]
    targets: list[str] = []
    if prog in _MUTATE_ANY_ARG or prog == "tee":
        targets = args
    elif prog in _MUTATE_LAST_ARG:
        targets = args[-1:]
    elif prog in _IN_PLACE_EDITORS and any(a.startswith("-i") or a == "-pi" for a in argv[1:]):
        targets = args
    elif prog == "dd":
        targets = [a[3:] for a in argv[1:] if a.startswith("of=")]
    for t in targets:
        reason = _is_protected_target(_rel(t, root))
        if reason:
            raise Blocked(f"`{prog}` on {t}: {reason}.")


def _check_git(argv: list[str]) -> None:
    if not argv or os.path.basename(argv[0]) != "git":
        return
    rest = argv[1:]
    while rest and rest[0].startswith("-"):  # global options; -C/-c take a value
        rest = rest[2:] if rest[0] in {"-C", "-c"} else rest[1:]
    sub = rest[0] if rest else ""
    flags = set(rest[1:])
    if sub == "push" and (
        flags & {"-f", "--force", "--force-with-lease", "--mirror", "--delete", "-d"}
        or any(a.startswith(("+", "--force")) for a in rest[1:])
    ):
        raise Blocked("Force-push, mirror, and remote deletes are never run by Claude.")
    if sub == "commit" and flags & {"--no-verify", "-n"}:
        raise Blocked("Rule 12/S3: commits never skip the pre-commit hooks (gitleaks).")
    if sub == "push" and "--no-verify" in flags:
        raise Blocked("Pushes never skip hooks.")
    if sub == "reset" and "--hard" in flags:
        raise Blocked("`git reset --hard` discards work; ask the user to run it.")
    if sub == "clean" and any(re.match(r"^-[a-zA-Z]*[xX]", a) for a in rest[1:]):
        raise Blocked("`git clean -x` deletes ignored files, including .env and data/.")
    if sub == "config" and any(a.lower() == "core.hookspath" for a in rest[1:]):
        raise Blocked("Git hook configuration is changed only by `make setup`.")


def _check_network(argv: list[str]) -> None:
    if argv and os.path.basename(argv[0]) in _NETWORK and _EMAIL.search(" ".join(argv[1:])):
        raise Blocked(
            "Rule 3 / privacy: no script sends an email address over the network "
            "(Milan guestbook downloads are manual)."
        )


def _check_docker(argv: list[str]) -> None:
    if not argv or os.path.basename(argv[0]) not in {"docker", "docker-compose"}:
        return
    sub = [a for a in argv[1:] if not a.startswith("-")]
    # `docker compose config` prints .env values interpolated into the file.
    is_compose = "compose" in sub or argv[0].endswith("docker-compose")
    safe = {"-q", "--quiet", "--services", "--volumes", "--images"} & set(argv)
    if "config" in sub and is_compose and not safe:
        raise Blocked(
            "Rule 8: `docker compose config` prints secrets from .env; use `--quiet` to validate."
        )
    if "run" in sub or "create" in sub:
        for i, a in enumerate(argv):
            spec = None
            if a in {"-p", "--publish"} and i + 1 < len(argv):
                spec = argv[i + 1]
            elif a.startswith("--publish="):
                spec = a.split("=", 1)[1]
            elif a.startswith("-p") and len(a) > 2:
                spec = a[2:]
            if spec is not None and not spec.startswith(("127.0.0.1:", "[::1]:")):
                raise Blocked(f"Rule 12/S1: publish ports on 127.0.0.1 only (got `-p {spec}`).")


def _check_secret_ref(seg: list[str]) -> None:
    # A git segment may mention .env in a commit message; it is exempt unless it also
    # carries a command substitution that the shell would execute.
    argv = _strip_prefix(seg)
    is_git = bool(argv) and os.path.basename(argv[0]) == "git"
    if is_git and not any("$(" in a or "`" in a for a in argv):
        return
    if any(_SECRET_IN_SHELL.search(a) for a in seg):
        raise Blocked(
            "Rule 8: commands never read, source, copy, or print .env. "
            "Use the make targets (they load .env without printing it); the user edits .env."
        )


def check_bash(command: str, root: str) -> None:
    for seg in _segments(_tokens(command)):
        argv = _strip_prefix(seg)
        _check_secret_ref(seg)
        _check_redirects(seg, root)
        _check_mutation(seg, root)
        _check_git(argv)
        _check_network(argv)
        _check_docker(argv)


# --- Entry point ------------------------------------------------------------------------


def evaluate(payload: dict[str, Any]) -> str | None:
    """Return a block reason, or None to allow."""
    tool = str(payload.get("tool_name") or "")
    tool_input = payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return None
    root = os.environ.get("CLAUDE_PROJECT_DIR") or str(payload.get("cwd") or os.getcwd())
    try:
        if tool == "Bash":
            check_bash(str(tool_input.get("command") or ""), root)
        elif tool in _WRITE_TOOLS or tool in _READ_TOOLS:
            check_file_tool(tool, tool_input, root)
    except Blocked as exc:
        return str(exc)
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        print("guard.py: could not parse hook input; blocking to be safe.", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        return 0
    reason = evaluate(payload)
    if reason:
        print(f"Blocked by .claude/hooks/guard.py — {reason}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
