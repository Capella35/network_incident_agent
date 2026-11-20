"""`data/manifest.json`: one entry per source file (data-sources.md rule 4)."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

VERSION = 1


def load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": VERSION, "files": {}}
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("version") != VERSION:
        raise ValueError(f"{path}: unsupported manifest version {doc.get('version')!r}")
    return doc


def save(path: Path, doc: dict[str, Any]) -> None:
    """Write atomically, with sorted keys so diffs stay stable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".manifest-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def upsert(doc: dict[str, Any], name: str, entry: dict[str, Any]) -> None:
    """Replace a file's entry, keeping fields not given (e.g. `processed` on a re-verify)."""
    files: dict[str, Any] = doc["files"]
    files[name] = {**files.get(name, {}), **entry}
