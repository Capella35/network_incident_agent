"""Shared fixtures for the domain-model tests: a writable copy of the domain YAML files."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

DOMAIN_DIR = Path(__file__).resolve().parents[2] / "src" / "common" / "domain"


@pytest.fixture
def domain_copy(tmp_path: Path) -> Path:
    """Every domain YAML file, copied to a temp dir that a test may edit."""
    for path in DOMAIN_DIR.glob("*.yaml"):
        shutil.copy(path, tmp_path / path.name)
    return tmp_path


def replace_once(path: Path, old: str, new: str) -> None:
    """Edit a fixture file; fails loudly if the text to replace is not there."""
    text = path.read_text()
    assert old in text, f"fixture text not found: {old!r}"
    path.write_text(text.replace(old, new, 1))
