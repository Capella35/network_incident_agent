"""File checksums and verification against published Dataverse values (specs/02-telemetry.md)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

_CHUNK = 8 * 1024 * 1024


class IntegrityError(RuntimeError):
    """A file is unknown or does not match its published checksum."""


@dataclass(frozen=True)
class PublishedFile:
    name: str
    doi: str
    dataverse_file_id: int
    md5: str
    size_bytes: int


@dataclass(frozen=True)
class Hashes:
    md5: str
    sha256: str
    size_bytes: int


def load_published() -> dict[str, PublishedFile]:
    """Published checksums, committed in `producer/milan_files.json`."""
    raw: dict[str, Any] = json.loads(
        files("producer").joinpath("milan_files.json").read_text(encoding="utf-8")
    )
    return {name: PublishedFile(name=name, **spec) for name, spec in raw["files"].items()}


def hash_file(path: Path) -> Hashes:
    """MD5 (to compare with Dataverse) and SHA-256 (for the manifest) in one pass."""
    md5 = hashlib.md5(usedforsecurity=False)
    sha = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            md5.update(chunk)
            sha.update(chunk)
            size += len(chunk)
    return Hashes(md5=md5.hexdigest(), sha256=sha.hexdigest(), size_bytes=size)


def sha256_file(path: Path) -> str:
    return hash_file(path).sha256


def verify(path: Path, published: dict[str, PublishedFile]) -> tuple[PublishedFile, Hashes]:
    """Raise IntegrityError unless `path` is a known file whose size and MD5 match."""
    spec = published.get(path.name)
    if spec is None:
        raise IntegrityError(f"{path.name}: not a known Dataverse file; refusing to use it")
    hashes = hash_file(path)
    if hashes.size_bytes != spec.size_bytes:
        raise IntegrityError(
            f"{path.name}: size {hashes.size_bytes} != published {spec.size_bytes} "
            "(incomplete download?)"
        )
    if hashes.md5 != spec.md5:
        raise IntegrityError(f"{path.name}: MD5 {hashes.md5} != published {spec.md5}")
    return spec, hashes
