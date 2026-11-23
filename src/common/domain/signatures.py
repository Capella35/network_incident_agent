"""Fault signatures: how each fault type shows in the telemetry, with default thresholds.

Kept apart from the fault types (`loader.py`) on purpose: the fault injector shares the types
but must never import this module (Rule 15; `tests/test_import_boundaries.py`), so the evals
test whether these signatures hold on injected faults instead of restating them.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

from pydantic import Field

from common.domain.loader import (
    DEFAULT_DIR,
    DomainError,
    FrozenModel,
    load_domain_from,
    parse_named,
    read_yaml,
)


class Signature(FrozenModel):
    fault: str
    description: str
    thresholds: dict[str, float] = Field(min_length=1)


def load_signatures_from(directory: Path | Any) -> dict[str, Signature]:
    """Load `fault_signatures.yaml`; every fault type with a root needs exactly one signature."""
    raw = read_yaml(directory / "fault_signatures.yaml")
    try:
        sigs = parse_named(raw, "signatures", Signature, field="fault")
    except (ValueError, TypeError) as exc:
        raise DomainError(str(exc)) from exc
    faults = load_domain_from(directory).faults  # the fault types in the same directory
    if unknown := set(sigs) - set(faults):
        raise DomainError(f"signatures for unknown fault types: {sorted(unknown)}")
    expected = {f.name for f in faults.values() if f.is_real}
    if missing := expected - set(sigs):
        raise DomainError(f"fault types without a signature: {sorted(missing)}")
    if extra := set(sigs) - expected:
        raise DomainError(f"signatures for fault types that have no root: {sorted(extra)}")
    return sigs


@cache
def load_signatures() -> dict[str, Signature]:
    """The project's fault signatures (cached)."""
    return load_signatures_from(DEFAULT_DIR)
