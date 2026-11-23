from __future__ import annotations

from pathlib import Path

import pytest

from common.domain import DomainError, load_domain
from common.domain.signatures import load_signatures, load_signatures_from
from tests.common.conftest import replace_once


def test_every_rooted_fault_has_one_signature() -> None:
    sigs = load_signatures()
    assert set(sigs) == {f.name for f in load_domain().faults.values() if f.is_real}
    assert sigs["site_power"].thresholds == {
        "cells_down_share_min": 0.9,
        "traffic_ratio_max": 0.05,
        "onset_spread_max_intervals": 1,
    }


def test_every_threshold_is_named_in_its_description() -> None:
    """The description is the only prose statement of the predicate (until M3 implements it);
    a threshold it does not mention is a sign the two have drifted apart."""
    for sig in load_signatures().values():
        for name in sig.thresholds:
            assert name in sig.description, f"{sig.fault}: {name} not in its description"


def test_rejects_missing_signature(domain_copy: Path) -> None:
    text = (domain_copy / "fault_signatures.yaml").read_text()
    start = text.index("  backhaul:")
    end = text.index("  congestion:")
    (domain_copy / "fault_signatures.yaml").write_text(text[:start] + text[end:])
    with pytest.raises(DomainError, match="without a signature: \\['backhaul'\\]"):
        load_signatures_from(domain_copy)


def test_rejects_signature_for_unknown_fault(domain_copy: Path) -> None:
    replace_once(domain_copy / "fault_signatures.yaml", "  backhaul:", "  fibre_cut:")
    with pytest.raises(DomainError, match="unknown fault types"):
        load_signatures_from(domain_copy)


def test_checks_against_the_fault_types_in_the_same_directory(domain_copy: Path) -> None:
    replace_once(domain_copy / "faults.yaml", "  backhaul:", "  fibre_cut:")
    with pytest.raises(DomainError, match="unknown fault types: \\['backhaul'\\]"):
        load_signatures_from(domain_copy)


def test_rejects_signature_for_rootless_fault(domain_copy: Path) -> None:
    extra = "\n  no_fault:\n    description: nothing\n    thresholds:\n      x: 1\n"
    with (domain_copy / "fault_signatures.yaml").open("a") as fh:
        fh.write(extra)
    with pytest.raises(DomainError, match="have no root: \\['no_fault'\\]"):
        load_signatures_from(domain_copy)


def test_rejects_signature_without_thresholds(domain_copy: Path) -> None:
    replace_once(
        domain_copy / "fault_signatures.yaml",
        "    thresholds:\n      load_ratio_min: 0.9",
        "    thresholds: {}",
    )
    with pytest.raises(DomainError):
        load_signatures_from(domain_copy)
