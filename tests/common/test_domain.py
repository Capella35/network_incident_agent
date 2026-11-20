from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from common.domain import DomainError, load_domain, load_domain_from

DOMAIN_DIR = Path(__file__).resolve().parents[2] / "src" / "common" / "domain"


@pytest.fixture
def domain_copy(tmp_path: Path) -> Path:
    for name in ("entities.yaml", "kpis.yaml"):
        shutil.copy(DOMAIN_DIR / name, tmp_path / name)
    return tmp_path


def _replace(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    assert old in text, f"fixture text not found: {old!r}"
    path.write_text(text.replace(old, new, 1))


def test_project_domain_loads() -> None:
    d = load_domain()
    assert [e.name for e in d.topology_types()] == ["Core", "AggregationNode", "Site", "Cell"]
    assert d.kpi("latency_ms").unit == "ms"
    assert d.team("transport").name == "Transport"


@pytest.mark.parametrize(
    ("entity", "good", "bad"),
    [
        ("Core", "CORE-1", "core-1"),
        ("AggregationNode", "AGG-12", "AGG-"),
        ("Site", "SITE-7", "SITE-7a"),
        ("Cell", "CELL-7-2", "CELL-SITE-7-2"),
        ("Incident", "INC-123e4567-e89b-12d3-a456-426614174000", "INC-123"),
        ("ChangeEvent", "CHG-3", "CHG-x"),
    ],
)
def test_id_patterns(entity: str, good: str, bad: str) -> None:
    ent = load_domain().entity(entity)
    assert ent.matches(good)
    assert not ent.matches(bad)


def test_make_id() -> None:
    d = load_domain()
    assert d.entity("Cell").make_id(site=4, sector=2) == "CELL-4-2"
    assert d.entity("AggregationNode").make_id(n=3) == "AGG-3"
    with pytest.raises(ValueError, match="not a valid"):
        d.entity("Site").make_id(n="x")


def test_parent_relation() -> None:
    rel = load_domain().parent_relation("Cell")
    assert (rel.name, rel.src, rel.dst) == ("parent_of", "Site", "Cell")


def test_rejects_template_not_matching_pattern(domain_copy: Path) -> None:
    _replace(domain_copy / "entities.yaml", "id_template: 'SITE-{n}'", "id_template: 'ST-{n}'")
    with pytest.raises(DomainError, match="id_template"):
        load_domain_from(domain_copy)


def test_real_kpis_map_to_milan_fields() -> None:
    d = load_domain()
    assert d.kpi("traffic_voice").milan_fields == ("call_in", "call_out")


def test_unknown_lookups_raise() -> None:
    d = load_domain()
    with pytest.raises(KeyError):
        d.kpi("throughput")
    with pytest.raises(KeyError):
        d.entity("Router")


def test_domain_is_immutable() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        load_domain().kpi("latency_ms").unit = "s"  # type: ignore[misc]


def test_rejects_duplicate_kpi(domain_copy: Path) -> None:
    _replace(domain_copy / "kpis.yaml", "name: traffic_sms", "name: traffic_voice")
    with pytest.raises(DomainError, match="duplicate KPI"):
        load_domain_from(domain_copy)


def test_rejects_unknown_unit(domain_copy: Path) -> None:
    _replace(domain_copy / "kpis.yaml", "unit: ms", "unit: seconds")
    with pytest.raises(DomainError):
        load_domain_from(domain_copy)


def test_rejects_unknown_milan_field(domain_copy: Path) -> None:
    _replace(domain_copy / "kpis.yaml", "milan_fields: [internet]", "milan_fields: [lte_prb]")
    with pytest.raises(DomainError, match="unknown Milan fields"):
        load_domain_from(domain_copy)


def test_rejects_unanchored_id_pattern(domain_copy: Path) -> None:
    _replace(domain_copy / "entities.yaml", "'^SITE-[0-9]+$'", "'SITE-[0-9]+'")
    with pytest.raises(DomainError, match="anchored"):
        load_domain_from(domain_copy)


def test_rejects_unknown_parent(domain_copy: Path) -> None:
    _replace(domain_copy / "entities.yaml", "parent: Site", "parent: Tower")
    with pytest.raises(DomainError, match="unknown parent"):
        load_domain_from(domain_copy)


def test_rejects_bad_topology_level(domain_copy: Path) -> None:
    _replace(domain_copy / "entities.yaml", "topology_level: 3", "topology_level: 5")
    with pytest.raises(DomainError, match="topology_level"):
        load_domain_from(domain_copy)


def test_rejects_unknown_relation_end(domain_copy: Path) -> None:
    _replace(domain_copy / "entities.yaml", "dst: Incident", "dst: Ticket")
    with pytest.raises(DomainError, match="unknown entity"):
        load_domain_from(domain_copy)
