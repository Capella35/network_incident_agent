from __future__ import annotations

from pathlib import Path

import pytest

from common.domain import DomainError, load_domain, load_domain_from
from common.domain.loader import Fault, Severity, parse_named
from tests.common.conftest import replace_once


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
    replace_once(domain_copy / "entities.yaml", "id_template: 'SITE-{n}'", "id_template: 'ST-{n}'")
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
    replace_once(domain_copy / "kpis.yaml", "name: traffic_sms", "name: traffic_voice")
    with pytest.raises(DomainError, match="duplicate KPI"):
        load_domain_from(domain_copy)


def test_rejects_unknown_unit(domain_copy: Path) -> None:
    replace_once(domain_copy / "kpis.yaml", "unit: ms", "unit: seconds")
    with pytest.raises(DomainError):
        load_domain_from(domain_copy)


def test_rejects_unknown_milan_field(domain_copy: Path) -> None:
    replace_once(domain_copy / "kpis.yaml", "milan_fields: [internet]", "milan_fields: [lte_prb]")
    with pytest.raises(DomainError, match="unknown Milan fields"):
        load_domain_from(domain_copy)


def test_rejects_unanchored_id_pattern(domain_copy: Path) -> None:
    replace_once(domain_copy / "entities.yaml", "'^SITE-[0-9]+$'", "'SITE-[0-9]+'")
    with pytest.raises(DomainError, match="anchored"):
        load_domain_from(domain_copy)


def test_rejects_unknown_parent(domain_copy: Path) -> None:
    replace_once(domain_copy / "entities.yaml", "parent: Site", "parent: Tower")
    with pytest.raises(DomainError, match="unknown parent"):
        load_domain_from(domain_copy)


def test_rejects_bad_topology_level(domain_copy: Path) -> None:
    replace_once(domain_copy / "entities.yaml", "topology_level: 3", "topology_level: 5")
    with pytest.raises(DomainError, match="topology_level"):
        load_domain_from(domain_copy)


def test_rejects_unknown_relation_end(domain_copy: Path) -> None:
    replace_once(domain_copy / "entities.yaml", "dst: Incident", "dst: Ticket")
    with pytest.raises(DomainError, match="unknown entity"):
        load_domain_from(domain_copy)


# --- fault types (faults.yaml) ------------------------------------------------------------


def test_fault_types_load_with_roots_severities_teams() -> None:
    d = load_domain()
    assert set(d.faults) == {
        "cell_outage",
        "site_power",
        "backhaul",
        "congestion",
        "config_push",
        "no_fault",
    }
    backhaul = d.fault("backhaul")
    assert (backhaul.root_levels, backhaul.severities, backhaul.team) == (
        ("AggregationNode",),
        ("critical",),
        "transport",
    )
    assert d.fault("congestion").root_levels == ("Cell", "Site")
    assert d.fault("config_push").needs_change_event
    none = d.fault("no_fault")
    assert (none.root_levels, none.severities, none.team, none.is_real) == ((), (), None, False)
    assert list(Severity) == ["minor", "major", "critical"]  # ordered, lowest first


def test_unknown_fault_raises() -> None:
    with pytest.raises(KeyError, match="unknown fault type"):
        load_domain().fault("cosmic_rays")


@pytest.mark.parametrize(
    ("old", "new", "match"),
    [
        ("root_levels: [AggregationNode]", "root_levels: [Router]", "not in the topology"),
        ("team: transport", "team: noc", "unknown team"),
        ("severities: [critical]", "severities: [catastrophic]", "severities"),
        ("team: transport", "team: null", "all or none"),
        ("root_levels: [Cell, Site]", "root_levels: [Cell, Cell]", "duplicate root_levels"),
        ("severities: [minor, major]", "severities: [major, major]", "duplicate severities"),
        ("  backhaul:\n", "  backhaul:\n    name: cell_outage\n", "must not set 'name'"),
        ("    team: null", "    team: null\n    needs_change_event: true", "only for a real fault"),
    ],
)
def test_rejects_bad_fault_definitions(domain_copy: Path, old: str, new: str, match: str) -> None:
    replace_once(domain_copy / "faults.yaml", old, new)
    with pytest.raises(DomainError, match=match):
        load_domain_from(domain_copy)


def test_severity_orders_by_rank_not_alphabet() -> None:
    assert Severity.MINOR < Severity.MAJOR < Severity.CRITICAL
    assert max(load_domain().fault("congestion").severities) is Severity.MAJOR
    assert sorted([Severity.CRITICAL, Severity.MINOR]) == [Severity.MINOR, Severity.CRITICAL]
    assert Severity.MINOR == "minor"  # still the YAML string


def test_list_shaped_section_is_a_clear_error() -> None:
    with pytest.raises(ValueError, match="must be a mapping"):
        parse_named({"faults": [{"name": "cell_outage"}]}, "faults", Fault)
