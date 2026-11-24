"""Load and validate the domain model (entities, KPI catalog, fault types).

Every component gets entity names, ID formats, team codes, KPI definitions, and fault types
from here,
never from string literals elsewhere (CLAUDE.md Rule 15, specs/01-domain-model.md).
"""

from __future__ import annotations

import re
import string
from collections.abc import Iterable, Mapping
from enum import StrEnum
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Columns of the Milan activity files (specs/02-telemetry.md). Real KPIs may only map to these.
MILAN_FIELDS = frozenset({"sms_in", "sms_out", "call_in", "call_out", "internet"})

DEFAULT_DIR = files("common.domain")
T = TypeVar("T")
M = TypeVar("M", bound=BaseModel)

# Sample values used to check that each id_template produces IDs matching its id_pattern.
_TEMPLATE_SAMPLES = {
    "n": 7,
    "site": 12,
    "sector": 3,
    "uuid": "123e4567-e89b-12d3-a456-426614174000",
}


class DomainError(ValueError):
    """The domain YAML is invalid."""


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Unit(StrEnum):
    SCALED_ACTIVITY = "scaled_activity"
    RATIO = "ratio"
    MS = "ms"
    Z_SCORE = "z_score"


class BadDirection(StrEnum):
    DROP = "drop"
    HIGH = "high"
    EITHER = "either"


class KpiSource(StrEnum):
    REAL = "real"
    MODELED = "modeled"
    DERIVED = "derived"


class Severity(StrEnum):
    """Incident severity, lowest first. Compares by that order, not alphabetically (a plain
    StrEnum would put critical < major < minor), so `max(severities)` is the worst one."""

    MINOR = "minor"
    MAJOR = "major"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return list(Severity).index(self)

    def __lt__(self, other: object) -> bool:
        return self.rank < other.rank if isinstance(other, Severity) else NotImplemented

    def __le__(self, other: object) -> bool:
        return self.rank <= other.rank if isinstance(other, Severity) else NotImplemented

    def __gt__(self, other: object) -> bool:
        return self.rank > other.rank if isinstance(other, Severity) else NotImplemented

    def __ge__(self, other: object) -> bool:
        return self.rank >= other.rank if isinstance(other, Severity) else NotImplemented


class EntityType(FrozenModel):
    name: str
    id_pattern: str
    id_template: str
    parent: str | None = None
    topology_level: int | None = None

    @field_validator("id_pattern")
    @classmethod
    def _compiles(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"id_pattern does not compile: {exc}") from exc
        if not (v.startswith("^") and v.endswith("$")):
            raise ValueError("id_pattern must be anchored with ^ and $")
        return v

    @model_validator(mode="after")
    def _template_fits_pattern(self) -> EntityType:
        fields = [f for _, f, _, _ in string.Formatter().parse(self.id_template) if f]
        unknown = set(fields) - set(_TEMPLATE_SAMPLES)
        if unknown:
            raise ValueError(f"{self.name}: id_template uses unknown fields {sorted(unknown)}")
        sample = self.id_template.format(**_TEMPLATE_SAMPLES)
        if not self.matches(sample):
            raise ValueError(f"{self.name}: id_template gives {sample!r}, not matching id_pattern")
        return self

    def matches(self, entity_id: str) -> bool:
        return re.fullmatch(self.id_pattern, entity_id) is not None

    def make_id(self, **values: object) -> str:
        """Build an ID from the template; raises if the result breaks the pattern."""
        entity_id = self.id_template.format(**values)
        if not self.matches(entity_id):
            raise ValueError(f"{entity_id!r} is not a valid {self.name} ID")
        return entity_id


class Relation(FrozenModel):
    name: str
    src: str
    dst: str


class Team(FrozenModel):
    code: str = Field(pattern=r"^[a-z][a-z_]*$")
    name: str


class Kpi(FrozenModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    unit: Unit
    description: str
    bad_direction: BadDirection
    source: KpiSource
    milan_fields: tuple[str, ...] = ()
    range: tuple[float | None, float | None] | None = None

    @model_validator(mode="after")
    def _check_source_fields(self) -> Kpi:
        if self.source is KpiSource.REAL:
            if not self.milan_fields:
                raise ValueError(f"real KPI {self.name!r} needs milan_fields")
            unknown = set(self.milan_fields) - MILAN_FIELDS
            if unknown:
                raise ValueError(f"KPI {self.name!r}: unknown Milan fields {sorted(unknown)}")
        elif self.milan_fields:
            raise ValueError(f"non-real KPI {self.name!r} must not map to Milan fields")
        if self.range is not None:
            lo, hi = self.range
            if lo is not None and hi is not None and lo > hi:
                raise ValueError(f"KPI {self.name!r}: range min > max")
        return self


class Fault(FrozenModel):
    """A fault type: where it can be rooted, how severe it is, who owns it (faults.yaml).
    Its signature lives in `common.domain.signatures`, apart, so the injector cannot use it."""

    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    description: str
    root_levels: tuple[str, ...]
    severities: tuple[Severity, ...]
    team: str | None
    needs_change_event: bool = False

    @property
    def is_real(self) -> bool:
        """False only for the "nothing wrong" label (no root, no severity, no team)."""
        return bool(self.root_levels)

    @model_validator(mode="after")
    def _real_or_label(self) -> Fault:
        if self.is_real != bool(self.severities) or self.is_real != (self.team is not None):
            raise ValueError(f"fault {self.name!r}: root_levels, severities, team: all or none")
        if self.needs_change_event and not self.is_real:
            raise ValueError(f"fault {self.name!r}: needs_change_event only for a real fault")
        for field in ("root_levels", "severities"):
            if dupes := _dupes(map(str, getattr(self, field))):
                raise ValueError(f"fault {self.name!r}: duplicate {field} {dupes}")
        return self


class Domain(FrozenModel):
    entities: dict[str, EntityType]
    relations: tuple[Relation, ...]
    teams: tuple[Team, ...]
    kpis: dict[str, Kpi]
    faults: dict[str, Fault]

    @model_validator(mode="after")
    def _check_references(self) -> Domain:
        names = set(self.entities)
        for ent in self.entities.values():
            if ent.parent is not None and ent.parent not in names:
                raise ValueError(f"{ent.name}: unknown parent {ent.parent!r}")
        for rel in self.relations:
            for end in (rel.src, rel.dst):
                if end not in names:
                    raise ValueError(f"relation {rel.name}: unknown entity {end!r}")
        self._check_hierarchy()
        codes = [t.code for t in self.teams]
        if dupes := _dupes(codes):
            raise ValueError(f"duplicate team codes: {dupes}")
        self._check_faults(set(codes))
        return self

    def _check_faults(self, team_codes: set[str]) -> None:
        topology = {e.name for e in self.topology_types()}
        for f in self.faults.values():
            if unknown := set(f.root_levels) - topology:
                raise ValueError(f"fault {f.name!r}: root levels not in the topology {unknown}")
            if f.team is not None and f.team not in team_codes:
                raise ValueError(f"fault {f.name!r}: unknown team {f.team!r}")

    def _check_hierarchy(self) -> None:
        """Parent chains must end at a root, and levels must increase by one per step."""
        for ent in self.entities.values():
            seen: set[str] = set()
            node = ent
            while node.parent is not None:
                if node.name in seen:
                    raise ValueError(f"cycle in parent chain at {node.name}")
                seen.add(node.name)
                parent = self.entities[node.parent]
                if node.topology_level is None or parent.topology_level is None:
                    raise ValueError(f"{node.name} and its parent need topology_level")
                if node.topology_level != parent.topology_level + 1:
                    raise ValueError(f"{node.name}: topology_level must be parent's + 1")
                node = parent

    # --- lookups -------------------------------------------------------------------------
    def entity(self, name: str) -> EntityType:
        return _lookup(self.entities, name, "entity type")

    def kpi(self, name: str) -> Kpi:
        return _lookup(self.kpis, name, "KPI")

    def fault(self, name: str) -> Fault:
        return _lookup(self.faults, name, "fault type")

    def parent_relation(self, child: str) -> Relation:
        """The relation from `child`'s parent type to `child` (e.g. Site -> Cell)."""
        parent = self.entity(child).parent
        for rel in self.relations:
            if rel.src == parent and rel.dst == child:
                return rel
        raise KeyError(f"no relation from {parent!r} to {child!r}")

    def team(self, code: str) -> Team:
        return _lookup({t.code: t for t in self.teams}, code, "team")

    def topology_types(self) -> list[EntityType]:
        """Topology entity types ordered root (Core) first."""
        levelled = [e for e in self.entities.values() if e.topology_level is not None]
        return sorted(levelled, key=lambda e: e.topology_level or 0)


def _lookup(mapping: Mapping[str, T], key: str, what: str) -> T:
    try:
        return mapping[key]
    except KeyError:
        raise KeyError(f"unknown {what} {key!r}") from None


def _dupes(values: Iterable[str]) -> list[str]:
    """Values that occur more than once, sorted (for error messages that name them)."""
    seen = list(values)
    return sorted({v for v in seen if seen.count(v) > 1})


def parse_named(raw: dict[str, Any], key: str, model: type[M], field: str = "name") -> dict[str, M]:
    """`{name: spec}` under `key` in a YAML document -> `{name: model(<field>=name, **spec)}`.
    The mapping key is the name; a spec may not set `field` itself (it could disagree)."""
    section = raw.get(key) or {}
    if not isinstance(section, dict):
        raise ValueError(f"{key!r} must be a mapping of name -> definition")
    for name, spec in section.items():
        if not isinstance(spec, dict):
            raise ValueError(f"{key}.{name}: expected a mapping")
        if field in spec:
            raise ValueError(f"{key}.{name}: must not set {field!r}; the key is the {field}")
    return {name: model(**{field: name, **spec}) for name, spec in section.items()}


def read_yaml(path: Path | Any) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise DomainError(f"{path}: expected a mapping at top level")
    if data.get("version") != 1:
        raise DomainError(f"{path}: unsupported or missing version")
    return data


def read_config(model: type[M], path: Path | Any) -> M:
    """A versioned YAML config (`read_yaml`) validated as `model`, the `version` key dropped."""
    return model(**{k: v for k, v in read_yaml(path).items() if k != "version"})


def load_domain_from(directory: Path | Any) -> Domain:
    """Load and validate the domain model from a directory holding the YAML files."""
    ent_raw = read_yaml(directory / "entities.yaml")
    kpi_raw = read_yaml(directory / "kpis.yaml")
    fault_raw = read_yaml(directory / "faults.yaml")

    if dupes := _dupes(k.get("name") for k in kpi_raw.get("kpis", [])):
        raise DomainError(f"duplicate KPI names: {dupes}")

    try:
        return Domain(
            entities=parse_named(ent_raw, "entities", EntityType),
            relations=tuple(Relation(**r) for r in ent_raw.get("relations", [])),
            teams=tuple(Team(**t) for t in ent_raw.get("teams", [])),
            kpis={k["name"]: Kpi(**k) for k in kpi_raw.get("kpis", [])},
            faults=parse_named(fault_raw, "faults", Fault),
        )
    except (ValueError, TypeError) as exc:  # pydantic ValidationError is a ValueError
        raise DomainError(str(exc)) from exc


@cache
def load_domain() -> Domain:
    """The project's domain model (cached; immutable)."""
    return load_domain_from(DEFAULT_DIR)
