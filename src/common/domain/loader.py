"""Load and validate the domain model (entities + KPI catalog).

Every component gets entity names, ID formats, team codes, and KPI definitions from here,
never from string literals elsewhere (CLAUDE.md Rule 15, docs/domain-model.md).
"""

from __future__ import annotations

import re
import string
from enum import StrEnum
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Columns of the Milan activity files (docs/data-sources.md). Real KPIs may only map to these.
MILAN_FIELDS = frozenset({"sms_in", "sms_out", "call_in", "call_out", "internet"})

_DEFAULT_DIR = files("common.domain")

# Sample values used to check that each id_template produces IDs matching its id_pattern.
_TEMPLATE_SAMPLES = {
    "n": 7,
    "site": 12,
    "sector": 3,
    "uuid": "123e4567-e89b-12d3-a456-426614174000",
}


class DomainError(ValueError):
    """The domain YAML is invalid."""


class _Frozen(BaseModel):
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


class EntityType(_Frozen):
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


class Relation(_Frozen):
    name: str
    src: str
    dst: str


class Team(_Frozen):
    code: str = Field(pattern=r"^[a-z][a-z_]*$")
    name: str


class Kpi(_Frozen):
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


class Domain(_Frozen):
    entities: dict[str, EntityType]
    relations: tuple[Relation, ...]
    teams: tuple[Team, ...]
    kpis: dict[str, Kpi]

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
        if len(codes) != len(set(codes)):
            raise ValueError("duplicate team code")
        return self

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
        try:
            return self.entities[name]
        except KeyError:
            raise KeyError(f"unknown entity type {name!r}") from None

    def kpi(self, name: str) -> Kpi:
        try:
            return self.kpis[name]
        except KeyError:
            raise KeyError(f"unknown KPI {name!r}") from None

    def parent_relation(self, child: str) -> Relation:
        """The relation from `child`'s parent type to `child` (e.g. Site -> Cell)."""
        parent = self.entity(child).parent
        for rel in self.relations:
            if rel.src == parent and rel.dst == child:
                return rel
        raise KeyError(f"no relation from {parent!r} to {child!r}")

    def team(self, code: str) -> Team:
        for t in self.teams:
            if t.code == code:
                return t
        raise KeyError(f"unknown team {code!r}")

    def topology_types(self) -> list[EntityType]:
        """Topology entity types ordered root (Core) first."""
        levelled = [e for e in self.entities.values() if e.topology_level is not None]
        return sorted(levelled, key=lambda e: e.topology_level or 0)


def _read_yaml(path: Path | Any) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise DomainError(f"{path}: expected a mapping at top level")
    if data.get("version") != 1:
        raise DomainError(f"{path}: unsupported or missing version")
    return data


def load_domain_from(directory: Path | Any) -> Domain:
    """Load and validate the domain model from a directory holding the YAML files."""
    ent_raw = _read_yaml(directory / "entities.yaml")
    kpi_raw = _read_yaml(directory / "kpis.yaml")

    kpi_names = [k.get("name") for k in kpi_raw.get("kpis", [])]
    dupes = sorted({n for n in kpi_names if kpi_names.count(n) > 1})
    if dupes:
        raise DomainError(f"duplicate KPI names: {dupes}")

    try:
        return Domain(
            entities={
                name: EntityType(name=name, **spec)
                for name, spec in (ent_raw.get("entities") or {}).items()
            },
            relations=tuple(Relation(**r) for r in ent_raw.get("relations", [])),
            teams=tuple(Team(**t) for t in ent_raw.get("teams", [])),
            kpis={k["name"]: Kpi(**k) for k in kpi_raw.get("kpis", [])},
        )
    except (ValueError, TypeError) as exc:  # pydantic ValidationError is a ValueError
        raise DomainError(str(exc)) from exc


@cache
def load_domain() -> Domain:
    """The project's domain model (cached; immutable)."""
    return load_domain_from(_DEFAULT_DIR)
