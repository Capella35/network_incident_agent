"""Shared domain model: entity types, ID formats, teams, KPI catalog, fault types.

Fault SIGNATURES are deliberately not re-exported here: they live in
`common.domain.signatures`, which the fault injector must never import (Rule 15).
"""

from common.domain.loader import Domain, DomainError, Fault, Kpi, load_domain, load_domain_from

__all__ = ["Domain", "DomainError", "Fault", "Kpi", "load_domain", "load_domain_from"]
