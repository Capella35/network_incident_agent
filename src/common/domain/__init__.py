"""Shared domain model: entity types, ID formats, teams, KPI catalog."""

from common.domain.loader import Domain, DomainError, Kpi, load_domain, load_domain_from

__all__ = ["Domain", "DomainError", "Kpi", "load_domain", "load_domain_from"]
