"""This process's resource use, for the memory budgets in the step checks (guardrail R1)."""

from __future__ import annotations

import resource
import sys


def peak_rss_mib() -> float:
    """Peak resident memory of this process so far, in MiB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10  # bytes on macOS, KiB


class OverBudgetError(RuntimeError):
    """A process used more memory than its budget (R1)."""


def check_budget(name: str, budget_mib: float) -> float:
    """This process's peak memory in MiB; OverBudgetError above `budget_mib` (R1). Call it
    before logging anything a later step could promote."""
    peak = peak_rss_mib()
    if peak > budget_mib:
        raise OverBudgetError(
            f"{name}: peak memory {peak:.0f} MiB > {budget_mib:.0f} MiB budget (R1); not logged"
        )
    return peak
