"""This process's resource use, for the memory budgets in the step checks (guardrail R1)."""

from __future__ import annotations

import resource
import sys


def peak_rss_mib() -> float:
    """Peak resident memory of this process so far, in MiB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10  # bytes on macOS, KiB
