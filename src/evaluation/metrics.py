"""Detection metrics against the ground truth (milestone 2, step 5; specs/09-evals.md).

- **Provisional incidents (M2 only):** alarms on cells under the same aggregation node whose
  times are at most one interval apart form one incident (chained). Sites sit under exactly one
  aggregation node, so "same site or same aggregation node" is the aggregation node. Milestone 3's
  correlator replaces this rule and re-measures.
- **Matching:** an incident matches a fault if they share a cell and the incident overlaps the
  fault window widened by one interval on each side. Incidents matching no fault are false
  incidents. A fault is detected iff an alarm falls on one of its cells inside that widened
  window (every such alarm belongs to a matching incident, so this is the same thing).
- **Delay** in intervals from the fault onset to its first matching alarm (0 if the alarm is at
  or one interval before onset). Missed faults are right-censored at the last interval that could
  still have matched (end + tolerance, or the evaluated window's end if sooner), and the median is
  estimated with **Kaplan-Meier** (`DECISIONS.md` 2026-11-25), so missing slow faults cannot
  lower it. A fault whose every cell-interval is a data gap is **unobservable**: it is reported but
  left out of rates and delay (gaps are not faults, `specs/02-telemetry.md`).
- Faults with onset in the window are scored; faults that only overlap it still match incidents.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from statistics import NormalDist

import polars as pl

from common import telemetry
from common.domain import load_domain
from evaluation.ground_truth import Fault
from producer import milan, topology

INTERVAL = telemetry.INTERVAL
TOLERANCE = 1  # intervals, each side (specs/09-evals.md matching rule)
CONFIDENCE = 0.95
HALF = 0.5  # the median: where survival first drops to one half


# --- Kaplan-Meier ----------------------------------------------------------------------------


@dataclass(frozen=True)
class Median:
    """KM median and its confidence interval, in intervals. `value` None: S never falls to one
    half (not reached). `low` / `high` None: that band never reaches one half before S hits 0,
    where the log-scale variance is infinite (R's `survfit` gives NA there too)."""

    value: int | None
    low: int | None
    high: int | None


def km_median(times: Sequence[int], detected: Sequence[bool]) -> Median:
    """Kaplan-Meier median of time-to-detection. `times[i]` is the delay if `detected[i]`, else
    the censoring time (survived through it undetected). CI: Greenwood variance on log S (R's
    `survfit` default, `conf.type = "log"`), bounds where each band first reaches one half
    (Brookmeyer-Crowley). Ties: detections at t count before censorings at t. When S equals
    one half exactly, the median is that time, not R's midpoint to the next event (whole
    intervals; at most half an interval lower)."""
    z = NormalDist().inv_cdf((1 + CONFIDENCE) / 2)
    s, greenwood = 1.0, 0.0
    value = low = high = None
    for t in sorted({t for t, d in zip(times, detected, strict=True) if d}):
        at_risk = sum(1 for u in times if u >= t)
        events = sum(1 for u, d in zip(times, detected, strict=True) if d and u == t)
        s *= 1 - events / at_risk
        if value is None and s <= HALF:
            value = t
        if s == 0:  # infinite variance: no band from here on
            break
        greenwood += events / (at_risk * (at_risk - events))
        spread = math.exp(z * math.sqrt(greenwood))
        if low is None and s / spread <= HALF:
            low = t
        if high is None and s * spread <= HALF:
            high = t
    return Median(value, low, high)


# --- Incidents and matching ------------------------------------------------------------------


def incidents(alarms: pl.DataFrame, index: topology.Index) -> pl.DataFrame:
    """Provisional M2 incidents from `detectors.alarms` rows: `incident`, `group` (aggregation
    node), `start`, `end` (exclusive), `cells` (sorted list)."""
    group = {s: index.parent(s) or s for s in index.of_type(index.site_type)}
    unknown = set(alarms["site_id"]) - group.keys()
    if unknown:
        raise ValueError(f"alarm sites not in the topology: {sorted(unknown)[:3]} (rebuilt?)")
    starts = (pl.col("interval_start").diff().over("group") > INTERVAL).fill_null(value=True)
    return (
        alarms.with_columns(group=pl.col("site_id").replace_strict(group, return_dtype=pl.String))
        .sort("group", "interval_start")
        .with_columns(incident=starts.cast(pl.Int64).cum_sum())
        .group_by("incident", "group")
        .agg(
            start=pl.col("interval_start").min(),
            end=pl.col("interval_start").max() + INTERVAL,
            cells=pl.col("cell_id").unique().sort(),
        )
        .sort("start", "group")
    )


def _widened(f: Fault) -> tuple[datetime, datetime]:
    return f.onset - TOLERANCE * INTERVAL, f.end + TOLERANCE * INTERVAL


def matches(start: datetime, end: datetime, cells: Sequence[str], f: Fault) -> bool:
    """Does the incident [start, end) on `cells` match fault `f`?"""
    lo, hi = _widened(f)
    return not set(cells).isdisjoint(f.cells) and start < hi and lo < end


def false_incidents(inc: pl.DataFrame, faults: Sequence[Fault]) -> int:
    """Incidents matching no fault; only faults sharing a cell are compared."""
    by_cell: dict[str, list[Fault]] = {}
    for f in faults:
        for c in f.cells:
            by_cell.setdefault(c, []).append(f)
    return sum(
        1
        for i in inc.iter_rows(named=True)
        if not any(
            matches(i["start"], i["end"], i["cells"], f)
            for c in i["cells"]
            for f in by_cell.get(c, ())
        )
    )


# --- Per fault -------------------------------------------------------------------------------


@dataclass(frozen=True)
class FaultOutcome:
    fault_id: str
    fault_type: str
    observable: bool
    detected: bool
    time: int  # delay in intervals if detected, else the censoring time


def outcome(
    f: Fault, alarms: pl.DataFrame, gaps: pl.DataFrame, window_end: datetime
) -> FaultOutcome:
    """One scored fault: detected or censored, observable or all gaps."""
    lo, hi = _widened(f)
    hi = min(hi, window_end)
    on_cells = pl.col("cell_id").is_in(f.cells)
    first = alarms.filter(on_cells, pl.col("interval_start").is_between(lo, hi, closed="left"))
    if first.height:
        t: datetime = first["interval_start"].min()  # type: ignore[assignment]
        delay = max(0, (t - f.onset) // INTERVAL)
        return FaultOutcome(f.fault_id, f.fault_type, observable=True, detected=True, time=delay)
    cut = min(f.end, window_end)
    in_gap = gaps.filter(on_cells, pl.col("interval_start").is_between(f.onset, cut, closed="left"))
    all_gaps = in_gap.height >= len(f.cells) * ((cut - f.onset) // INTERVAL)
    censored = (hi - f.onset) // INTERVAL - 1  # the last delay that could still have matched
    return FaultOutcome(f.fault_id, f.fault_type, not all_gaps, detected=False, time=censored)


# --- Result ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Summary:
    faults: int
    unobservable: int
    detected: int
    delay: Median

    @property
    def detection_rate(self) -> float | None:
        observable = self.faults - self.unobservable
        return self.detected / observable if observable else None


def summarize(outcomes: Sequence[FaultOutcome]) -> Summary:
    seen = [o for o in outcomes if o.observable]
    delay = km_median([o.time for o in seen], [o.detected for o in seen])
    return Summary(len(outcomes), len(outcomes) - len(seen), sum(o.detected for o in seen), delay)


@dataclass(frozen=True)
class Result:
    days: tuple[date, ...]
    overall: Summary
    by_type: dict[str, Summary]  # every real fault type, sorted by name
    incidents: int
    false_incidents: int
    outcomes: tuple[FaultOutcome, ...]

    @property
    def false_per_day(self) -> float:
        return self.false_incidents / len(self.days)


def evaluate(
    alarms: pl.DataFrame,
    faults: Sequence[Fault],
    *,
    days: Sequence[date],
    index: topology.Index,
    gaps: pl.DataFrame,
) -> Result:
    """Score `detectors.alarms` rows over consecutive local `days`. `gaps`: `cell_id`,
    `interval_start` of the evaluated data-gap rows. `faults`: the split's ground truth."""
    if any((b - a).days != 1 for a, b in itertools.pairwise(days)):
        raise ValueError(f"days must be consecutive ({days[0]}..{days[-1]})")
    start, end = milan.window(days)
    scored = [f for f in faults if start <= f.onset < end]
    outcomes = tuple(outcome(f, alarms, gaps, end) for f in scored)
    inc = incidents(alarms, index)
    types = sorted(f.name for f in load_domain().faults.values() if f.is_real)
    return Result(
        days=tuple(days),
        overall=summarize(outcomes),
        by_type={t: summarize([o for o in outcomes if o.fault_type == t]) for t in types},
        incidents=inc.height,
        false_incidents=false_incidents(inc, faults),
        outcomes=outcomes,
    )
