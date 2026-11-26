"""A hand-made 1-day fixture whose every number is worked out by hand (milestone 2, step 5).

Topology (SYNTHETIC): AGG-1 -> SITE-1 (CELL-1-1, CELL-1-2), SITE-2 (CELL-2-1); AGG-2 -> SITE-3
(CELL-3-1). `t(i)` = the i-th 10-minute interval of the validation day DAY (UTC).

Fault  Type         Cells          Window      Alarms on its cells (widened +-1)  Outcome
F1     cell_outage  1-1            [10, 13)    11                                  hit, delay 1
F2     site_power   1-1, 1-2       [30, 40)    29 (1-2, one early), 30 (1-1)       hit, delay 0
F3     backhaul     1-1, 1-2, 2-1  [60, 66)    66 (2-1, at end: tolerance)         late hit, delay 6
F4     congestion   3-1            [80, 84)    none                                miss, censored 4
F5     config_push  3-1            [100, 103)  none; all 3 rows are gaps           unobservable
F6     congestion   2-1            [120, 126)  121                                 hit, delay 1
F7     cell_outage  1-1            [122, 125)  122 (same incident as F6's alarm)   hit, delay 0

Censoring: the last delay that could still match, end + 1 - onset - 1 (F4: 85 - 80 - 1 = 4).

Incidents (same aggregation node, consecutive alarms <= 1 interval apart): {11}, {29, 30},
{50 on 3-1: false}, {66}, {70, 71 on 1-1: false, F3's widened window ends at 67}, {73 on 2-1:
false, 2 intervals after 71}, {121, 122}. So 7 incidents, 3 false, 3.00 per day.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from evaluation.ground_truth import Fault
from producer import milan, topology

DAY = date(2013, 12, 2)  # first validation day
SLOTS = milan.day_intervals(DAY)


def t(i: int) -> datetime:
    return SLOTS[i]


def _node(node_id: str, node_type: str, parent: str | None) -> topology.Node:
    return topology.Node(node_id, node_type, parent, 45.46, 9.19)


INDEX = topology.Index(
    [
        _node("CORE-1", "Core", None),
        _node("AGG-1", "AggregationNode", "CORE-1"),
        _node("AGG-2", "AggregationNode", "CORE-1"),
        _node("SITE-1", "Site", "AGG-1"),
        _node("SITE-2", "Site", "AGG-1"),
        _node("SITE-3", "Site", "AGG-2"),
        _node("CELL-1-1", "Cell", "SITE-1"),
        _node("CELL-1-2", "Cell", "SITE-1"),
        _node("CELL-2-1", "Cell", "SITE-2"),
        _node("CELL-3-1", "Cell", "SITE-3"),
    ]
)
SITE = INDEX.cell_sites()


def fault(fid: str, ftype: str, cells: tuple[str, ...], start: int, end: int) -> Fault:
    return Fault(fid, ftype, cells, t(start), t(end))


FAULTS = (
    fault("F1", "cell_outage", ("CELL-1-1",), 10, 13),
    fault("F2", "site_power", ("CELL-1-1", "CELL-1-2"), 30, 40),
    fault("F3", "backhaul", ("CELL-1-1", "CELL-1-2", "CELL-2-1"), 60, 66),
    fault("F4", "congestion", ("CELL-3-1",), 80, 84),
    fault("F5", "config_push", ("CELL-3-1",), 100, 103),
    fault("F6", "congestion", ("CELL-2-1",), 120, 126),
    fault("F7", "cell_outage", ("CELL-1-1",), 122, 125),
)

ALARM_AT = (
    ("CELL-1-1", 11),
    ("CELL-1-2", 29),
    ("CELL-1-1", 30),
    ("CELL-3-1", 50),
    ("CELL-2-1", 66),
    ("CELL-1-1", 70),
    ("CELL-1-1", 71),
    ("CELL-2-1", 73),
    ("CELL-2-1", 121),
    ("CELL-1-1", 122),
)
GAP_AT = (("CELL-3-1", 100), ("CELL-3-1", 101), ("CELL-3-1", 102))


def alarms(at: tuple[tuple[str, int], ...] = ALARM_AT) -> pl.DataFrame:
    """Rows shaped like `models.detectors.alarms` output."""
    return pl.DataFrame(
        {
            "cell_id": [c for c, _ in at],
            "site_id": [SITE[c] for c, _ in at],
            "interval_start": [t(i) for _, i in at],
            "score": [9.0] * len(at),
            "kpis": [["traffic_internet"]] * len(at),
        },
        schema={
            "cell_id": pl.String,
            "site_id": pl.String,
            "interval_start": pl.Datetime("ms", "UTC"),
            "score": pl.Float64,
            "kpis": pl.List(pl.String),
        },
    )


def gaps(at: tuple[tuple[str, int], ...] = GAP_AT) -> pl.DataFrame:
    return pl.DataFrame(
        {"cell_id": [c for c, _ in at], "interval_start": [t(i) for _, i in at]},
        schema={"cell_id": pl.String, "interval_start": pl.Datetime("ms", "UTC")},
    )
