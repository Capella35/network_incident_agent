"""Read and aggregate one Milan activity file (format verified 2026-11-14, specs/02-telemetry.md).

Rules (DECISIONS.md 2026-11-14): sum over country codes per (square, interval); empty field = 0;
a missing (square, interval) row = 0 with `data_gap = true`; timestamps in UTC.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from common.domain.loader import MILAN_FIELDS

MILAN_TZ = ZoneInfo("Europe/Rome")
INTERVAL = timedelta(minutes=10)

# Column order per the dataset paper; the five activity fields must match the domain model.
ACTIVITY_FIELDS = ("sms_in", "sms_out", "call_in", "call_out", "internet")
COLUMNS = ("square_id", "time_interval", "country_code", *ACTIVITY_FIELDS)
if frozenset(ACTIVITY_FIELDS) != MILAN_FIELDS:
    raise ImportError("Milan activity fields drifted from common.domain MILAN_FIELDS")

_SCHEMA = pl.Schema(
    {
        "square_id": pl.Int32,
        "time_interval": pl.Int64,
        "country_code": pl.Int32,
        **{f: pl.Float64 for f in ACTIVITY_FIELDS},
    }
)
_FILE_RE = re.compile(r"^sms-call-internet-mi-(\d{4}-\d{2}-\d{2})\.txt$")


@dataclass(frozen=True)
class DayResult:
    frame: pl.DataFrame  # one row per (subset square, interval), sorted
    rows_in: int  # rows in the raw file
    rows_subset: int  # raw rows for subset squares (before summing country codes)
    rows_out_of_day: int  # subset rows outside the file's local day (dropped)
    gap_rows: int  # (square, interval) pairs with no source row


def file_date(path: Path) -> date:
    m = _FILE_RE.match(path.name)
    if not m:
        raise ValueError(f"not a Milan activity file name: {path.name}")
    return date.fromisoformat(m.group(1))


def day_intervals(day: date) -> list[datetime]:
    """UTC starts of the 10-minute intervals of one Milan local day (handles DST)."""
    start = datetime.combine(day, time(), MILAN_TZ).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time(), MILAN_TZ).astimezone(UTC)
    out = []
    t = start
    while t < end:
        out.append(t)
        t += INTERVAL
    return out


def _scan(path: Path) -> pl.LazyFrame:
    return pl.scan_csv(
        path,
        separator="\t",
        has_header=False,
        schema=_SCHEMA,
        # Empty fields become null; they are summed as 0 below.
    )


def process_day(path: Path, subset: list[int]) -> DayResult:
    """Aggregate one daily file to the subset, gap-filled, as a sorted frame."""
    day = file_date(path)
    intervals = day_intervals(day)
    start_ms = int(intervals[0].timestamp() * 1000)
    end_ms = int((intervals[-1] + INTERVAL).timestamp() * 1000)

    lf = _scan(path)
    rows_in = lf.select(pl.len()).collect(engine="streaming").item()

    sub = lf.filter(pl.col("square_id").is_in(subset))
    in_day = pl.col("time_interval").is_between(start_ms, end_ms, closed="left")
    stats = sub.select(
        pl.len().alias("rows_subset"),
        (~in_day).sum().alias("rows_out_of_day"),
        (pl.col("time_interval") % (INTERVAL.seconds * 1000) != 0).sum().alias("misaligned"),
    ).collect(engine="streaming")
    if stats["misaligned"].item():
        raise ValueError(f"{path.name}: timestamps not on 10-minute boundaries")

    agg = (
        sub.filter(in_day)
        .group_by("square_id", "time_interval")
        .agg(
            pl.len().alias("_source_rows"),
            *[pl.col(f).sum() for f in ACTIVITY_FIELDS],  # nulls are skipped: empty = 0
        )
        .collect(engine="streaming")
    )

    full = pl.DataFrame(
        {
            "square_id": [s for s in sorted(subset) for _ in intervals],
            "time_interval": [int(t.timestamp() * 1000) for _ in subset for t in intervals],
        },
        schema={"square_id": pl.Int32, "time_interval": pl.Int64},
    )
    joined = full.join(agg, on=["square_id", "time_interval"], how="left")
    gap = pl.col("_source_rows").is_null()  # no source row at all for (square, interval)
    frame = (
        joined.with_columns(gap.alias("data_gap"))
        .with_columns(
            *[pl.col(f).fill_null(0.0) for f in ACTIVITY_FIELDS],
            pl.from_epoch("time_interval", time_unit="ms")
            .dt.cast_time_unit("ms")  # match the source (epoch ms)
            .dt.replace_time_zone("UTC")
            .alias("interval_start"),
        )
        .select("square_id", "interval_start", *ACTIVITY_FIELDS, "data_gap")
        .sort("interval_start", "square_id")
    )
    return DayResult(
        frame=frame,
        rows_in=rows_in,
        rows_subset=stats["rows_subset"].item(),
        rows_out_of_day=stats["rows_out_of_day"].item(),
        gap_rows=int(frame["data_gap"].sum()),
    )
