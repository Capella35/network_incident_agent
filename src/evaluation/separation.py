"""Does a signal separate one fault type from normal busy periods? (milestone 2, step 6d)

    python -m evaluation.separation --fault-type congestion [--data-dir data]

Validation weeks with SYNTHETIC faults (`streaming.offline`). For each signal: the peak per fault
of the type on its cells over its match window (`metrics.widened`; a detector needs one alarm),
against every normal busy row: local hours `BUSY_HOURS`, outside every fault's match window on
its cells. A signal separates if the median fault peak exceeds the normal busy rows'
`SEPARATES_AT` quantile (decided before measuring, DECISIONS.md 2026-11-26). A peak is a max over
many rows, so it beats a row quantile more easily the longer and wider the fault; the report also
gives the same peak over fault-free control windows of the same shape (`control_windows`), and
how many faults beat all of their own. Faults with no data are left out. Signals: the load
KPI's z and the internet KPI's z (features; since Step 6d the load is judged like the real KPIs,
`features.yaml` level.modeled), plus `load_surprise`, computed here: log1p MODELED load minus
its mean over the same local hour of the cell's previous `features.yaml` level.days days, null
with fewer than level.min_days, divided by that window's std floored at baseline.std_floor_log.
Ground truth stays in this package (Rule 6). Markdown to stdout; writes nothing.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import statistics
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from common import process, splits
from common.domain import load_domain
from evaluation import ground_truth, metrics, report
from models import cli, detectors
from producer import data_prep, kpi_model
from streaming import baseline, features, offline

log = logging.getLogger("evaluation.separation")

BUSY_HOURS = (11, 15)  # local, inclusive: where Step 6 found the busy-hour false alarms
SEPARATES_AT = 0.999  # quantile of normal busy rows the median fault peak must exceed
SURPRISE = "load_surprise"
SHIFTED = "@"  # control window id: <fault_id>@<shift in days>


def signals(cfg: features.Config) -> list[str]:
    """The compared signals: load z and internet z (features), and the load surprise."""
    return [f"{cfg.load_curve.load}_z", f"{kpi_model.load_params().load_kpi}_z", SURPRISE]


def with_load_surprise(rows: pl.DataFrame, cfg: features.Config) -> pl.DataFrame:
    """`cell_id`, `interval_start`, `load_surprise` for telemetry `rows` (history days included:
    the first days get null). MODELED: load is `cfg.load_curve.load`. The window is the level
    offset's (`features.trailing_days`); gap rows neither count nor get a value. Unlike
    `features.level_offsets`, a day with zero load counts as history, and the std is over the
    window's intervals, not over days (as measured in Step 6d; kept for comparability)."""
    x = pl.col("x")
    work = rows.select(
        "cell_id",
        "interval_start",
        "data_gap",
        pl.col(cfg.load_curve.load).log1p().alias("x"),
        features.LOCAL_TIME.dt.date().alias("local_date"),
        features.LOCAL_TIME.dt.hour().alias("hour"),
    )
    hour_keys = ("cell_id", "local_date", "hour")
    hourly = (
        work.filter(~pl.col("data_gap"))
        .group_by(hour_keys)
        .agg(
            x.count().cast(pl.Float64).alias("n"),
            x.sum().alias("s"),
            (x * x).sum().alias("s2"),
            (x.count() > 0).cast(pl.Float64).alias("days"),
        )
        .sort(*features.LEVEL_GROUP, "local_date")
    )
    n, s, s2 = (features.trailing_days(pl.col(c), cfg) for c in ("n", "s", "s2"))
    mean = s / n
    std = ((s2 / n - mean * mean).clip(lower_bound=0) * n / (n - 1)).sqrt()
    enough = features.trailing_days(pl.col("days"), cfg) >= cfg.level.min_days
    past = hourly.select(
        *hour_keys,
        pl.when(enough).then(mean).alias("mean"),
        pl.max_horizontal(std, pl.lit(cfg.baseline.std_floor_log)).alias("std"),
    )
    z = features.z_score(x, pl.col("mean"), pl.col("std"), cfg)
    return work.join(past, on=hour_keys, how="left").select(
        "cell_id", "interval_start", z.alias(SURPRISE)
    )


def _windows(faults: Sequence[ground_truth.Fault], when: pl.DataType) -> pl.DataFrame:
    """`fault_id`, `cell_id`, `lo`, `hi`: each fault's match window on each of its cells."""
    return pl.DataFrame(
        [(f.fault_id, c, *metrics.widened(f)) for f in faults for c in f.cells],
        schema={"fault_id": pl.String, "cell_id": pl.String, "lo": when, "hi": when},
        orient="row",
    )


def _in_windows(frame: pl.DataFrame, faults: Sequence[ground_truth.Fault]) -> pl.DataFrame:
    """`frame` rows inside a fault's match window on its cells, with that `fault_id`."""
    windows = _windows(faults, frame.schema["interval_start"])
    return frame.join(windows, on="cell_id").filter(
        pl.col("interval_start").is_between(pl.col("lo"), pl.col("hi"), closed="left")
    )


def fault_peaks(
    frame: pl.DataFrame, faults: Sequence[ground_truth.Fault], names: Sequence[str]
) -> pl.DataFrame:
    """One row per fault, in order: `fault_id` and each signal's peak on its cells over its
    match window (null when it has no rows)."""
    peaks = _in_windows(frame, faults).group_by("fault_id").agg(pl.col(s).max() for s in names)
    ids = pl.DataFrame({"fault_id": [f.fault_id for f in faults]}, schema={"fault_id": pl.String})
    return ids.join(peaks, on="fault_id", how="left", maintain_order="left")


def normal_busy(frame: pl.DataFrame, faults: Sequence[ground_truth.Fault]) -> pl.DataFrame:
    """Rows without a data gap in local hours `BUSY_HOURS`, outside every fault's match window
    on its cells."""
    busy = frame.filter(features.LOCAL_TIME.dt.hour().is_between(*BUSY_HOURS), ~pl.col("data_gap"))
    keys = ["cell_id", "interval_start"]
    return busy.join(_in_windows(busy, faults).select(keys).unique(), on=keys, how="anti")


def control_windows(
    of: Sequence[ground_truth.Fault],
    faults: Sequence[ground_truth.Fault],
    span: tuple[datetime, datetime],
) -> list[ground_truth.Fault]:
    """Fault-free windows shaped like each fault in `of`: its cells, its window shifted by whole
    days, kept if the match window lies in `span` [start, end) and overlaps no fault's match
    window on those cells. Id: `<fault_id>@<shift>`. Their peaks are what the same max over the
    same number of rows reaches without a fault (a row quantile alone favours long faults)."""
    days = (span[1] - span[0]).days
    out = []
    for f in of:
        for k in (k for k in range(-days, days + 1) if k):
            shift = timedelta(days=k)
            g = dataclasses.replace(
                f,
                fault_id=f"{f.fault_id}{SHIFTED}{k:+d}d",
                onset=f.onset + shift,
                end=f.end + shift,
            )
            lo, hi = metrics.widened(g)
            inside = span[0] <= lo and hi <= span[1]
            if inside and not any(metrics.matches(lo, hi, g.cells, h) for h in faults):
                out.append(g)
    return out


def above_controls(peaks: pl.DataFrame, control: pl.DataFrame, name: str) -> tuple[int, int]:
    """(faults whose `name` peak exceeds every one of their control windows' peaks, faults with
    a peak and at least one control peak)."""
    parent = pl.col("fault_id").str.replace(rf"{SHIFTED}[+-]\d+d$", "")
    ctrl = control.group_by(parent).agg(pl.col(name).max().alias("ctrl"))
    both = peaks.join(ctrl, on="fault_id").drop_nulls([name, "ctrl"])
    return int((both[name] > both["ctrl"]).sum()), both.height


def separates(peaks: pl.Series, normal: pl.Series) -> tuple[float | None, float | None, bool]:
    """(median fault peak, normal `SEPARATES_AT` quantile, does the first exceed the second);
    nulls ignored."""
    p = peaks.drop_nulls().to_list()
    median = statistics.median(p) if p else None
    q = normal.quantile(SEPARATES_AT, interpolation="linear")
    return median, q, median is not None and q is not None and median > q


@dataclass(frozen=True)
class Measured:
    peaks: pl.DataFrame  # `fault_peaks` of the faults of the type
    control: pl.DataFrame  # `fault_peaks` of their `control_windows`
    normal: pl.DataFrame  # `normal_busy` rows


def render(m: Measured, names: Sequence[str], fault_type: str) -> str:
    num, pct = report.num, f"p{SEPARATES_AT * 100:g}"
    lines = [
        f"## Separation: {fault_type} faults vs normal busy rows (validation, SYNTHETIC faults)",
        "",
        f"{m.peaks.height} {fault_type} faults; {m.normal.height} normal busy rows (local hours "
        f"{BUSY_HOURS[0]}-{BUSY_HOURS[1]}); {m.control.height} control windows (same cells and "
        f"shape, shifted by whole days, no fault). Separates (decided before measuring): median "
        f"fault peak > normal {pct}. Faults with no data are left out of every count.",
        "",
        f"| signal | fault peak min / median / max | normal p99 / {pct} / max "
        f"| faults above normal {pct} | control peak median | faults above all own controls "
        f"| separates |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for s in names:
        median, q, ok = separates(m.peaks[s], m.normal[s])
        seen = m.peaks[s].drop_nulls()
        above = 0 if q is None else int((seen > q).sum())
        beat, paired = above_controls(m.peaks, m.control, s)
        low, high = m.peaks.select(pl.col(s).min().alias("a"), pl.col(s).max().alias("b")).row(0)
        p99, top = m.normal.select(
            pl.col(s).quantile(0.99, "linear").alias("a"), pl.col(s).max().alias("b")
        ).row(0)
        ctrl = m.control.select(pl.col(s).median()).item()
        lines.append(
            f"| {s} | {num(low)} / {num(median)} / {num(high)} "
            f"| {num(p99)} / {num(q)} / {num(top)} | {above} / {seen.len()} "
            f"| {num(ctrl)} | {beat} / {paired} | {'yes' if ok else 'no'} |"
        )
    lines += ["", "| fault | " + " | ".join(names) + " |", "|---|" + "---:|" * len(names)]
    lines += [
        f"| {r['fault_id']} | " + " | ".join(num(r[s]) for s in names) + " |"
        for r in m.peaks.iter_rows(named=True)
    ]
    return "\n".join(lines) + "\n"


def measure(data_dir: Path, fault_type: str, names: Sequence[str]) -> Measured:
    """Validation weeks, SYNTHETIC faults injected; see the module docstring."""
    cfg = features.load_config()
    ref = baseline.load(data_dir)  # fails before any telemetry is loaded
    days = sorted(data_prep.require_processed_days(data_dir, splits.Split.VALIDATION))
    loaded = offline.telemetry_with_history(data_dir, days, inject=True)
    surprise = with_load_surprise(loaded.rows, cfg)
    feats = offline.features_of(loaded.rows, days, ref)
    del loaded  # raw telemetry with history: the largest frame, not needed below
    frame = feats.select(*features.KEYS, *(n for n in names if n != SURPRISE)).join(
        surprise, on=["cell_id", "interval_start"], how="left"
    )
    del feats, surprise
    process.check_budget("separation", detectors.load_config().memory_budget_mib)
    faults = ground_truth.read(data_dir, splits.Split.VALIDATION).faults
    of_type = [f for f in faults if f.fault_type == fault_type]
    first, last = frame.select(
        pl.col("interval_start").min().alias("a"), pl.col("interval_start").max().alias("b")
    ).row(0)
    span = (first, last + metrics.INTERVAL)
    return Measured(
        fault_peaks(frame, of_type, names),
        fault_peaks(frame, control_windows(of_type, faults, span), names),
        normal_busy(frame, faults),
    )


def run(data_dir: Path, fault_type: str) -> str:
    """The Markdown report for `fault_type` on the validation weeks."""
    try:
        load_domain().fault(fault_type)  # an unknown type fails before any data is loaded
    except KeyError as exc:
        raise ValueError(str(exc)) from None
    names = signals(features.load_config())
    return render(measure(data_dir, fault_type, names), names, fault_type)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--fault-type", required=True)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        sys.stdout.write(run(args.data_dir, args.fault_type))
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
