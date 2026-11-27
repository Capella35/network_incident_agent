"""Why faults are missed (milestone 2, step 6c): a per-fault diagnosis of the tuned detectors.

    python -m evaluation.misses [--data-dir data]   # feasible candidates of the last `make train`

Scores the validation weeks (SYNTHETIC faults injected) with each candidate's tuned model from its
latest `detector-tuning` study (threshold inside), exactly as tuning scored them, and joins every
fault's outcome with what the data showed during the fault: duration, number of cells, the worst
bad-direction KPI z on its cells (deviation size, from the features), and the detector's peak
score relative to its threshold (how near a miss came). The ground truth stays in this package
(Rule 6). Markdown to stdout and, per candidate, to MLflow (`detector-evaluation`). Writes nothing
else; validation days only (Rule 7).
"""

from __future__ import annotations

import argparse
import itertools
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import mlflow
import polars as pl
from mlflow.entities import Run

from common import process, splits
from evaluation import ground_truth, metrics, report
from models import cli, detectors, tracking, tune
from streaming import features

log = logging.getLogger("evaluation.misses")

DEVIATION_BINS = (3.0, 6.0, 12.0)  # worst bad z on the fault's cells: < 3 is within normal noise
DURATION_BINS = (6, 18, 36)  # intervals: < 1 h, 1-3 h, 3-6 h, >= 6 h (bins are left-closed)


def per_fault(
    result: metrics.Result,
    faults: Sequence[ground_truth.Fault],
    rows: pl.DataFrame,
    threshold: float,
) -> pl.DataFrame:
    """One row per scored, observable fault. `rows`: `cell_id`, `interval_start`, `score`, and
    one bad-direction z column per KPI (named by the KPI, `features.oriented`). Columns:
    `fault_id`, `fault_type`, `cells` (count), `duration` (intervals), `detected`, `delay`
    (intervals; null if missed), `peak_ratio` (peak score on its cells / `threshold`, > 0),
    `worst_kpi`, `worst_bad` (that KPI's peak bad z); null when no row was scored. Peaks are taken
    over the window an alarm may match in (`metrics.widened`), so a detected fault peaks >= 1."""
    if threshold <= 0:
        raise ValueError("per_fault: threshold must be > 0 for a peak / threshold ratio")
    by_id = {f.fault_id: f for f in faults}
    kpis = [c for c in rows.columns if c not in ("cell_id", "interval_start", "score")]
    out = []
    for o in result.outcomes:
        if not o.observable:
            continue
        f = by_id[o.fault_id]
        lo, hi = metrics.widened(f)
        during = rows.filter(
            pl.col("cell_id").is_in(f.cells),
            pl.col("interval_start").is_between(lo, hi, closed="left"),
        )
        peaks = during.select(pl.col("score").max(), *[pl.col(k).max() for k in kpis]).row(
            0, named=True
        )
        worst = max((k for k in kpis if peaks[k] is not None), key=lambda k: peaks[k], default=None)
        out.append(
            {
                "fault_id": f.fault_id,
                "fault_type": f.fault_type,
                "cells": len(f.cells),
                "duration": (f.end - f.onset) // metrics.INTERVAL,
                "detected": o.detected,
                "delay": o.time if o.detected else None,
                "peak_ratio": None if peaks["score"] is None else peaks["score"] / threshold,
                "worst_kpi": worst,
                "worst_bad": None if worst is None else peaks[worst],
            }
        )
    return pl.DataFrame(out, schema=_SCHEMA)


_SCHEMA = {
    "fault_id": pl.String,
    "fault_type": pl.String,
    "cells": pl.Int64,
    "duration": pl.Int64,
    "detected": pl.Boolean,
    "delay": pl.Int64,
    "peak_ratio": pl.Float64,
    "worst_kpi": pl.String,
    "worst_bad": pl.Float64,
}


def _bucket(col: str, edges: Sequence[float], unit: str) -> pl.Expr:
    labels = [f"< {edges[0]:g}{unit}"]
    labels += [f"{a:g}-{b:g}{unit}" for a, b in itertools.pairwise(edges)]
    labels.append(f">= {edges[-1]:g}{unit}")
    return pl.col(col).bin_intervals(list(edges), labels=labels).cast(pl.String)


def _breakdown(table: pl.DataFrame, name: str, label: pl.Expr, order: str) -> str:
    """Markdown: missed / total per `label` group, groups in order of column `order`."""
    groups = (
        table.group_by(label.alias(name))
        .agg(
            pl.len().alias("faults"),
            (~pl.col("detected")).sum().alias("missed"),
            pl.col(order).min().alias("_order"),
        )
        .sort("_order", nulls_last=True)
    )
    lines = [f"| {name} | missed / faults |", "|---|---:|"]
    lines += [
        f"| {r[name]} | {r['missed']} / {r['faults']} |" for r in groups.iter_rows(named=True)
    ]
    return "\n".join(lines)


def render(table: pl.DataFrame, result: metrics.Result, *, title: str) -> str:
    missed = table.filter(~pl.col("detected")).sort("fault_type", "worst_bad")
    median, worst = metrics.caught_delays(result.outcomes)
    km = result.overall.delay
    lines = [
        f"## {title}",
        "",
        f"Faults detected: {result.overall.detected} of {result.overall.observable} observable; "
        f"{result.false_per_day:.2f} false incidents/day.",
        f"Delay of the detected faults (intervals): median {median}, worst {worst}; "
        f"Kaplan-Meier median (missed censored): {report.delay_text(km)}.",
        "",
        _breakdown(table, "fault type", pl.col("fault_type"), "fault_type"),
        "",
        _breakdown(table, "duration", _bucket("duration", DURATION_BINS, " int"), "duration"),
        "",
        _breakdown(table, "worst bad z", _bucket("worst_bad", DEVIATION_BINS, ""), "worst_bad"),
        "",
        _breakdown(table, "cells", pl.col("cells").cast(pl.String), "cells"),
        "",
        "| fault | type | cells | duration | worst KPI | worst bad z | peak score / threshold |",
        "|---|---|---:|---:|---|---:|---:|",
        *[
            f"| {r['fault_id']} | {r['fault_type']} | {r['cells']} | {r['duration']} | "
            f"{r['worst_kpi']} | {_num(r['worst_bad'])} | {_num(r['peak_ratio'])} |"
            for r in missed.iter_rows(named=True)
        ],
    ]
    return "\n".join(lines) + "\n"


def _num(v: float | None) -> str:
    return "n/a" if v is None else f"{v:.2f}"


def diagnose(data_dir: Path, name: str, study: Run, v: tune.Validation) -> str:
    """Score the validation weeks with the study's tuned model (`tune.score_study`, which
    refuses a stale study); returns the Markdown report and logs it to MLflow."""
    s = tune.score_study(data_dir, study, v)
    rows = pl.concat(
        [
            s.scored.select("cell_id", "interval_start", "score"),
            v.frame.select(features.oriented(s.tuned.detector.kpis)),
        ],
        how="horizontal",
    )
    truth = ground_truth.read(data_dir, splits.Split.VALIDATION)
    table = per_fault(s.result, truth.faults, rows, s.tuned.threshold)
    run_id = study.info.run_id
    md = render(table, s.result, title=f"Missed faults: {name} (study {run_id[:8]})")
    with tracking.start_run(report.EXPERIMENT, run_name=f"misses-{name}"):
        mlflow.set_tags(
            {
                "role": "misses",
                "study": run_id,
                "data_snapshot": v.snapshot,
                "code_version": tracking.code_version(),
                "fault_schedule_fingerprint": s.fault_schedule,
                "synthetic_faults": "yes",
            }
        )
        mlflow.log_text(md, "misses.md")
        mlflow.log_text(table.write_csv(), "fault_diagnosis.csv")
    return md


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        studies = tune.feasible_studies()
        if not studies:
            raise ValueError("no feasible study in `detector-tuning`: run `make train`")
        v = tune.load_validation(args.data_dir)
        for name, study in sorted(studies.items()):
            sys.stdout.write(diagnose(args.data_dir, name, study, v) + "\n")
        process.check_budget("diagnose", detectors.load_config().memory_budget_mib)
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
