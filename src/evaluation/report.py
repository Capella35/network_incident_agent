"""Validation report for a detector (milestone 2, step 5): Markdown table + MLflow artifact.

    python -m evaluation.report --threshold 4 [--start 2013-12-02] [--end 2013-12-15]

Scores validation days with SYNTHETIC faults injected (`streaming.offline.features_for`), using
the z-score baseline at a fixed threshold (nothing to fit; Step 6 tunes every candidate and
calls `evaluation.metrics` directly). Then matches alarms to the ground truth and logs the
report, its metrics, and the per-fault outcomes to the MLflow experiment `detector-evaluation`.
Validation days only (Rule 7).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import mlflow
import polars as pl

from common import splits, telemetry
from evaluation import ground_truth, metrics
from models import detectors, tracking
from producer import data_prep, replay, topology
from streaming import offline

log = logging.getLogger("evaluation.report")

EXPERIMENT = "detector-evaluation"
CANDIDATE = "zscore"


def _rate(s: metrics.Summary) -> str:
    return "n/a" if s.detection_rate is None else f"{s.detection_rate:.1%}"


def _delay(m: metrics.Median) -> str:
    def bound(v: int | None) -> str:
        return "n/a" if v is None else str(v)

    value = "not reached" if m.value is None else str(m.value)
    return f"{value} [{bound(m.low)}, {bound(m.high)}]"


def render(result: metrics.Result, *, title: str) -> str:
    """The report as Markdown: one row per fault type plus the overall row."""
    days = result.days
    lines = [
        f"# {title}",
        "",
        f"Days measured: {days[0]} .. {days[-1]} ({len(days)} days). Faults are SYNTHETIC; "
        f"topology and latency / load / drop KPIs are SYNTHETIC or MODELED.",
        "",
        f"Delay: Kaplan-Meier median in intervals ({telemetry.INTERVAL_MINUTES} min) from onset "
        f"to first matching alarm [{metrics.CONFIDENCE:.0%} CI, n/a: undefined]; missed faults "
        "censored.",
        "",
        "| Fault type | Faults | Unobservable | Detected | Detection rate | Median delay [CI] |",
        "|---|---:|---:|---:|---:|---|",
    ]
    rows = [*result.by_type.items(), ("**overall**", result.overall)]
    lines += [
        f"| {n} | {s.faults} | {s.unobservable} | {s.detected} | {_rate(s)} | {_delay(s.delay)} |"
        for n, s in rows
    ]
    lines += [
        "",
        f"Incidents (provisional M2 grouping): {result.incidents}; false incidents: "
        f"{result.false_incidents} ({result.false_per_day:.2f} per day).",
    ]
    return "\n".join(lines) + "\n"


def _summary_metrics(s: metrics.Summary, suffix: str = "") -> dict[str, float]:
    """Detection rate and KM delay (+ CI bounds) where defined, named `<metric>[.<type>]`."""
    values = {
        "faults": s.faults,
        "detected": s.detected,
        "detection_rate": s.detection_rate,
        "median_delay_intervals": s.delay.value,
        "median_delay_low": s.delay.low,
        "median_delay_high": s.delay.high,
    }
    return {k + suffix: float(v) for k, v in values.items() if v is not None}


def _metrics(result: metrics.Result) -> dict[str, float]:
    out = {
        "incidents": float(result.incidents),
        "false_incidents": float(result.false_incidents),
        "false_incidents_per_day": result.false_per_day,
        **_summary_metrics(result.overall),
    }
    for name, s in result.by_type.items():
        out |= _summary_metrics(s, f".{name}")
    return out


def score(
    data_dir: Path, alarms: pl.DataFrame, *, days: list[date], gaps: pl.DataFrame
) -> tuple[metrics.Result, str]:
    """Score alarms on validation `days` against the ground truth, which is read here and never
    handed out (Rule 6): the entry point for this report and Step 6's tuning. `gaps`: `cell_id`,
    `interval_start` of the days' data-gap rows. Returns the result and the ground truth's
    fingerprint. Raises ValueError for a non-validation day, FileNotFoundError without it."""
    for d in days:
        splits.require(d, splits.Split.VALIDATION)
    truth = ground_truth.read(data_dir, splits.Split.VALIDATION)
    index = topology.load_index(data_dir / "processed")
    return metrics.evaluate(
        alarms, truth.faults, days=days, index=index, gaps=gaps
    ), truth.fingerprint


def run(data_dir: Path, days: list[date], threshold: float) -> str:
    """Score the z-score baseline, evaluate, log to MLflow; returns the Markdown report. Raises
    ValueError for a non-validation day, FileNotFoundError / `replay.ReplayError` for missing
    inputs."""
    for d in days:
        splits.require(d, splits.Split.VALIDATION)
    data_prep.require_processed_days(data_dir, splits.Split.VALIDATION)
    frame = offline.features_for(data_dir, days, inject=True).frame
    cfg = detectors.load_config()
    alarms = detectors.alarms(detectors.build(CANDIDATE, cfg).score(frame), threshold, cfg.alarm)
    gaps = frame.filter("data_gap").select("cell_id", "interval_start")
    del frame  # all days' features; not needed for matching
    result, fingerprint = score(data_dir, alarms, days=days, gaps=gaps)
    report = render(result, title=f"Detection report: {CANDIDATE}, threshold {threshold:g}")
    with tracking.start_run(EXPERIMENT, run_name=f"{CANDIDATE}@{threshold:g}"):
        mlflow.log_params(
            {
                "candidate": CANDIDATE,
                "threshold": threshold,
                "days": f"{days[0]}..{days[-1]} ({len(days)})",
                "ground_truth_fingerprint": fingerprint,
            }
        )
        mlflow.log_metrics(_metrics(result))
        mlflow.log_text(report, "report.md")
        outcomes = pl.DataFrame([asdict(o) for o in result.outcomes]).write_csv()
        mlflow.log_text(outcomes, "fault_outcomes.csv")
        mlflow.set_tags({"synthetic_faults": "yes", "split": str(splits.Split.VALIDATION)})
    return report


def main(argv: list[str] | None = None) -> int:
    validation = splits.days(splits.Split.VALIDATION)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--threshold", type=float, required=True)
    ap.add_argument("--start", type=date.fromisoformat, default=validation[0])
    ap.add_argument("--end", type=date.fromisoformat, default=validation[-1])
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report = run(args.data_dir, replay.days_in_range(args.start, args.end), args.threshold)
    except (
        FileNotFoundError,
        ValueError,
        replay.ReplayError,
        mlflow.exceptions.MlflowException,
    ) as exc:
        log.error("%s", exc)
        return 1
    sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
