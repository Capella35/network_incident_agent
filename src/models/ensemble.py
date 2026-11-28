"""Union of the tuned detectors (milestone 2, step 6c; specs/03-detection.md).

    python -m models.ensemble [--data-dir data]   # each feasible candidate of the last `make train`

A cell-interval alarms if any member's score reaches that member's own tuned threshold, taken
from its latest `detector-tuning` study: nothing is re-tuned. The union alarm's score is the
highest member score / threshold (>= 1 on every alarm), and its KPIs come from that member.
Scored like tuning: validation weeks with SYNTHETIC faults, through `evaluation.report.score`
(Rule 6). Each member alone and the union are measured in one run (the ablation) and logged to
MLflow `detector-tuning` (role `ensemble`).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import mlflow
import polars as pl

from common import process
from evaluation import metrics, report
from models import cli, detectors, tracking, tune

log = logging.getLogger("models.ensemble")

MIN_MEMBERS = 2  # a union of one detector is that detector


def union(members: Sequence[tuple[pl.DataFrame, float]]) -> pl.DataFrame:
    """`members`: (`detectors.alarms` rows, the threshold they were cut at). One row per alarmed
    (cell, interval), shaped like `detectors.alarms`, from the member whose score / threshold is
    highest (that ratio is the row's `score`; ties: the earlier member). Thresholds must be > 0,
    or the ratios would not rank the members."""
    if any(threshold <= 0 for _, threshold in members):
        raise ValueError("union: every member threshold must be > 0")
    parts = [alarms.with_columns(pl.col("score") / threshold) for alarms, threshold in members]
    return (
        pl.concat(parts)
        .sort("score", descending=True, maintain_order=True)
        .unique(["cell_id", "interval_start"], keep="first", maintain_order=True)
        .sort("interval_start", "cell_id")
    )


def run(data_dir: Path) -> str:
    """Measure every feasible member and their union on the validation weeks; log to MLflow.
    Raises ValueError with fewer than two feasible members."""
    studies = tune.feasible_studies()
    if len(studies) < MIN_MEMBERS:
        raise ValueError(f"need >= {MIN_MEMBERS} feasible studies for a union: {sorted(studies)}")
    t0 = time.perf_counter()
    v = tune.load_validation(data_dir)
    members: list[tuple[pl.DataFrame, float]] = []
    results: dict[str, metrics.Result] = {}
    fingerprint = ""
    for name, study in sorted(studies.items()):
        s = tune.score_study(data_dir, study, v)
        results[name], fingerprint = s.result, s.fault_schedule
        members.append((s.alarms, s.tuned.threshold))
    name = "union(" + "+".join(sorted(studies)) + ")"
    results[name], _ = report.score(data_dir, union(members), days=v.days, gaps=v.gaps)
    seconds = time.perf_counter() - t0
    peak = process.check_budget("ensemble", detectors.load_config().memory_budget_mib)
    md = report.comparison_table(results)
    with tracking.start_run(tune.EXPERIMENT, run_name=name):
        mlflow.set_tags(
            {
                **tracking.validation_tags(v.snapshot, fingerprint),
                "role": "ensemble",
                "members": ",".join(f"{n}:{s.info.run_id}" for n, s in sorted(studies.items())),
            }
        )
        mlflow.log_metrics(
            {**report.mlflow_metrics(results[name]), "seconds": seconds, "peak_rss_mib": peak}
        )
        mlflow.log_text(md, "ablation.md")
    log.info("%s: %.0f s, peak memory %.0f MiB", name, seconds, peak)
    return md


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        sys.stdout.write(run(args.data_dir))
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
