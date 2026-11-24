"""Train one detector candidate, score one validation day, log both to MLflow (M2 step 4).

    python -m models.candidates --candidate iforest [--score-day 2013-12-02] [--data-dir data]

Trains on every processed training day (clean, Rule 7) through `streaming.offline.features_for`;
scores one validation day with its SYNTHETIC faults injected. One candidate per process, so the
peak memory is that candidate's: over `memory_budget_mib` (detectors.yaml, guardrail R1) nothing
is logged and the run fails, so an over-budget model can never be promoted. No threshold is
applied: thresholds are tuned in step 6.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import mlflow

from common import splits
from common.process import peak_rss_mib
from models import detectors, tracking
from producer import data_prep, replay
from streaming import features, offline

log = logging.getLogger("models.candidates")


class OverBudgetError(RuntimeError):
    """The candidate's process used more memory than `memory_budget_mib` (R1)."""


def run(data_dir: Path, name: str, score_day: date) -> dict[str, float]:
    """Fit, score, check the memory budget, then log one MLflow run; returns its metrics.
    Raises ValueError on a non-validation `score_day`, OverBudgetError before logging."""
    splits.require(score_day, splits.Split.VALIDATION)
    train_days = list(data_prep.require_processed_days(data_dir, splits.Split.TRAIN))
    cfg = detectors.load_config()
    t0 = time.perf_counter()
    train = offline.features_for(data_dir, train_days).frame
    t1 = time.perf_counter()
    train_rows = train.height
    detector = detectors.build(name, cfg).fit(train)
    del train  # 28 days of features; not needed while the validation day is built and scored
    t2 = time.perf_counter()
    day = offline.features_for(data_dir, [score_day], inject=True)
    scores = detector.score(day.frame)["score"].drop_nulls()
    metrics = {
        "train_rows": float(train_rows),
        "features_seconds": t1 - t0,
        "fit_seconds": t2 - t1,
        "scored_rows": float(scores.len()),
        "score_p50": float(scores.quantile(0.5) or 0.0),
        "score_p99": float(scores.quantile(0.99) or 0.0),
        "score_max": float(scores.max() or 0.0),  # type: ignore[arg-type]
        "peak_rss_mib": peak_rss_mib(),
    }
    if metrics["peak_rss_mib"] > cfg.memory_budget_mib:
        raise OverBudgetError(
            f"{name}: peak memory {metrics['peak_rss_mib']:.0f} MiB > "
            f"{cfg.memory_budget_mib} MiB budget (R1); not logged"
        )
    mlflow.set_experiment(tracking.EXPERIMENT)
    with mlflow.start_run(run_name=name):
        tracking.log_detector(
            detector,
            params={
                "features": features.load_config().model_dump_json(),
                "score_day": str(score_day),
            },
            metrics=metrics,
        )
        mlflow.set_tags(
            {
                "train_days": f"{train_days[0]}..{train_days[-1]} ({len(train_days)})",
                "synthetic_faults_on_score_day": "yes",
            }
        )
    return metrics


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--candidate", required=True, choices=detectors.CANDIDATES)
    ap.add_argument(
        "--score-day", type=date.fromisoformat, default=splits.days(splits.Split.VALIDATION)[0]
    )
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        m = run(args.data_dir, args.candidate, args.score_day)
    except (
        FileNotFoundError,
        ValueError,
        OverBudgetError,
        replay.ReplayError,
        mlflow.exceptions.MlflowException,
    ) as exc:
        log.error("%s", exc)
        return 1
    budget = detectors.load_config().memory_budget_mib
    shown = {k: round(v, 3) for k, v in m.items()}
    log.info("%s on %s: %s (budget %d MiB)", args.candidate, args.score_day, shown, budget)
    return 0


if __name__ == "__main__":
    sys.exit(main())
