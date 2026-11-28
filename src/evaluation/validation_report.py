"""Milestone 2 validation report: the promoted detector vs the z-score baseline (step 11).

    python -m evaluation.validation_report [--data-dir data]
    make validation-report

Both detectors come from MLflow and nothing is fitted or tuned here: the champion
(`models:/detector@champion`, loaded by version) and the baseline, the latest feasible z-score
tuning study at its own tuned threshold (so both sit under the same false-incident budget,
`tune.yaml`). Both score one frame, the validation weeks with SYNTHETIC faults that every trial
was scored on (`tune.load_validation`), so the champion must have been packaged with exactly
those features (`features.Spec.matches`); its alarms are then the job's. Alarms are matched to
the ground truth through `evaluation.report.score` (Rule 6). The report (a comparison table, then
one per-fault-type table per detector), both detectors' metrics, and their per-fault outcomes
go to one run in the MLflow experiment `detector-evaluation` (role `validation-report`).
Validation days only (Rule 7); the honest numbers come once, from the test weeks, at Milestone 6.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import mlflow
from mlflow import MlflowClient
from mlflow.entities import Run

from common import process, splits
from evaluation import metrics, report
from models import cli, detectors, promote, tracking, tune
from producer import injector
from streaming import features

log = logging.getLogger("evaluation.validation_report")

BASELINE = report.CANDIDATE
_STALE_FEATURES = (
    "{model} v{version} was packaged with another features.yaml or reference than the current "
    "one: re-run `make baseline` / `make train` and promote"
)


@dataclass(frozen=True)
class Scored:
    champion_version: str
    champion: metrics.Result
    baseline: metrics.Result
    fault_schedule: str  # the ground truth's fingerprint (the same for both)
    snapshot: str  # data_prep.snapshot of the days scored


def _refuse_stale(champion: tracking.Champion, now: promote.Numbers) -> None:
    """ValueError if the champion was promoted on other days, ground truth, or data than
    `now` was scored on (its numbers and threshold were judged on those)."""
    registered = MlflowClient().get_model_version(tracking.MODEL, champion.version)
    gated = promote.Numbers.from_tags(registered.tags)
    if gated.scored_on != now.scored_on:
        raise ValueError(
            f"{tracking.MODEL} v{champion.version} was promoted on {gated.scored_on}, the "
            f"validation data is now {now.scored_on}: promote again (make train, make promote)"
        )


def _champion_result(
    data_dir: Path, champion: tracking.Champion, v: tune.Validation
) -> metrics.Result:
    """The champion scored on `v`, after the checks `score` names."""
    if not champion.package.spec.matches(features.Spec(features.load_config(), v.reference)):
        raise ValueError(_STALE_FEATURES.format(model=tracking.MODEL, version=champion.version))
    scored = tune.score_tuned(data_dir, champion.package.tuned, v)
    now = promote.Numbers.of(
        scored.result, fault_schedule=scored.fault_schedule, snapshot=v.snapshot
    )
    _refuse_stale(champion, now)
    return scored.result


def score(data_dir: Path, baseline: Run) -> Scored:
    """Score the champion and `baseline` on the validation weeks. Raises ValueError if the
    champion was packaged with other features than the current ones, or promoted on other
    validation data (`_refuse_stale`); `tune.score_study` refuses a baseline tuned under another
    features.yaml or fault schedule (not a refitted reference: open item)."""
    # A one-off load, no copy worth keeping; the folder outlives scoring because the loaded
    # model's bundled code (`code_paths`) is imported from it.
    with tempfile.TemporaryDirectory() as cache:
        champion = tracking.load_champion(MlflowClient(), cache=Path(cache))
        version = champion.version
        if champion.package.spec.config != features.load_config():  # before any data is read
            raise ValueError(_STALE_FEATURES.format(model=tracking.MODEL, version=version))
        v = tune.load_validation(data_dir)
        promoted = _champion_result(data_dir, champion, v)
        del champion  # its model and reference: not needed while the baseline scores
        base = tune.score_study(data_dir, baseline, v)
    return Scored(version, promoted, base.result, base.fault_schedule, v.snapshot)


def render(results: dict[str, metrics.Result], provenance: dict[str, str]) -> str:
    """The report: provenance, the comparison table, then each detector's per-type table."""
    head = [
        "# Milestone 2 validation report: promoted detector vs z-score baseline",
        "",
        *(f"- {k}: {v}" for k, v in provenance.items()),
        "",
        report.comparison_table(results),
    ]
    per_type = [report.render(r, title=f"Per fault type: {name}") for name, r in results.items()]
    return "\n".join([*head, *per_type])


def run(data_dir: Path) -> str:
    """Score both detectors, log the report to MLflow, return it. Raises ValueError if there is
    no champion or no feasible z-score study, or as `score` for a stale one."""
    baseline = tune.feasible_studies().get(BASELINE)
    if baseline is None:
        raise ValueError(f"{BASELINE}: no feasible tuning study for the baseline; `make train`")
    tune.require_current(baseline)
    s = score(data_dir, baseline)
    results = {
        f"{tracking.MODEL} v{s.champion_version} (champion)": s.champion,
        f"{BASELINE} (baseline, study {baseline.info.run_id[:8]})": s.baseline,
    }
    dcfg = detectors.load_config()
    md = render(
        results,
        {
            "Split": "validation (tuning used these days: optimistic until Milestone 6)",
            "Days": f"{s.champion.days[0]} .. {s.champion.days[-1]} ({len(s.champion.days)})",
            "Fault schedule": f"SYNTHETIC, fingerprint {s.fault_schedule[:8]}, "
            f"injector seed {injector.load_config().seeds[splits.Split.VALIDATION]} "
            "(current config)",
            "Detector seed": f"{dcfg.seed} (current config)",
            "Champion": f"{tracking.MODEL} v{s.champion_version}",
            "Baseline": f"{BASELINE} tuning study {baseline.info.run_id}",
            "Code": tracking.code_version(),
        },
    )
    peak = process.check_budget("validation-report", dcfg.memory_budget_mib)
    with tracking.start_run(report.EXPERIMENT, run_name="validation-report"):
        mlflow.set_tags(
            {
                **tracking.validation_tags(s.snapshot, s.fault_schedule),
                "role": "validation-report",
                "champion_version": s.champion_version,
                "baseline_study": baseline.info.run_id,
            }
        )
        mlflow.log_metrics(
            {
                **report.mlflow_metrics(s.champion),
                **{f"baseline.{k}": x for k, x in report.mlflow_metrics(s.baseline).items()},
                "peak_rss_mib": peak,
            }
        )
        mlflow.log_text(md, "report.md")
        mlflow.log_text(report.outcomes_csv(s.champion), "champion_fault_outcomes.csv")
        mlflow.log_text(report.outcomes_csv(s.baseline), "baseline_fault_outcomes.csv")
    return md


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        md = run(args.data_dir)
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    sys.stdout.write(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
