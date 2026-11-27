"""Optuna tuning of the detector candidates (milestone 2, step 6; specs/03-detection.md).

    python -m models.tune --candidate iforest [--data-dir data]   # one study (make train: all)
    python -m models.tune --compare                               # table of the latest studies

One study per candidate, each in its own process so the memory budget (R1) is that study's.
Detectors fit on the clean training weeks; trials are scored on the validation weeks with
SYNTHETIC faults injected (Rule 7). A trial sets the candidate's parameters (`tune.yaml` search)
and the alarm threshold, then `evaluation.report.score` returns its numbers (the ground truth
never leaves `evaluation`, Rule 6). Objective: most faults detected, then the lowest
Kaplan-Meier median delay; a trial over `max_false_incidents_per_day` is infeasible (an Optuna
constraint) and never wins. MLflow: one parent run per study, one nested run per trial; the
best feasible trial's model is logged on the parent for promotion (step 7). Feature settings
are not tuned (DECISIONS.md 2026-11-25).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import mlflow
import optuna
import polars as pl
from mlflow.entities import Run
from pydantic import Field, PositiveInt, model_validator

from common import process, splits
from common.domain.loader import FrozenModel, read_config
from evaluation import metrics, report
from models import cli, detectors, tracking
from producer import data_prep
from streaming import features, offline

log = logging.getLogger("models.tune")

EXPERIMENT = "detector-tuning"
# A "not reached" median ranks as this delay: the worst (DECISIONS.md 2026-11-25).
NOT_REACHED = 10_000  # intervals


class IntRange(FrozenModel):
    low: PositiveInt
    high: PositiveInt
    log: bool = False

    @model_validator(mode="after")
    def _ordered(self) -> IntRange:
        if self.low > self.high:
            raise ValueError(f"low {self.low} > high {self.high}")
        return self


class Budget(FrozenModel):
    n_trials: PositiveInt
    timeout_s: PositiveInt


# Sampled once per study (`load_study_data`), so a trial must not change it.
_FIXED_PER_STUDY = "max_train_rows"


class Config(FrozenModel):
    budget: Budget
    max_false_incidents_per_day: float = Field(gt=0)
    alarm_share: tuple[float, float]
    search: dict[str, dict[str, IntRange]]

    @model_validator(mode="after")
    def _valid(self) -> Config:
        lo, hi = self.alarm_share
        if not 0 < lo <= hi < 1:
            raise ValueError(f"alarm_share: need 0 < low <= high < 1, got {(lo, hi)}")
        if set(self.search) != set(detectors.CANDIDATES):
            raise ValueError(f"search: one entry per candidate {list(detectors.CANDIDATES)}")
        for name, ranges in self.search.items():
            params = cast(
                type[FrozenModel], detectors.CandidateParams.model_fields[name].annotation
            )
            tunable = set(params.model_fields) - {_FIXED_PER_STUDY}
            if unknown := set(ranges) - tunable:
                raise ValueError(f"search.{name}: not tunable parameters: {sorted(unknown)}")
        return self


@cache
def load_config() -> Config:
    return read_config(Config, files("models").joinpath("tune.yaml"))


def objective_value(result: metrics.Result) -> float:
    """Lexicographic in one number: faults detected (an integer) plus a fraction below 1 that
    grows as the Kaplan-Meier median delay shrinks; "not reached" counts as `NOT_REACHED`."""
    delay = result.overall.delay.value
    d = NOT_REACHED if delay is None else min(delay, NOT_REACHED)
    return result.overall.detected + (NOT_REACHED - d) / (NOT_REACHED + 1)


@dataclass(frozen=True)
class StudyData:
    train: pl.DataFrame  # the candidate's training rows (detectors.training_rows), clean weeks
    validation: pl.DataFrame  # features, validation weeks, SYNTHETIC faults injected
    gaps: pl.DataFrame  # report.gaps(validation)
    days: list[date]  # validation days
    snapshot: str  # data_prep.snapshot of every processed day used


@dataclass(frozen=True)
class Validation:
    frame: pl.DataFrame  # features, validation weeks, SYNTHETIC faults injected
    gaps: pl.DataFrame  # report.gaps(frame)
    days: list[date]  # validation days
    train_days: list[date]
    snapshot: str  # data_prep.snapshot of every processed day used
    reference: features.Reference  # the one `frame` was computed against


def load_validation(data_dir: Path) -> Validation:
    """The validation-week features every trial (and `score_study`) is scored on."""
    train_days = list(data_prep.require_processed_days(data_dir, splits.Split.TRAIN))
    days = list(data_prep.require_processed_days(data_dir, splits.Split.VALIDATION))
    out = offline.features_for(data_dir, days, inject=True)
    snapshot = data_prep.snapshot(data_dir, [*train_days, *days])
    return Validation(out.frame, report.gaps(out.frame), days, train_days, snapshot, out.reference)


def load_study_data(data_dir: Path, name: str) -> StudyData:
    """Features once per study. Only the candidate's training sample is kept (the same rows every
    fit would sample); the z-score, which fits nothing, gets no training rows."""
    dcfg = detectors.load_config()
    v = load_validation(data_dir)
    max_rows: int | None = getattr(getattr(dcfg.candidates, name), _FIXED_PER_STUDY, None)
    train = (
        v.frame.head(0)
        if max_rows is None
        else detectors.training_rows(
            offline.features_for(data_dir, v.train_days).frame, max_rows, dcfg.seed
        )
    )
    return StudyData(train, v.frame, v.gaps, v.days, v.snapshot)


@dataclass(frozen=True)
class Best:
    """The best feasible trial so far, kept fitted so it is logged without refitting."""

    value: float
    trial: int
    detector: detectors.Detector
    threshold: float
    params: dict[str, float]
    result: metrics.Result


class Objective:
    """One Optuna trial: fit with the trial's parameters (reusing the last fit when they repeat,
    e.g. every z-score trial), threshold at a validation-score quantile, score, log a nested
    MLflow run with the study's provenance tags, remember the best feasible trial."""

    def __init__(self, name: str, data: StudyData, data_dir: Path, tags: dict[str, str]) -> None:
        self.name, self.data, self.data_dir, self.tags = name, data, data_dir, tags
        self.cfg, self.dcfg = load_config(), detectors.load_config()
        self.best: Best | None = None
        self._last: tuple[dict[str, int], detectors.Detector, pl.DataFrame] | None = None
        self.fault_schedule = ""  # fingerprint of the fault schedule the trials were scored on

    def _fit_and_score(self, params: dict[str, int]) -> tuple[detectors.Detector, pl.DataFrame]:
        if self._last is None or self._last[0] != params:
            self._last = None  # free the previous scored frame before building the next
            detector = detectors.build(self.name, self.dcfg, params).fit(self.data.train)
            self._last = (params, detector, detector.score(self.data.validation))
        return self._last[1], self._last[2]

    def __call__(self, trial: optuna.Trial) -> float:
        ranges = self.cfg.search[self.name]
        params = {p: trial.suggest_int(p, r.low, r.high, log=r.log) for p, r in ranges.items()}
        share = trial.suggest_float("alarm_share", *self.cfg.alarm_share, log=True)
        detector, scored = self._fit_and_score(params)
        threshold = scored["score"].drop_nulls().quantile(1 - share)
        if threshold is None:
            raise ValueError("no validation rows scored: cannot set a threshold")
        alarms = detectors.alarms(scored, threshold, self.dcfg.alarm)
        result, self.fault_schedule = report.score(
            self.data_dir, alarms, days=self.data.days, gaps=self.data.gaps
        )
        value = objective_value(result)
        excess = result.false_per_day - self.cfg.max_false_incidents_per_day
        trial.set_constraint("false_incidents_per_day", excess)  # feasible iff <= 0 (Optuna)
        logged: dict[str, Any] = {**params, "alarm_share": share, "threshold": threshold}
        with tracking.start_run(EXPERIMENT, f"{self.name}-{trial.number}", nested=True):
            mlflow.set_tags(
                {**self.tags, "role": "trial", "fault_schedule_fingerprint": self.fault_schedule}
            )
            mlflow.log_params(logged)
            mlflow.log_metrics(
                {
                    **report.mlflow_metrics(result),
                    "objective": value,
                    "feasible": float(excess <= 0),
                }
            )
        if excess <= 0 and (self.best is None or value > self.best.value):
            self.best = Best(value, trial.number, detector, threshold, logged, result)
        return value


def run_study(data_dir: Path, name: str) -> Best | None:
    """Tune one candidate within the budget and log it; returns the best feasible trial (None if
    no trial met the false-incident budget). Raises `process.OverBudgetError` (R1) before the
    model is logged."""
    data = load_study_data(data_dir, name)
    tags = {
        **tracking.validation_tags(data.snapshot),
        "candidate": name,
        "days": f"{data.days[0]}..{data.days[-1]} ({len(data.days)})",
    }
    objective = Objective(name, data, data_dir, tags)
    cfg, dcfg = objective.cfg, objective.dcfg
    sampler = optuna.samplers.TPESampler(seed=dcfg.seed)
    study = optuna.create_study(direction="maximize", sampler=sampler, study_name=name)
    with tracking.start_run(EXPERIMENT, run_name=name):
        mlflow.set_tags({**tags, "role": "study"})
        mlflow.log_params(
            {
                "tune": cfg.model_dump_json(),
                "features": features.load_config().model_dump_json(),
            }
        )
        t0 = time.perf_counter()
        study.optimize(objective, n_trials=cfg.budget.n_trials, timeout=cfg.budget.timeout_s)
        seconds = time.perf_counter() - t0
        peak = process.check_budget(name, dcfg.memory_budget_mib)
        mlflow.log_metrics({"trials": len(study.trials), "seconds": seconds, "peak_rss_mib": peak})
        best = objective.best
        mlflow.set_tags(
            {
                "feasible": "yes" if best else "no",
                "fault_schedule_fingerprint": objective.fault_schedule,
            }
        )
        if best is not None:
            tracking.log_detector(
                best.detector,
                params={**best.params, "best_trial": best.trial},
                metrics={**report.mlflow_metrics(best.result), "objective": best.value},
                threshold=best.threshold,
            )
    return best


def latest_studies() -> dict[str, Run]:
    """The latest finished study run per candidate (what the comparison table and the promotion
    gate read)."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT],
        filter_string="tags.role = 'study' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        output_format="list",
    )
    latest: dict[str, Run] = {}
    for run in runs:
        latest.setdefault(run.data.tags["candidate"], run)
    return latest


def feasible_studies() -> dict[str, Run]:
    """`latest_studies()` that logged a model (met the false-incident budget)."""
    return {n: r for n, r in latest_studies().items() if r.data.tags.get("feasible") == "yes"}


@dataclass(frozen=True)
class ScoredStudy:
    tuned: tracking.Tuned
    scored: pl.DataFrame  # `Detector.score` of the validation features
    alarms: pl.DataFrame  # `detectors.alarms` at the tuned threshold
    result: metrics.Result
    fault_schedule: str  # fingerprint of the ground truth it was scored against


def require_current(study: Run) -> None:
    """ValueError if `study` was tuned under another `features.yaml` (cheap: no data read)."""
    if study.data.params.get("features") != features.load_config().model_dump_json():
        raise ValueError(
            f"study {study.info.run_id[:8]}: tuned under another features.yaml; `make train`"
        )


def score_study(data_dir: Path, study: Run, v: Validation) -> ScoredStudy:
    """Score the validation weeks with a study's tuned model, as its trials were scored. Raises
    ValueError if the study was tuned under another `features.yaml` or fault schedule (its
    threshold would mean something else): re-run `make train`."""
    run_id = study.info.run_id
    require_current(study)
    tuned = tracking.load_tuned(run_id)
    scored = tuned.detector.score(v.frame)
    alarms = detectors.alarms(scored, tuned.threshold, tuned.alarm)
    result, fingerprint = report.score(data_dir, alarms, days=v.days, gaps=v.gaps)
    if study.data.tags.get("fault_schedule_fingerprint") != fingerprint:
        raise ValueError(f"study {run_id[:8]}: tuned on another fault schedule; `make train`")
    return ScoredStudy(tuned, scored, alarms, result, fingerprint)


def _cell(m: dict[str, float], key: str, fmt: str = "{:.0f}") -> str:
    return fmt.format(m[key]) if key in m else "n/a"


def compare() -> str:
    """Markdown table of `latest_studies()`, also logged to MLflow as `comparison.md`. Raises
    ValueError if no study was logged."""
    latest = latest_studies()
    if not latest:
        raise ValueError(f"no finished studies in MLflow experiment {EXPERIMENT!r}: make train")
    lines = [
        "| Candidate | Feasible | Detection rate | KM median delay [CI] | False incidents/day "
        "| Trials | Run |",
        "|---|---|---:|---|---:|---:|---|",
    ]
    for name in detectors.CANDIDATES:
        if (r := latest.get(name)) is None:
            continue
        m = r.data.metrics
        delay = (
            f"{_cell(m, 'median_delay_intervals')} "
            f"[{_cell(m, 'median_delay_low')}, {_cell(m, 'median_delay_high')}]"
        )
        lines.append(
            f"| {name} | {r.data.tags.get('feasible', 'n/a')} "
            f"| {_cell(m, 'detection_rate', '{:.1%}')} | {delay} "
            f"| {_cell(m, 'false_incidents_per_day', '{:.2f}')} | {_cell(m, 'trials')} "
            f"| {r.info.run_id[:8]} |"
        )
    table = "\n".join(lines) + "\n"
    with tracking.start_run(EXPERIMENT, run_name="comparison"):
        mlflow.set_tag("role", "comparison")
        mlflow.log_text(table, "comparison.md")
    return table


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--candidate", choices=detectors.CANDIDATES)
    what.add_argument("--compare", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    try:
        if args.compare:
            sys.stdout.write(compare())
            return 0
        best = run_study(args.data_dir, args.candidate)
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    if best is None:
        log.warning("%s: no trial met the false-incident budget; no model logged", args.candidate)
    else:
        r = best.result
        log.info(
            "%s: best trial %d, detected %d/%d, %.2f false incidents/day",
            args.candidate,
            best.trial,
            r.overall.detected,
            r.overall.observable,
            r.false_per_day,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
