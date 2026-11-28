"""Promotion gate and rollback for the detector (milestone 2, step 7; specs/03-detection.md).

    python -m models.promote --candidate autoencoder [--data-dir data]   # gate, then promote
    python -m models.promote --rollback                                  # previous champion

The candidate is named, never chosen here (DECISIONS.md 2026-11-27): its latest feasible tuning
study is re-scored on the validation weeks (SYNTHETIC faults; `tune.score_study`, which refuses a
study tuned under another `features.yaml` or fault schedule) and judged by `gate` against the
incumbent, the version the registered model `MODEL` alias `ALIAS` points to. Promoted, it is
registered as one package: fitted model, alarm threshold and settings, feature config, and the
fitted reference (`tracking.Package`), with its validation numbers as version tags; the alias
moves to it and the new version remembers the one it replaced, which `--rollback` restores.
Every decision, refused or not, and every rollback is logged to MLflow (`EXPERIMENT`).
Validation only: the test weeks are touched once, at M6 (Rule 7).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import mlflow
from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion

from common import process
from evaluation import metrics, report
from models import cli, detectors, tracking, tune
from models.tracking import ALIAS, MODEL, champion
from streaming import features

log = logging.getLogger("models.promote")

EXPERIMENT = "detector-promotion"
PREVIOUS = "previous_champion"  # version tag: the version this one replaced
ROLLED_BACK = "rolled_back"  # version tag: `--rollback` moved the alias off it
REFUSED = 2  # exit code when the gate refuses (1: the run failed)
TAG = "validation."  # prefix of the version tags holding `Numbers`


@dataclass(frozen=True)
class Numbers:
    """What the gate judges, from one validation scoring."""

    detected: int
    observable: int
    false_per_day: float
    km_median: int | None  # intervals; None = not reached
    days: str  # first..last validation day
    fault_schedule: str  # ground-truth fingerprint
    snapshot: str  # data_prep.snapshot of the processed days scored

    @property
    def scored_on(self) -> tuple[str, str, str]:
        """What must match for two `Numbers` to be compared."""
        return self.days, self.fault_schedule, self.snapshot

    @classmethod
    def of(cls, result: metrics.Result, *, fault_schedule: str, snapshot: str) -> Numbers:
        o = result.overall
        return cls(
            detected=o.detected,
            observable=o.observable,
            false_per_day=result.false_per_day,
            km_median=o.delay.value,
            days=f"{result.days[0]}..{result.days[-1]}",
            fault_schedule=fault_schedule,
            snapshot=snapshot,
        )

    def tags(self) -> dict[str, str]:
        return {f"{TAG}{k}": "" if v is None else str(v) for k, v in asdict(self).items()}

    @classmethod
    def from_tags(cls, tags: dict[str, str]) -> Numbers:
        t = {k.removeprefix(TAG): v for k, v in tags.items() if k.startswith(TAG)}
        try:
            median = t["km_median"]
            return cls(
                detected=int(t["detected"]),
                observable=int(t["observable"]),
                false_per_day=float(t["false_per_day"]),
                km_median=int(median) if median else None,
                days=t["days"],
                fault_schedule=t["fault_schedule"],
                snapshot=t["snapshot"],
            )
        except KeyError as exc:
            raise ValueError(f"incumbent has no validation tag {exc}") from None


@dataclass(frozen=True)
class Verdict:
    promote: bool
    reason: str


def gate(candidate: Numbers, incumbent: Numbers | None, max_false_per_day: float) -> Verdict:
    """Promotable only if at or under `max_false_per_day` AND it catches more faults than the
    incumbent, both scored on the same validation days, fault schedule, and data snapshot. With no
    incumbent, the cap alone decides (the first champion). The incumbent's numbers are the ones
    tagged at its promotion: if the validation set changes, it must be re-scored first (not built:
    the validation set has not changed since step 5)."""
    if candidate.false_per_day > max_false_per_day:
        return Verdict(
            False, f"{candidate.false_per_day:.2f} false incidents/day > cap {max_false_per_day:g}"
        )
    if incumbent is None:
        return Verdict(True, "first champion: within the false-incident cap")
    if candidate.scored_on != incumbent.scored_on:
        return Verdict(
            False, "incumbent was judged on other validation days or faults: not comparable"
        )
    if candidate.detected <= incumbent.detected:
        return Verdict(
            False,
            f"catches {candidate.detected}, not more than the incumbent's {incumbent.detected}",
        )
    return Verdict(
        True, f"catches {candidate.detected} > incumbent's {incumbent.detected}, within the cap"
    )


def _row(name: str, n: Numbers | None) -> str:
    if n is None:
        return f"| {name} | none | | | |"
    km = "not reached" if n.km_median is None else str(n.km_median)
    return f"| {name} | {n.detected}/{n.observable} | {n.false_per_day:.2f} | {km} | {n.days} |"


def render(candidate: str, cand: Numbers, incumbent: Numbers | None, verdict: Verdict) -> str:
    return "\n".join(
        [
            f"## Promotion gate: {candidate} (validation weeks, SYNTHETIC faults)",
            "",
            "| | Faults caught | False incidents/day | KM median delay | Days |",
            "|---|---:|---:|---|---|",
            _row(candidate, cand),
            _row("incumbent", incumbent),
            "",
            f"**{'Promoted' if verdict.promote else 'Not promoted'}:** {verdict.reason}.",
            "",
        ]
    )


@dataclass(frozen=True)
class Decision:
    report: str  # Markdown
    promoted: bool


def promote(data_dir: Path, candidate: str) -> Decision:
    """Run the gate for `candidate`'s latest feasible study; register and alias it if it passes.
    Raises ValueError, before any data is read, if the candidate has no feasible study, it is
    stale, it is the champion's or a rolled-back version's, or the champion has no numbers."""
    study = tune.feasible_studies().get(candidate)
    if study is None:
        raise ValueError(f"{candidate}: no feasible tuning study; run `make train`")
    tune.require_current(study)
    client = MlflowClient()
    current = champion(client)
    _refuse_known(client, current, study.info.run_id)
    incumbent = None if current is None else Numbers.from_tags(current.tags)
    v = tune.load_validation(data_dir)
    scored = tune.score_study(data_dir, study, v)
    spec = features.Spec(features.load_config(), v.reference)  # exactly what was scored
    tuned, result = scored.tuned, scored.result
    cand = Numbers.of(result, fault_schedule=scored.fault_schedule, snapshot=v.snapshot)
    del v, scored  # the validation features and scores: not needed to log or register
    budget = detectors.load_config().memory_budget_mib
    peak = process.check_budget("promote", budget)
    verdict = gate(cand, incumbent, tune.load_config().max_false_incidents_per_day)
    md = render(candidate, cand, incumbent, verdict)
    with tracking.start_run(EXPERIMENT, run_name=candidate):
        mlflow.set_tags(
            {
                **tracking.validation_tags(cand.snapshot, cand.fault_schedule),
                "role": "promotion",
                "candidate": candidate,
                "study": study.info.run_id,
                "incumbent_version": "" if current is None else str(current.version),
            }
        )
        mlflow.log_metrics({**report.mlflow_metrics(result), "peak_rss_mib": peak})
        mlflow.log_text(md, "promotion.md")
        if verdict.promote:
            uri = tracking.log_package(tuned, spec)
            process.check_budget("promote", budget)  # pickling the reference is the last peak
            tags = {**cand.tags(), "candidate": candidate, "study": study.info.run_id}
            if current is not None:
                tags[PREVIOUS] = str(current.version)
            mlflow.set_tag("registered_version", _register(client, uri, tags))
        # last: a failure above leaves the run FAILED, never claiming a decision it did not make
        mlflow.set_tag("verdict", "promoted" if verdict.promote else "refused")
    return Decision(md, verdict.promote)


def _refuse_known(client: MlflowClient, current: ModelVersion | None, study_id: str) -> None:
    """ValueError if `study_id` is the champion's, or a rolled-back version's (a rollback must
    not be undone by the next `make promote`)."""
    if current is not None and current.tags.get("study") == study_id:
        raise ValueError(f"study {study_id[:8]} is already {ALIAS} (version {current.version})")
    for version in client.search_model_versions(f"name='{MODEL}'"):
        if version.tags.get("study") == study_id and version.tags.get(ROLLED_BACK):
            raise ValueError(
                f"study {study_id[:8]} was rolled back (version {version.version}); tune again"
            )


def _register(client: MlflowClient, uri: str, tags: dict[str, str]) -> str:
    """Register the logged model `uri` (the registered model is created on first use) and point
    `ALIAS` at the new version."""
    version = mlflow.register_model(uri, MODEL, tags=tags)
    client.set_registered_model_alias(MODEL, ALIAS, version.version)
    log.info("%s version %s is now %s", MODEL, version.version, ALIAS)
    return str(version.version)


def rollback() -> str:
    """Move `ALIAS` back to the version the current champion replaced. Raises ValueError if
    there is no champion, or it replaced none."""
    client = MlflowClient()
    current = champion(client)
    if current is None:
        raise ValueError(f"{MODEL}: no {ALIAS} to roll back")
    previous = current.tags.get(PREVIOUS)
    if not previous:
        raise ValueError(f"{MODEL} version {current.version} replaced no earlier {ALIAS}")
    md = f"{MODEL}: {ALIAS} moved back from version {current.version} to {previous}.\n"
    with tracking.start_run(EXPERIMENT, run_name="rollback"):  # aliases keep no history
        mlflow.set_tags(
            {"role": "rollback", "from_version": str(current.version), "to_version": previous}
        )
        client.set_model_version_tag(MODEL, str(current.version), ROLLED_BACK, "yes")
        client.set_registered_model_alias(MODEL, ALIAS, previous)
        mlflow.log_text(md, "rollback.md")
    return md


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--candidate", choices=tune.load_config().search)
    what.add_argument("--rollback", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.rollback:
            sys.stdout.write(rollback())
            return 0
        decision = promote(args.data_dir, args.candidate)
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    sys.stdout.write(decision.report)
    return 0 if decision.promoted else REFUSED


if __name__ == "__main__":
    sys.exit(main())
