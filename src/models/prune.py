"""Prune old MLflow runs (milestone 2, step 10; guardrail R3, `make clean`).

    python -m models.prune [--dry-run]        # make clean runs it, then `mlflow gc` on the server

Per experiment, top-level runs are grouped by their `role` and `candidate` tags (a study, a
candidate fit, a promotion decision...); each group keeps its newest `keep_per_group` finished
runs (`retention.yaml`) and every run still going. A run is never deleted if a registered model
version came from it (the champion and the versions a rollback can return to) or if
`evals/results-log.md` cites it (every reported number traces to its MLflow run). Nested runs
(Optuna trials) go and stay with their parent. Deleted runs are only marked deleted here;
`mlflow gc` on the server then removes them and their artifacts for good.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from pathlib import Path

from mlflow import MlflowClient
from mlflow.entities import Run, RunStatus
from pydantic import PositiveInt

from common.domain.loader import FrozenModel, read_config
from models import cli

log = logging.getLogger("models.prune")

PARENT_TAG = "mlflow.parentRunId"
RESULTS_LOG = Path("evals/results-log.md")
_CITED = re.compile(r"\b[0-9a-f]{8,32}\b")  # run IDs or their 8-character prefixes


class Retention(FrozenModel):
    keep_per_group: PositiveInt


@cache
def load_retention() -> Retention:
    return read_config(Retention, files("models").joinpath("retention.yaml"))


@dataclass(frozen=True)
class RunRef:
    """What pruning needs to know of a run."""

    run_id: str
    experiment_id: str
    parent: str | None
    group: tuple[str, str]  # (role, candidate) tags; "" if missing
    status: str  # mlflow RunStatus name: FINISHED, RUNNING, FAILED...
    start_time: int

    @classmethod
    def of(cls, run: Run) -> RunRef:
        tags = run.data.tags
        return cls(
            run.info.run_id,
            run.info.experiment_id,
            tags.get(PARENT_TAG),
            (tags.get("role", ""), tags.get("candidate", "")),
            run.info.status,
            run.info.start_time or 0,
        )


def cited(text: str) -> set[str]:
    """Hex tokens of 8-32 characters in `text`: run IDs or prefixes a report cites."""
    return set(_CITED.findall(text))


_FINISHED = RunStatus.to_string(RunStatus.FINISHED)
_RUNNING = RunStatus.to_string(RunStatus.RUNNING)


def to_delete(runs: Sequence[RunRef], *, keep: int, protected: set[str]) -> list[str]:
    """IDs of the runs to delete (see the module docstring), parents before their children.
    `protected`: run IDs or prefixes never deleted (registered versions' runs, cited runs)."""
    children: dict[str, list[RunRef]] = defaultdict(list)
    for r in runs:
        if r.parent:
            children[r.parent].append(r)
    groups: dict[tuple[str, tuple[str, str]], list[RunRef]] = defaultdict(list)
    for r in runs:
        if not r.parent:
            groups[(r.experiment_id, r.group)].append(r)
    out: list[str] = []
    for members in groups.values():
        finished = 0
        for r in sorted(members, key=lambda m: m.start_time, reverse=True):
            family = [r, *children.get(r.run_id, [])]
            finished += r.status == _FINISHED
            newest = r.status == _FINISHED and finished <= keep
            if (
                newest
                or r.status == _RUNNING
                or any(m.run_id.startswith(p) for m in family for p in protected)
            ):
                continue
            out += [m.run_id for m in family]
    return out


def _all_runs(client: MlflowClient) -> Iterable[Run]:
    experiment_ids = [e.experiment_id for e in client.search_experiments()]
    token = None
    while True:
        page = client.search_runs(experiment_ids, max_results=1000, page_token=token)
        yield from page
        token = page.token
        if not token:
            return


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="list what would be deleted")
    ap.add_argument("--results-log", type=Path, default=RESULTS_LOG)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    client = MlflowClient()
    try:
        runs = [RunRef.of(r) for r in _all_runs(client)]
        registered = {v.run_id for v in client.search_model_versions() if v.run_id}
        protected = registered | cited(args.results_log.read_text(encoding="utf-8"))
        doomed = to_delete(runs, keep=load_retention().keep_per_group, protected=protected)
        log.info(
            "%d runs; %s %d (%d protected IDs or prefixes)",
            len(runs),
            "would delete" if args.dry_run else "deleting",
            len(doomed),
            len(protected),
        )
        for run_id in doomed:
            if args.dry_run:
                print(run_id)
            else:
                client.delete_run(run_id)
    except cli.RUN_ERRORS as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
