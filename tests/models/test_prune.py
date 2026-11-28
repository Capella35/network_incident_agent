"""MLflow run retention (milestone 2, step 10): what `make clean` deletes, without a server for
the rule and with a throwaway file store for the client calls."""

from __future__ import annotations

from pathlib import Path

import pytest
from mlflow import MlflowClient

from models import prune
from models.prune import RunRef


def ref(
    run_id: str,
    t: int,
    *,
    group: tuple[str, str] = ("study", "zscore"),
    parent: str | None = None,
    status: str = "FINISHED",
) -> RunRef:
    return RunRef(run_id, "1", parent, group, status, t)


def test_each_group_keeps_its_newest_finished_runs() -> None:
    runs = [ref(f"run{t}", t) for t in range(5)] + [ref("other", 0, group=("study", "iforest"))]
    assert prune.to_delete(runs, keep=2, protected=set()) == [
        "run2",
        "run1",
        "run0",
    ]


def test_failed_runs_do_not_count_and_running_ones_stay() -> None:
    runs = [ref("old", 0), ref("failed", 1, status="FAILED"), ref("live", 2, status="RUNNING")]
    assert prune.to_delete(runs, keep=1, protected=set()) == ["failed"]


def test_registered_and_cited_runs_stay_with_their_family() -> None:
    runs = [
        ref("new", 9),
        ref("champ", 1),
        ref("study", 2),
        ref("trial-cited", 3, parent="study", group=("", "")),
        ref("gone", 0),
        ref("gone-trial", 0, parent="gone", group=("", "")),
    ]
    out = prune.to_delete(runs, keep=1, protected={"champ", "trial-ci"})
    assert out == ["gone", "gone-trial"]  # the parent first, then its trials


def test_cited_finds_run_ids_and_prefixes() -> None:
    text = "Study 2d41d0aa (detector/1), full id 0123456789abcdef0123456789abcdef; not deadbee."
    assert prune.cited(text) == {"2d41d0aa", "0123456789abcdef0123456789abcdef"}


def test_retention_config_loads() -> None:
    assert prune.load_retention().keep_per_group >= 1


@pytest.mark.usefixtures("local_mlflow")
def test_main_deletes_old_runs_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prune, "load_retention", lambda: prune.Retention(keep_per_group=1))
    client = MlflowClient()
    exp = client.create_experiment("e")
    ids = []
    for t in range(3):  # explicit start times (0 would mean "now")
        run = client.create_run(
            exp, start_time=1000 * (t + 1), tags={"role": "study", "candidate": "z"}
        )
        client.set_terminated(run.info.run_id)
        ids.append(run.info.run_id)
    log = tmp_path / "results-log.md"
    log.write_text(f"cited: {ids[0][:8]}\n", encoding="utf-8")
    assert prune.main(["--dry-run", "--results-log", str(log)]) == 0
    assert all(client.get_run(i).info.lifecycle_stage == "active" for i in ids)
    assert prune.main(["--results-log", str(log)]) == 0
    stages = [client.get_run(i).info.lifecycle_stage for i in ids]
    assert stages == ["active", "deleted", "active"]  # cited, old, newest
