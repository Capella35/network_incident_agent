"""Milestone 2 Definition of done: the Spark job's alarms equal offline scoring (step 9).

    make detect-check ARGS="--day 2013-12-03"     # runs this in the detector image
    python -m evaluation.detect_check --day 2013-12-03 [--data-dir data] [--timeout 900]

Needs the stack and the detector job (`make up`, `make detect`), and reads the job's checkpoint
(mounted read-only). For one validation day under a new replay run ID:

1. Replays the day after its history, under the rule of `streaming.offline.features_for`:
   history days from an earlier split clean, from the day's split with its SYNTHETIC faults.
2. Waits until the job has committed a micro-batch that reaches the topic's end offsets after
   the replay (Spark's commit log: a batch is committed after its alarms are stored).
3. Scores the same rows offline with the same champion (`streaming.scoring.Scorer`, same code,
   same libraries: this runs in the detector image), on one frame holding the day and its
   history.
4. Compares the day's alarms: the same IDs, and per ID the same score (exactly: no tolerance,
   `DECISIONS.md` 2026-11-27) and KPIs; then the validation report (`evaluation.report`) of
   each set against the ground truth, which must be identical too.

Exit 0 only if everything matches. Validation days only (Rule 7); the ground truth is read only
through `evaluation.report.score` (Rule 6).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
from confluent_kafka import KafkaException
from psycopg import sql

from common import kafka, splits, telemetry
from common.db import DbConfigError, DbSettings, Role
from evaluation import report
from models import cli
from producer import milan, replay
from streaming import job, offline, scoring

log = logging.getLogger("evaluation.detect_check")

POLL_S = 5.0
SHOWN = 20  # differences listed in the report


class CheckError(RuntimeError):
    pass


@dataclass(frozen=True)
class Plan:
    """How the day's rows are replayed: `clean` days (an earlier split's history), then
    `injected` days (history from the day's split, then the day), under one run ID."""

    clean: list[date]
    injected: list[date]


def plan(day: date, history: list[date]) -> Plan:
    """`history`: `offline.history_days`, which must reach the day without a gap (a replay
    sends a run of consecutive days). Raises CheckError otherwise."""
    if history and (day - history[0]).days != len(history):
        raise CheckError(f"the history before {day} has an unprocessed day: {history}")
    clean, same = offline.split_history(history, splits.split_of(day))
    return Plan(clean, [*same, day])


@dataclass(frozen=True)
class Report:
    day: date
    run: str
    streamed: int  # alarms on the day
    offline: int
    differences: list[str]
    validation_report: str  # of the streamed alarms (equal to the offline one unless listed)

    @property
    def ok(self) -> bool:
        return not self.differences


def compare(streamed: pl.DataFrame, offline_: pl.DataFrame) -> list[str]:
    """Differences between two alarm sets (`scoring.NO_ALARMS` columns): IDs only in one set,
    then per shared ID any score (exact) or KPI difference."""
    out: list[str] = []
    s_ids, o_ids = set(streamed["alarm_id"]), set(offline_["alarm_id"])
    out += [f"only streamed: {i}" for i in sorted(s_ids - o_ids)]
    out += [f"only offline: {i}" for i in sorted(o_ids - s_ids)]
    both = streamed.join(offline_, on="alarm_id", suffix="_offline").sort("alarm_id")
    for row in both.iter_rows(named=True):
        if row["score"] != row["score_offline"]:
            out.append(f"{row['alarm_id']}: score {row['score']!r} vs {row['score_offline']!r}")
        if row["kpis"] != row["kpis_offline"]:
            out.append(f"{row['alarm_id']}: kpis {row['kpis']} vs {row['kpis_offline']}")
    return out


def render(r: Report) -> str:
    verdict = "MATCH" if r.ok else f"MISMATCH ({len(r.differences)} differences)"
    lines = [
        f"# Detector parity check: {r.day} (validation, SYNTHETIC faults): {verdict}",
        "",
        f"Replay run `{r.run}`. Alarms on the day: streamed {r.streamed}, offline {r.offline}.",
        "",
    ]
    lines += [f"- {d}" for d in r.differences[:SHOWN]]
    if len(r.differences) > SHOWN:
        lines.append(f"- … {len(r.differences) - SHOWN} more")
    return "\n".join(lines) + "\n\n" + r.validation_report


def committed_offsets(checkpoint: Path) -> dict[int, int]:
    """Per `telemetry.raw` partition, the end offset of the job's latest committed micro-batch
    (Spark's checkpoint: `commits/<N>` marks batch N done, `offsets/<N>` ends with its source
    offsets as JSON); empty before the first commit."""
    commits = [int(f.name) for f in (checkpoint / "commits").glob("[0-9]*")]
    if not commits:
        return {}
    last_line = (checkpoint / "offsets" / str(max(commits))).read_text().strip().splitlines()[-1]
    return {int(p): int(o) for p, o in json.loads(last_line)[telemetry.TOPIC_RAW].items()}


def wait_for_stream(checkpoint: Path, ends: dict[int, int], timeout_s: float) -> None:
    """Until the job has committed a micro-batch at or past `ends` in every partition."""
    deadline = time.monotonic() + timeout_s
    while True:
        done = committed_offsets(checkpoint)
        behind = sum(max(0, end - done.get(p, 0)) for p, end in ends.items())
        if not behind:
            return
        if time.monotonic() > deadline:
            raise CheckError(
                f"the detector is {behind} messages behind after {timeout_s:.0f} s: is it "
                "running (make detect)?"
            )
        time.sleep(POLL_S)


def streamed_alarms(conn: psycopg.Connection[Any], run: str, day: date) -> pl.DataFrame:
    start, end = milan.window([day])
    query = sql.SQL(
        "SELECT {} FROM {} WHERE replay_run_id = %s AND interval_start >= %s"
        " AND interval_start < %s"
    ).format(
        sql.SQL(", ").join(map(sql.Identifier, scoring.NO_ALARMS.columns)),
        sql.Identifier(job.ALARMS),
    )
    rows = conn.execute(query, (run, start, end)).fetchall()
    return pl.DataFrame(rows, schema=scoring.NO_ALARMS.schema, orient="row")


def _replay(data_dir: Path, producer: Any, days: list[date], run: str, *, inject: bool) -> int:
    if not days:
        return 0
    summary = replay.run(
        data_dir,
        days[0],
        days[-1],
        producer,
        replay.Pacer(speed=0),
        options=replay.ReplayOptions(replay_run_id=run, inject=inject),
    )
    if summary.failed or summary.dead_lettered or summary.delivered != summary.sent:
        raise CheckError(
            f"replay {run} {days[0]}..{days[-1]}: sent {summary.sent}, delivered "
            f"{summary.delivered}, failed {summary.failed}, dead-lettered {summary.dead_lettered}"
        )
    return summary.sent


def _report(data_dir: Path, alarms: pl.DataFrame, day: date, gaps: pl.DataFrame) -> str:
    result = report.score(data_dir, alarms, days=[day], gaps=gaps)[0]
    return report.render(result, title=f"Validation report, {day}")


def run_check(
    data_dir: Path, checkpoint: Path, day: date, scorer: scoring.Scorer, *, timeout_s: float
) -> Report:
    """`checkpoint`: the detector job's (read-only)."""
    from confluent_kafka import Producer as KafkaProducer

    splits.require(day, splits.Split.VALIDATION)  # Rule 7
    steps = plan(day, offline.history_days(data_dir, day, scorer.spec.config))
    run = f"detect-check-{day}-{uuid.uuid4().hex[:8]}"
    producer = KafkaProducer(replay.kafka_config(kafka.bootstrap_servers()))
    sent = _replay(data_dir, producer, steps.clean, run, inject=False)
    sent += _replay(data_dir, producer, steps.injected, run, inject=True)
    log.info("replayed %d rows as %s; waiting for the detector", sent, run)
    wait_for_stream(checkpoint, job.topic_end_offsets(), timeout_s)
    with DbSettings.from_env(Role.READONLY).connect() as conn:
        streamed = streamed_alarms(conn, run, day)
    # Loaded only now, the same rule as the replay's, so it never sits beside the replay's copy.
    rows = offline.telemetry_with_history(data_dir, [day], inject=True).rows
    frame = offline.features_of(rows, [day], scorer.spec.reference, scorer.spec.config)
    offline_ = scorer.alarms(frame, run)
    differences = compare(streamed, offline_)
    gaps = report.gaps(frame)
    streamed_report = _report(data_dir, streamed, day, gaps)
    if streamed_report != _report(data_dir, offline_, day, gaps):
        differences.append("the validation reports differ")
    return Report(day, run, streamed.height, offline_.height, differences, streamed_report)


def main(argv: list[str] | None = None) -> int:
    from mlflow import MlflowClient

    from models import tracking

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--day", type=date.fromisoformat, required=True)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--checkpoint", type=Path, default=Path(job.DEFAULT_CHECKPOINT))
    ap.add_argument("--timeout", type=float, default=900.0, help="seconds to wait for the job")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        scorer = scoring.Scorer.of(tracking.load_champion(MlflowClient()), tracking.MODEL)
        r = run_check(args.data_dir, args.checkpoint, args.day, scorer, timeout_s=args.timeout)
    except (CheckError, DbConfigError, psycopg.Error, KafkaException, *cli.RUN_ERRORS) as exc:
        log.error("%s", exc)
        return 1
    print(render(r))
    return 0 if r.ok else 1


if __name__ == "__main__":
    sys.exit(main())
