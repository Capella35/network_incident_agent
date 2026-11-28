"""Per-cell streaming scoring, without Spark (milestone 2, step 8; specs/03-detection.md).

The Spark job (`streaming.job`) keeps a `CellState` per replay run and cell and hands each
micro-batch's new rows to `step`.

Arrival order: `telemetry.raw` is keyed by cell and interval, so one cell's intervals sit in
different partitions and a fast replay delivers them out of order across micro-batches (measured
2026-11-27: dropping rows behind the latest scored one lost 6% of a backlog). So new rows wait in
`pending` until every interval before them has arrived, and only the gap-free run after the last
scored interval is scored. The replay sends every cell for every interval (gaps as `data_gap`
rows), so a hole means "not here yet". A cell's first rows in a run wait for the start of their
local day (a replay sends whole local days; measured 2026-11-27, starting at the first row to
arrive instead lost ~280 of a backlog's first rows to lagging partitions).

A hole is given up as lost only once every partition has delivered a later row of the cell
(`CellState.seen`): a partition holds a run's rows in interval order (the replay sends them in
order, the producer keeps per-partition order), so a row still missing behind all partitions'
newest rows will never come. This needs no clock and does not depend on how far partitions are
apart: during a fast replay one partition can be a local day of data behind another, and a hold
measured in data time gave up 2,218 rows that were only late (Step 9, 2026-11-27); a wall-clock
hold would give up every waiting hole after a restart with a backlog. A lost message whose cell
never again reaches every partition keeps its rows waiting until the run goes quiet (its state
times out); then they are scored, the hole given up. A row at or
before the last scored interval arrives too late to score (scores are final) and is only counted.

Features: each local day the released rows fall on is computed with `features.compute` on that
day's rows plus the `history_days` local days before it: the `level.days` a day's level offsets
need, plus the days the longest window reaches back into (a day's first rows average the previous
day's z, whose level needs `level.days` before *that* day). So a day's features equal those of
one frame holding every earlier day, the way the detector was tuned (multi-day offline frames),
and with no more history than `level.days` (a stream's first days, or the offline single-day
path) they equal `streaming.offline.features_for`'s. The frame starts where the day says, not
where the batch does, because the level offsets are rolling sums whose last bits depend on where
the frame starts (measured 2026-11-27). Then the champion's detector scores the released rows and
`models.detectors.alarms` applies its threshold.
"""

from __future__ import annotations

import io
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl

from common import alarms, telemetry
from models import detectors
from streaming import features

if TYPE_CHECKING:  # not at run time: Spark workers unpickle a Scorer without importing mlflow
    from models import tracking

SCHEMA: dict[str, pl.DataType] = {
    "cell_id": pl.String(),
    "site_id": pl.String(),
    "interval_start": pl.Datetime("us", "UTC"),  # as `producer.replay.telemetry_for` gives it
    **{f: pl.Float64() for f in telemetry.TRAFFIC_FIELDS},
    "data_gap": pl.Boolean(),
    **{f: pl.Float64() for f in telemetry.MODELED_FIELDS},
}
_local_date = features.LOCAL_TIME.dt.date()
_PENDING = "_pending"  # marks pending rows in an encoded state
NO_ALARMS = pl.DataFrame(  # an `AlarmMessage` per row, `schema_version` aside
    schema={
        "alarm_id": pl.String(),
        "replay_run_id": pl.String(),
        "cell_id": pl.String(),
        "site_id": pl.String(),
        "interval_start": SCHEMA["interval_start"],
        "score": pl.Float64(),
        "kpis": pl.List(pl.String()),
        "detector_version": pl.String(),
    }
)


@dataclass(frozen=True)
class Scorer:
    """What `step` scores with: the champion's alarm rule and feature spec, and the version
    alarms name (`alarms.detector_version`). No mlflow types: Spark workers unpickle it."""

    detector: detectors.Detector
    threshold: float
    alarm: detectors.AlarmParams
    spec: features.Spec
    detector_version: str

    @classmethod
    def of(cls, champion: tracking.Champion, model: str) -> Scorer:
        """The scorer for a loaded champion package (`tracking.load_champion`) of `model`."""
        tuned = champion.package.tuned
        version = alarms.detector_version(model, champion.version)
        return cls(tuned.detector, tuned.threshold, tuned.alarm, champion.package.spec, version)

    def alarms(self, frame: pl.DataFrame, run: str) -> pl.DataFrame:
        """Alarms (as `NO_ALARMS`) of feature rows `frame` of replay run `run`."""
        hits = detectors.alarms(self.detector.score(frame), self.threshold, self.alarm)
        ids = [
            alarms.alarm_id(run, cell, start, self.detector_version)
            for cell, start in hits.select("cell_id", "interval_start").iter_rows()
        ]
        return hits.with_columns(
            alarm_id=pl.Series(ids, dtype=pl.String),
            replay_run_id=pl.lit(run),
            detector_version=pl.lit(self.detector_version),
        ).select(NO_ALARMS.columns)


@dataclass(frozen=True)
class CellState:
    """One cell's rows (`SCHEMA`): `scored` (the last `history_days` local days of them, what
    new rows' features need), `pending` (arrived, waiting for the rows before them), and `seen`:
    per Kafka partition, the latest interval of the cell it has delivered."""

    scored: pl.DataFrame
    pending: pl.DataFrame
    seen: dict[int, datetime]

    @classmethod
    def empty(cls) -> CellState:
        rows = pl.DataFrame(schema=SCHEMA)
        return cls(rows, rows, {})

    def encode(self) -> tuple[bytes, str]:
        """(the rows as compressed Arrow IPC, `seen` as JSON): the Spark state's two fields."""
        both = pl.concat(
            [
                self.scored.with_columns(pl.lit(value=False).alias(_PENDING)),
                self.pending.with_columns(pl.lit(value=True).alias(_PENDING)),
            ]
        )
        buf = io.BytesIO()
        both.write_ipc(buf, compression="lz4")
        return buf.getvalue(), json.dumps({str(p): t.isoformat() for p, t in self.seen.items()})

    @classmethod
    def decode(cls, blob: bytes, seen: str) -> CellState:
        both = pl.read_ipc(io.BytesIO(blob))
        pending = pl.col(_PENDING)
        return cls(
            both.filter(~pending).drop(_PENDING),
            both.filter(pending).drop(_PENDING),
            {int(p): datetime.fromisoformat(t) for p, t in json.loads(seen).items()},
        )


@dataclass(frozen=True)
class Step:
    alarms: pl.DataFrame  # one row per alarm, columns and types as `NO_ALARMS`
    state: CellState  # what to keep for the next batch
    n_scored: int  # rows released and scored (gap rows count; they get no score)
    n_late: int  # rows that came after a later interval was scored: dropped


def rows_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """`frame`'s telemetry columns (any extra dropped), cast to `SCHEMA`."""
    return frame.select(pl.col(c).cast(t) for c, t in SCHEMA.items())


def history_days(cfg: features.Config) -> int:
    """Local days of history a day's features need (see the module docstring)."""
    reach = telemetry.INTERVAL * max(cfg.windows)
    return cfg.level.days + math.ceil(reach / timedelta(days=1))


def releasable(pool: pl.DataFrame, after: datetime, lost_before: datetime | None) -> int:
    """How many leading rows of `pool` (sorted, one per interval, all after `after`) can be
    scored now: the run with no hole after `after`, past holes whose intervals are all before
    `lost_before` (given up as lost; None: none is)."""
    times: list[datetime] = pool["interval_start"].to_list()
    expected = after + telemetry.INTERVAL
    n = 0
    for t in times:
        if t != expected and (lost_before is None or t > lost_before):
            break
        n += 1
        expected = t + telemetry.INTERVAL
    return n


def _lost_before(seen: dict[int, datetime], partitions: int) -> datetime | None:
    """Missing intervals before this are lost: every partition has delivered a row at or after
    it (None until each of the topic's `partitions` has delivered one)."""
    return min(seen.values()) if len(seen) == partitions else None


def new_features(scored: pl.DataFrame, fresh: pl.DataFrame, spec: features.Spec) -> pl.DataFrame:
    """Features of `fresh` (one cell's rows, sorted, one per interval, all after `scored`'s,
    at least one), each local day computed on its `history_days` (see the module docstring)."""
    cell = fresh["cell_id"][0]
    ref = features.Reference(  # this cell's buckets only: the same join result, a smaller join
        spec.reference.seasonal.filter(pl.col("cell_id") == cell), spec.reference.load_curve
    )
    rows = pl.concat([scored, fresh]).with_columns(_local_date.alias("_day"))
    reach = timedelta(days=history_days(spec.config))
    return pl.concat(
        features.compute(
            rows.filter(pl.col("_day").is_between(day - reach, day)).drop("_day"), ref, spec.config
        )
        .filter(_local_date == day)
        .join(fresh.select("interval_start"), on="interval_start", how="semi")
        for day in fresh.select(_local_date.unique().sort()).to_series()
    )


def step(
    state: CellState,
    new: pl.DataFrame,
    scorer: Scorer,
    *,
    run: str,
    partitions: int,
    quiet: bool = False,
) -> Step:
    """Score one cell's released rows of replay run `run` (see the module docstring). `new`:
    the rows plus `partition`, the Kafka partition each came from (of `partitions`). `quiet`:
    the run has sent nothing for a while (its state timed out), so every hole still open is
    given up and every waiting row is scored."""
    seen = dict(state.seen)
    if not new.is_empty():
        newest = new.group_by("partition").agg(pl.col("interval_start").max())
        for p, t in newest.iter_rows():
            seen[p] = max(t, seen.get(p, t))
    last = state.scored["interval_start"].max()  # None while nothing is scored
    incoming = rows_frame(new)
    on_time = incoming.filter(
        pl.lit(value=True) if last is None else pl.col("interval_start") > last
    )
    late = incoming.height - on_time.height
    pool = (
        pl.concat([state.pending, on_time])
        .unique("interval_start", keep="first", maintain_order=True)
        .sort("interval_start")
    )
    if isinstance(last, datetime):
        after = last
    elif pool.is_empty():
        return Step(NO_ALARMS, CellState(state.scored, pool, seen), 0, late)
    else:  # nothing scored yet: wait for the run's first interval (a replay sends whole days)
        after = telemetry.local_day_start(pool["interval_start"][0]) - telemetry.INTERVAL
    # Quiet with nothing waiting (the usual timeout) leaves the pool empty: nothing to give up.
    lost = pool.item(-1, "interval_start") + telemetry.INTERVAL if quiet and pool.height else None
    n = releasable(pool, after, lost or _lost_before(seen, partitions))
    if n == 0:  # nothing released: a detector cannot score zero rows
        return Step(NO_ALARMS, CellState(state.scored, pool, seen), 0, late)
    fresh = pool.head(n)
    frame = new_features(state.scored, fresh, scorer.spec)
    rows = pl.concat([state.scored, fresh])
    day = fresh.select(_local_date.max()).item()
    keep = rows.filter(_local_date >= day - timedelta(days=history_days(scorer.spec.config)))
    state_ = CellState(keep, pool.slice(n), seen)
    return Step(scorer.alarms(frame, run), state_, frame.height, late)
