"""Hand-written telemetry messages for tests (never Milan data). One place to build them."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import polars as pl

from common import telemetry
from common.telemetry import TelemetryMessage
from streaming import features

T0 = datetime(2013, 11, 5, tzinfo=UTC)  # a training day, on the 10-minute grid


def telemetry_message(
    *, run: str = "run-1", cell: int = 1, minutes: int = 0, traffic: float = 1.0
) -> TelemetryMessage:
    """A valid message for `CELL-1-<cell>` at T0 + `minutes`, every traffic field = `traffic`.
    Variants: `.model_copy(update=...)`."""
    return TelemetryMessage(
        replay_run_id=run,
        cell_id=f"CELL-1-{cell}",
        site_id="SITE-1",
        square_id=cell,
        interval_start=T0 + timedelta(minutes=minutes),
        **dict.fromkeys(telemetry.TRAFFIC_FIELDS, traffic),
        data_gap=False,
        load_ratio=0.3,
        latency_ms=25.0 + cell,
        drop_rate=0.002,
    )


def message_grid(run: str, n_cells: int, n_intervals: int) -> list[TelemetryMessage]:
    """`n_cells` x `n_intervals` messages in replay order (interval_start, cell_id); traffic
    differs per cell and interval, so a changed value changes a digest."""
    msgs = [
        telemetry_message(run=run, cell=c, minutes=10 * i, traffic=c + i / 7)
        for i in range(n_intervals)
        for c in range(1, n_cells + 1)
    ]
    return sorted(msgs, key=lambda m: (m.interval_start, m.cell_id))


def telemetry_frame(
    n_cells: int = 1, n_intervals: int = 12, *, gaps: Iterable[int] = (), drop: Iterable[int] = ()
) -> pl.DataFrame:
    """`message_grid` as rows shaped like `producer.replay.telemetry_for` output (no run ID).
    `gaps`: interval indexes flagged as data gaps (modeled KPIs null, like the replay);
    `drop`: interval indexes removed entirely (no row at all)."""
    frame = pl.DataFrame(
        [
            m.model_dump(exclude={"schema_version", "replay_run_id"})
            for m in message_grid("r", n_cells, n_intervals)
        ]
    ).with_columns((pl.col("interval_start").rank("dense") - 1).alias("_i"))
    gap = pl.col("_i").is_in(list(gaps))
    return (
        frame.with_columns(
            gap.alias("data_gap"),
            *[
                pl.when(gap).then(None).otherwise(pl.col(c)).alias(c)
                for c in telemetry.MODELED_FIELDS
            ],
        )
        .filter(~pl.col("_i").is_in(list(drop)))
        .drop("_i")
    )


def flat_baseline(cells: Iterable[str], mean: float = 1.0, std: float = 0.5) -> features.Reference:
    """Every (weekday, hour) bucket for `cells`, each KPI judged against the clock with the
    same mean and std (on its detection scale); a one-bin load curve with that mean and std."""
    buckets = [(c, wd, h) for c in cells for wd in range(1, 8) for h in range(24)]
    frame = pl.DataFrame(
        buckets,
        schema=list(features.BUCKET),
        orient="row",
        schema_overrides={"weekday": pl.Int8, "hour": pl.Int8},
    )
    kpis = features.clock_kpis(features.load_config())
    seasonal = frame.with_columns(
        *[pl.lit(mean).alias(f"{k.name}_mean") for k in kpis],
        *[pl.lit(std).alias(f"{k.name}_std") for k in kpis],
    )
    curve = pl.DataFrame({"upper": [float("inf")], "mean": [mean], "std": [std]})
    return features.Reference(seasonal, curve)
