"""Causal per-cell detection features (milestone 2, step 3; specs/03-detection.md).

Input: telemetry rows shaped like `producer.replay.telemetry_for` output (one row per cell and
interval), and the seasonal baseline (`streaming.baseline`). For every real and MODELED KPI in the
catalog, per cell and interval *t*, using only intervals <= *t*:

- `<kpi>_z`: deviation from the baseline for that cell, weekday, and hour (Europe/Rome), clipped;
- `<kpi>_bad`: the same, oriented so that positive is always worse (the KPI's bad direction);
- `<kpi>_z_mean_<w>`: mean of `_z` over the last *w* intervals (by time, so a missing interval
  shortens the window instead of reaching further back);
- `<kpi>_z_delta`: `_z(t) - _z(t - 1 interval)`; null if that interval is missing or a gap.

Data-gap rows get no features at all (null), and windows skip them: a gap alone never alarms.
A cell with no baseline bucket (no training data) gets null features. Imports only `common`, so
the Spark job (step 8) can call `compute` without the producer.
"""

from __future__ import annotations

from functools import cache
from importlib.resources import files

import polars as pl
from pydantic import Field, PositiveInt, model_validator

from common import telemetry
from common.domain import load_domain
from common.domain.loader import BadDirection, FrozenModel, Kpi, KpiSource, read_yaml

BUCKET = ("cell_id", "weekday", "hour")  # seasonal baseline key
KEYS = ("cell_id", "site_id", "interval_start", "data_gap")
LOCAL_TIME = pl.col("interval_start").dt.convert_time_zone(str(telemetry.LOCAL_TZ))
_ORIENTATION = {BadDirection.DROP: -1.0, BadDirection.HIGH: 1.0}  # EITHER: |z|


class BaselineParams(FrozenModel):
    std_floor_abs: float = Field(gt=0)
    std_floor_rel: float = Field(ge=0)


class Config(FrozenModel):
    windows: tuple[PositiveInt, ...] = Field(min_length=1)
    baseline: BaselineParams
    z_clip: float = Field(gt=0)

    @model_validator(mode="after")
    def _distinct_windows(self) -> Config:
        if len(set(self.windows)) != len(self.windows):
            raise ValueError("windows must be distinct")
        return self


@cache
def load_config() -> Config:
    raw = read_yaml(files("streaming").joinpath("features.yaml"))
    return Config(**{k: v for k, v in raw.items() if k != "version"})


def feature_kpis() -> list[Kpi]:
    """KPIs measured per cell (real and modeled); derived ones are what the features compute."""
    return [k for k in load_domain().kpis.values() if k.source is not KpiSource.DERIVED]


def feature_columns(cfg: Config) -> list[str]:
    cols = []
    for k in feature_kpis():
        cols += [f"{k.name}_z", f"{k.name}_bad"]
        cols += [f"{k.name}_z_mean_{w}" for w in cfg.windows]
        cols.append(f"{k.name}_z_delta")
    return cols


def with_kpis_and_bucket(rows: pl.DataFrame) -> pl.DataFrame:
    """Add the real KPI columns and the seasonal bucket (weekday 1-7, hour, Europe/Rome)."""
    return telemetry.add_real_kpis(rows).with_columns(
        LOCAL_TIME.dt.weekday().alias("weekday"), LOCAL_TIME.dt.hour().alias("hour")
    )


def _oriented(kpi: Kpi, z: pl.Expr) -> pl.Expr:
    if kpi.bad_direction is BadDirection.EITHER:
        return z.abs()
    return z * _ORIENTATION[kpi.bad_direction]


def compute(rows: pl.DataFrame, baseline: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """`KEYS` + `feature_columns(cfg)`, sorted by (cell_id, interval_start). Causal: a row's
    features depend only on rows of the same cell at or before it."""
    kpis = feature_kpis()
    ok = ~pl.col("data_gap")
    z_frame = (
        with_kpis_and_bucket(rows)
        .join(baseline, on=BUCKET, how="left")
        .select(
            *KEYS,
            *[
                pl.when(ok)
                .then((pl.col(k.name) - pl.col(f"{k.name}_mean")) / pl.col(f"{k.name}_std"))
                .clip(-cfg.z_clip, cfg.z_clip)
                .alias(f"{k.name}_z")
                for k in kpis
            ],
        )
        .sort("cell_id", "interval_start")
    )
    prev_is_adjacent = (
        pl.col("interval_start") - pl.col("interval_start").shift(1).over("cell_id")
    ) == telemetry.INTERVAL
    derived: list[pl.Expr] = []
    for k in kpis:
        z = pl.col(f"{k.name}_z")
        derived.append(_oriented(k, z).alias(f"{k.name}_bad"))
        derived += [
            pl.when(ok)
            .then(
                z.rolling_mean_by("interval_start", window_size=telemetry.INTERVAL * w).over(
                    "cell_id"
                )
            )
            .alias(f"{k.name}_z_mean_{w}")
            for w in cfg.windows
        ]
        derived.append(
            pl.when(prev_is_adjacent)
            .then(z - z.shift(1).over("cell_id"))
            .alias(f"{k.name}_z_delta")
        )
    return z_frame.with_columns(derived).select(*KEYS, *feature_columns(cfg))
