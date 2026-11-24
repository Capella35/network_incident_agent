"""Causal per-cell detection features (milestone 2, step 3; specs/03-detection.md).

Input: telemetry rows shaped like `producer.replay.telemetry_for` output (one row per cell and
interval), and the seasonal baseline (`streaming.baseline`). For every real and MODELED KPI in the
catalog, per cell and interval *t*, using only intervals <= *t*:

- `<kpi>_z`: deviation from the baseline for that cell, weekday, and hour (Europe/Rome), clipped.
  For real (activity) KPIs the baseline mean and std are first scaled by the cell's recent level
  (DECISIONS.md 2026-11-24, option b): for the same hour over the previous `level.days` local
  days, actual sum / baseline-mean sum. A lasting level shift (real Milan internet, December
  2013) becomes normal after about a week; a fault lasting hours barely moves it. With fewer than
  `level.min_days` days of history, the plain seasonal z. Not for MODELED KPIs: they follow load
  nonlinearly (fixed base latency, logistic drops), so a multiplicative level misfits them
  (measured: more false highs);
- `<kpi>_bad`: `_z` oriented so that positive is always worse (the KPI's bad direction);
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
from common.domain.loader import BadDirection, FrozenModel, Kpi, KpiSource, read_config

BUCKET = ("cell_id", "weekday", "hour")  # seasonal baseline key
KEYS = ("cell_id", "site_id", "interval_start", "data_gap")
LOCAL_TIME = pl.col("interval_start").dt.convert_time_zone(str(telemetry.LOCAL_TZ))
_ORIENTATION = {BadDirection.DROP: -1.0, BadDirection.HIGH: 1.0}  # EITHER: |z|


class BaselineParams(FrozenModel):
    std_floor_abs: float = Field(gt=0)
    std_floor_rel: float = Field(ge=0)


class LevelParams(FrozenModel):
    days: PositiveInt  # trailing local days for the level factor (the current day excluded)
    min_days: PositiveInt  # fewer days of history: no level-adjusted features

    @model_validator(mode="after")
    def _min_le_days(self) -> LevelParams:
        if self.min_days > self.days:
            raise ValueError("level.min_days must be <= level.days")
        return self


class Config(FrozenModel):
    windows: tuple[PositiveInt, ...] = Field(min_length=1)
    baseline: BaselineParams
    level: LevelParams
    z_clip: float = Field(gt=0)

    @model_validator(mode="after")
    def _distinct_windows(self) -> Config:
        if len(set(self.windows)) != len(self.windows):
            raise ValueError("windows must be distinct")
        return self


@cache
def load_config() -> Config:
    return read_config(Config, files("streaming").joinpath("features.yaml"))


def feature_kpis() -> list[Kpi]:
    """KPIs measured per cell (real and modeled); derived ones are what the features compute."""
    return [k for k in load_domain().kpis.values() if k.source is not KpiSource.DERIVED]


def level_kpis() -> list[Kpi]:
    """KPIs whose z is level-adjusted: real activity counts, which scale with level."""
    return [k for k in feature_kpis() if k.source is KpiSource.REAL]


def feature_columns_by_kpi(cfg: Config) -> dict[str, list[str]]:
    return {
        k.name: [
            f"{k.name}_z",
            f"{k.name}_bad",
            *[f"{k.name}_z_mean_{w}" for w in cfg.windows],
            f"{k.name}_z_delta",
        ]
        for k in feature_kpis()
    }


def feature_columns(cfg: Config) -> list[str]:
    return [c for cols in feature_columns_by_kpi(cfg).values() for c in cols]


def oriented(kpis: list[str]) -> list[pl.Expr]:
    """Per KPI, how bad it is (positive = worse), named by the KPI: what detectors attribute
    alarms to. Takes the KPI names so a fitted detector keeps the set it was trained with."""
    return [pl.col(f"{k}_bad").alias(k) for k in kpis]


def with_kpis_and_bucket(rows: pl.DataFrame) -> pl.DataFrame:
    """Add the real KPI columns and the seasonal bucket (weekday 1-7, hour, Europe/Rome)."""
    return telemetry.add_real_kpis(rows).with_columns(
        LOCAL_TIME.dt.weekday().alias("weekday"), LOCAL_TIME.dt.hour().alias("hour")
    )


def _oriented(kpi: Kpi, z: pl.Expr) -> pl.Expr:
    if kpi.bad_direction is BadDirection.EITHER:
        return z.abs()
    return z * _ORIENTATION[kpi.bad_direction]


def _level_factors(frame: pl.DataFrame, kpis: list[Kpi], cfg: Config) -> pl.DataFrame:
    """Per (cell, local day, hour) and KPI: actual / baseline-mean over the same hour of the
    previous `cfg.level.days` local days (the current day excluded, so it stays causal)."""
    group = ("cell_id", "hour")

    def trailing_sum(e: pl.Expr) -> pl.Expr:
        window = f"{cfg.level.days}d"
        return e.rolling_sum_by("local_date", window_size=window, closed="left").over(group)

    hourly = (
        frame.filter(~pl.col("data_gap"))
        .group_by("cell_id", "local_date", "hour")
        .agg(
            pl.lit(1).alias("_day"),
            *[pl.col(k.name).sum().alias(f"{k.name}_a") for k in kpis],
            *[pl.col(f"{k.name}_mean").sum().alias(f"{k.name}_b") for k in kpis],
        )
        .sort(*group, "local_date")
    )
    days = trailing_sum(pl.col("_day"))
    factors = []
    for k in kpis:
        a, b = trailing_sum(pl.col(f"{k.name}_a")), trailing_sum(pl.col(f"{k.name}_b"))
        factors.append(
            pl.when((days >= cfg.level.min_days) & (a > 0) & (b > 0))
            .then(a / b)
            .alias(f"{k.name}_level")
        )
    return hourly.select("cell_id", "local_date", "hour", *factors)


def _scaled_z(kpi: Kpi, level: pl.Expr, cfg: Config) -> pl.Expr:
    """z against the seasonal baseline scaled by `level`; null on gap rows, clipped. The std
    floor is applied again after scaling, so a small level cannot shrink the std to ~0; any
    non-finite result is null (so the caller's seasonal fallback applies)."""
    floor = cfg.baseline
    mean = pl.col(f"{kpi.name}_mean") * level
    std = pl.max_horizontal(
        pl.col(f"{kpi.name}_std") * level,
        mean.abs() * floor.std_floor_rel,
        pl.lit(floor.std_floor_abs),
    )
    z = (pl.col(kpi.name) - mean) / std
    return pl.when(~pl.col("data_gap") & z.is_finite()).then(z).clip(-cfg.z_clip, cfg.z_clip)


def _z_frame(rows: pl.DataFrame, baseline: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """`KEYS` + `<kpi>_z`, sorted by (cell_id, interval_start). Kept apart from `compute` so the
    wide joined frame (telemetry, baseline, level factors) is freed before the windows run."""
    frame = (
        with_kpis_and_bucket(rows)
        .join(baseline, on=BUCKET, how="left")
        .with_columns(LOCAL_TIME.dt.date().alias("local_date"))
    )
    frame = frame.join(
        _level_factors(frame, level_kpis(), cfg), on=["cell_id", "local_date", "hour"], how="left"
    )
    level = {k.name for k in level_kpis()}
    seasonal = pl.lit(1.0)
    return frame.select(
        *KEYS,
        *[
            (
                pl.coalesce(
                    _scaled_z(k, pl.col(f"{k.name}_level"), cfg), _scaled_z(k, seasonal, cfg)
                )
                if k.name in level
                else _scaled_z(k, seasonal, cfg)
            ).alias(f"{k.name}_z")
            for k in feature_kpis()
        ],
    ).sort("cell_id", "interval_start")


def compute(rows: pl.DataFrame, baseline: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """`KEYS` + `feature_columns(cfg)`, sorted by (cell_id, interval_start). Causal: a row's
    features depend only on rows of the same cell at or before it. Level-adjusted z needs
    `cfg.level.min_days` days of history in `rows` before a day (else seasonal z)."""
    ok = ~pl.col("data_gap")
    prev_is_adjacent = (
        pl.col("interval_start") - pl.col("interval_start").shift(1).over("cell_id")
    ) == telemetry.INTERVAL
    derived: list[pl.Expr] = []
    for k in feature_kpis():
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
    return _z_frame(rows, baseline, cfg).with_columns(derived).select(*KEYS, *feature_columns(cfg))
