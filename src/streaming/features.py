"""Causal per-cell detection features (milestone 2, step 3; specs/03-detection.md).

Input: telemetry rows shaped like `producer.replay.telemetry_for` output (one row per cell and
interval), and the `Reference` fitted on training weeks (`streaming.baseline`). For every
detected KPI (the catalog's real and MODELED KPIs minus `not_detected`), per cell and interval
*t*, using only intervals <= *t*:

- `<kpi>_z`, clipped, on the KPI's detection scale (`with_kpis_and_bucket`):
  - judged against the clock (every detected KPI except the load-curve one): deviation from the
    seasonal baseline for that cell, weekday, and hour (Europe/Rome). Real (activity) KPIs are
    on a `log1p` scale (their deviations are right-skewed; DECISIONS.md "Skewed KPI
    deviations") and their baseline mean is shifted by the cell's recent level (DECISIONS.md
    2026-11-24, option b): the mean deviation over the same hour of the previous `level.days`
    local days. On the log scale that shift is a multiplicative level. A lasting level shift
    (real Milan internet, December 2013) becomes normal after about a week; a fault lasting
    hours barely moves it. With fewer than `level.min_days` days of history, no shift. MODELED
    KPIs listed in `level.modeled` (MODELED load, Step 6d) are judged the same way, `log1p`
    included (load is mostly below 1, so there the level is additive and the log std floor is
    about 0.05 of capacity, on purpose: DECISIONS.md 2026-11-27); the other MODELED KPIs follow
    load nonlinearly, so a level misfits them (measured: more false highs);
  - judged against the load (`load_curve.kpi`, MODELED latency): deviation of log(latency)
    from what the training weeks show at the same `load_curve.load` (equal-count load bins,
    one curve for all cells because load is already per-cell capacity-normalized). A busy
    hour's expected queueing delay is normal; latency high for the load is not (DECISIONS.md
    2026-11-26);
- `<kpi>_bad`: `_z` oriented so that positive is always worse (the KPI's bad direction);
- `<kpi>_z_mean_<w>`: mean of `_z` over the last *w* intervals (by time, so a missing interval
  shortens the window instead of reaching further back);
- `<kpi>_z_delta`: `_z(t) - _z(t - 1 interval)`; null if that interval is missing or a gap.

Data-gap rows get no features at all (null), and windows skip them: a gap alone never alarms.
A cell with no baseline bucket (no training data) gets null clock features; the load curve is
shared by every cell. Imports only `common`, so the Spark job (step 8) can call `compute`
without the producer.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    std_floor_log: float = Field(gt=0)  # for KPIs on a log scale: a relative change


class LoadCurveParams(FrozenModel):
    kpi: str  # MODELED KPI judged against load instead of the clock
    load: str
    bins: int = Field(ge=2)


class LevelParams(FrozenModel):
    days: PositiveInt  # trailing local days for the level offset (the current day excluded)
    min_days: PositiveInt  # fewer days of history: no level-adjusted features
    modeled: tuple[str, ...] = ()  # MODELED KPIs judged like real ones (log1p + level offset)

    @model_validator(mode="after")
    def _min_le_days(self) -> LevelParams:
        if self.min_days > self.days:
            raise ValueError("level.min_days must be <= level.days")
        return self


class Config(FrozenModel):
    windows: tuple[PositiveInt, ...] = Field(min_length=1)
    baseline: BaselineParams
    level: LevelParams
    load_curve: LoadCurveParams
    not_detected: tuple[str, ...]  # KPIs left out of detection, with the reason in the YAML
    z_clip: float = Field(gt=0)

    @model_validator(mode="after")
    def _distinct_windows(self) -> Config:
        if len(set(self.windows)) != len(self.windows):
            raise ValueError("windows must be distinct")
        return self

    @model_validator(mode="after")
    def _known_kpis(self) -> Config:
        domain = load_domain()
        curve = self.load_curve
        if unknown := {curve.kpi, curve.load, *self.not_detected} - set(domain.kpis):
            raise ValueError(f"unknown KPIs {sorted(unknown)}")
        if domain.kpi(curve.kpi).source is not KpiSource.MODELED:
            raise ValueError(f"load_curve.kpi {curve.kpi!r} must be a MODELED KPI")
        if curve.load == curve.kpi or domain.kpi(curve.load).source is KpiSource.DERIVED:
            raise ValueError(f"load_curve.load {curve.load!r}: a measured KPI other than kpi")
        if curve.kpi in self.not_detected:
            raise ValueError(f"load_curve.kpi {curve.kpi!r} must be detected")
        if unknown := set(self.level.modeled) - set(domain.kpis):
            raise ValueError(f"level.modeled: unknown KPIs {sorted(unknown)}")
        for name in self.level.modeled:
            if domain.kpi(name).source is not KpiSource.MODELED:
                raise ValueError(f"level.modeled {name!r} must be a MODELED KPI")
            if name == curve.kpi or name in self.not_detected:
                raise ValueError(f"level.modeled {name!r}: must be detected against the clock")
        return self


@dataclass(frozen=True)
class Reference:
    """What the features are judged against, fitted on clean training weeks (MODELED for the
    MODELED KPIs). `seasonal`: one row per `BUCKET`, `<kpi>_mean` / `<kpi>_std` for every
    `clock_kpis(cfg)` KPI on its detection scale. `load_curve`: one row per load bin, sorted:
    `upper` (the bin's inclusive upper load edge; the last bin also takes any load above it),
    `mean`, `std` of the curve KPI on its detection scale."""

    seasonal: pl.DataFrame
    load_curve: pl.DataFrame


@cache
def load_config() -> Config:
    return read_config(Config, files("streaming").joinpath("features.yaml"))


def feature_kpis(cfg: Config) -> list[Kpi]:
    """KPIs detected per cell: real and modeled ones (derived ones are what the features
    compute), minus `cfg.not_detected`."""
    skip = set(cfg.not_detected)
    return [
        k
        for k in load_domain().kpis.values()
        if k.source is not KpiSource.DERIVED and k.name not in skip
    ]


def clock_kpis(cfg: Config) -> list[Kpi]:
    """Detected KPIs judged against the seasonal baseline (all but the load-curve one)."""
    return [k for k in feature_kpis(cfg) if k.name != cfg.load_curve.kpi]


def _levelled(kpi: Kpi, cfg: Config) -> bool:
    return kpi.source is KpiSource.REAL or kpi.name in cfg.level.modeled


def level_kpis(cfg: Config) -> list[Kpi]:
    """KPIs whose z is level-adjusted: real activity counts, which scale with level, and
    `level.modeled`. On log1p, the additive level offset is a multiplicative level for counts
    well above 1; for MODELED load (mostly below 1, where log1p is near linear) it is additive."""
    return [k for k in feature_kpis(cfg) if _levelled(k, cfg)]


def _log_scale(kpi: Kpi, cfg: Config) -> bool:
    """Whether a KPI is detected on a log scale (the one rule `detection_value` and
    `std_floor` share)."""
    return _levelled(kpi, cfg) or kpi.name == cfg.load_curve.kpi


def detection_value(kpi: Kpi, cfg: Config) -> pl.Expr:
    """The KPI's column on its detection scale: level-adjusted KPIs `log1p` (right-skewed
    counts and load; zero stays finite), the load-curve KPI `log` (MODELED latency is always
    > 0), the rest unchanged."""
    col = pl.col(kpi.name)
    if not _log_scale(kpi, cfg):
        return col
    return col.log1p() if _levelled(kpi, cfg) else col.log()


def std_floor(kpi: Kpi, mean: pl.Expr, cfg: Config) -> pl.Expr:
    """The smallest std a KPI's z may divide by, so near-constant KPIs don't turn noise into
    huge z-scores: on a log scale a fixed relative change, else a share of |mean|."""
    floor = cfg.baseline
    if _log_scale(kpi, cfg):
        return pl.lit(floor.std_floor_log)
    return pl.max_horizontal(mean.abs() * floor.std_floor_rel, pl.lit(floor.std_floor_abs))


def feature_columns_by_kpi(cfg: Config) -> dict[str, list[str]]:
    return {
        k.name: [
            f"{k.name}_z",
            f"{k.name}_bad",
            *[f"{k.name}_z_mean_{w}" for w in cfg.windows],
            f"{k.name}_z_delta",
        ]
        for k in feature_kpis(cfg)
    }


def feature_columns(cfg: Config) -> list[str]:
    return [c for cols in feature_columns_by_kpi(cfg).values() for c in cols]


def oriented(kpis: list[str]) -> list[pl.Expr]:
    """Per KPI, how bad it is (positive = worse), named by the KPI: what detectors attribute
    alarms to. Takes the KPI names so a fitted detector keeps the set it was trained with."""
    return [pl.col(f"{k}_bad").alias(k) for k in kpis]


def with_kpis_and_bucket(rows: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """Add the real KPI columns and the seasonal bucket (weekday 1-7, hour, Europe/Rome), with
    every detected KPI on its detection scale (`detection_value`)."""
    return telemetry.add_real_kpis(rows).with_columns(
        *[detection_value(k, cfg).alias(k.name) for k in feature_kpis(cfg)],
        LOCAL_TIME.dt.weekday().alias("weekday"),
        LOCAL_TIME.dt.hour().alias("hour"),
    )


def load_bin(uppers: pl.Series, cfg: Config) -> pl.Expr:
    """Index of the load bin (sorted inclusive upper edges `uppers`) each row's load falls in;
    the last bin also takes any load above its edge; null load: null."""
    load = pl.col(cfg.load_curve.load)
    edges = pl.lit(uppers.head(-1))
    return pl.when(load.is_not_null()).then(edges.search_sorted(load, side="left"))


def _oriented(kpi: Kpi, z: pl.Expr) -> pl.Expr:
    if kpi.bad_direction is BadDirection.EITHER:
        return z.abs()
    return z * _ORIENTATION[kpi.bad_direction]


def level_offsets(frame: pl.DataFrame, kpis: list[Kpi], cfg: Config) -> pl.DataFrame:
    """Per (cell, local day, hour) and KPI: mean of (value - baseline mean) over the same hour
    of the previous `cfg.level.days` local days (the current day excluded, so it stays causal);
    null with fewer than `cfg.level.min_days` such days. Only rows with traffic (log1p > 0) and
    a baseline count: a cell that stays dead is an outage, never a new level, so it keeps
    alarming."""
    hourly = (
        frame.filter(~pl.col("data_gap"))
        .group_by("cell_id", "local_date", "hour")
        .agg(
            *[
                e
                for k in kpis
                for dev in [
                    pl.when(pl.col(k.name) > 0).then(pl.col(k.name) - pl.col(f"{k.name}_mean"))
                ]
                for e in (
                    dev.sum().alias(f"{k.name}_sum"),
                    dev.count().alias(f"{k.name}_n"),
                    (dev.count() > 0).cast(pl.Int32).alias(f"{k.name}_days"),
                )
            ],
        )
        .sort(*LEVEL_GROUP, "local_date")
    )

    def past(col: str) -> pl.Expr:
        return trailing_days(pl.col(col), cfg)

    offsets = [
        pl.when(past(f"{k.name}_days") >= cfg.level.min_days)
        .then(past(f"{k.name}_sum") / past(f"{k.name}_n"))
        .alias(f"{k.name}_level")
        for k in kpis
    ]
    return hourly.select("cell_id", "local_date", "hour", *offsets)


LEVEL_GROUP = ("cell_id", "hour")  # a level is per cell and local hour of day


def trailing_days(e: pl.Expr, cfg: Config) -> pl.Expr:
    """Sum of `e` over the same hour of the previous `cfg.level.days` local days, the current
    day excluded (causal). On a frame with one row per (cell, `local_date`, `hour`), sorted."""
    window = f"{cfg.level.days}d"
    return e.rolling_sum_by("local_date", window_size=window, closed="left").over(LEVEL_GROUP)


def z_score(value: pl.Expr, mean: pl.Expr, std: pl.Expr, cfg: Config) -> pl.Expr:
    """(value - mean) / std; null on gap rows and where not finite, clipped."""
    z = (value - mean) / std
    return pl.when(~pl.col("data_gap") & z.is_finite()).then(z).clip(-cfg.z_clip, cfg.z_clip)


def _reference(kpi: Kpi, ref: Reference, cfg: Config, *, levelled: bool) -> tuple[pl.Expr, pl.Expr]:
    """(mean, std) a KPI's value is judged against: its load bin's for the load-curve KPI,
    else its seasonal bucket's, the mean shifted by the level offset when `levelled`."""
    if kpi.name == cfg.load_curve.kpi:
        b = load_bin(ref.load_curve["upper"], cfg)
        return pl.lit(ref.load_curve["mean"]).gather(b), pl.lit(ref.load_curve["std"]).gather(b)
    mean = pl.col(f"{kpi.name}_mean")
    if levelled:
        mean = mean + pl.col(f"{kpi.name}_level").fill_null(0.0)
    return mean, pl.col(f"{kpi.name}_std")


def _z_frame(rows: pl.DataFrame, ref: Reference, cfg: Config) -> pl.DataFrame:
    """`KEYS` + `<kpi>_z`, sorted by (cell_id, interval_start). Kept apart from `compute` so the
    wide joined frame (telemetry, baseline, level offsets) is freed before the windows run."""
    frame = (
        with_kpis_and_bucket(rows, cfg)
        .join(ref.seasonal, on=BUCKET, how="left")
        .with_columns(LOCAL_TIME.dt.date().alias("local_date"))
    )
    levelled = level_kpis(cfg)
    frame = frame.join(
        level_offsets(frame, levelled, cfg), on=["cell_id", "local_date", "hour"], how="left"
    )
    z = [
        z_score(pl.col(k.name), *_reference(k, ref, cfg, levelled=k in levelled), cfg).alias(
            f"{k.name}_z"
        )
        for k in feature_kpis(cfg)
    ]
    return frame.select(*KEYS, *z).sort("cell_id", "interval_start")


def compute(rows: pl.DataFrame, ref: Reference, cfg: Config) -> pl.DataFrame:
    """`KEYS` + `feature_columns(cfg)`, sorted by (cell_id, interval_start). Causal: a row's
    features depend only on rows of the same cell at or before it. Level-adjusted z needs
    `cfg.level.min_days` days of history in `rows` before a day (else no level shift)."""
    ok = ~pl.col("data_gap")
    prev_is_adjacent = (
        pl.col("interval_start") - pl.col("interval_start").shift(1).over("cell_id")
    ) == telemetry.INTERVAL
    derived: list[pl.Expr] = []
    for k in feature_kpis(cfg):
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
    return _z_frame(rows, ref, cfg).with_columns(derived).select(*KEYS, *feature_columns(cfg))
