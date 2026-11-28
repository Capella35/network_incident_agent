"""Detector candidates (milestone 2, step 4; specs/03-detection.md).

One interface: `fit(train)` on feature rows (`streaming.features.KEYS` + feature columns) from
the clean training weeks, `score(frame)` -> `KEYS` + `score` (higher = more anomalous) +
`contrib_<kpi>` per KPI, and `alarms(scored, threshold, params)`. Gap rows get no score, so a gap
alone never alarms. Thresholds are not chosen here: Optuna tunes them on validation (step 6).

- `zscore`: the worst bad-direction z across KPIs (`features.oriented`); nothing to fit.
- `iforest`: scikit-learn Isolation Forest on every feature column; score = -score_samples.
  Contributions: the KPIs' bad-direction z (the forest itself does not attribute).
- `autoencoder`: a scikit-learn MLP trained to reconstruct its standardized input through a
  small bottleneck; score = mean squared reconstruction error; contributions = per-KPI error.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from functools import cache
from importlib.resources import files
from typing import Any, ClassVar

import numpy as np
import polars as pl
from pydantic import Field, PositiveInt
from sklearn.ensemble import IsolationForest
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler

from common.domain.loader import FrozenModel, read_config
from streaming import features

CONTRIB = "contrib_"
# The autoencoder scores in chunks of exactly this many rows, the last one padded with zero rows,
# so every matrix product has the same shape and a row's score cannot depend on how many rows
# were scored with it. BLAS takes other paths for small matrices and for the rows past its last
# full block (measured 2026-11-27 in the detector image: 511 of 20,736 rows differed by up to
# 1.8e-15 in batches of 1-39 rows, 22 in batches of 129-900). The Spark job scores what each
# micro-batch releases; offline paths score whole days.
SCORE_BLOCK = 128


class AlarmParams(FrozenModel):
    kpi_share: float = Field(gt=0, le=1)
    max_kpis: PositiveInt


class ZScoreParams(FrozenModel):
    """Nothing to set: the z-score detector has no parameters."""


class IForestParams(FrozenModel):
    n_estimators: PositiveInt
    max_samples: PositiveInt
    max_train_rows: PositiveInt


class AutoencoderParams(FrozenModel):
    hidden: PositiveInt
    max_iter: PositiveInt
    max_train_rows: PositiveInt


class CandidateParams(FrozenModel):
    zscore: ZScoreParams
    iforest: IForestParams
    autoencoder: AutoencoderParams


class Config(FrozenModel):
    seed: int
    memory_budget_mib: PositiveInt
    alarm: AlarmParams
    candidates: CandidateParams


@cache
def load_config() -> Config:
    return read_config(Config, files("models").joinpath("detectors.yaml"))


def _matrix(frame: pl.DataFrame, columns: list[str]) -> np.ndarray:
    """Float matrix of `columns`; a missing feature (null: first interval, no window yet; or
    non-finite) is neutral 0, so scikit-learn never sees NaN or inf."""
    values = pl.col(columns).cast(pl.Float64)
    return frame.select(pl.when(values.is_finite()).then(values).otherwise(0.0)).to_numpy()


def training_rows(train: pl.DataFrame, max_rows: int, seed: int) -> pl.DataFrame:
    """Non-gap training rows, a seeded sample of at most `max_rows`: what a fit uses. Taking it
    again from its own output returns it unchanged, so callers may sample once and fit often."""
    rows = train.filter(~pl.col("data_gap"))
    return rows.sample(n=max_rows, seed=seed) if rows.height > max_rows else rows


def _train_matrix(train: pl.DataFrame, columns: list[str], max_rows: int, seed: int) -> np.ndarray:
    return _matrix(training_rows(train.select(*columns, "data_gap"), max_rows, seed), columns)


class Detector(ABC):
    name: ClassVar[str]

    def __init__(self, seed: int, p: FrozenModel) -> None:
        self.seed = seed
        self.p = p
        # Fixed at construction, so a logged model keeps scoring the KPIs and columns it was
        # trained on even if the catalog or features.yaml changes later.
        cfg = features.load_config()
        self.kpis = [k.name for k in features.feature_kpis(cfg)]
        self.columns = features.feature_columns(cfg)

    @abstractmethod
    def fit(self, train: pl.DataFrame) -> Detector: ...

    @abstractmethod
    def _score(self, frame: pl.DataFrame) -> tuple[np.ndarray, pl.DataFrame]:
        """(score per row, one contribution column per KPI, named by KPI)."""

    def score(self, frame: pl.DataFrame) -> pl.DataFrame:
        scores, contrib = self._score(frame)
        ok = ~pl.col("data_gap")
        return pl.concat(
            [
                frame.select(features.KEYS),
                contrib.rename(lambda k: CONTRIB + k),
                pl.DataFrame({"score": scores}).cast(pl.Float64).fill_nan(None),
            ],
            how="horizontal",
        ).with_columns(pl.when(ok).then(pl.exclude(features.KEYS)).name.keep())

    def params(self) -> dict[str, Any]:
        return {"candidate": self.name, "seed": self.seed, **self.p.model_dump()}


class ZScore(Detector):
    name = "zscore"

    def fit(self, train: pl.DataFrame) -> ZScore:
        return self

    def _score(self, frame: pl.DataFrame) -> tuple[np.ndarray, pl.DataFrame]:
        contrib = frame.select(features.oriented(self.kpis))
        return contrib.select(pl.max_horizontal(pl.all())).to_series().to_numpy(), contrib


class IForest(Detector):
    name = "iforest"
    p: IForestParams

    def __init__(self, seed: int, p: IForestParams) -> None:
        super().__init__(seed, p)
        self.model = IsolationForest(
            n_estimators=p.n_estimators, max_samples=p.max_samples, random_state=seed
        )

    def fit(self, train: pl.DataFrame) -> IForest:
        self.model.fit(_train_matrix(train, self.columns, self.p.max_train_rows, self.seed))
        return self

    def _score(self, frame: pl.DataFrame) -> tuple[np.ndarray, pl.DataFrame]:
        scores = -self.model.score_samples(_matrix(frame, self.columns))
        return scores, frame.select(features.oriented(self.kpis))


class Autoencoder(Detector):
    name = "autoencoder"
    p: AutoencoderParams

    def __init__(self, seed: int, p: AutoencoderParams) -> None:
        super().__init__(seed, p)
        self.scaler = StandardScaler()
        self.model = MLPRegressor(
            hidden_layer_sizes=(p.hidden,), max_iter=p.max_iter, random_state=seed
        )
        by_kpi = features.feature_columns_by_kpi(features.load_config())
        self.kpi_slices = {k: [self.columns.index(c) for c in cols] for k, cols in by_kpi.items()}

    def fit(self, train: pl.DataFrame) -> Autoencoder:
        x = self.scaler.fit_transform(
            _train_matrix(train, self.columns, self.p.max_train_rows, self.seed)
        )
        self.model.fit(x, x)
        return self

    def _score(self, frame: pl.DataFrame) -> tuple[np.ndarray, pl.DataFrame]:
        rows = self.scaler.transform(_matrix(frame, self.columns))
        n = rows.shape[0]
        x = np.zeros((n + (-n % SCORE_BLOCK), rows.shape[1]))  # zero rows pad the last chunk
        x[:n] = rows
        chunks = [self.model.predict(x[i : i + SCORE_BLOCK]) for i in range(0, len(x), SCORE_BLOCK)]
        recon = np.concatenate(chunks) if chunks else x
        err = ((x - recon) ** 2)[:n]
        contrib = pl.DataFrame({k: err[:, idx].mean(axis=1) for k, idx in self.kpi_slices.items()})
        return err.mean(axis=1), contrib


_CLASSES: dict[str, type[Detector]] = {c.name: c for c in (ZScore, IForest, Autoencoder)}
CANDIDATES = tuple(_CLASSES)


def build(name: str, cfg: Config, overrides: Mapping[str, Any] | None = None) -> Detector:
    """A fresh, unfitted candidate by name, with its parameters from `cfg.candidates.<name>`,
    `overrides` (e.g. an Optuna trial's values) replacing some of them, validated."""
    if name not in _CLASSES:
        raise ValueError(f"unknown detector {name!r}; one of {sorted(_CLASSES)}")
    defaults: FrozenModel = getattr(cfg.candidates, name)
    params = type(defaults).model_validate({**defaults.model_dump(), **(overrides or {})})
    return _CLASSES[name](cfg.seed, params)


def alarms(scored: pl.DataFrame, threshold: float, p: AlarmParams) -> pl.DataFrame:
    """Rows with `score >= threshold`: `cell_id`, `site_id`, `interval_start`, `score`, and
    `kpis`, the contributing KPIs (share of the top contribution >= `p.kpi_share`, at most
    `p.max_kpis`, largest first; the top KPI alone when no contribution is positive)."""
    hits = scored.filter(pl.col("score") >= threshold).with_row_index("_row")
    contrib = [c for c in hits.columns if c.startswith(CONTRIB)]
    top = pl.col("v").max().over("_row")
    kpis = (
        hits.unpivot(index="_row", on=contrib, variable_name="kpi", value_name="v")
        .drop_nulls("v")
        .filter(
            pl.when(top > 0).then(pl.col("v") >= top * p.kpi_share).otherwise(pl.col("v") == top)
        )
        .sort(["_row", "v"], descending=[False, True])
        .group_by("_row", maintain_order=True)
        .agg(pl.col("kpi").str.strip_prefix(CONTRIB).head(p.max_kpis).alias("kpis"))
    )
    return (
        hits.join(kpis, on="_row", how="left")
        .select("cell_id", "site_id", "interval_start", "score", "kpis")
        .sort("interval_start", "cell_id")
    )
