"""Hand-written baselines for the streaming tests (never Milan data)."""

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from streaming import features


def flat_baseline(cells: Iterable[str], mean: float = 1.0, std: float = 0.5) -> pl.DataFrame:
    """Every (weekday, hour) bucket for `cells`, each KPI with the same mean and std."""
    buckets = [(c, wd, h) for c in cells for wd in range(1, 8) for h in range(24)]
    frame = pl.DataFrame(
        buckets,
        schema=list(features.BUCKET),
        orient="row",
        schema_overrides={"weekday": pl.Int8, "hour": pl.Int8},
    )
    kpis = features.feature_kpis()
    return frame.with_columns(
        *[pl.lit(mean).alias(f"{k.name}_mean") for k in kpis],
        *[pl.lit(std).alias(f"{k.name}_std") for k in kpis],
    )
