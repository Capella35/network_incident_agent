"""Seasonal KPI baseline per cell, weekday, and hour (milestone 2, step 3; specs/03-detection.md).

    python -m streaming.baseline [--data-dir data]     # fit on processed training days

Fits the `features.Reference` on the clean training weeks (Rule 7: never validation or test
days; no injected faults), every KPI on its detection scale, gap rows left out:
- seasonal: mean and std per (cell, weekday, hour) in Europe/Rome of every KPI judged against
  the clock;
- load curve: mean and std of the load-curve KPI per equal-count bin of load.
Stds are floored (`features.std_floor`) so near-constant KPIs don't turn noise into huge
z-scores. Writes `data/processed/baseline.parquet`, `load_curve.parquet`, and a `.meta.json`
sidecar with the training days, input hashes, and config. MODELED KPIs give a MODELED baseline.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import polars as pl

from common import splits
from common.domain import load_domain
from common.domain.loader import Kpi
from producer import data_prep, integrity, kpi_model, manifest, replay
from streaming.features import (
    BUCKET,
    LOCAL_TIME,
    Config,
    Reference,
    clock_kpis,
    feature_kpis,
    load_bin,
    load_config,
    std_floor,
    with_kpis_and_bucket,
)

log = logging.getLogger("streaming.baseline")

BASELINE_FILE = "baseline.parquet"  # in data/processed/
LOAD_CURVE_FILE = "load_curve.parquet"


def _provenance(data_dir: Path, cfg: Config) -> dict[str, Any]:
    """What the baseline depends on besides the (immutable, verified) training traffic: if any
    of it changes, the baseline must be refitted."""
    processed = data_dir / "processed"
    return {
        "config": cfg.model_dump(mode="json"),
        "kpis": {k.name: k.model_dump(mode="json") for k in feature_kpis(cfg)},
        "kpi_model_params": kpi_model.load_params().model_dump(mode="json"),
        "calibration_sha256": integrity.sha256_file(processed / kpi_model.CALIBRATION_FILE),
        "topology_nodes_sha256": integrity.sha256_file(processed / "topology_nodes.csv"),
    }


def _mean_std(kpi: Kpi, cfg: Config, *, names: tuple[str, str]) -> list[pl.Expr]:
    """Aggregations: the KPI's mean and floored std, aliased `names`."""
    col = pl.col(kpi.name)
    return [
        col.mean().alias(names[0]),
        pl.max_horizontal(col.std(), std_floor(kpi, col.mean(), cfg)).alias(names[1]),
    ]


def _load_curve(frame: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """`features.Reference.load_curve` from non-gap rows on the detection scale. Bin edges are
    load quantiles; tied edges are merged, and a bin with no rows is left out (its loads fall in
    the next bin)."""
    curve = cfg.load_curve
    rows = frame.select(curve.load, curve.kpi).drop_nulls()  # only these two: memory (R1)
    if rows.is_empty():
        raise ValueError(f"no training rows with {curve.load} and {curve.kpi} for the load curve")
    quantiles = rows.select(
        pl.col(curve.load).quantile(i / curve.bins, interpolation="linear").alias(str(i))
        for i in range(1, curve.bins)
    )
    uppers = pl.Series("upper", [*sorted({float(q) for q in quantiles.row(0)}), float("inf")])
    kpi = load_domain().kpi(curve.kpi)
    return (
        rows.group_by(load_bin(uppers, cfg).alias("_bin"))
        .agg(_mean_std(kpi, cfg, names=("mean", "std")))
        .sort("_bin")
        .select(pl.lit(uppers).gather(pl.col("_bin")).alias("upper"), "mean", "std")
    )


def fit(rows: pl.DataFrame, cfg: Config) -> Reference:
    """Raises if any row is outside the training weeks (Rule 7)."""
    for d in rows.select(LOCAL_TIME.dt.date().unique()).to_series():
        splits.require(d, splits.Split.TRAIN)
    stats = [
        e
        for k in clock_kpis(cfg)
        for e in _mean_std(k, cfg, names=(f"{k.name}_mean", f"{k.name}_std"))
    ]
    frame = with_kpis_and_bucket(rows, cfg).filter(~pl.col("data_gap"))
    seasonal = frame.group_by(BUCKET).agg(stats).sort(BUCKET)
    return Reference(seasonal, _load_curve(frame, cfg))


def run(data_dir: Path) -> Path:
    """Fit on every processed training day present; write the baseline and its sidecar."""
    processed = data_dir / "processed"
    paths = data_prep.require_processed_days(data_dir, splits.Split.TRAIN)
    days = list(paths)
    cfg = load_config()
    ref = fit(replay.telemetry_for(data_dir, days).rows, cfg)
    out = processed / BASELINE_FILE
    # Sidecar last, after removing the old one: an interrupted refit reads as missing, never as
    # a seasonal baseline paired with another fit's load curve.
    out.with_suffix(".meta.json").unlink(missing_ok=True)
    ref.seasonal.write_parquet(out)
    ref.load_curve.write_parquet(processed / LOAD_CURVE_FILE)
    manifest.save(
        out.with_suffix(".meta.json"),
        {
            "modeled": True,
            "description": (
                "Seasonal mean/std per cell, weekday, hour (Europe/Rome) of real and MODELED "
                f"KPIs, and a MODELED load curve ({LOAD_CURVE_FILE}), from clean training weeks."
            ),
            "train_days": [d.isoformat() for d in days],
            "traffic_sha256": {p.name: integrity.sha256_file(p) for p in paths.values()},
            "buckets": ref.seasonal.height,
            "load_bins": ref.load_curve.height,
            **_provenance(data_dir, cfg),
        },
    )
    log.info(
        "baseline: %d buckets, %d load bins from %d training day(s) (%s .. %s) -> %s",
        ref.seasonal.height,
        ref.load_curve.height,
        len(days),
        days[0],
        days[-1],
        out,
    )
    return out


def load(data_dir: Path) -> Reference:
    """The fitted reference. Raises if it is missing, or stale: fitted under another feature
    config, KPI catalog, KPI model, calibration, or topology than the current ones."""
    path = data_dir / "processed" / BASELINE_FILE
    meta_path, curve_path = path.with_suffix(".meta.json"), path.with_name(LOAD_CURVE_FILE)
    if not all(p.exists() for p in (path, meta_path, curve_path)):
        raise FileNotFoundError(f"{path} missing: run `make baseline`")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    current = _provenance(data_dir, load_config())
    if stale := sorted(k for k, v in current.items() if meta.get(k) != v):
        raise ValueError(f"{path} is stale ({', '.join(stale)} changed): run `make baseline`")
    return Reference(pl.read_parquet(path), pl.read_parquet(curve_path))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.data_dir)
    except (FileNotFoundError, replay.ReplayError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
