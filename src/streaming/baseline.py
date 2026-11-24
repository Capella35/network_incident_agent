"""Seasonal KPI baseline per cell, weekday, and hour (milestone 2, step 3; specs/03-detection.md).

    python -m streaming.baseline [--data-dir data]     # fit on processed training days

For every real and MODELED KPI: mean and std over the clean training weeks (Rule 7: never
validation or test days; no injected faults), per (cell, weekday, hour) in Europe/Rome. Gap
rows are left out. The std is floored (`features.yaml` -> baseline) so near-constant KPIs don't
turn noise into huge z-scores. Writes `data/processed/baseline.parquet` and a `.meta.json`
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
from producer import data_prep, integrity, kpi_model, manifest, replay
from streaming.features import (
    BUCKET,
    LOCAL_TIME,
    Config,
    feature_kpis,
    load_config,
    with_kpis_and_bucket,
)

log = logging.getLogger("streaming.baseline")

BASELINE_FILE = "baseline.parquet"  # in data/processed/


def _provenance(data_dir: Path, cfg: Config) -> dict[str, Any]:
    """What the baseline depends on besides the (immutable, verified) training traffic: if any
    of it changes, the baseline must be refitted."""
    processed = data_dir / "processed"
    return {
        "config": cfg.model_dump(mode="json"),
        "kpis": {k.name: k.model_dump(mode="json") for k in feature_kpis()},
        "kpi_model_params": kpi_model.load_params().model_dump(mode="json"),
        "calibration_sha256": integrity.sha256_file(processed / kpi_model.CALIBRATION_FILE),
        "topology_nodes_sha256": integrity.sha256_file(processed / "topology_nodes.csv"),
    }


def fit(rows: pl.DataFrame, cfg: Config) -> pl.DataFrame:
    """One row per (cell, weekday, hour): `<kpi>_mean` and `<kpi>_std` (floored).
    Raises if any row is outside the training weeks (Rule 7)."""
    for d in rows.select(LOCAL_TIME.dt.date().unique()).to_series():
        splits.require(d, splits.Split.TRAIN)
    floor = cfg.baseline
    stats: list[pl.Expr] = []
    for k in feature_kpis():
        mean, std = pl.col(k.name).mean(), pl.col(k.name).std()
        stats += [
            mean.alias(f"{k.name}_mean"),
            pl.max_horizontal(
                std, mean.abs() * floor.std_floor_rel, pl.lit(floor.std_floor_abs)
            ).alias(f"{k.name}_std"),
        ]
    return (
        with_kpis_and_bucket(rows)
        .filter(~pl.col("data_gap"))
        .group_by(BUCKET)
        .agg(stats)
        .sort(BUCKET)
    )


def run(data_dir: Path) -> Path:
    """Fit on every processed training day present; write the baseline and its sidecar."""
    processed = data_dir / "processed"
    paths = data_prep.require_processed_days(data_dir, splits.Split.TRAIN)
    days = list(paths)
    cfg = load_config()
    base = fit(replay.telemetry_for(data_dir, days).rows, cfg)
    out = processed / BASELINE_FILE
    base.write_parquet(out)
    manifest.save(
        out.with_suffix(".meta.json"),
        {
            "modeled": True,
            "description": (
                "Seasonal mean/std per cell, weekday, hour (Europe/Rome) of real and MODELED "
                "KPIs, from clean training weeks."
            ),
            "train_days": [d.isoformat() for d in days],
            "traffic_sha256": {p.name: integrity.sha256_file(p) for p in paths.values()},
            "buckets": base.height,
            **_provenance(data_dir, cfg),
        },
    )
    log.info(
        "baseline: %d buckets from %d training day(s) (%s .. %s) -> %s",
        base.height,
        len(days),
        days[0],
        days[-1],
        out,
    )
    return out


def load(data_dir: Path) -> pl.DataFrame:
    """The fitted baseline. Raises if it is missing, or stale: fitted under another feature
    config, KPI catalog, KPI model, calibration, or topology than the current ones."""
    path = data_dir / "processed" / BASELINE_FILE
    meta_path = path.with_suffix(".meta.json")
    if not path.exists() or not meta_path.exists():
        raise FileNotFoundError(f"{path} missing: run `make baseline`")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    current = _provenance(data_dir, load_config())
    if stale := sorted(k for k, v in current.items() if meta.get(k) != v):
        raise ValueError(f"{path} is stale ({', '.join(stale)} changed): run `make baseline`")
    return pl.read_parquet(path)


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
