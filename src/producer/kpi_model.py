"""Real and MODELED per-cell KPIs (milestone 1, step 5; docs/domain-model.md §3).

    python -m producer.kpi_model [--data-dir data]     # calibrate on processed training days

Real KPIs are sums of Milan fields, as listed in `kpis.yaml`. Modeled KPIs are computed from real
load: `load_ratio` = load / capacity, where each cell's capacity comes from its own
*training-week* load; `latency_ms` follows a queueing curve and `drop_rate` a logistic rise as
the cell nears saturation. Noise is seeded per (cell, interval), so the same input always gives
the same output, whatever the batch or order. Rows flagged as data gaps get no modeled values.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import struct
import sys
from datetime import date
from importlib.resources import files
from pathlib import Path
from statistics import NormalDist
from typing import Any

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field

from common import splits
from common.domain import load_domain
from common.domain.loader import KpiSource
from producer import integrity, topology

log = logging.getLogger("producer.kpi_model")

CALIBRATION_FILE = "kpi_calibration.json"
_NORMAL = NormalDist()


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CapacityParams(_Frozen):
    quantile: float = Field(gt=0, le=1)
    headroom: float = Field(gt=0)
    min_fraction_of_median: float = Field(gt=0, le=1)


class LatencyParams(_Frozen):
    base: float = Field(gt=0)
    max_utilisation: float = Field(gt=0, lt=1)
    noise_sigma: float = Field(ge=0)


class DropParams(_Frozen):
    base: float = Field(ge=0, le=1)
    max: float = Field(gt=0, le=1)
    midpoint: float = Field(gt=0)
    width: float = Field(gt=0)
    noise_sigma: float = Field(ge=0)


class Params(_Frozen):
    version: int = Field(ge=1, le=1)
    seed: int
    load_kpi: str
    capacity: CapacityParams
    latency_ms: LatencyParams
    drop_rate: DropParams


def load_params(path: Path | None = None) -> Params:
    text = (
        path.read_text(encoding="utf-8")
        if path is not None
        else files("producer").joinpath("kpi_model.yaml").read_text(encoding="utf-8")
    )
    params = Params(**yaml.safe_load(text))
    domain = load_domain()
    if domain.kpi(params.load_kpi).source is not KpiSource.REAL:
        raise ValueError(f"load_kpi {params.load_kpi!r} must be a real KPI")
    for name in ("load_ratio", "latency_ms", "drop_rate"):
        if domain.kpi(name).source is not KpiSource.MODELED:
            raise ValueError(f"KPI {name!r} is not marked modeled in kpis.yaml")
    return params


def add_real_kpis(frame: pl.DataFrame) -> pl.DataFrame:
    """Add each real KPI as the sum of its Milan fields (mapping from `kpis.yaml`)."""
    real = [k for k in load_domain().kpis.values() if k.source is KpiSource.REAL]
    return frame.with_columns(
        pl.sum_horizontal(*[pl.col(f) for f in k.milan_fields]).alias(k.name) for k in real
    )


def calibrate(
    days: dict[date, pl.DataFrame], cells: dict[int, str], params: Params
) -> dict[str, Any]:
    """Per-cell capacity from training days only (Rule 7: never validation or test days)."""
    if not days:
        raise ValueError("no training days to calibrate on")
    for d in days:
        splits.require(d, splits.Split.TRAIN)
    frame = add_real_kpis(pl.concat(list(days.values())))
    cap = params.capacity
    per_square = (
        frame.filter(~pl.col("data_gap"))
        .group_by("square_id")
        .agg(pl.col(params.load_kpi).quantile(cap.quantile, interpolation="linear").alias("q"))
    )
    q = dict(zip(per_square["square_id"].to_list(), per_square["q"].to_list(), strict=True))
    missing = sorted(set(cells) - set(q))
    if missing:
        raise ValueError(f"no training data for squares {missing[:5]}")
    qs = sorted(q[s] for s in cells)
    floor = qs[len(qs) // 2] * cap.min_fraction_of_median
    capacity = {cells[s]: max(q[s], floor) * cap.headroom for s in sorted(cells)}
    return {
        "modeled": True,
        "description": (
            "MODELED cell capacity: training-week load quantile x headroom. Not a real "
            "radio capacity; used to derive load_ratio, latency_ms, drop_rate."
        ),
        "params": params.model_dump(),
        "train_days": sorted(d.isoformat() for d in days),
        "capacity": capacity,
    }


def _unit_noise(seed: int, salt: str, squares: list[int], times_ms: list[int]) -> list[float]:
    """Standard-normal noise that depends only on (seed, salt, square, interval)."""
    out = []
    prefix = f"{seed}:{salt}:".encode()
    for s, t in zip(squares, times_ms, strict=True):
        h = hashlib.blake2b(prefix + struct.pack(">iq", s, t), digest_size=8).digest()
        u = (int.from_bytes(h, "big") + 0.5) / 2**64  # strictly inside (0, 1)
        out.append(_NORMAL.inv_cdf(u))
    return out


def add_kpis(
    frame: pl.DataFrame,
    cells: dict[int, str],
    calibration: dict[str, Any],
    params: Params,
) -> pl.DataFrame:
    """Add cell_id, real KPIs, and modeled KPIs to processed traffic rows."""
    unknown = set(frame["square_id"].unique().to_list()) - set(cells)
    if unknown:
        raise ValueError(f"squares not in the topology: {sorted(unknown)[:5]}")
    capacity: dict[str, float] = calibration["capacity"]
    lat, drop = params.latency_ms, params.drop_rate

    squares = frame["square_id"].to_list()
    times = frame["interval_start"].dt.epoch("ms").to_list()
    out = add_real_kpis(frame).with_columns(
        pl.col("square_id").replace_strict(cells, return_dtype=pl.String).alias("cell_id"),
        pl.Series("_z_lat", _unit_noise(params.seed, "latency", squares, times)),
        pl.Series("_z_drop", _unit_noise(params.seed, "drop", squares, times)),
    )
    rho = pl.col(params.load_kpi) / pl.col("cell_id").replace_strict(
        capacity, return_dtype=pl.Float64
    )
    out = out.with_columns(rho.alias("load_ratio"))
    rho_c = pl.col("load_ratio").clip(upper_bound=lat.max_utilisation)
    latency = lat.base / (1.0 - rho_c) * (pl.col("_z_lat") * lat.noise_sigma).exp()
    logistic = 1.0 / (1.0 + (-(pl.col("load_ratio") - drop.midpoint) / drop.width).exp())
    drop_rate = (drop.base + (drop.max - drop.base) * logistic) * (
        pl.col("_z_drop") * drop.noise_sigma
    ).exp()
    gap = pl.col("data_gap")
    return out.with_columns(
        pl.when(gap).then(None).otherwise(pl.col("load_ratio")).alias("load_ratio"),
        pl.when(gap).then(None).otherwise(latency).alias("latency_ms"),
        pl.when(gap).then(None).otherwise(drop_rate.clip(0.0, 1.0)).alias("drop_rate"),
    ).drop("_z_lat", "_z_drop")


def run(data_dir: Path, params_path: Path | None = None) -> dict[str, Any]:
    """Calibrate on every processed training day present; write `kpi_calibration.json`."""
    processed = data_dir / "processed"
    nodes = processed / "topology_nodes.csv"
    if not nodes.exists():
        raise FileNotFoundError(f"{nodes} missing: run `make data` first")
    cells = topology.load_cells(nodes)
    params = load_params(params_path)

    train = set(splits.days(splits.Split.TRAIN))
    paths = {
        date.fromisoformat(p.stem): p
        for p in sorted((processed / "traffic").glob("*.parquet"))
        if date.fromisoformat(p.stem) in train  # validation/test files are never read here
    }
    if not paths:
        raise FileNotFoundError(
            "no processed training days (2013-11-04 onwards) in data/processed/traffic: "
            "download and `make data` them first"
        )
    calib = calibrate({d: pl.read_parquet(p) for d, p in paths.items()}, cells, params)
    calib["inputs_sha256"] = {d.isoformat(): integrity.sha256_file(p) for d, p in paths.items()}
    calib["topology_nodes_sha256"] = integrity.sha256_file(nodes)
    out = processed / CALIBRATION_FILE
    out.write_text(json.dumps(calib, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    sample = add_kpis(pl.read_parquet(next(iter(paths.values()))), cells, calib, params)
    log.info("calibrated on %d training day(s): %s", len(paths), ", ".join(calib["train_days"]))
    for k in ("load_ratio", "latency_ms", "drop_rate"):
        s = sample[k].drop_nulls()
        log.info(
            "  %-10s p50 %.4g  p99 %.4g  max %.4g  (modeled, first day)",
            k,
            s.quantile(0.5),
            s.quantile(0.99),
            s.max(),
        )
    return calib


def load_calibration(data_dir: Path) -> dict[str, Any]:
    path = data_dir / "processed" / CALIBRATION_FILE
    if not path.exists():
        raise FileNotFoundError(f"{path} missing: run `make calibrate`")
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return doc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--params", type=Path, default=None, help="override kpi_model.yaml")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.data_dir, args.params)
    except (FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
