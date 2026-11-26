"""The ground-truth fault log: its only reader (Rule 6; tests/test_import_boundaries.py).

`make inject` writes one SYNTHETIC schedule per split to
`data/processed/ground_truth/<split>-<seed>.parquet` with a `.meta.json` sidecar holding its
fingerprint. Detectors, the correlator, the classifier, and the agent never see it; they get
scores from `evaluation.metrics` as numbers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from common import splits
from producer import injector


@dataclass(frozen=True)
class Fault:
    """One SYNTHETIC injected fault: its type, the cells it affects, and its window."""

    fault_id: str
    fault_type: str
    cells: tuple[str, ...]
    onset: datetime
    end: datetime  # exclusive


@dataclass(frozen=True)
class GroundTruth:
    faults: tuple[Fault, ...]
    fingerprint: str  # of the schedule that `make inject` wrote (injector.fingerprint)


def read(data_dir: Path, split: splits.Split) -> GroundTruth:
    """The split's faults, sorted by onset. FileNotFoundError if `make inject` has not run."""
    path = injector.ground_truth_path(data_dir / "processed", split, injector.load_config())
    if not path.exists():
        raise FileNotFoundError(f'{path} missing: run `make inject ARGS="--split {split}"`')
    meta = json.loads(path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    frame = pl.read_parquet(path, columns=["fault_id", "fault_type", "cells", "onset", "end"])
    faults = tuple(
        Fault(r["fault_id"], r["fault_type"], tuple(r["cells"]), r["onset"], r["end"])
        for r in frame.sort("onset", "fault_id").iter_rows(named=True)
    )
    return GroundTruth(faults, meta["fingerprint"])
