"""Compute detection features for a split and report size, time, and peak memory (M2 step 3).

    python -m streaming.features_check --split validation [--inject] [--data-dir data]

Uses the split's processed days and the current baseline (refused if stale). Telemetry comes
from `producer.replay.telemetry_for` (`--inject`: validation's SYNTHETIC faults). Writes nothing.
Kept apart from `streaming.features` so the feature code the Spark job imports does not pull in
the producer.
"""

from __future__ import annotations

import argparse
import logging
import resource
import sys
import time
from pathlib import Path

from common import splits
from producer import data_prep, replay
from streaming import baseline, features

log = logging.getLogger("streaming.features_check")


def _peak_rss_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10  # bytes on macOS, KiB


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    choices = [splits.Split.TRAIN, splits.Split.VALIDATION]  # test weeks: final eval only
    ap.add_argument("--split", type=splits.Split, required=True, choices=choices)
    ap.add_argument("--inject", action="store_true", help="the split's SYNTHETIC faults")
    args = ap.parse_args(argv)
    if args.inject and args.split is not splits.Split.VALIDATION:
        ap.error("--inject: only validation weeks get faults here (detectors train on clean weeks)")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = features.load_config()
    t0 = time.perf_counter()
    try:
        base = baseline.load(args.data_dir)
        days = list(data_prep.processed_days(args.data_dir, args.split))
        if not days:
            raise FileNotFoundError(f"no processed {args.split} days: run `make data`")
        rows = replay.telemetry_for(args.data_dir, days, inject=args.inject)
    except (FileNotFoundError, replay.ReplayError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    t1 = time.perf_counter()
    out = features.compute(rows.rows, base, cfg)
    t2 = time.perf_counter()
    log.info(
        "%s (%d days)%s: %d rows x %d features (%d gap rows); telemetry %.1fs, features %.1fs; "
        "peak memory %.0f MiB",
        args.split,
        len(days),
        f" + {rows.n_faults} SYNTHETIC faults" if args.inject else "",
        out.height,
        len(features.feature_columns(cfg)),
        out["data_gap"].sum(),
        t1 - t0,
        t2 - t1,
        _peak_rss_mib(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
