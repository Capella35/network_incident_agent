"""Compute detection features for a split and report size, time, and peak memory (M2 step 3).

    python -m streaming.features_check --split validation [--inject] [--data-dir data]

Uses the split's processed days, with history, through `streaming.offline.features_for`
(`--inject`: validation's SYNTHETIC faults). Writes nothing.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from common import splits
from common.process import peak_rss_mib
from producer import data_prep, replay
from streaming import features, offline

log = logging.getLogger("streaming.features_check")


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
    t0 = time.perf_counter()
    try:
        days = list(data_prep.require_processed_days(args.data_dir, args.split))
        result = offline.features_for(args.data_dir, days, inject=args.inject)
    except (FileNotFoundError, replay.ReplayError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    out = result.frame
    log.info(
        "%s (%d days + %d history)%s: %d rows x %d features (%d gap rows); %.1fs; "
        "peak memory %.0f MiB",
        args.split,
        len(days),
        len(result.history_days),
        f" + {result.n_faults} SYNTHETIC faults (history days included)" if args.inject else "",
        out.height,
        len(features.feature_columns(features.load_config())),
        out["data_gap"].sum(),
        time.perf_counter() - t0,
        peak_rss_mib(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
