"""`make data`: verify manually downloaded Milan files, then process them one day at a time.

    python -m producer.data_prep [--data-dir data] [--date 2013-11-01 ...] [--delete-raw] [--force]

Per day: check size + MD5 against the published value -> filter to the subset -> sum over
country codes -> gap-fill -> write `processed/traffic/<date>.parquet` -> record the manifest
entry -> optionally delete the raw file. Reads text only; never executes downloaded content.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from common import splits
from producer import grid, integrity, manifest, milan

log = logging.getLogger("producer.data_prep")

GRID_FILE = "milano-grid.geojson"
LICENSE = "ODbL-1.0"


class Paths:
    def __init__(self, data_dir: Path) -> None:
        self.raw = data_dir / "raw"
        self.processed = data_dir / "processed"
        self.traffic = self.processed / "traffic"
        self.subset = self.processed / "subset.json"
        self.manifest = data_dir / "manifest.json"

    def traffic_day(self, day: date) -> Path:
        return self.traffic / f"{day.isoformat()}.parquet"


def processed_days(data_dir: Path, split: splits.Split) -> dict[date, Path]:
    """The split's days that are processed on disk, with their traffic files, in date order.
    Only that split's files are looked at (Rule 7)."""
    paths = Paths(data_dir)
    return {d: p for d in splits.days(split) if (p := paths.traffic_day(d)).exists()}


def require_processed_days(data_dir: Path, split: splits.Split) -> dict[date, Path]:
    """`processed_days`, but at least one: raises FileNotFoundError naming the fix."""
    if days := processed_days(data_dir, split):
        return days
    raise FileNotFoundError(
        f"no processed {split} days in {Paths(data_dir).traffic}: run `make data`"
    )


# Derived inputs every feature depends on (topology, capacity calibration, seasonal baseline).
SNAPSHOT_DERIVED = ("topology_nodes.csv", "kpi_calibration.json", "baseline.parquet")


def snapshot(data_dir: Path, days: Iterable[date]) -> str:
    """SHA-256 over the manifest's hashes of these days' processed files plus the derived inputs
    (`SNAPSHOT_DERIVED`, "absent" if missing): one ID for the data a run used (MLflow,
    `evals/results-log.md`). Raises FileNotFoundError for a day not in the manifest."""
    paths = Paths(data_dir)
    recorded = {
        e["processed"]["output"]: e["processed"]["output_sha256"]
        for e in manifest.load(paths.manifest).get("files", {}).values()
        if "processed" in e
    }
    wanted = [paths.traffic_day(d).relative_to(data_dir).as_posix() for d in days]
    if missing := [w for w in wanted if w not in recorded]:
        raise FileNotFoundError(f"not in the manifest: {missing[:3]}; run `make data`")
    derived = [
        [name, integrity.sha256_file(f) if (f := paths.processed / name).exists() else "absent"]
        for name in SNAPSHOT_DERIVED
    ]
    blob = json.dumps([*([w, recorded[w]] for w in sorted(wanted)), *derived])
    return hashlib.sha256(blob.encode()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _source_entry(
    path: Path, spec: integrity.PublishedFile, hashes: integrity.Hashes
) -> dict[str, Any]:
    # Local calendar date of the file mtime = the day it was downloaded in the browser.
    mtime = datetime.fromtimestamp(path.stat().st_mtime, UTC).astimezone()
    return {
        "source_doi": spec.doi,
        "dataverse_file_id": spec.dataverse_file_id,
        "license": LICENSE,
        "downloaded_on": mtime.date().isoformat(),
        "size_bytes": hashes.size_bytes,
        "md5": hashes.md5,
        "md5_matches_published": True,
        "sha256": hashes.sha256,
        "verified_at": _now(),
    }


def prepare_grid(
    paths: Paths,
    published: dict[str, integrity.PublishedFile],
    doc: dict[str, Any],
    *,
    force: bool = False,
) -> list[int]:
    """Verify the grid, (re)write the subset file, and return the subset square IDs."""
    path = paths.raw / GRID_FILE
    if not path.exists():
        raise FileNotFoundError(f"{path} missing: download it manually (specs/02-telemetry.md)")
    spec, hashes = integrity.verify(path, published)
    changed = doc["files"].get(path.name, {}).get("sha256") != hashes.sha256
    if changed or force:
        manifest.upsert(doc, path.name, _source_entry(path, spec, hashes))
    subset = grid.build_subset(path)
    paths.subset.parent.mkdir(parents=True, exist_ok=True)
    paths.subset.write_text(json.dumps(subset, indent=2) + "\n", encoding="utf-8")
    log.info("subset: %d squares around square %d", subset["size"], subset["centre_square_id"])
    ids: list[int] = subset["square_ids"]
    return ids


def process_file(
    path: Path,
    subset: list[int],
    paths: Paths,
    published: dict[str, integrity.PublishedFile],
    doc: dict[str, Any],
    *,
    force: bool = False,
) -> bool:
    """Verify and process one daily file. Returns False if it was already up to date."""
    day = milan.file_date(path)
    out = paths.traffic / f"{day.isoformat()}.parquet"
    subset_sha = integrity.sha256_file(paths.subset)
    prev = doc["files"].get(path.name, {}).get("processed", {})

    spec, hashes = integrity.verify(path, published)
    entry = _source_entry(path, spec, hashes)
    up_to_date = (
        out.exists()
        and prev.get("subset_sha256") == subset_sha
        and prev.get("source_sha256") == hashes.sha256
        and prev.get("output_sha256") == integrity.sha256_file(out)
    )
    if up_to_date and not force:  # manifest left unchanged, so reruns cause no git diff
        log.info("%s: verified, already processed", path.name)
        return False

    result = milan.process_day(path, subset)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    result.frame.write_parquet(tmp, compression="zstd", statistics=False)
    tmp.replace(out)

    entry["processed"] = {
        "output": str(out.relative_to(paths.raw.parent)),
        "output_sha256": integrity.sha256_file(out),
        "source_sha256": hashes.sha256,
        "subset_sha256": subset_sha,
        "rows_in": result.rows_in,
        "rows_subset": result.rows_subset,
        "rows_out_of_day_dropped": result.rows_out_of_day,
        "rows_out": result.frame.height,
        "gap_rows": result.gap_rows,
        "processed_at": _now(),
    }
    manifest.upsert(doc, path.name, entry)
    log.info(
        "%s: %d rows in -> %d out (%d gaps, %d out-of-day dropped)",
        path.name,
        result.rows_in,
        result.frame.height,
        result.gap_rows,
        result.rows_out_of_day,
    )
    return True


def run(
    data_dir: Path,
    dates: list[date] | None = None,
    *,
    delete_raw: bool = False,
    force: bool = False,
) -> int:
    paths = Paths(data_dir)
    published = integrity.load_published()
    doc = manifest.load(paths.manifest)

    subset = prepare_grid(paths, published, doc, force=force)
    manifest.save(paths.manifest, doc)

    if dates:
        files = [paths.raw / f"sms-call-internet-mi-{d.isoformat()}.txt" for d in dates]
        missing = [f.name for f in files if not f.exists()]
        if missing:
            raise FileNotFoundError(f"not in {paths.raw}: {missing}")
    else:
        files = sorted(paths.raw.glob("sms-call-internet-mi-*.txt"))
    if not files:
        log.warning("no activity files in %s", paths.raw)

    for f in files:
        process_file(f, subset, paths, published, doc, force=force)
        manifest.save(paths.manifest, doc)  # after each day, so progress survives a crash
        if delete_raw:
            f.unlink()
            log.info("%s: raw file deleted", f.name)
    return len(files)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument(
        "--date",
        type=date.fromisoformat,
        action="append",
        dest="dates",
        help="process only this day (repeatable); default: every file in data/raw",
    )
    ap.add_argument(
        "--delete-raw",
        action="store_true",
        help="delete each raw file after it is verified and processed",
    )
    ap.add_argument("--force", action="store_true", help="reprocess even if up to date")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.data_dir, args.dates, delete_raw=args.delete_raw, force=args.force)
    except (integrity.IntegrityError, FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
