"""SYNTHETIC fault injection (milestone 2, step 2).

    python -m producer.injector --split validation      # or: make inject ARGS="--split validation"

Builds a seeded fault **schedule** per split from `injector_config.yaml`: fault type (by
weight), root node at one of the type's root levels (`faults.yaml`), onset, duration, severity,
and effect magnitudes; faults never overlap on a shared cell. The same seed, config, and topology
always give the same schedule (and the same `fingerprint`), so the replay regenerates it instead
of reading any file.

Effects are physical and come only from the injector's own parameters, never from the fault
signatures (Rule 15, enforced by tests/test_import_boundaries.py): traffic is scaled before the
modeled KPIs are computed, latency after (see the config for the formula).

`write_ground_truth` saves the schedule to `data/processed/ground_truth/<split>-<seed>.parquet`
(+ a `.meta.json` with its fingerprint) for `src/evaluation/` only (Rule 6). The injector also
produces the **change events** (operational data: one before each config push, plus harmless
decoys), which the replay writes to Postgres for its run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
from pydantic import Field, model_validator

from common import splits, telemetry
from common.domain import load_domain
from common.domain.loader import FrozenModel, read_yaml
from producer import manifest, milan, topology

log = logging.getLogger("producer.injector")

INTERVAL = telemetry.INTERVAL
GROUND_TRUTH_DIR = "ground_truth"  # under data/processed/; read only by src/evaluation/
CHANGE_DESCRIPTION = "Configuration change (SYNTHETIC)"
_MAX_TRIES = 200  # placement attempts per fault before giving up on that slot


class InjectorError(RuntimeError):
    pass


# --- Config ----------------------------------------------------------------------------------


class TypeParams(FrozenModel):
    weight: float = Field(gt=0)
    duration: tuple[int, int]
    traffic_factor: tuple[float, float]
    latency_factor: tuple[float, float] = (1.0, 1.0)
    ramp: tuple[int, int] = (1, 1)

    @model_validator(mode="after")
    def _ordered(self) -> TypeParams:
        for name in ("duration", "traffic_factor", "latency_factor", "ramp"):
            lo, hi = getattr(self, name)
            if lo > hi or lo < 0:
                raise ValueError(f"{name}: need 0 <= low <= high, got {(lo, hi)}")
        if self.duration[0] < 1 or self.ramp[0] < 1:
            raise ValueError("duration and ramp are at least 1 interval")
        return self


class Config(FrozenModel):
    seeds: dict[splits.Split, int]
    faults_per_day: tuple[int, int]
    decoy_changes_per_fault: int = Field(ge=0)
    change_lead_intervals: tuple[int, int]
    types: dict[str, TypeParams]

    @model_validator(mode="after")
    def _matches_domain(self) -> Config:
        real = {f.name for f in load_domain().faults.values() if f.is_real}
        if set(self.types) != real:
            raise ValueError(f"types must be exactly the real fault types {sorted(real)}")
        if set(self.seeds) != set(splits.Split):
            raise ValueError("one seed per split")
        return self


def load_config(path: Path | None = None) -> Config:
    raw = read_yaml(path or files("producer").joinpath("injector_config.yaml"))
    return Config(**{k: v for k, v in raw.items() if k != "version"})


# --- Schedule --------------------------------------------------------------------------------


@dataclass(frozen=True)
class InjectedFault:
    """One SYNTHETIC fault: where, when, how strong. The ground truth for evaluation."""

    fault_id: str
    fault_type: str
    root_node: str
    root_level: str
    cells: tuple[str, ...]
    onset: datetime
    duration: int  # intervals
    severity: str
    traffic_factor: float
    latency_factor: float
    ramp: int
    change_id: str | None = None

    @property
    def end(self) -> datetime:
        """Exclusive end of the fault window."""
        return self.onset + self.duration * INTERVAL

    def overlaps(self, other: InjectedFault) -> bool:
        shares_cells = not set(self.cells).isdisjoint(other.cells)
        return shares_cells and self.onset < other.end and other.onset < self.end


@dataclass(frozen=True)
class ChangeEvent:
    """A change-log entry the system may see (SYNTHETIC). Not ground truth: most are decoys."""

    change_id: str
    site_id: str
    applied_at: datetime


def _roots(index: topology.Index, level: str) -> list[str]:
    """Nodes of a level that have at least one cell under them (a fault must affect something)."""
    roots = [n for n in index.of_type(level) if index.cells_under(n)]
    if not roots:
        raise InjectorError(f"no {level} with cells in the topology to root a fault on")
    return roots


def _draw(
    rng: random.Random, slots: Sequence[datetime], index: topology.Index, cfg: Config, fid: str
) -> InjectedFault:
    """One candidate fault on one day (the caller rejects it if it overlaps another)."""
    names = sorted(cfg.types)
    ftype = rng.choices(names, [cfg.types[t].weight for t in names])[0]
    fault, params = load_domain().fault(ftype), cfg.types[ftype]
    level = rng.choice(fault.root_levels)
    root = rng.choice(_roots(index, level))
    duration = rng.randint(*params.duration)
    return InjectedFault(
        fault_id=fid,
        fault_type=ftype,
        root_node=root,
        root_level=level,
        cells=index.cells_under(root),
        onset=slots[rng.randrange(len(slots) - duration + 1)],
        duration=duration,
        severity=str(rng.choice(fault.severities)),
        traffic_factor=rng.uniform(*params.traffic_factor),
        latency_factor=rng.uniform(*params.latency_factor),
        ramp=rng.randint(*params.ramp),
    )


def schedule(
    split: splits.Split, index: topology.Index, cfg: Config, *, final_eval: bool = False
) -> list[InjectedFault]:
    """The split's fault schedule. Deterministic in (split seed, config, topology)."""
    if split is splits.Split.TEST and not final_eval:
        raise InjectorError("test-week faults are generated once, for the final numbers")
    seed = cfg.seeds[split]
    rng = random.Random(seed)  # noqa: S311 - seeded simulation, reproducible on purpose
    change = load_domain().entity("ChangeEvent")
    placed: list[InjectedFault] = []
    n_changes = 0
    for day in splits.days(split):
        slots = milan.day_intervals(day)
        for _ in range(rng.randint(*cfg.faults_per_day)):
            fid = f"{split}-{seed}-{len(placed):03d}"
            tries = (_draw(rng, slots, index, cfg, fid) for _ in range(_MAX_TRIES))
            f = next((c for c in tries if not any(c.overlaps(p) for p in placed)), None)
            if f is None:
                log.warning("%s: no free slot for a fault on %s; skipped", split, day)
                continue
            if load_domain().fault(f.fault_type).needs_change_event:
                n_changes += 1
                f = replace(f, change_id=change.make_id(n=n_changes))
            placed.append(f)
    return placed


def change_events(
    faults: Sequence[InjectedFault], index: topology.Index, cfg: Config, split: splits.Split
) -> list[ChangeEvent]:
    """One change before each config push (`change_lead_intervals` earlier) + random decoys."""
    # own stream, so decoys never shift the schedule; seeded simulation, not cryptography
    rng = random.Random(cfg.seeds[split] + 1)  # noqa: S311
    change = load_domain().entity("ChangeEvent")
    real = [
        (f.change_id, f.root_node, f.onset - rng.randint(*cfg.change_lead_intervals) * INTERVAL)
        for f in faults
        if f.change_id is not None
    ]
    sites = index.of_type(index.site_type)
    slots = [t for day in splits.days(split) for t in milan.day_intervals(day)]
    decoys = [
        (change.make_id(n=len(real) + 1 + i), rng.choice(sites), rng.choice(slots))
        for i in range(len(faults) * cfg.decoy_changes_per_fault)
    ]
    events = [ChangeEvent(*e) for e in (*real, *decoys)]
    return sorted(events, key=lambda e: (e.applied_at, e.change_id))


def for_days(
    split: splits.Split,
    days: Sequence[date],
    index: topology.Index,
    cfg: Config,
    *,
    final_eval: bool = False,
) -> tuple[tuple[InjectedFault, ...], list[ChangeEvent], str]:
    """The split's faults that touch `days`, the change events logged on those days, and the
    fingerprint of the whole schedule (to check it against the ground truth file)."""
    whole = schedule(split, index, cfg, final_eval=final_eval)
    start = milan.day_intervals(days[0])[0]
    end = milan.day_intervals(days[-1])[-1] + INTERVAL
    faults, events = in_window(whole, change_events(whole, index, cfg, split), start, end)
    return faults, events, fingerprint(whole)


def in_window(
    faults: Sequence[InjectedFault], events: Sequence[ChangeEvent], start: datetime, end: datetime
) -> tuple[tuple[InjectedFault, ...], list[ChangeEvent]]:
    """Faults overlapping [start, end); change events logged then, plus each kept fault's own
    change (it may fall just before `start`)."""
    kept = tuple(f for f in faults if f.onset < end and f.end > start)
    theirs = {f.change_id for f in kept if f.change_id is not None}
    return kept, [e for e in events if start <= e.applied_at < end or e.change_id in theirs]


def check_schedule(processed: Path, split: splits.Split, cfg: Config, fp: str) -> None:
    """Refuse a schedule that differs from the ground truth written by `make inject` (config or
    topology changed since): evaluation would score against the wrong labels."""
    meta_path = processed / GROUND_TRUTH_DIR / f"{split}-{cfg.seeds[split]}.meta.json"
    if not meta_path.exists():
        log.warning('no ground truth for %s yet; run `make inject ARGS="--split %s"`', split, split)
        return
    stored = json.loads(meta_path.read_text(encoding="utf-8")).get("fingerprint")
    if stored != fp:
        raise InjectorError(
            f"{split} schedule differs from its ground truth file (config or topology changed); "
            f're-run `make inject ARGS="--split {split}"`'
        )


def fingerprint(faults: Iterable[InjectedFault]) -> str:
    """SHA-256 of the schedule's content (not of any file): equal iff the schedules are equal."""
    rows = [{**asdict(f), "onset": f.onset.isoformat()} for f in faults]
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


# --- Effects ---------------------------------------------------------------------------------


def effects(faults: Iterable[InjectedFault], cell_squares: dict[str, int]) -> pl.DataFrame:
    """Per (square_id, interval_start): traffic and latency multipliers (product if several)."""
    rows: list[tuple[int, datetime, float, float]] = []
    for f in faults:
        for k in range(f.duration):
            intensity = min(1.0, (k + 1) / f.ramp)
            t = f.onset + k * INTERVAL
            tm = 1 + intensity * (f.traffic_factor - 1)
            lm = 1 + intensity * (f.latency_factor - 1)
            rows.extend((_square(cell_squares, c), t, tm, lm) for c in f.cells)
    frame = pl.DataFrame(
        rows,
        schema={
            "square_id": pl.Int32,
            "interval_start": pl.Datetime("ms", "UTC"),
            "traffic_mult": pl.Float64,
            "latency_mult": pl.Float64,
        },
        orient="row",
    )
    return frame.group_by("square_id", "interval_start").agg(
        pl.col("traffic_mult").product(), pl.col("latency_mult").product()
    )


def _square(cell_squares: dict[str, int], cell: str) -> int:
    try:
        return cell_squares[cell]
    except KeyError:
        raise InjectorError(f"{cell} has no Milan square in the topology") from None


def inject(
    traffic: pl.DataFrame,
    fx: pl.DataFrame,
    kpi_model: Callable[[pl.DataFrame], pl.DataFrame],
) -> pl.DataFrame:
    """Traffic x multiplier -> `kpi_model` -> latency x multiplier. Data-gap rows stay all-zero
    with null modeled values (0 x anything = 0; null x anything = null)."""
    key = ["square_id", "interval_start"]
    scaled = (
        traffic.join(fx.select(*key, "traffic_mult"), on=key, how="left")
        .with_columns(
            pl.col(f) * pl.col("traffic_mult").fill_null(1.0) for f in telemetry.TRAFFIC_FIELDS
        )
        .drop("traffic_mult")
    )
    rows = kpi_model(scaled)
    return (
        rows.join(fx.select(*key, "latency_mult"), on=key, how="left")
        .with_columns(pl.col("latency_ms") * pl.col("latency_mult").fill_null(1.0))
        .drop("latency_mult")
    )


# --- Outputs ---------------------------------------------------------------------------------


def _ground_truth_frame(faults: Sequence[InjectedFault]) -> pl.DataFrame:
    domain = load_domain()
    return pl.DataFrame(
        [{**asdict(f), "end": f.end, "team": domain.fault(f.fault_type).team} for f in faults],
        schema_overrides={"onset": pl.Datetime("ms", "UTC"), "end": pl.Datetime("ms", "UTC")},
    )


def write_ground_truth(
    faults: Sequence[InjectedFault], split: splits.Split, cfg: Config, processed: Path
) -> Path:
    """Write the schedule for src/evaluation/ (Rule 6), with a sidecar of what made it."""
    seed = cfg.seeds[split]
    path = processed / GROUND_TRUTH_DIR / f"{split}-{seed}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _ground_truth_frame(faults).write_parquet(path)
    manifest.save(
        path.with_suffix(".meta.json"),
        {
            "synthetic": True,
            "split": str(split),
            "seed": seed,
            "faults": len(faults),
            "fingerprint": fingerprint(faults),
            "config": cfg.model_dump(mode="json"),
        },
    )
    return path


def write_change_events(
    conn: psycopg.Connection[Any], replay_run_id: str, events: Sequence[ChangeEvent]
) -> None:
    """Upsert a run's change events (operational data, not ground truth)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO change_events (replay_run_id, change_id, site_id, applied_at, description)"
            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (replay_run_id, change_id, site_id)"
            " DO UPDATE SET applied_at = EXCLUDED.applied_at, description = EXCLUDED.description",
            [
                (replay_run_id, e.change_id, e.site_id, e.applied_at, CHANGE_DESCRIPTION)
                for e in events
            ],
        )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--split", type=splits.Split, required=True, choices=list(splits.Split))
    ap.add_argument("--final-eval", action="store_true", help="allow the test split (Rule 7)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    processed = args.data_dir / "processed"
    try:
        cfg = load_config()
        index = topology.load_index(processed)
        faults = schedule(args.split, index, cfg, final_eval=args.final_eval)
    except (InjectorError, FileNotFoundError, ValueError) as exc:  # ValueError: bad config
        log.error("%s", exc)
        return 1
    path = write_ground_truth(faults, args.split, cfg, processed)
    counts = dict(sorted(Counter(f.fault_type for f in faults).items()))
    log.info("%s: %d SYNTHETIC faults %s -> %s", args.split, len(faults), counts, path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
