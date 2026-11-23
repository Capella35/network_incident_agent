from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from common import splits
from common.domain import load_domain
from producer import grid, kpi_model, milan, replay, topology
from producer import injector as inj
from tests.producer.conftest import write_grid
from tests.producer.test_kpi_model import traffic

VAL = splits.Split.VALIDATION
DAY = splits.days(VAL)[0]
T0 = datetime.combine(DAY, datetime.min.time(), UTC)  # traffic() starts at UTC midnight


def _at(i: int) -> datetime:
    return T0 + i * inj.INTERVAL


def _node(node_id: str, node_type: str, parent: str | None, sq: int | None = None) -> topology.Node:
    return topology.Node(node_id, node_type, parent, 45.46, 9.19, sq)


@pytest.fixture
def index() -> topology.Index:
    """CORE-1 > AGG-1 > SITE-1, SITE-2; AGG-2 > SITE-3; 3 cells per site (squares 1..9)."""
    nodes = [_node("CORE-1", "Core", None)]
    nodes += [_node(a, "AggregationNode", "CORE-1") for a in ("AGG-1", "AGG-2")]
    for s, agg in ((1, "AGG-1"), (2, "AGG-1"), (3, "AGG-2")):
        nodes.append(_node(f"SITE-{s}", "Site", agg))
        nodes += [_node(f"CELL-{s}-{k}", "Cell", f"SITE-{s}", 3 * (s - 1) + k) for k in (1, 2, 3)]
    return topology.Index(nodes)


@pytest.fixture
def cfg() -> inj.Config:
    return inj.load_config()


CELL_SQUARES = {f"CELL-{s}-{k}": 3 * (s - 1) + k for s in (1, 2, 3) for k in (1, 2, 3)}

# SITE-1 loses power for 3 intervals from interval 10 (variants via dataclasses.replace)
SITE_POWER = inj.InjectedFault(
    fault_id="f-0",
    fault_type="site_power",
    root_node="SITE-1",
    root_level="Site",
    cells=("CELL-1-1", "CELL-1-2", "CELL-1-3"),
    onset=_at(10),
    duration=3,
    severity="major",
    traffic_factor=0.0,
    latency_factor=1.0,
    ramp=1,
)


# --- config and topology ---------------------------------------------------------------------


def test_config_covers_exactly_the_real_fault_types(cfg: inj.Config) -> None:
    assert set(cfg.types) == {f.name for f in load_domain().faults.values() if f.is_real}
    assert set(cfg.seeds) == set(splits.Split)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("duration: [3, 36]", "duration: [36, 3]"),  # low > high
        ("ramp: [3, 6]", "ramp: [0, 6]"),  # ramp below 1
        ("  backhaul:", "  fibre_cut:"),  # not a real fault type
    ],
)
def test_config_rejects_bad_values(tmp_path: Path, old: str, new: str) -> None:
    text = Path(inj.__file__).with_name("injector_config.yaml").read_text()
    assert old in text
    (tmp_path / "c.yaml").write_text(text.replace(old, new, 1))
    with pytest.raises(ValueError):
        inj.load_config(tmp_path / "c.yaml")


def test_cells_under_follow_the_topology(index: topology.Index) -> None:
    assert index.cells_under("CELL-2-2") == ("CELL-2-2",)
    assert index.cells_under("SITE-3") == ("CELL-3-1", "CELL-3-2", "CELL-3-3")
    assert len(index.cells_under("AGG-1")) == 6
    assert len(index.cells_under("CORE-1")) == 9


def test_index_loads_a_generated_topology(tmp_path: Path) -> None:
    """The real generator's output, not a hand-made CSV: catches a column change."""
    write_grid(tmp_path / "grid.geojson")
    squares = grid.load_squares(tmp_path / "grid.geojson")
    subset = grid.select_block(
        squares, grid.nearest_square(squares, *grid.DUOMO_LON_LAT), grid.SUBSET_BLOCK
    )
    topology.write(topology.build(squares, subset, topology.load_config()), tmp_path, {})
    index = topology.load_index(tmp_path)
    assert len(index.cells_by_square()) == 144 and len(index.cell_sites()) == 144
    assert len(index.cells_under(index.of_type("Core")[0])) == 144
    assert all(len(index.cells_under(s)) == 3 for s in index.of_type(index.site_type))


# --- schedule --------------------------------------------------------------------------------


def test_schedule_is_deterministic_and_seed_dependent(
    index: topology.Index, cfg: inj.Config
) -> None:
    a, b = inj.schedule(VAL, index, cfg), inj.schedule(VAL, index, cfg)
    assert a == b and a and inj.fingerprint(a) == inj.fingerprint(b)
    other = cfg.model_copy(update={"seeds": {**cfg.seeds, VAL: cfg.seeds[VAL] + 1}})
    assert inj.fingerprint(inj.schedule(VAL, index, other)) != inj.fingerprint(a)


def test_schedule_respects_domain_config_and_calendar(
    index: topology.Index, cfg: inj.Config
) -> None:
    domain = load_domain()
    split_slots = {t for d in splits.days(VAL) for t in milan.day_intervals(d)}
    faults = inj.schedule(VAL, index, cfg)
    for f in faults:
        spec, params = domain.fault(f.fault_type), cfg.types[f.fault_type]
        assert f.root_level in spec.root_levels and f.severity in spec.severities
        assert f.cells == index.cells_under(f.root_node)
        assert params.duration[0] <= f.duration <= params.duration[1]
        assert params.traffic_factor[0] <= f.traffic_factor <= params.traffic_factor[1]
        assert params.latency_factor[0] <= f.latency_factor <= params.latency_factor[1]
        assert params.ramp[0] <= f.ramp <= params.ramp[1]
        assert f.onset in split_slots and f.end - inj.INTERVAL in split_slots
        assert (f.change_id is not None) == spec.needs_change_event
    for i, f in enumerate(faults):
        assert not any(f.overlaps(g) for g in faults[i + 1 :])


def test_test_split_only_for_the_final_evaluation(index: topology.Index, cfg: inj.Config) -> None:
    with pytest.raises(inj.InjectorError, match="final numbers"):
        inj.schedule(splits.Split.TEST, index, cfg)
    assert inj.schedule(splits.Split.TEST, index, cfg, final_eval=True)


def test_change_events_precede_config_pushes_and_include_decoys(
    index: topology.Index, cfg: inj.Config
) -> None:
    faults = inj.schedule(VAL, index, cfg)
    events = inj.change_events(faults, index, cfg, VAL)
    pattern = load_domain().entity("ChangeEvent")
    assert all(pattern.matches(e.change_id) for e in events)
    assert len({e.change_id for e in events}) == len(events)
    by_id = {e.change_id: e for e in events}
    lo, hi = cfg.change_lead_intervals
    pushes = [f for f in faults if f.change_id is not None]
    for f in pushes:
        e = by_id[str(f.change_id)]
        assert e.site_id == f.root_node
        assert lo * inj.INTERVAL <= f.onset - e.applied_at <= hi * inj.INTERVAL
    assert len(events) == len(pushes) + len(faults) * cfg.decoy_changes_per_fault


def test_for_days_keeps_exactly_the_faults_touching_the_days(
    index: topology.Index, cfg: inj.Config
) -> None:
    days = splits.days(VAL)[2:4]
    faults, events, fp = inj.for_days(VAL, days, index, cfg)
    whole = inj.schedule(VAL, index, cfg)
    start, end = milan.day_intervals(days[0])[0], milan.day_intervals(days[-1])[-1]
    assert faults and fp == inj.fingerprint(whole)
    for f in whole:  # kept iff one of its intervals lies inside [start, end]
        inside = any(start <= f.onset + k * inj.INTERVAL <= end for k in range(f.duration))
        assert (f in faults) == inside
    assert events and all(start <= e.applied_at <= end for e in events)


def test_window_keeps_a_kept_faults_change_from_before_the_window() -> None:
    push = replace(SITE_POWER, fault_type="config_push", onset=_at(0), change_id="CHG-1")
    early = inj.ChangeEvent("CHG-1", "SITE-1", _at(0) - inj.INTERVAL)  # day before
    decoy = inj.ChangeEvent("CHG-2", "SITE-2", _at(0) - inj.INTERVAL)
    late = replace(SITE_POWER, fault_id="f-9", onset=_at(200))
    kept, events = inj.in_window([push, late], [early, decoy], _at(0), _at(144))
    assert kept == (push,)
    assert events == [early]  # its own change comes along; a decoy outside the window does not


def test_faults_only_rooted_where_there_are_cells(index: topology.Index) -> None:
    lonely = topology.Index([*index.nodes.values(), _node("SITE-9", "Site", "AGG-2")])
    assert "SITE-9" not in inj._roots(lonely, "Site")
    empty = topology.Index([_node("CORE-1", "Core", None)])
    with pytest.raises(inj.InjectorError, match="no Site with cells"):
        inj._roots(empty, "Site")


def test_cell_without_a_square_is_a_clear_error() -> None:
    with pytest.raises(inj.InjectorError, match="CELL-1-2 has no Milan square"):
        inj.effects([SITE_POWER], {"CELL-1-1": 1})


def test_replayed_schedule_must_match_its_ground_truth(
    tmp_path: Path, index: topology.Index, cfg: inj.Config, caplog: pytest.LogCaptureFixture
) -> None:
    faults = inj.schedule(VAL, index, cfg)
    fp = inj.fingerprint(faults)
    inj.check_schedule(tmp_path, VAL, cfg, fp)  # none written yet: a warning only
    assert "no ground truth" in caplog.text
    inj.write_ground_truth(faults, VAL, cfg, tmp_path)
    inj.check_schedule(tmp_path, VAL, cfg, fp)  # same schedule: fine
    with pytest.raises(inj.InjectorError, match="differs from its ground truth"):
        inj.check_schedule(tmp_path, VAL, cfg, inj.fingerprint(faults[1:]))


def test_offline_telemetry_never_carries_the_faults() -> None:
    """Rule 6: detectors and tuning get rows and counts from `telemetry_for`, not labels."""
    fields = set(replay.Telemetry.__dataclass_fields__)
    assert "faults" not in fields and "n_faults" in fields


# --- effects ---------------------------------------------------------------------------------


def _traffic(squares: list[int], gaps: frozenset[tuple[int, int]] = frozenset()) -> pl.DataFrame:
    return traffic(DAY, {sq: [10.0] * 20 for sq in squares}, gaps=set(gaps))


def _apply(frame: pl.DataFrame, faults: list[inj.InjectedFault]) -> pl.DataFrame:
    """Inject with a stand-in KPI model that adds a constant latency (the real one: below)."""

    def model(d: pl.DataFrame) -> pl.DataFrame:
        return d.with_columns(pl.lit(20.0).alias("latency_ms"))

    return inj.inject(frame, inj.effects(faults, CELL_SQUARES), model)


def test_effects_land_only_on_the_faulted_cells_and_window() -> None:
    out = _apply(_traffic([1, 2, 3, 4]), [replace(SITE_POWER, traffic_factor=0.02)])
    hit = (pl.col("square_id") <= 3) & pl.col("interval_start").is_between(
        _at(10), _at(12), closed="both"
    )
    assert out.filter(hit)["internet"].to_list() == pytest.approx([0.2] * 9)
    assert set(out.filter(~hit)["internet"].to_list()) == {10.0}


def test_ramp_reaches_full_effect_gradually() -> None:
    f = replace(SITE_POWER, cells=("CELL-1-1",), traffic_factor=0.5, latency_factor=2.0)
    fx = inj.effects([replace(f, duration=4, ramp=3)], CELL_SQUARES).sort("interval_start")
    assert fx["traffic_mult"].to_list() == pytest.approx([1 - 0.5 / 3, 1 - 1 / 3, 0.5, 0.5])
    assert fx["latency_mult"].to_list() == pytest.approx([1 + 1 / 3, 1 + 2 / 3, 2.0, 2.0])


def test_overlapping_effects_multiply() -> None:
    a = replace(SITE_POWER, cells=("CELL-1-1",), traffic_factor=0.5)
    fx = inj.effects([a, replace(a, fault_id="f-1")], CELL_SQUARES)
    assert fx["traffic_mult"].to_list() == pytest.approx([0.25] * 3)


def test_no_values_created_on_a_data_gap() -> None:
    f = replace(SITE_POWER, cells=("CELL-1-1",), traffic_factor=2.0)
    out = _apply(_traffic([1], gaps=frozenset({(1, 11)})), [f])
    assert out.filter(pl.col("interval_start") == _at(11))["internet"].to_list() == [0.0]


def test_replay_scales_traffic_before_and_latency_after_the_kpi_model() -> None:
    params = kpi_model.load_params()
    cells = {sq: c for c, sq in CELL_SQUARES.items()}
    sites = {c: c.rsplit("-", 1)[0].replace("CELL", "SITE") for c in CELL_SQUARES}
    calib = {"capacity": dict.fromkeys(CELL_SQUARES, 100.0)}  # fixed capacity, no fitting
    f = replace(SITE_POWER, cells=("CELL-1-1",), traffic_factor=2.0, latency_factor=1.5)
    fx = inj.effects([f], CELL_SQUARES)
    plain, _ = replay.build_rows(_traffic([1]), cells, sites, calib, params)
    faulted, _ = replay.build_rows(_traffic([1]), cells, sites, calib, params, fault_effects=fx)
    in_win = pl.col("interval_start").is_between(_at(10), _at(12), closed="both")
    p, q = plain.filter(in_win), faulted.filter(in_win)
    assert q["internet"].to_list() == pytest.approx([20.0] * 3)  # traffic x2
    assert (q["load_ratio"] / p["load_ratio"]).to_list() == pytest.approx([2.0] * 3)  # via model
    assert (q["latency_ms"] > p["latency_ms"] * 1.5).all()  # higher load, then x1.5
    assert faulted.filter(~in_win).equals(plain.filter(~in_win))
    assert faulted.columns == plain.columns


# --- outputs ---------------------------------------------------------------------------------


def test_ground_truth_file_and_sidecar(
    tmp_path: Path, index: topology.Index, cfg: inj.Config
) -> None:
    faults = inj.schedule(VAL, index, cfg)
    path = inj.write_ground_truth(faults, VAL, cfg, tmp_path)
    assert path == tmp_path / "ground_truth" / f"validation-{cfg.seeds[VAL]}.parquet"
    gt = pl.read_parquet(path)
    assert gt.height == len(faults)
    row = gt.row(0, named=True)
    assert row["end"] - row["onset"] == faults[0].duration * inj.INTERVAL
    assert row["team"] == load_domain().fault(row["fault_type"]).team
    meta = json.loads(path.with_suffix(".meta.json").read_text())
    assert meta["synthetic"] is True and meta["fingerprint"] == inj.fingerprint(faults)
    assert meta["faults"] == len(faults) and meta["seed"] == cfg.seeds[VAL]


def test_main_refuses_test_split(caplog: pytest.LogCaptureFixture) -> None:
    assert inj.main(["--split", "test"]) == 1
    assert "final numbers" in caplog.text
