from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import fmean, pstdev

import polars as pl
import pytest

from producer import grid, kpi_model, milan, topology
from tests.producer.conftest import write_grid

CELLS = {1: "CELL-1-1", 2: "CELL-1-2", 3: "CELL-1-3"}
FIELDS = milan.ACTIVITY_FIELDS


def traffic(
    day: date, internet: dict[int, list[float]], gaps: set[tuple[int, int]] = frozenset()
) -> pl.DataFrame:
    """Processed-traffic-shaped frame: one row per (square, interval index)."""
    start = datetime.combine(day, datetime.min.time(), UTC)
    rows = []
    for sq, values in internet.items():
        for i, v in enumerate(values):
            gap = (sq, i) in gaps
            rows.append(
                {
                    "square_id": sq,
                    "interval_start": start + timedelta(minutes=10 * i),
                    "sms_in": 0.0 if gap else 1.0,
                    "sms_out": 0.0 if gap else 2.0,
                    "call_in": 0.0 if gap else 3.0,
                    "call_out": 0.0 if gap else 4.0,
                    "internet": 0.0 if gap else v,
                    "data_gap": gap,
                }
            )
    return pl.DataFrame(
        rows,
        schema={
            "square_id": pl.Int32,
            "interval_start": pl.Datetime("ms", "UTC"),
            **{f: pl.Float64 for f in FIELDS},
            "data_gap": pl.Boolean,
        },
    )


@pytest.fixture
def params() -> kpi_model.Params:
    return kpi_model.load_params()


@pytest.fixture
def quiet(params: kpi_model.Params) -> kpi_model.Params:
    """Same params without noise, for exact checks."""
    return params.model_copy(
        update={
            "latency_ms": params.latency_ms.model_copy(update={"noise_sigma": 0.0}),
            "drop_rate": params.drop_rate.model_copy(update={"noise_sigma": 0.0}),
        }
    )


@pytest.fixture
def calib(params: kpi_model.Params) -> dict:
    ramp = [float(v) for v in range(1, 101)]  # q99 (linear) = 99.01
    days = {date(2013, 11, 4): traffic(date(2013, 11, 4), {1: ramp, 2: ramp, 3: [0.0] * 100})}
    return kpi_model.calibrate(days, CELLS, params)


def test_params_load_and_reference_domain(params: kpi_model.Params) -> None:
    assert params.load_kpi == "traffic_internet"


def test_real_kpis_from_domain_mapping() -> None:
    f = kpi_model.add_real_kpis(traffic(date(2013, 11, 4), {1: [10.0]}))
    row = f.row(0, named=True)
    assert row["traffic_internet"] == 10.0
    assert row["traffic_voice"] == 7.0  # call_in + call_out
    assert row["traffic_sms"] == 3.0


def test_calibration_capacity_and_floor(calib: dict) -> None:
    cap = calib["capacity"]
    assert cap["CELL-1-1"] == pytest.approx(99.01 * 1.25)
    # silent cell gets the floor: 10% of the median q99, times headroom
    assert cap["CELL-1-3"] == pytest.approx(99.01 * 0.1 * 1.25)
    assert calib["modeled"] is True
    assert calib["train_days"] == ["2013-11-04"]


@pytest.mark.parametrize("day", [date(2013, 12, 5), date(2013, 12, 20), date(2013, 11, 1)])
def test_calibration_refuses_non_training_days(day: date, params: kpi_model.Params) -> None:
    with pytest.raises(ValueError, match="only train"):
        kpi_model.calibrate({day: traffic(day, {1: [1.0]})}, {1: "CELL-1-1"}, params)


def test_calibration_needs_every_cell(params: kpi_model.Params) -> None:
    days = {date(2013, 11, 4): traffic(date(2013, 11, 4), {1: [1.0]})}
    with pytest.raises(ValueError, match="no training data"):
        kpi_model.calibrate(days, CELLS, params)


def test_load_ratio_and_monotonic_curves(calib: dict, quiet: kpi_model.Params) -> None:
    loads = [0.0, 30.0, 60.0, 90.0, 110.0, 120.0, 500.0]
    f = kpi_model.add_kpis(traffic(date(2013, 11, 5), {1: loads}), CELLS, calib, quiet)
    cap = calib["capacity"]["CELL-1-1"]
    assert f["load_ratio"].to_list() == pytest.approx([v / cap for v in loads])
    lat, drop = f["latency_ms"].to_list(), f["drop_rate"].to_list()
    assert lat == sorted(lat) and drop == sorted(drop)
    assert lat[0] == pytest.approx(20.0)  # unloaded
    assert lat[-1] == pytest.approx(20.0 / (1 - 0.98))  # capped at max utilisation
    assert drop[0] < 0.01 and 0.15 < drop[-1] <= 0.2
    assert f["cell_id"].unique().to_list() == ["CELL-1-1"]


def test_gap_rows_have_no_modeled_values(calib: dict, params: kpi_model.Params) -> None:
    f = kpi_model.add_kpis(
        traffic(date(2013, 11, 5), {1: [50.0, 50.0]}, gaps={(1, 1)}), CELLS, calib, params
    )
    assert f["latency_ms"].to_list()[1] is None
    assert f["drop_rate"].to_list()[1] is None
    assert f["load_ratio"].to_list()[1] is None
    assert f["latency_ms"].to_list()[0] is not None


def test_noise_is_deterministic_and_order_independent(
    calib: dict, params: kpi_model.Params
) -> None:
    frame = traffic(date(2013, 11, 5), {1: [40.0] * 50, 2: [40.0] * 50})
    a = kpi_model.add_kpis(frame, CELLS, calib, params).sort("square_id", "interval_start")
    b = kpi_model.add_kpis(frame.reverse(), CELLS, calib, params).sort(
        "square_id", "interval_start"
    )
    assert a.equals(b)
    # noise varies between cells and intervals
    assert a["latency_ms"].n_unique() > 90


def test_noise_is_standard_normal() -> None:
    z = kpi_model._unit_noise(1, "x", list(range(20_000)), [0] * 20_000)
    assert abs(fmean(z)) < 0.03
    assert abs(pstdev(z) - 1.0) < 0.03


def test_unknown_square_rejected(calib: dict, params: kpi_model.Params) -> None:
    with pytest.raises(ValueError, match="not in the topology"):
        kpi_model.add_kpis(traffic(date(2013, 11, 5), {99: [1.0]}), CELLS, calib, params)


def test_drop_rate_stays_a_ratio(calib: dict, params: kpi_model.Params) -> None:
    f = kpi_model.add_kpis(traffic(date(2013, 11, 5), {1: [1e6] * 200}), CELLS, calib, params)
    assert f["drop_rate"].max() <= 1.0  # type: ignore[operator]


def _data_dir(tmp_path: Path) -> tuple[Path, list[int]]:
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    raw.mkdir()
    (processed / "traffic").mkdir(parents=True)
    write_grid(raw / "milano-grid.geojson")
    sub = grid.build_subset(raw / "milano-grid.geojson")
    (processed / "subset.json").write_text(json.dumps(sub))
    assert topology.main(["--data-dir", str(tmp_path)]) == 0
    return tmp_path, sub["square_ids"]


def test_run_uses_only_training_days(tmp_path: Path) -> None:
    data_dir, squares = _data_dir(tmp_path)
    ramp = [float(v) for v in range(1, 145)]
    for day in (date(2013, 11, 4), date(2013, 12, 20)):  # train + test
        traffic(day, dict.fromkeys(squares, ramp)).write_parquet(
            data_dir / "processed" / "traffic" / f"{day.isoformat()}.parquet"
        )
    assert kpi_model.main(["--data-dir", str(data_dir)]) == 0
    calib = kpi_model.load_calibration(data_dir)
    assert calib["train_days"] == ["2013-11-04"]  # test day never read
    assert list(calib["inputs_sha256"]) == ["2013-11-04"]
    assert len(calib["capacity"]) == 144


def test_run_without_training_days_fails(tmp_path: Path) -> None:
    data_dir, squares = _data_dir(tmp_path)
    traffic(date(2013, 11, 1), {key: [1.0] for key in squares}).write_parquet(
        data_dir / "processed" / "traffic" / "2013-11-01.parquet"
    )
    assert kpi_model.main(["--data-dir", str(data_dir)]) == 1
