from __future__ import annotations

import statistics

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ValidationError

from common.domain import load_domain
from common.domain.loader import BadDirection, KpiSource
from streaming import features as ft
from tests.factories import telemetry_frame
from tests.streaming.conftest import flat_baseline

CFG = ft.load_config()


def _compute(frame: pl.DataFrame, std: float = 0.5) -> pl.DataFrame:
    cells = frame["cell_id"].unique().to_list()
    return ft.compute(frame, flat_baseline(cells, mean=1.0, std=std), CFG)


def test_feature_columns_come_from_the_kpi_catalog() -> None:
    kpis = [k.name for k in load_domain().kpis.values() if k.source is not KpiSource.DERIVED]
    assert [k.name for k in ft.feature_kpis()] == kpis
    suffixes = ["_z", "_bad", *[f"_z_mean_{w}" for w in CFG.windows], "_z_delta"]
    assert ft.feature_columns(CFG) == [f"{k}{s}" for k in kpis for s in suffixes]


def test_z_score_and_bad_direction() -> None:
    out = _compute(telemetry_frame(n_intervals=1))  # every traffic field = 1.0; latency 26 ms
    row = out.row(0, named=True)
    # traffic_internet = internet = 1.0 -> z 0; traffic_voice = call_in + call_out = 2.0 -> z 2
    assert row["traffic_internet_z"] == 0.0
    assert row["traffic_voice_z"] == 2.0
    for k in ft.feature_kpis():
        z, bad = row[f"{k.name}_z"], row[f"{k.name}_bad"]
        assert bad == (-z if k.bad_direction is BadDirection.DROP else z)


def test_either_direction_is_absolute() -> None:
    kpi = load_domain().kpi("latency_ms").model_copy(update={"bad_direction": BadDirection.EITHER})
    out = pl.DataFrame({"z": [-3.0, 2.0]}).select(ft._oriented(kpi, pl.col("z")))
    assert out["z"].to_list() == [3.0, 2.0]


def test_z_is_clipped() -> None:
    out = _compute(telemetry_frame(n_intervals=1), std=1e-6)
    assert out["latency_ms_z"][0] == CFG.z_clip
    assert out["traffic_internet_z"][0] == 0.0


def test_rolling_mean_and_delta() -> None:
    out = _compute(telemetry_frame(n_intervals=8))  # internet = 1 + i/7 -> z = 2i/7
    z = [2 * i / 7 for i in range(8)]
    assert out["traffic_internet_z"].to_list() == pytest.approx(z)
    w = CFG.windows[0]
    expected = [statistics.mean(z[max(0, i - w + 1) : i + 1]) for i in range(8)]
    assert out[f"traffic_internet_z_mean_{w}"].to_list() == pytest.approx(expected)
    delta = out["traffic_internet_z_delta"].to_list()
    assert delta[0] is None
    assert delta[1:] == pytest.approx([2 / 7] * 7)


def test_gap_rows_get_no_features_and_windows_skip_them() -> None:
    out = _compute(telemetry_frame(n_intervals=6, gaps=[2]))
    gap = out.row(2, named=True)
    assert gap["data_gap"]
    assert all(gap[c] is None for c in ft.feature_columns(CFG))
    after = out.row(3, named=True)
    assert after["traffic_internet_z_delta"] is None  # the previous interval was a gap
    w = max(CFG.windows)
    z = [2 * i / 7 for i in range(6) if i != 2]
    assert after[f"traffic_internet_z_mean_{w}"] == pytest.approx(sum(z[:3]) / 3)


def test_missing_interval_shortens_the_window() -> None:
    out = _compute(telemetry_frame(n_intervals=6, drop=[3]))
    assert out.height == 5
    row = out.filter(pl.col("interval_start") == pl.col("interval_start").max()).row(0, named=True)
    assert row["traffic_internet_z_delta"] == pytest.approx(2 / 7)  # interval 4 -> 5, adjacent
    w = CFG.windows[0]  # 3 intervals back from 5 would be 3, 4, 5; 3 is missing
    assert row[f"traffic_internet_z_mean_{w}"] == pytest.approx((8 / 7 + 10 / 7) / 2)
    four = out.row(3, named=True)
    assert four["traffic_internet_z_delta"] is None  # interval 3 is missing


def test_features_are_causal() -> None:
    frame = telemetry_frame(n_cells=2, n_intervals=10)
    cut = frame["interval_start"].sort()[10]  # past = the first 5 intervals
    future_changed = frame.with_columns(
        pl.when(pl.col("interval_start") >= cut)
        .then(pl.col("internet") * 9 + 3)
        .otherwise(pl.col("internet"))
        .alias("internet"),
        pl.when(pl.col("interval_start") >= cut)
        .then(True)
        .otherwise(pl.col("data_gap"))
        .alias("data_gap"),
    )
    past = pl.col("interval_start") < cut
    assert_frame_equal(_compute(frame).filter(past), _compute(future_changed).filter(past))
    truncated = frame.filter(pl.col("interval_start") < cut)
    assert_frame_equal(_compute(frame).filter(past), _compute(truncated))


def test_deterministic_and_order_independent() -> None:
    frame = telemetry_frame(n_cells=3, n_intervals=6)
    assert_frame_equal(_compute(frame), _compute(frame.sample(fraction=1.0, shuffle=True, seed=7)))


def test_cell_without_baseline_gets_no_features() -> None:
    out = ft.compute(telemetry_frame(n_intervals=2), flat_baseline(["CELL-9-9"]), CFG)
    assert out.height == 2
    assert all(out[c].is_null().all() for c in ft.feature_columns(CFG))


@pytest.mark.parametrize("windows", [[3, 3], [0], []])
def test_config_rejects_bad_windows(windows: list[int]) -> None:
    with pytest.raises(ValidationError):
        ft.Config(**{**CFG.model_dump(), "windows": windows})
