from __future__ import annotations

import statistics

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ValidationError

from common import telemetry
from common.domain import load_domain
from common.domain.loader import BadDirection, KpiSource
from streaming import features as ft
from tests.factories import flat_baseline, telemetry_frame

CFG = ft.load_config()


def _compute(frame: pl.DataFrame, std: float = 0.5) -> pl.DataFrame:
    cells = frame["cell_id"].unique().to_list()
    return ft.compute(frame, flat_baseline(cells, mean=1.0, std=std), CFG)


def test_feature_columns_come_from_the_kpi_catalog() -> None:
    kpis = [k.name for k in load_domain().kpis.values() if k.source is not KpiSource.DERIVED]
    assert [k.name for k in ft.feature_kpis()] == kpis
    suffixes = ["_z", "_bad", *[f"_z_mean_{w}" for w in CFG.windows], "_z_delta"]
    expected = {k: [f"{k}{s}" for s in suffixes] for k in kpis}
    assert ft.feature_columns_by_kpi(CFG) == expected
    assert ft.feature_columns(CFG) == [c for cols in expected.values() for c in cols]
    frame = pl.DataFrame({f"{k}_bad": [float(i)] for i, k in enumerate(kpis)})
    assert frame.select(ft.oriented(kpis)).row(0, named=True) == {
        k: float(i) for i, k in enumerate(kpis)
    }


def _level_shift(days: int, shift_day: int) -> pl.DataFrame:
    """One cell, `days` local days of 10-minute rows; every traffic field 1.0, dropping to 1/3
    from local day `shift_day` on (a lasting level shift, like Milan internet in Dec 2013)."""
    frame = telemetry_frame(n_intervals=144 * days)
    first = frame.select(ft.LOCAL_TIME.dt.date().min()).item()
    day = (ft.LOCAL_TIME.dt.date() - first).dt.total_days()
    value = pl.when(day >= shift_day).then(1 / 3).otherwise(1.0)
    return frame.with_columns(value.alias(f) for f in telemetry.TRAFFIC_FIELDS)


def _median_by_local_day(out: pl.DataFrame, col: str) -> dict[int, float]:
    """Local day index (0 = the first, partial day) -> median of `col` that day."""
    days = out.with_columns(ft.LOCAL_TIME.dt.date().rank("dense").sub(1).alias("day"))
    medians = days.group_by("day").agg(pl.col(col).median()).sort("day")
    return dict(medians.iter_rows())


def test_level_adjusted_z_absorbs_a_lasting_shift_after_a_week() -> None:
    out = _compute(_level_shift(days=14, shift_day=5))
    z = _median_by_local_day(out, "traffic_internet_z")
    assert z[4] == pytest.approx(0.0)  # level 1, factor 1
    assert z[5] == pytest.approx(-4 / 3)  # the shift; history still at level 1
    assert z[12] == pytest.approx(0.0)  # 7 days at the new level: normal again
    assert _median_by_local_day(out, "traffic_internet_bad")[12] == pytest.approx(0.0)
    latency = _median_by_local_day(out, "latency_ms_z")  # MODELED: never level-adjusted
    assert latency[4] == latency[12]


def test_too_little_history_falls_back_to_seasonal_z() -> None:
    z = _median_by_local_day(_compute(_level_shift(days=6, shift_day=2)), "traffic_internet_z")
    assert CFG.level.min_days == 3  # the expected values below assume it
    assert z[2] == pytest.approx(-4 / 3)  # 2 days of history: seasonal z
    # 3 days of history (1, 1, 1/3): factor 7/9, so z = (1/3 - 7/9) / (0.5 * 7/9) = -8/7
    assert z[3] == pytest.approx(-8 / 7)


def test_level_config_rejects_min_days_above_days() -> None:
    with pytest.raises(ValidationError):
        ft.LevelParams(days=3, min_days=4)


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


def test_a_dead_week_never_gives_nan_or_a_zero_std() -> None:
    """Zero traffic for a week makes the level factor 0; the z must stay finite (seasonal)."""
    frame = _level_shift(days=10, shift_day=1).with_columns(
        pl.when(pl.col("internet") < 1).then(0.0).otherwise(pl.col("internet")).alias("internet")
    )
    out = _compute(frame)
    z = out["traffic_internet_z"].drop_nulls()
    assert z.is_finite().all() and z.len() == out.height
    assert _median_by_local_day(out, "traffic_internet_z")[9] == pytest.approx(-2.0)  # (0-1)/0.5


def test_scaled_std_keeps_the_floor() -> None:
    frame = pl.DataFrame(
        {
            "traffic_sms": [0.5],
            "traffic_sms_mean": [1.0],
            "traffic_sms_std": [0.5],
            "data_gap": [False],
        }
    )
    kpi = load_domain().kpi("traffic_sms")
    z = frame.select(ft._scaled_z(kpi, pl.lit(1e-9), CFG)).item()
    assert z == CFG.z_clip  # mean ~0, std floored at std_floor_abs: big, but finite and clipped
