from __future__ import annotations

import itertools
import math
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
    kpis = [
        k.name
        for k in load_domain().kpis.values()
        if k.source is not KpiSource.DERIVED and k.name not in CFG.not_detected
    ]
    assert [k.name for k in ft.feature_kpis(CFG)] == kpis
    assert [k.name for k in ft.clock_kpis(CFG)] == [k for k in kpis if k != CFG.load_curve.kpi]
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


HIGH, LOW = math.log1p(1.0), math.log1p(1 / 3)  # `_level_shift` values on the log1p scale


def test_level_adjusted_z_absorbs_a_lasting_shift_after_a_week() -> None:
    out = _compute(_level_shift(days=14, shift_day=5))
    z = _median_by_local_day(out, "traffic_internet_z")
    assert z[4] == pytest.approx(0.0)  # offset = HIGH - 1: the level so far
    assert z[5] == pytest.approx((LOW - HIGH) / 0.5)  # the shift; history still at HIGH
    assert z[12] == pytest.approx(0.0)  # 7 days at the new level: normal again
    assert _median_by_local_day(out, "traffic_internet_bad")[12] == pytest.approx(0.0)
    latency = _median_by_local_day(out, "latency_ms_z")  # MODELED: never level-adjusted
    assert latency[4] == latency[12]


def test_too_little_history_falls_back_to_seasonal_z() -> None:
    z = _median_by_local_day(_compute(_level_shift(days=6, shift_day=2)), "traffic_internet_z")
    assert CFG.level.min_days == 3  # the expected values below assume it
    assert z[2] == pytest.approx((LOW - 1) / 0.5)  # 2 days of history: seasonal z
    # 3 days of history (HIGH, HIGH, LOW): offset = mean deviation from the baseline mean 1
    offset = (2 * HIGH + LOW) / 3 - 1
    assert z[3] == pytest.approx((LOW - 1 - offset) / 0.5)


def test_level_config_rejects_min_days_above_days() -> None:
    with pytest.raises(ValidationError):
        ft.LevelParams(days=3, min_days=4)


def test_z_score_and_bad_direction() -> None:
    out = _compute(telemetry_frame(n_intervals=1))  # every traffic field = 1.0; latency 26 ms
    row = out.row(0, named=True)
    # real KPIs on log1p: internet = 1.0, voice = call_in + call_out = 2.0
    assert row["traffic_internet_z"] == pytest.approx((math.log1p(1.0) - 1) / 0.5)
    assert row["traffic_voice_z"] == pytest.approx((math.log1p(2.0) - 1) / 0.5)
    # latency against the (one-bin) load curve, on a log scale
    assert row["latency_ms_z"] == pytest.approx((math.log(26.0) - 1) / 0.5)
    for k in ft.feature_kpis(CFG):
        z, bad = row[f"{k.name}_z"], row[f"{k.name}_bad"]
        assert bad == (-z if k.bad_direction is BadDirection.DROP else z)


def test_either_direction_is_absolute() -> None:
    kpi = load_domain().kpi("latency_ms").model_copy(update={"bad_direction": BadDirection.EITHER})
    out = pl.DataFrame({"z": [-3.0, 2.0]}).select(ft._oriented(kpi, pl.col("z")))
    assert out["z"].to_list() == [3.0, 2.0]


def test_z_is_clipped() -> None:
    out = _compute(telemetry_frame(n_intervals=1), std=1e-6)
    assert out["latency_ms_z"][0] == CFG.z_clip
    assert out["traffic_internet_z"][0] == -CFG.z_clip


def test_latency_is_judged_against_its_load_bin() -> None:
    """Same latency, two loads: normal where the curve expects it, high where it doesn't."""
    frame = telemetry_frame(n_intervals=2).with_columns(
        pl.Series("load_ratio", [0.9, 0.2]), pl.Series("latency_ms", [200.0, 200.0])
    )
    ref = flat_baseline(["CELL-1-1"])
    curve = pl.DataFrame(
        {"upper": [0.5, math.inf], "mean": [math.log(20.0), math.log(200.0)], "std": [0.1, 0.1]}
    )
    out = ft.compute(frame, ft.Reference(ref.seasonal, curve), CFG)
    busy, quiet = out["latency_ms_z"].to_list()
    assert busy == pytest.approx(0.0)  # congestion delay the curve expects
    assert quiet == CFG.z_clip  # (ln 200 - ln 20) / 0.1 = 23 -> clipped


def _internet_z(i: int) -> float:
    """`traffic_internet_z` of `telemetry_frame` interval i (internet = 1 + i/7) in `_compute`."""
    return (math.log1p(1 + i / 7) - 1) / 0.5


def test_rolling_mean_and_delta() -> None:
    out = _compute(telemetry_frame(n_intervals=8))
    z = [_internet_z(i) for i in range(8)]
    assert out["traffic_internet_z"].to_list() == pytest.approx(z)
    w = CFG.windows[0]
    expected = [statistics.mean(z[max(0, i - w + 1) : i + 1]) for i in range(8)]
    assert out[f"traffic_internet_z_mean_{w}"].to_list() == pytest.approx(expected)
    delta = out["traffic_internet_z_delta"].to_list()
    assert delta[0] is None
    assert delta[1:] == pytest.approx([b - a for a, b in itertools.pairwise(z)])


def test_gap_rows_get_no_features_and_windows_skip_them() -> None:
    out = _compute(telemetry_frame(n_intervals=6, gaps=[2]))
    gap = out.row(2, named=True)
    assert gap["data_gap"]
    assert all(gap[c] is None for c in ft.feature_columns(CFG))
    after = out.row(3, named=True)
    assert after["traffic_internet_z_delta"] is None  # the previous interval was a gap
    w = max(CFG.windows)
    z = [_internet_z(i) for i in range(6) if i != 2]
    assert after[f"traffic_internet_z_mean_{w}"] == pytest.approx(sum(z[:3]) / 3)


def test_missing_interval_shortens_the_window() -> None:
    out = _compute(telemetry_frame(n_intervals=6, drop=[3]))
    assert out.height == 5
    row = out.filter(pl.col("interval_start") == pl.col("interval_start").max()).row(0, named=True)
    assert row["traffic_internet_z_delta"] == pytest.approx(_internet_z(5) - _internet_z(4))
    w = CFG.windows[0]  # 3 intervals back from 5 would be 3, 4, 5; 3 is missing
    assert row[f"traffic_internet_z_mean_{w}"] == pytest.approx(
        (_internet_z(4) + _internet_z(5)) / 2
    )
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


def test_cell_without_baseline_gets_only_load_curve_features() -> None:
    out = ft.compute(telemetry_frame(n_intervals=2), flat_baseline(["CELL-9-9"]), CFG)
    assert out.height == 2
    by_kpi = ft.feature_columns_by_kpi(CFG)
    for kpi, cols in by_kpi.items():
        nulls = all(out[c].is_null().all() for c in cols if not c.endswith("_delta"))
        assert nulls is (kpi != CFG.load_curve.kpi)  # the load curve is the same for every cell


@pytest.mark.parametrize("windows", [[3, 3], [0], []])
def test_config_rejects_bad_windows(windows: list[int]) -> None:
    with pytest.raises(ValidationError):
        ft.Config(**{**CFG.model_dump(), "windows": windows})


def test_a_dead_week_never_gives_nan_or_a_zero_std() -> None:
    """Zero traffic for a week: z stays finite, and the dead cell keeps alarming (an outage is
    never absorbed as a new level)."""
    frame = _level_shift(days=10, shift_day=1).with_columns(
        pl.when(pl.col("internet") < 1).then(0.0).otherwise(pl.col("internet")).alias("internet")
    )
    out = _compute(frame)
    z = out["traffic_internet_z"].drop_nulls()
    assert z.is_finite().all() and z.len() == out.height
    assert _median_by_local_day(out, "traffic_internet_z")[9] == pytest.approx(-2.0)  # (0-1)/0.5


def test_level_offset_counts_only_rows_with_a_baseline() -> None:
    """Days without a baseline bucket neither dilute the offset nor count toward min_days."""
    frame = _level_shift(days=14, shift_day=5)
    ref = flat_baseline(["CELL-1-1"])
    no_sundays = ft.Reference(ref.seasonal.filter(pl.col("weekday") != 7), ref.load_curve)
    z = _median_by_local_day(ft.compute(frame, no_sundays, CFG), "traffic_internet_z")
    assert z[13] == pytest.approx(0.0)  # window: days 6-12, all shifted; 12 is a Sunday (none)


@pytest.mark.parametrize(
    "update",
    [
        {"load_curve": {"kpi": "traffic_sms", "load": "load_ratio", "bins": 10}},  # real
        {"load_curve": {"kpi": "latency_ms", "load": "no_such_kpi", "bins": 10}},
        {"not_detected": ["latency_ms"]},  # the curve KPI must be detected
        {"load_curve": {"kpi": "latency_ms", "load": "latency_ms", "bins": 10}},  # itself
        {"load_curve": {"kpi": "latency_ms", "load": "baseline_deviation", "bins": 10}},
    ],
)
def test_config_rejects_a_bad_load_curve(update: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ft.Config(**{**CFG.model_dump(), **update})
