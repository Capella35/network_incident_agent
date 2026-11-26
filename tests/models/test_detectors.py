from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from models import detectors as det
from streaming import features
from tests.models.conftest import OUTAGE, normal_rows, to_features

CFG = det.load_config()


@pytest.mark.parametrize("name", det.CANDIDATES)
def test_planted_outage_scores_above_normal(
    name: str, train: pl.DataFrame, normal_with_outage: pl.DataFrame
) -> None:
    scores = det.build(name, CFG).fit(train).score(normal_with_outage)["score"].to_list()
    after = OUTAGE.stop + max(features.load_config().windows)  # windows still hold the outage
    normal = scores[: OUTAGE.start] + scores[after:]
    assert max(scores[i] for i in OUTAGE) > max(normal)


@pytest.mark.parametrize("name", det.CANDIDATES)
def test_same_seed_same_scores(name: str, train: pl.DataFrame) -> None:
    a = det.build(name, CFG).fit(train).score(train)
    b = det.build(name, CFG).fit(train).score(train)
    assert_frame_equal(a, b)


@pytest.mark.parametrize("name", det.CANDIDATES)
def test_gap_rows_get_no_score(name: str, train: pl.DataFrame) -> None:
    frame = to_features(normal_rows(n_intervals=20, gaps=(5,)))
    scored = det.build(name, CFG).fit(train).score(frame)
    assert scored["score"][5] is None
    assert scored.row(5, named=True)[f"{det.CONTRIB}traffic_internet"] is None
    assert scored["score"].null_count() == 1
    assert scored.columns[: len(features.KEYS)] == list(features.KEYS)


def test_zscore_is_the_worst_oriented_kpi(normal_with_outage: pl.DataFrame) -> None:
    scored = det.build("zscore", CFG).score(normal_with_outage)
    oriented = normal_with_outage.select(
        pl.max_horizontal(features.oriented([k.name for k in features.feature_kpis()]))
    )
    assert scored["score"].to_list() == oriented.to_series().to_list()


def test_autoencoder_contributions_are_per_kpi(train: pl.DataFrame) -> None:
    scored = det.build("autoencoder", CFG).fit(train).score(train)
    contrib = [c for c in scored.columns if c.startswith(det.CONTRIB)]
    assert contrib == [det.CONTRIB + k.name for k in features.feature_kpis()]
    assert (scored.select(contrib).min_horizontal() >= 0).all()


def test_build_rejects_an_unknown_name() -> None:
    with pytest.raises(ValueError, match="unknown detector"):
        det.build("magic", CFG)


def _scored(rows: list[tuple[float, dict[str, float | None]]]) -> pl.DataFrame:
    t = datetime(2013, 12, 2, tzinfo=UTC)
    return pl.DataFrame(
        [
            {
                "cell_id": f"CELL-1-{i + 1}",
                "site_id": "SITE-1",
                "interval_start": t,
                "data_gap": False,
                "score": s,
                **{det.CONTRIB + k: v for k, v in c.items()},
            }
            for i, (s, c) in enumerate(rows)
        ]
    )


def test_alarms_carry_the_contributing_kpis() -> None:
    p = det.AlarmParams(kpi_share=0.5, max_kpis=2)
    scored = _scored(
        [
            (9.0, {"a": 9.0, "b": 5.0, "c": 4.0, "d": 1.0}),  # b >= 4.5; c too, but max_kpis 2
            (2.0, {"a": 2.0, "b": 0.0, "c": None, "d": 0.0}),  # below the threshold
            (6.0, {"a": -1.0, "b": -3.0, "c": None, "d": -2.0}),  # nothing positive: top only
        ]
    )
    out = det.alarms(scored, 5.0, p)
    assert out.columns == ["cell_id", "site_id", "interval_start", "score", "kpis"]
    assert out["cell_id"].to_list() == ["CELL-1-1", "CELL-1-3"]
    assert out["kpis"].to_list() == [["a", "b"], ["a"]]


def test_alarms_with_none_above_threshold() -> None:
    p = det.AlarmParams(kpi_share=0.5, max_kpis=2)
    assert det.alarms(_scored([(1.0, {"a": 1.0})]), 5.0, p).is_empty()


def test_non_finite_features_reach_models_as_neutral_zero() -> None:
    frame = pl.DataFrame({"a": [1.0, None, float("nan"), float("inf")]})
    assert det._matrix(frame, ["a"]).ravel().tolist() == [1.0, 0.0, 0.0, 0.0]


def test_training_rows_sample_once_then_stay_put(train: pl.DataFrame) -> None:
    once = det.training_rows(train, 500, seed=1)
    assert once.height == 500 and not once["data_gap"].any()
    assert_frame_equal(det.training_rows(once, 500, seed=1), once)  # fit can resample: no-op
