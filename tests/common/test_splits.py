from __future__ import annotations

from datetime import date

import pytest

from common.splits import Split, days, require, split_of


def test_split_of() -> None:
    assert split_of(date(2013, 11, 1)) is None  # holiday file, before training
    assert split_of(date(2013, 11, 4)) is Split.TRAIN
    assert split_of(date(2013, 12, 1)) is Split.TRAIN
    assert split_of(date(2013, 12, 2)) is Split.VALIDATION
    assert split_of(date(2013, 12, 16)) is Split.TEST
    assert split_of(date(2014, 1, 1)) is Split.TEST


def test_day_counts() -> None:
    assert len(days(Split.TRAIN)) == 28
    assert len(days(Split.VALIDATION)) == 14
    assert len(days(Split.TEST)) == 17


def test_require_blocks_other_splits() -> None:
    require(date(2013, 11, 10), Split.TRAIN)
    with pytest.raises(ValueError, match="test"):
        require(date(2013, 12, 20), Split.TRAIN)
    with pytest.raises(ValueError, match="None"):
        require(date(2013, 11, 1), Split.TRAIN)
