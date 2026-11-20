"""Train / validation / test days. The single place the time split is defined."""

from __future__ import annotations

import itertools
from datetime import date, timedelta
from enum import StrEnum
from functools import cache
from importlib.resources import files

import yaml


class Split(StrEnum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


@cache
def bounds() -> dict[Split, tuple[date, date]]:
    raw = yaml.safe_load(files("common").joinpath("splits.yaml").read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise ValueError("splits.yaml: unsupported version")
    out = {Split(k): (v["start"], v["end"]) for k, v in raw["splits"].items()}
    ordered = sorted(out.values())
    for (_, prev_end), (next_start, _) in itertools.pairwise(ordered):
        if next_start <= prev_end:
            raise ValueError("splits.yaml: splits overlap")
    return out


def split_of(day: date) -> Split | None:
    """The split a day belongs to, or None (e.g. 2013-11-01, before training starts)."""
    for split, (start, end) in bounds().items():
        if start <= day <= end:
            return split
    return None


def days(split: Split) -> list[date]:
    start, end = bounds()[split]
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def require(day: date, allowed: Split) -> None:
    """Raise unless `day` is in the `allowed` split (guards against test-week leakage)."""
    actual = split_of(day)
    if actual is not allowed:
        raise ValueError(f"{day} is in split {actual}, only {allowed} days are allowed here")
