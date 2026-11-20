"""Milan grid: square centroids and the fixed cell subset (milestone 1, step 4).

Grid IDs run row by row from the south-west corner: id = row * 100 + col + 1 (verified 2026-11-20).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

GRID_SIDE = 100

# Subset rule (DECISIONS.md 2026-11-19/20): a square block of squares centred on the
# square nearest Milan's Duomo. 12 x 12 = 144 squares, sized for an 8 GB laptop.
DUOMO_LON_LAT = (9.1919, 45.4641)
SUBSET_BLOCK = 12


@dataclass(frozen=True)
class Square:
    square_id: int
    lon: float
    lat: float

    @property
    def row(self) -> int:
        return (self.square_id - 1) // GRID_SIDE

    @property
    def col(self) -> int:
        return (self.square_id - 1) % GRID_SIDE


def load_squares(grid_path: Path) -> dict[int, Square]:
    """Centroid of every grid square, keyed by `cellId` (= `square_id` in the activity files)."""
    doc: dict[str, Any] = json.loads(grid_path.read_text(encoding="utf-8"))
    squares: dict[int, Square] = {}
    for feat in doc["features"]:
        sid = int(feat["properties"]["cellId"])
        ring = feat["geometry"]["coordinates"][0]
        pts = ring[:-1] if ring[0] == ring[-1] else ring
        lon = sum(p[0] for p in pts) / len(pts)
        lat = sum(p[1] for p in pts) / len(pts)
        squares[sid] = Square(sid, lon, lat)
    return squares


def nearest_square(squares: dict[int, Square], lon: float, lat: float) -> Square:
    return min(squares.values(), key=lambda s: (s.lon - lon) ** 2 + (s.lat - lat) ** 2)


def select_block(
    squares: dict[int, Square], centre: Square, size: int, side: int = GRID_SIDE
) -> list[int]:
    """IDs of the `size` x `size` block of squares around `centre`, sorted."""
    lo_row = centre.row - size // 2
    lo_col = centre.col - size // 2
    if lo_row < 0 or lo_col < 0 or lo_row + size > side or lo_col + size > side:
        raise ValueError("subset block falls outside the grid")
    ids = [
        r * side + c + 1 for r in range(lo_row, lo_row + size) for c in range(lo_col, lo_col + size)
    ]
    missing = [i for i in ids if i not in squares]
    if missing:
        raise ValueError(f"grid is missing squares {missing[:5]}")
    return sorted(ids)


def build_subset(grid_path: Path) -> dict[str, Any]:
    """The project's fixed subset, as written to `data/processed/subset.json`."""
    squares = load_squares(grid_path)
    centre = nearest_square(squares, *DUOMO_LON_LAT)
    ids = select_block(squares, centre, SUBSET_BLOCK)
    return {
        "rule": (
            f"{SUBSET_BLOCK}x{SUBSET_BLOCK} block of Milan grid squares centred on the square "
            f"nearest the Duomo {DUOMO_LON_LAT} (lon, lat)"
        ),
        "centre_square_id": centre.square_id,
        "size": len(ids),
        "square_ids": ids,
    }
