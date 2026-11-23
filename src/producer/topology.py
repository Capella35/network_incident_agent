"""Generate the SYNTHETIC network topology from the cell subset (milestone 1, step 4).

    python -m producer.topology [--data-dir data]

One grid square = one Cell. Each row of the subset block is cut, west to east, into strips of
`sectors_per_site` squares; each strip is one Site. Sites are clustered into AggregationNodes by
seeded k-means on location, and every AggregationNode hangs off one Core. Real networks are not
built like this; the topology is labelled synthetic everywhere it is used (CLAUDE.md Rule 9).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from common.domain import Domain, load_domain
from producer import grid, integrity

log = logging.getLogger("producer.topology")

NODE_COLUMNS = ("node_id", "node_type", "parent_id", "lat", "lon", "square_id")
EDGE_COLUMNS = ("src", "dst", "relation")


class TopologyConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int = Field(ge=1, le=1)
    seed: int
    sectors_per_site: int = Field(ge=1)
    aggregation_nodes: int = Field(ge=1)
    min_sites_per_aggregation_node: int = Field(ge=1)
    max_sites_per_aggregation_node: int = Field(ge=1)
    kmeans_iterations: int = Field(ge=1)

    @model_validator(mode="after")
    def _bounds(self) -> TopologyConfig:
        if self.min_sites_per_aggregation_node > self.max_sites_per_aggregation_node:
            raise ValueError("min_sites_per_aggregation_node > max_sites_per_aggregation_node")
        return self


def load_config(path: Path | None = None) -> TopologyConfig:
    text = (
        path.read_text(encoding="utf-8")
        if path is not None
        else files("producer").joinpath("topology_config.yaml").read_text(encoding="utf-8")
    )
    return TopologyConfig(**yaml.safe_load(text))


@dataclass(frozen=True)
class Node:
    node_id: str
    node_type: str
    parent_id: str | None
    lat: float
    lon: float
    square_id: int | None = None


@dataclass(frozen=True)
class Topology:
    nodes: tuple[Node, ...]
    edges: tuple[tuple[str, str, str], ...]

    def by_type(self, node_type: str) -> list[Node]:
        return [n for n in self.nodes if n.node_type == node_type]


def _mean(points: list[tuple[float, float]]) -> tuple[float, float]:
    return (sum(p[0] for p in points) / len(points), sum(p[1] for p in points) / len(points))


def _kmeans(points: list[tuple[float, float]], k: int, seed: int, iterations: int) -> list[int]:
    """Plain, deterministic k-means (k-means++ init) on planar points; returns labels."""
    if k > len(points):
        raise ValueError(f"cannot form {k} clusters from {len(points)} points")
    rng = random.Random(seed)  # noqa: S311 -- reproducible simulation, not security

    def d2(a: tuple[float, float], b: tuple[float, float]) -> float:
        return (a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2

    centres = [points[rng.randrange(len(points))]]
    while len(centres) < k:
        weights = [min(d2(p, c) for c in centres) for p in points]
        centres.append(points[rng.choices(range(len(points)), weights=weights)[0]])

    def nearest(p: tuple[float, float]) -> int:
        # ties go to the lower index, so the result never depends on float noise ordering
        return min(range(k), key=lambda j: (d2(p, centres[j]), j))

    labels = [-1] * len(points)
    for _ in range(iterations):
        new = [nearest(p) for p in points]
        if new == labels:
            break
        labels = new
        for j in range(k):
            members = [p for p, lab in zip(points, labels, strict=True) if lab == j]
            if members:
                centres[j] = _mean(members)
    return labels


def build(
    squares: dict[int, grid.Square],
    subset_ids: list[int],
    cfg: TopologyConfig,
    domain: Domain | None = None,
) -> Topology:
    """Build the topology for a rectangular block of grid squares."""
    domain = domain or load_domain()
    core_t, agg_t, site_t, cell_t = domain.topology_types()  # ordered root first

    block = [squares[i] for i in sorted(subset_ids)]
    rows = sorted({s.row for s in block}, reverse=True)  # north first
    cols = sorted({s.col for s in block})  # west first
    if len(block) != len(rows) * len(cols):
        raise ValueError("subset is not a full rectangular block of squares")
    if cols != list(range(cols[0], cols[0] + len(cols))):
        raise ValueError("subset columns are not contiguous")
    if len(cols) % cfg.sectors_per_site:
        raise ValueError(
            f"block width {len(cols)} is not a multiple of sectors_per_site {cfg.sectors_per_site}"
        )
    by_pos = {(s.row, s.col): s for s in block}

    # Sites: reading order (north-west first), strips of `sectors_per_site` squares.
    site_cells: list[list[grid.Square]] = []
    for r in rows:
        for c0 in range(0, len(cols), cfg.sectors_per_site):
            strip = cols[c0 : c0 + cfg.sectors_per_site]
            site_cells.append([by_pos[(r, c)] for c in strip])
    site_pos = [_mean([(s.lon, s.lat) for s in cells]) for cells in site_cells]

    # Aggregation nodes: k-means on an equirectangular projection (fine at city scale).
    lat0 = math.radians(sum(p[1] for p in site_pos) / len(site_pos))
    planar = [(lon * math.cos(lat0), lat) for lon, lat in site_pos]
    labels = _kmeans(planar, cfg.aggregation_nodes, cfg.seed, cfg.kmeans_iterations)
    sizes = [labels.count(j) for j in range(cfg.aggregation_nodes)]
    lo, hi = cfg.min_sites_per_aggregation_node, cfg.max_sites_per_aggregation_node
    if not all(lo <= n <= hi for n in sizes):
        raise ValueError(f"aggregation node sizes {sizes} outside [{lo}, {hi}]; change the seed")

    # Number aggregation nodes by centroid reading order, so IDs don't depend on k-means labels.
    agg_pos = {
        j: _mean([site_pos[i] for i, lab in enumerate(labels) if lab == j])
        for j in range(cfg.aggregation_nodes)
    }
    order = sorted(agg_pos, key=lambda j: (-round(agg_pos[j][1], 9), round(agg_pos[j][0], 9)))
    agg_id = {j: agg_t.make_id(n=i + 1) for i, j in enumerate(order)}

    core_lon, core_lat = _mean(site_pos)
    core_id = core_t.make_id(n=1)
    nodes: list[Node] = [Node(core_id, core_t.name, None, core_lat, core_lon)]
    for j in order:
        lon, lat = agg_pos[j]
        nodes.append(Node(agg_id[j], agg_t.name, core_id, lat, lon))
    for i, cells in enumerate(site_cells):
        site_no = i + 1
        sid = site_t.make_id(n=site_no)
        lon, lat = site_pos[i]
        nodes.append(Node(sid, site_t.name, agg_id[labels[i]], lat, lon))
        for sector, sq in enumerate(cells, start=1):
            cid = cell_t.make_id(site=site_no, sector=sector)
            nodes.append(Node(cid, cell_t.name, sid, sq.lat, sq.lon, sq.square_id))

    edges = tuple(
        (n.parent_id, n.node_id, domain.parent_relation(n.node_type).name)
        for n in nodes
        if n.parent_id is not None
    )
    return Topology(nodes=tuple(nodes), edges=edges)


def _fmt(v: object) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(v)


def write(topo: Topology, out_dir: Path, meta: dict[str, Any]) -> dict[str, str]:
    """Write nodes/edges CSVs and a metadata file; returns output SHA-256s."""
    out_dir.mkdir(parents=True, exist_ok=True)
    nodes_path, edges_path = out_dir / "topology_nodes.csv", out_dir / "topology_edges.csv"
    with nodes_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(NODE_COLUMNS)
        for n in topo.nodes:
            row = asdict(n)
            w.writerow([_fmt(row[c]) for c in NODE_COLUMNS])
    with edges_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(EDGE_COLUMNS)
        w.writerows(topo.edges)
    shas = {
        "nodes_sha256": integrity.sha256_file(nodes_path),
        "edges_sha256": integrity.sha256_file(edges_path),
    }
    (out_dir / "topology_meta.json").write_text(
        json.dumps({**meta, **shas}, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return shas


def load_nodes(nodes_csv: Path) -> list[Node]:
    """All nodes of a written `topology_nodes.csv` (SYNTHETIC topology). The one CSV parser."""
    with nodes_csv.open(newline="", encoding="utf-8") as fh:
        return [
            Node(
                node_id=r["node_id"],
                node_type=r["node_type"],
                parent_id=r["parent_id"] or None,
                lat=float(r["lat"]),
                lon=float(r["lon"]),
                square_id=int(r["square_id"]) if r["square_id"] else None,
            )
            for r in csv.DictReader(fh)
        ]


class Index:
    """The SYNTHETIC topology as lookups: nodes by type, cells under any node, cell maps.
    Shared by the replay, the fault injector, and (M3) the correlator."""

    def __init__(self, nodes: Iterable[Node]) -> None:
        self.nodes = {n.node_id: n for n in nodes}
        types = load_domain().topology_types()
        self.cell_type, self.site_type = types[-1].name, types[-2].name
        self._by_type: dict[str, list[str]] = {}
        self._children: dict[str, list[str]] = {}
        for n in sorted(self.nodes.values(), key=lambda n: n.node_id):
            self._by_type.setdefault(n.node_type, []).append(n.node_id)
            if n.parent_id is not None:
                self._children.setdefault(n.parent_id, []).append(n.node_id)

    def of_type(self, node_type: str) -> list[str]:
        """Node IDs of one type, sorted."""
        return self._by_type.get(node_type, [])

    def cells_under(self, node_id: str) -> tuple[str, ...]:
        """The cells at or below a node, sorted."""
        if self.nodes[node_id].node_type == self.cell_type:
            return (node_id,)
        return tuple(
            sorted(c for k in self._children.get(node_id, ()) for c in self.cells_under(k))
        )

    def cells_by_square(self) -> dict[int, str]:
        """Milan square_id -> Cell node_id."""
        return {
            n.square_id: n.node_id
            for n in self.nodes.values()
            if n.node_type == self.cell_type and n.square_id is not None
        }

    def cell_sites(self) -> dict[str, str]:
        """Cell node_id -> parent Site node_id."""
        return {
            n.node_id: n.parent_id
            for n in self.nodes.values()
            if n.node_type == self.cell_type and n.parent_id is not None
        }


def load_index(processed: Path) -> Index:
    nodes_csv = processed / "topology_nodes.csv"
    if not nodes_csv.exists():
        raise FileNotFoundError(f"{nodes_csv} missing: run `make data` first")
    return Index(load_nodes(nodes_csv))


def load_cells(nodes_csv: Path) -> dict[int, str]:
    """Map Milan square_id -> Cell node_id from a written `topology_nodes.csv`."""
    return Index(load_nodes(nodes_csv)).cells_by_square()


def load_cell_sites(nodes_csv: Path) -> dict[str, str]:
    """Map Cell node_id -> parent Site node_id from a written `topology_nodes.csv`."""
    return Index(load_nodes(nodes_csv)).cell_sites()


def run(data_dir: Path, config_path: Path | None = None) -> Topology:
    grid_path = data_dir / "raw" / "milano-grid.geojson"
    subset_path = data_dir / "processed" / "subset.json"
    for p in (grid_path, subset_path):
        if not p.exists():
            raise FileNotFoundError(f"{p} missing: run `make data` first")
    cfg = load_config(config_path)
    subset = json.loads(subset_path.read_text(encoding="utf-8"))
    topo = build(grid.load_squares(grid_path), subset["square_ids"], cfg)
    counts = {t: len(topo.by_type(t)) for t in (e.name for e in load_domain().topology_types())}
    meta = {
        "synthetic": True,
        "description": (
            "SYNTHETIC topology: Milan grid squares mapped to cells, sites, aggregation nodes, "
            "and one core by proximity. Not a real network."
        ),
        "generator": "producer.topology",
        "config": cfg.model_dump(),
        "grid_sha256": integrity.sha256_file(grid_path),
        "subset_sha256": integrity.sha256_file(subset_path),
        "counts": counts,
    }
    write(topo, data_dir / "processed", meta)
    log.info("topology (synthetic): %s", ", ".join(f"{v} {k}" for k, v in counts.items()))
    return topo


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--config", type=Path, default=None, help="override topology_config.yaml")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.data_dir, args.config)
    except (FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
