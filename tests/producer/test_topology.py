from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from common.domain import load_domain
from producer import grid, topology
from tests.producer.conftest import write_grid


@pytest.fixture(scope="module")
def squares(tmp_path_factory: pytest.TempPathFactory) -> dict[int, grid.Square]:
    path = tmp_path_factory.mktemp("grid") / "milano-grid.geojson"
    write_grid(path)
    return grid.load_squares(path)


@pytest.fixture(scope="module")
def subset(squares: dict[int, grid.Square]) -> list[int]:
    centre = grid.nearest_square(squares, *grid.DUOMO_LON_LAT)
    return grid.select_block(squares, centre, grid.SUBSET_BLOCK)


@pytest.fixture(scope="module")
def topo(squares: dict[int, grid.Square], subset: list[int]) -> topology.Topology:
    return topology.build(squares, subset, topology.load_config())


def test_counts(topo: topology.Topology) -> None:
    assert len(topo.by_type("Core")) == 1
    assert len(topo.by_type("AggregationNode")) == 5
    assert len(topo.by_type("Site")) == 48
    assert len(topo.by_type("Cell")) == 144
    assert len(topo.edges) == 5 + 48 + 144


def test_ids_match_domain_patterns(topo: topology.Topology) -> None:
    d = load_domain()
    ids = [n.node_id for n in topo.nodes]
    assert len(ids) == len(set(ids))
    for n in topo.nodes:
        assert d.entity(n.node_type).matches(n.node_id), n.node_id


def test_every_cell_reaches_core_through_each_level(topo: topology.Topology) -> None:
    by_id = {n.node_id: n for n in topo.nodes}
    for cell in topo.by_type("Cell"):
        chain = [cell.node_type]
        node = cell
        while node.parent_id is not None:
            node = by_id[node.parent_id]
            chain.append(node.node_type)
        assert chain == ["Cell", "Site", "AggregationNode", "Core"]


def test_each_square_is_one_cell(topo: topology.Topology, subset: list[int]) -> None:
    assert sorted(n.square_id for n in topo.by_type("Cell") if n.square_id) == sorted(subset)
    assert all(n.square_id is None for n in topo.nodes if n.node_type != "Cell")


def test_sites_are_adjacent_strips(
    topo: topology.Topology, squares: dict[int, grid.Square]
) -> None:
    cells_by_site: dict[str, list[grid.Square]] = {}
    for c in topo.by_type("Cell"):
        assert c.parent_id is not None and c.square_id is not None
        cells_by_site.setdefault(c.parent_id, []).append(squares[c.square_id])
    for cells in cells_by_site.values():
        assert len(cells) == 3
        assert len({s.row for s in cells}) == 1
        cols = sorted(s.col for s in cells)
        assert cols == list(range(cols[0], cols[0] + 3))


def test_aggregation_node_sizes(topo: topology.Topology) -> None:
    cfg = topology.load_config()
    for agg in topo.by_type("AggregationNode"):
        n = sum(1 for s in topo.by_type("Site") if s.parent_id == agg.node_id)
        assert cfg.min_sites_per_aggregation_node <= n <= cfg.max_sites_per_aggregation_node


def test_edges_use_domain_relation(topo: topology.Topology) -> None:
    assert {rel for _, _, rel in topo.edges} == {"parent_of"}
    parent = {n.node_id: n.parent_id for n in topo.nodes}
    assert all(parent[dst] == src for src, dst, _ in topo.edges)


def test_deterministic(squares: dict[int, grid.Square], subset: list[int]) -> None:
    cfg = topology.load_config()
    assert topology.build(squares, subset, cfg) == topology.build(squares, subset, cfg)


def test_rejects_bad_block(squares: dict[int, grid.Square], subset: list[int]) -> None:
    cfg = topology.load_config()
    with pytest.raises(ValueError, match="rectangular"):
        topology.build(squares, subset[:-1], cfg)
    narrow = cfg.model_copy(update={"sectors_per_site": 5})
    with pytest.raises(ValueError, match="multiple"):
        topology.build(squares, subset, narrow)


def test_rejects_unbalanced_aggregation(squares: dict[int, grid.Square], subset: list[int]) -> None:
    cfg = topology.load_config().model_copy(update={"min_sites_per_aggregation_node": 20})
    with pytest.raises(ValueError, match="outside"):
        topology.build(squares, subset, cfg)


def test_config_validation(tmp_path: Path) -> None:
    p = tmp_path / "c.yaml"
    p.write_text(
        "version: 1\nseed: 1\nsectors_per_site: 3\naggregation_nodes: 5\n"
        "min_sites_per_aggregation_node: 9\nmax_sites_per_aggregation_node: 3\n"
        "kmeans_iterations: 10\n"
    )
    with pytest.raises(ValueError, match="min_sites"):
        topology.load_config(p)


def test_run_writes_files_and_is_reproducible(tmp_path: Path) -> None:
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    raw.mkdir()
    processed.mkdir()
    write_grid(raw / "milano-grid.geojson")
    sub = grid.build_subset(raw / "milano-grid.geojson")
    (processed / "subset.json").write_text(json.dumps(sub))

    assert topology.main(["--data-dir", str(tmp_path)]) == 0
    meta = json.loads((processed / "topology_meta.json").read_text())
    assert meta["synthetic"] is True
    assert meta["counts"] == {"Core": 1, "AggregationNode": 5, "Site": 48, "Cell": 144}

    with (processed / "topology_nodes.csv").open() as fh:
        rows = list(csv.DictReader(fh))
    assert list(rows[0]) == list(topology.NODE_COLUMNS)
    assert rows[0]["node_id"] == "CORE-1" and rows[0]["parent_id"] == ""

    first = (processed / "topology_nodes.csv").read_bytes()
    topology.main(["--data-dir", str(tmp_path)])
    assert (processed / "topology_nodes.csv").read_bytes() == first


def test_run_needs_subset(tmp_path: Path) -> None:
    assert topology.main(["--data-dir", str(tmp_path)]) == 1
