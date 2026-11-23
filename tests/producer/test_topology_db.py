from __future__ import annotations

import json
from pathlib import Path

import pytest

from producer import grid, topology, topology_db
from tests.producer.conftest import write_grid


@pytest.fixture(scope="module")
def topo(tmp_path_factory: pytest.TempPathFactory) -> topology.Topology:
    path = tmp_path_factory.mktemp("grid") / "milano-grid.geojson"
    write_grid(path)
    squares = grid.load_squares(path)
    centre = grid.nearest_square(squares, *grid.DUOMO_LON_LAT)
    subset = grid.select_block(squares, centre, grid.SUBSET_BLOCK)
    return topology.build(squares, subset, topology.load_config())


@pytest.fixture
def processed(tmp_path: Path, topo: topology.Topology) -> Path:
    topology.write(topo, tmp_path, {"synthetic": True})
    return tmp_path


def test_reads_generator_output(processed: Path, topo: topology.Topology) -> None:
    rows = topology_db.read_topology(processed)
    assert len(rows.nodes) == len(topo.nodes) == 1 + 5 + 48 + 144
    assert len(rows.edges) == len(topo.edges)
    core = rows.nodes[0]
    assert core[2] is None and core[5] is None  # root: no parent, no square
    cells = [n for n in rows.nodes if n[5] is not None]
    assert len(cells) == 144
    assert all(isinstance(n[3], float) and isinstance(n[4], float) for n in rows.nodes)


def test_rejects_csv_changed_after_generation(processed: Path) -> None:
    with (processed / "topology_nodes.csv").open("a", encoding="utf-8") as fh:
        fh.write("CELL-99-1,Cell,SITE-99,45.0,9.0,1\n")
    with pytest.raises(topology_db.TopologyFileError, match="nodes_sha256"):
        topology_db.read_topology(processed)


def test_rejects_topology_not_marked_synthetic(processed: Path) -> None:
    meta_path = processed / "topology_meta.json"
    meta = json.loads(meta_path.read_text())
    meta_path.write_text(json.dumps({**meta, "synthetic": False}))
    with pytest.raises(topology_db.TopologyFileError, match="synthetic"):
        topology_db.read_topology(processed)


def test_rejects_dangling_edge(tmp_path: Path, topo: topology.Topology) -> None:
    broken = topology.Topology(
        nodes=topo.nodes, edges=(*topo.edges, ("AGG-9", "SITE-1", "parent_of"))
    )
    topology.write(broken, tmp_path, {"synthetic": True})
    with pytest.raises(topology_db.TopologyFileError, match="unknown nodes"):
        topology_db.read_topology(tmp_path)


def test_missing_files(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="make topology"):
        topology_db.read_topology(tmp_path)


def test_main_reports_missing_password(processed: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_WRITER_PASSWORD", raising=False)
    assert topology_db.main(["--data-dir", str(processed.parent)]) == 1
