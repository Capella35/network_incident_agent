"""Load the SYNTHETIC topology CSVs into Postgres (milestone 1, step 6).

    python -m producer.topology_db [--data-dir data]      # or: make db-topology

Reads `topology_nodes.csv`, `topology_edges.csv` and `topology_meta.json` from
`data/processed/`, checks the CSVs against the hashes recorded in the metadata, and replaces the
topology tables in a single transaction as `pipeline_writer`. Running it twice gives the same
tables.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from common.db import DbConfigError, DbSettings, Role
from producer import integrity, topology

log = logging.getLogger("producer.topology_db")

NodeRow = tuple[str, str, str | None, float, float, int | None]
EdgeRow = tuple[str, str, str]


class TopologyFileError(ValueError):
    pass


@dataclass(frozen=True)
class TopologyRows:
    nodes: list[NodeRow]
    edges: list[EdgeRow]
    meta: dict[str, Any]


def _read_csv(path: Path, columns: tuple[str, ...]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if tuple(reader.fieldnames or ()) != columns:
            raise TopologyFileError(f"{path.name}: columns {reader.fieldnames}, expected {columns}")
        return list(reader)


def read_topology(processed: Path) -> TopologyRows:
    """Read and check the generator output; nothing is written anywhere."""
    nodes_path = processed / "topology_nodes.csv"
    edges_path = processed / "topology_edges.csv"
    meta_path = processed / "topology_meta.json"
    for p in (nodes_path, edges_path, meta_path):
        if not p.exists():
            raise FileNotFoundError(f"{p} missing: run `make topology` first")

    meta: dict[str, Any] = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("synthetic") is not True:
        raise TopologyFileError("topology_meta.json is not marked synthetic (Rule 9)")
    for path, key in ((nodes_path, "nodes_sha256"), (edges_path, "edges_sha256")):
        if integrity.sha256_file(path) != meta.get(key):
            raise TopologyFileError(f"{path.name} does not match {key} in topology_meta.json")

    nodes: list[NodeRow] = [
        (
            r["node_id"],
            r["node_type"],
            r["parent_id"] or None,
            float(r["lat"]),
            float(r["lon"]),
            int(r["square_id"]) if r["square_id"] else None,
        )
        for r in _read_csv(nodes_path, topology.NODE_COLUMNS)
    ]
    edges: list[EdgeRow] = [
        (r["src"], r["dst"], r["relation"]) for r in _read_csv(edges_path, topology.EDGE_COLUMNS)
    ]
    ids = {n[0] for n in nodes}
    if len(ids) != len(nodes):
        raise TopologyFileError("duplicate node_id in topology_nodes.csv")
    dangling = sorted({e for s, d, _ in edges for e in (s, d)} - ids)
    if dangling:
        raise TopologyFileError(f"edges reference unknown nodes: {dangling[:5]}")
    return TopologyRows(nodes=nodes, edges=edges, meta=meta)


def load(conn: psycopg.Connection[Any], rows: TopologyRows) -> None:
    """Replace the topology tables with `rows`, atomically."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM topology_edges")
        cur.execute("DELETE FROM topology_nodes")
        cur.executemany(
            "INSERT INTO topology_nodes (node_id, node_type, parent_id, lat, lon, square_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            rows.nodes,
        )
        cur.executemany(
            "INSERT INTO topology_edges (src, dst, relation) VALUES (%s, %s, %s)", rows.edges
        )
        cur.execute(
            "INSERT INTO topology_meta (id, synthetic, meta, loaded_at) "
            "VALUES (1, true, %s, now()) "
            "ON CONFLICT (id) DO UPDATE SET meta = EXCLUDED.meta, loaded_at = EXCLUDED.loaded_at",
            (Jsonb(rows.meta),),
        )


def run(data_dir: Path, settings: DbSettings) -> TopologyRows:
    rows = read_topology(data_dir / "processed")
    with settings.connect() as conn:
        load(conn, rows)
    log.info(
        "loaded SYNTHETIC topology into %s: %d nodes, %d edges",
        settings.dbname,
        len(rows.nodes),
        len(rows.edges),
    )
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.data_dir, DbSettings.from_env(Role.PIPELINE_WRITER))
    except (FileNotFoundError, ValueError, DbConfigError, psycopg.Error) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
