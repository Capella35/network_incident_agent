"""Grafana dashboards generated from the domain model (milestone 1, step 9).

    python -m common.grafana            # writes infra/grafana/dashboards/*.json
    make dashboards

Panel titles, units, and the SQL for each KPI come from `kpis.yaml` (Rule 15): a REAL KPI is
the sum of its Milan fields, a MODELED KPI is its own column in `telemetry_raw`. The committed
JSON is checked against this generator by a unit test, so the two cannot drift. The data source
is the provisioned Postgres connection that logs in as the read-only role (guardrail S4).

The data is from 2013, so the dashboards open on the first training week (from
`common.splits`), not on "now".
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any

from common import splits
from common.domain import load_domain
from common.domain.loader import Kpi, KpiSource, Unit

DATASOURCE_UID = "netops-postgres"  # infra/grafana/provisioning/datasources/postgres.yaml
DATASOURCE = {"type": "grafana-postgresql-datasource", "uid": DATASOURCE_UID}
DASHBOARD_DIR = Path("infra/grafana/dashboards")
NETWORK_KPIS_UID = "netops-network-kpis"

# Which KPIs the milestone-1 panel shows (validated against kpis.yaml on build).
NETWORK_PANEL_KPIS = ("traffic_internet", "latency_ms")

# kpis.yaml unit -> Grafana unit id.
GRAFANA_UNIT = {
    Unit.SCALED_ACTIVITY: "none",
    Unit.RATIO: "percentunit",
    Unit.MS: "ms",
    Unit.Z_SCORE: "none",
}

DATA_LABEL = {
    KpiSource.REAL: "real, scaled",
    KpiSource.MODELED: "MODELED",
    KpiSource.DERIVED: "derived",
}

# ODbL attribution exactly as the publisher's terms of use require (Harvard Dataverse,
# doi:10.7910/DVN/EGZHFV, checked 2026-11-22; specs/02-telemetry.md, Rules).
BIGDATACHALLENGE_URL = "http://www.telecomitalia.com/tit/en/bigdatachallenge.html"
ODI_TRENTO_URL = "http://theodi.fbk.eu/"
DATASET_URL = "https://doi.org/10.7910/DVN/EGZHFV"
FOOTER = (
    f"Traffic: real ([from BigDataChallenge contest]({BIGDATACHALLENGE_URL}); "
    f"[ODI node Trento]({ODI_TRENTO_URL}); [dataset]({DATASET_URL}), ODbL 1.0). "
    "Topology and latency: synthetic/modeled. Not a real network."
)


def kpi_sql_expr(kpi: Kpi) -> str:
    """SQL expression for a KPI over `telemetry_raw` columns."""
    if kpi.source is KpiSource.REAL:
        return " + ".join(f'"{f}"' for f in kpi.milan_fields)
    if kpi.source is KpiSource.MODELED:
        return f'"{kpi.name}"'
    raise ValueError(f"KPI {kpi.name!r} is {kpi.source}; not stored in telemetry_raw")


def kpi_title(kpi: Kpi) -> str:
    return f"{kpi.description} ({DATA_LABEL[kpi.source]})"


def _variables() -> list[dict[str, Any]]:
    def query_var(name: str, label: str, sql: str, **extra: Any) -> dict[str, Any]:
        return {
            "name": name,
            "label": label,
            "type": "query",
            "datasource": DATASOURCE,
            "query": sql,
            "definition": sql,
            "refresh": 2,  # on time-range change
            "sort": 0,
            **extra,
        }

    return [
        query_var(
            "run",
            "Replay run",
            "SELECT replay_run_id FROM telemetry_raw GROUP BY 1 ORDER BY max(ingested_at) DESC",
        ),
        query_var(
            "site",
            "Site",
            "SELECT DISTINCT site_id FROM telemetry_raw WHERE replay_run_id = '$run' ORDER BY 1",
        ),
        query_var(
            "cell",
            "Cell",
            "SELECT DISTINCT cell_id FROM telemetry_raw "
            "WHERE replay_run_id = '$run' AND site_id = '$site' ORDER BY 1",
            multi=True,
            includeAll=True,
            current={"selected": True, "text": ["All"], "value": ["$__all"]},
        ),
    ]


def _kpi_panel(kpi: Kpi, panel_id: int, y: int) -> dict[str, Any]:
    sql = (
        f"SELECT interval_start AS time, cell_id AS metric, {kpi_sql_expr(kpi)} AS value\n"
        "FROM telemetry_raw\n"
        "WHERE replay_run_id = '$run' AND site_id = '$site' AND cell_id IN ($cell)\n"
        "  AND $__timeFilter(interval_start)\n"
        "ORDER BY 1, 2"
    )
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": kpi_title(kpi),
        "description": f"KPI `{kpi.name}` ({kpi.unit}); bad direction: {kpi.bad_direction}.",
        "datasource": DATASOURCE,
        "gridPos": {"h": 9, "w": 24, "x": 0, "y": y},
        "fieldConfig": {
            "defaults": {"unit": GRAFANA_UNIT[kpi.unit], "custom": {"spanNulls": False}},
            "overrides": [],
        },
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [
            {
                "refId": "A",
                "datasource": DATASOURCE,
                "editorMode": "code",
                "format": "time_series",
                "rawQuery": True,
                "rawSql": sql,
            }
        ],
    }


def network_kpis_dashboard() -> dict[str, Any]:
    domain = load_domain()
    kpis = [domain.kpi(name) for name in NETWORK_PANEL_KPIS]  # raises if a name is unknown
    train_start = splits.bounds()[splits.Split.TRAIN][0]
    t0 = datetime.combine(train_start, time(), UTC)
    panels = [_kpi_panel(k, i + 1, 9 * i) for i, k in enumerate(kpis)]
    panels.append(
        {
            "id": len(panels) + 1,
            "type": "text",
            "title": "",
            "gridPos": {"h": 2, "w": 24, "x": 0, "y": 9 * len(kpis)},
            "options": {"mode": "markdown", "content": FOOTER},
        }
    )
    return {
        "uid": NETWORK_KPIS_UID,
        "title": "Network KPIs (milestone 1)",
        "description": "Per-cell telemetry from telemetry_raw. " + FOOTER,
        "tags": ["netops", "synthetic-topology"],
        "timezone": "utc",
        "refresh": "5s",
        "time": {
            "from": t0.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": (t0 + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        },
        "editable": False,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "templating": {"list": _variables()},
        "panels": panels,
    }


def render(dashboard: dict[str, Any]) -> str:
    return json.dumps(dashboard, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


DASHBOARDS = {"network-kpis.json": network_kpis_dashboard}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--out-dir", type=Path, default=DASHBOARD_DIR)
    args = ap.parse_args(argv)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for name, build in DASHBOARDS.items():
        (args.out_dir / name).write_text(render(build()), encoding="utf-8")
        print(f"wrote {args.out_dir / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
