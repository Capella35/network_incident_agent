"""Grafana dashboards and alert rules generated as code (milestone 1 step 9, milestone 2 step 10).

    python -m common.grafana            # writes infra/grafana/dashboards/*.json + the alert rule
    make dashboards

Panel titles, units, and the SQL for each KPI come from `kpis.yaml` (Rule 15): a REAL KPI is
the sum of its Milan fields, a MODELED KPI is its own column in `telemetry_raw`. The committed
JSON is checked against this generator by a unit test, so the two cannot drift. The data source
is the provisioned Postgres connection that logs in as the read-only role (guardrail S4).

The data is from 2013, so the data dashboards open on a week of their split (from
`common.splits`), not on "now"; the detector-health dashboard shows wall-clock time.

Milestone 2 (step 10): the dead-detector alert (`grafana.yaml`; specs/03-detection.md) and its
health dashboard read `telemetry_raw.ingested_at` and `detector_heartbeats`; the detection
dashboard reads `alarms`, and `detection_reports` (totals that `make detect-check` scored
against the SYNTHETIC ground truth; Grafana never sees fault labels, Rule 6).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from pydantic import PositiveInt

from common import splits, telemetry
from common.domain import load_domain
from common.domain.loader import FrozenModel, Kpi, KpiSource, Unit, read_config

DATASOURCE_UID = "netops-postgres"  # infra/grafana/provisioning/datasources/postgres.yaml
DATASOURCE = {"type": "grafana-postgresql-datasource", "uid": DATASOURCE_UID}
DASHBOARD_DIR = Path("infra/grafana/dashboards")
ALERTING_DIR = Path("infra/grafana/provisioning/alerting")
NETWORK_KPIS_UID = "netops-network-kpis"
DETECTION_UID = "netops-detection"
DETECTOR_HEALTH_UID = "netops-detector-health"
DEAD_DETECTOR_UID = "netops-dead-detector"
FOLDER = "Network Incident Agent"  # provisioning/dashboards/netops.yaml

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


def query_var(name: str, label: str, sql: str, **extra: Any) -> dict[str, Any]:
    """A dashboard variable filled by a query (refreshed on time-range change unless `extra`
    says otherwise, e.g. refresh=1: on dashboard load)."""
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


def _variables() -> list[dict[str, Any]]:
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


def _target(ref: str, sql: str, fmt: str = "time_series") -> dict[str, Any]:
    """A raw-SQL query on the provisioned (readonly) data source: panels and alert rules."""
    return {
        "refId": ref,
        "datasource": DATASOURCE,
        "editorMode": "code",
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql,
    }


def _sql_panel(
    panel_type: str,
    title: str,
    queries: dict[str, str],
    *,
    grid: tuple[int, int, int, int],
    description: str | None = None,
    fmt: str = "time_series",
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A panel of raw-SQL queries (`queries`: refId -> SQL); `grid`: x, y, width, height;
    `defaults`: Grafana's field defaults (unit, draw settings; unit "none" if not given)."""
    x, y, w, h = grid
    panel: dict[str, Any] = {
        "type": panel_type,
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults or {"unit": "none"}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}},
        "targets": [_target(ref, sql, fmt) for ref, sql in queries.items()],
    }
    if description is not None:
        panel["description"] = description
    return panel


def _kpi_panel(kpi: Kpi, y: int) -> dict[str, Any]:
    sql = (
        f"SELECT interval_start AS time, cell_id AS metric, {kpi_sql_expr(kpi)} AS value\n"
        "FROM telemetry_raw\n"
        "WHERE replay_run_id = '$run' AND site_id = '$site' AND cell_id IN ($cell)\n"
        "  AND $__timeFilter(interval_start)\n"
        "ORDER BY 1, 2"
    )
    return _sql_panel(
        "timeseries",
        kpi_title(kpi),
        {"A": sql},
        grid=(0, y, 24, 9),
        description=f"KPI `{kpi.name}` ({kpi.unit}); bad direction: {kpi.bad_direction}.",
        defaults={"unit": GRAFANA_UNIT[kpi.unit], "custom": {"spanNulls": False}},
    )


def _time_range(start: date, days: int, tz: tzinfo = UTC) -> dict[str, str]:
    """`days` days from midnight of `start` in `tz`, as Grafana's UTC range."""
    t0 = datetime.combine(start, time(), tz).astimezone(UTC)
    return {
        "from": t0.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "to": (t0 + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }


def _dashboard(
    uid: str,
    title: str,
    description: str,
    *,
    time_range: dict[str, str],
    panels: list[dict[str, Any]],
    variables: list[dict[str, Any]] | None = None,
    refresh: str = "5s",
) -> dict[str, Any]:
    """A dashboard of `panels`, numbered in order."""
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["netops", "synthetic-topology"],
        "timezone": "utc",
        "refresh": refresh,
        "time": time_range,
        "editable": False,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "templating": {"list": variables or []},
        "panels": [{"id": i, **p} for i, p in enumerate(panels, start=1)],
    }


def network_kpis_dashboard() -> dict[str, Any]:
    domain = load_domain()
    kpis = [domain.kpi(name) for name in NETWORK_PANEL_KPIS]  # raises if a name is unknown
    footer = {
        "type": "text",
        "title": "",
        "gridPos": {"h": 2, "w": 24, "x": 0, "y": 9 * len(kpis)},
        "options": {"mode": "markdown", "content": FOOTER},
    }
    return _dashboard(
        NETWORK_KPIS_UID,
        "Network KPIs (milestone 1)",
        "Per-cell telemetry from telemetry_raw. " + FOOTER,
        time_range=_time_range(splits.bounds()[splits.Split.TRAIN][0], 7),
        panels=[*(_kpi_panel(k, 9 * i) for i, k in enumerate(kpis)), footer],
        variables=_variables(),
    )


# --- Milestone 2, step 10: detection and detector health -------------------------------------


class Health(FrozenModel):
    dead_detector_minutes: PositiveInt
    alert_every_seconds: PositiveInt
    alert_for_minutes: PositiveInt


@cache
def load_health() -> Health:
    return read_config(Health, files("common").joinpath("grafana.yaml"))


# Minutes the oldest telemetry row stored after the latest detector heartbeat has waited (0 if
# none): what the dead-detector alert fires on. It stays up until the detector reports again,
# however long ago the telemetry stopped. A live detector writes a heartbeat per micro-batch,
# also every trigger while any cell's state waits for its timeout, so telemetry the sink stores
# a little after the detector's last batch does not fire it. Known gap: telemetry stored long
# after the detector went idle (`make sink` started over an hour after a replay) fires it.
UNSCORED_MINUTES_SQL = (
    "SELECT coalesce(extract(epoch FROM now() - min(ingested_at)) / 60, 0) AS unscored_minutes\n"
    "FROM telemetry_raw\n"
    "WHERE ingested_at > coalesce((SELECT max(at) FROM detector_heartbeats), '-infinity')"
)


def dead_detector_rules() -> dict[str, Any]:
    """Grafana alert provisioning (one rule): telemetry flows, the detector does not report.
    Shown on the detector-health dashboard; no contact point is configured (local only)."""
    health = load_health()
    query = _target("A", UNSCORED_MINUTES_SQL, "table")
    threshold = {
        "refId": "B",
        "datasource": {"type": "__expr__", "uid": "__expr__"},
        "type": "threshold",
        "expression": "A",
        "conditions": [{"evaluator": {"type": "gt", "params": [health.dead_detector_minutes]}}],
    }
    rule = {
        "uid": DEAD_DETECTOR_UID,
        "title": "Dead detector",
        "condition": "B",
        "data": [
            {
                "refId": "A",
                "relativeTimeRange": {"from": 600, "to": 0},
                "datasourceUid": DATASOURCE_UID,
                "model": query,
            },
            {"refId": "B", "datasourceUid": "__expr__", "model": threshold},
        ],
        "noDataState": "OK",
        "execErrState": "Error",
        "for": f"{health.alert_for_minutes}m",
        "annotations": {
            "summary": (
                f"Telemetry has waited over {health.dead_detector_minutes} min with no detector "
                "heartbeat: is the Spark job running (make detect)?"
            ),
            "dashboardUId": DETECTOR_HEALTH_UID,
        },
        "labels": {"component": "detector"},
        "isPaused": False,
    }
    return {
        "apiVersion": 1,
        "groups": [
            {
                "orgId": 1,
                "name": "detector-health",
                "folder": FOLDER,
                "interval": f"{health.alert_every_seconds}s",
                "rules": [rule],
            }
        ],
    }


# Runs that raised alarms, newest first (read on dashboard load, not on every refresh).
_RUNS = "SELECT replay_run_id FROM alarms GROUP BY 1 ORDER BY max(stored_at) DESC"
_SYNTHETIC = "Faults are SYNTHETIC (validation weeks); incidents use the provisional M2 grouping."
_BARS = {"unit": "none", "custom": {"drawStyle": "bars", "fillOpacity": 60}}


def detection_dashboard() -> dict[str, Any]:
    alarms_vs_telemetry = _sql_panel(
        "timeseries",
        "Alarms vs telemetry per hour (replay run $run)",
        {
            "A": "SELECT $__timeGroupAlias(interval_start, '1h'), count(*) AS telemetry_rows\n"
            "FROM telemetry_raw\n"
            "WHERE replay_run_id = '$run' AND $__timeFilter(interval_start)\n"
            "GROUP BY 1 ORDER BY 1",
            "B": "SELECT $__timeGroupAlias(interval_start, '1h'), count(*) AS alarms\n"
            "FROM alarms\n"
            "WHERE replay_run_id = '$run' AND $__timeFilter(interval_start)\n"
            "GROUP BY 1 ORDER BY 1",
        },
        grid=(0, 0, 24, 9),
        description="Cell-intervals in telemetry_raw (filled by `make sink`) and alarms the "
        "detector raised. Topology and latency / load KPIs are synthetic/modeled.",
    )
    false_per_day = _sql_panel(
        "timeseries",
        "False incidents per checked day (SYNTHETIC faults)",
        {
            "A": "SELECT day::timestamptz AS time, false_incidents\n"
            "FROM detection_reports_latest WHERE $__timeFilter(day::timestamptz) ORDER BY 1"
        },
        grid=(0, 9, 12, 8),
        description=_SYNTHETIC,
        defaults=_BARS,
    )
    delay = _sql_panel(
        "timeseries",
        "Detection delay, Kaplan-Meier median (intervals; SYNTHETIC faults)",
        {
            "A": "SELECT day::timestamptz AS time, delay_median, delay_low, delay_high\n"
            "FROM detection_reports_latest WHERE $__timeFilter(day::timestamptz) ORDER BY 1"
        },
        grid=(12, 9, 12, 8),
        description=_SYNTHETIC,
        defaults=_BARS,
    )
    table = _sql_panel(
        "table",
        "Checked days (make detect-check; SYNTHETIC faults)",
        {
            "A": "SELECT day, detector_version, faults, unobservable, detected,\n"
            "  round(detected::numeric / nullif(faults - unobservable, 0), 3) AS detection_rate,\n"
            "  delay_median, delay_low, delay_high, incidents, false_incidents, parity,\n"
            "  replay_run_id\n"
            "FROM detection_reports_latest WHERE $__timeFilter(day::timestamptz) ORDER BY day"
        },
        grid=(0, 17, 24, 8),
        description=_SYNTHETIC + " parity: streamed alarms == offline scoring.",
        fmt="table",
    )
    return _dashboard(
        DETECTION_UID,
        "Detection (milestone 2)",
        "Detector alarms and their quality on checked validation days. " + _SYNTHETIC,
        # local days, as the checks count them (`day` bars sit at UTC midnight, 01:00 local)
        time_range=_time_range(splits.bounds()[splits.Split.VALIDATION][0], 14, telemetry.LOCAL_TZ),
        panels=[alarms_vs_telemetry, false_per_day, delay, table],
        variables=[query_var("run", "Replay run", _RUNS, refresh=1)],
        refresh="1m",  # historical data: changes only during a replay or a check
    )


def detector_health_dashboard() -> dict[str, Any]:
    health = load_health()
    waiting = _sql_panel(
        "stat",
        "Unscored telemetry waiting (minutes)",
        {"A": UNSCORED_MINUTES_SQL},
        grid=(0, 0, 8, 6),
        description=f"The dead-detector alert fires above {health.dead_detector_minutes} min "
        "(src/common/grafana.yaml).",
        fmt="table",
    )
    waiting["fieldConfig"]["defaults"]["thresholds"] = {
        "mode": "absolute",
        "steps": [
            {"color": "green", "value": None},
            {"color": "red", "value": health.dead_detector_minutes},
        ],
    }
    alert = {
        "type": "alertlist",
        "title": "Dead-detector alert",
        "gridPos": {"h": 6, "w": 16, "x": 8, "y": 0},
        "options": {
            "viewMode": "list",
            "groupMode": "default",
            "maxItems": 10,
            "sortOrder": 1,
            "alertName": "Dead detector",
            "stateFilter": {
                "firing": True,
                "pending": True,
                "normal": True,
                "error": True,
                "noData": True,
            },
        },
    }
    flow = _sql_panel(
        "timeseries",
        "Telemetry stored vs rows scored, per minute (wall clock)",
        {
            "A": "SELECT $__timeGroupAlias(ingested_at, '1m'), count(*) AS telemetry_stored\n"
            "FROM telemetry_raw WHERE $__timeFilter(ingested_at)\n"
            "GROUP BY 1 ORDER BY 1",
            "B": "SELECT $__timeGroupAlias(at, '1m'), sum(rows_scored) AS rows_scored,\n"
            "  sum(alarms) AS alarms, sum(rows_late) AS rows_late\n"
            "FROM detector_heartbeats WHERE $__timeFilter(at)\n"
            "GROUP BY 1 ORDER BY 1",
        },
        grid=(0, 6, 24, 9),
    )
    heartbeats = _sql_panel(
        "table",
        "Latest detector heartbeats",
        {
            "A": "SELECT at, job_id, batch_id, detector_version, rows_scored, rows_late, alarms,\n"
            "  latest_interval\n"
            "FROM detector_heartbeats ORDER BY at DESC LIMIT 20"
        },
        grid=(0, 15, 24, 8),
        fmt="table",
    )
    return _dashboard(
        DETECTOR_HEALTH_UID,
        "Detector health (milestone 2)",
        "Is the Spark detector keeping up with the telemetry? Wall-clock time.",
        time_range={"from": "now-1h", "to": "now"},
        panels=[waiting, alert, flow, heartbeats],
        refresh="30s",  # the alert itself is evaluated every alert_every_seconds
    )


def render(dashboard: dict[str, Any]) -> str:
    return json.dumps(dashboard, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def render_rules(rules: dict[str, Any]) -> str:
    header = "# Generated by `make dashboards` (src/common/grafana.py); never edit by hand.\n"
    return header + yaml.safe_dump(rules, sort_keys=True, allow_unicode=True)


DASHBOARDS = {
    "network-kpis.json": network_kpis_dashboard,
    "detection.json": detection_dashboard,
    "detector-health.json": detector_health_dashboard,
}
ALERT_RULES = {"detector.yaml": dead_detector_rules}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--out-dir", type=Path, default=DASHBOARD_DIR)
    ap.add_argument("--alerting-dir", type=Path, default=ALERTING_DIR)
    args = ap.parse_args(argv)
    outputs = [(args.out_dir / n, render(b())) for n, b in DASHBOARDS.items()]
    outputs += [(args.alerting_dir / n, render_rules(b())) for n, b in ALERT_RULES.items()]
    for path, text in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
