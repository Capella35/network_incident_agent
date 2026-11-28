"""Grafana against the running stack: provisioned data source and dashboard, through its API.

Needs GRAFANA_ADMIN_PASSWORD in the environment (`make test-integration` loads it); the value
is only sent to Grafana on 127.0.0.1 and never printed.
"""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest

from common import grafana

pytestmark = pytest.mark.integration

BASE = f"http://127.0.0.1:{os.environ.get('GRAFANA_PORT', '3000')}"


def _api(path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    password = os.environ.get("GRAFANA_ADMIN_PASSWORD")
    if not password:
        pytest.skip("GRAFANA_ADMIN_PASSWORD not in the environment; run via make")
    token = base64.b64encode(f"admin:{password}".encode()).decode()
    req = urllib.request.Request(  # noqa: S310 - fixed http://127.0.0.1 URL
        BASE + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
        method="GET" if body is None else "POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")
    except urllib.error.URLError:
        pytest.skip("Grafana not reachable; run `make up` first")


def _query(sql: str, time_from: str, time_to: str) -> tuple[int, Any]:
    return _api(
        "/api/ds/query",
        {
            "from": time_from,
            "to": time_to,
            "queries": [
                {
                    "refId": "A",
                    "datasource": grafana.DATASOURCE,
                    "rawSql": sql,
                    "format": "table",
                }
            ],
        },
    )


def _rows(result: Any) -> list[list[Any]]:
    frame = result["results"]["A"]["frames"][0]
    values: list[list[Any]] = frame["data"]["values"]
    return [list(r) for r in zip(*values, strict=True)] if values else []


def test_datasource_provisioned_as_readonly_and_healthy() -> None:
    status, ds = _api(f"/api/datasources/uid/{grafana.DATASOURCE_UID}")
    assert status == 200
    assert ds["type"] == grafana.DATASOURCE["type"] and ds["user"] == "readonly"
    assert ds["readOnly"] is True  # provisioned: not editable in the UI
    status, health = _api(f"/api/datasources/uid/{grafana.DATASOURCE_UID}/health")
    assert status == 200 and health["status"] == "OK", health


def test_datasource_cannot_write() -> None:
    """Guardrail S4 through Grafana: a write from a panel query is refused.

    Two layers refuse it: Grafana's Postgres plugin runs queries in a read-only transaction
    (SQLSTATE 25006), and the `readonly` role has no write grants (42501; proven directly in
    test_db_stack.py). Either refusal passes; any success fails.
    """
    _, result = _query(
        "INSERT INTO topology_meta (synthetic, meta) VALUES (true, '{}')",
        "now-1h",
        "now",
    )
    error = json.dumps(result).lower()
    assert "read-only transaction" in error or "permission denied" in error, result


def test_dashboard_provisioned_from_generator() -> None:
    status, doc = _api(f"/api/dashboards/uid/{grafana.NETWORK_KPIS_UID}")
    assert status == 200
    dash = doc["dashboard"]
    expected = grafana.network_kpis_dashboard()
    assert dash["title"] == expected["title"]
    assert [p["title"] for p in dash["panels"]] == [p["title"] for p in expected["panels"]]
    assert doc["meta"]["provisioned"] is True


def test_panel_query_returns_stored_rows() -> None:
    status, runs = _query(
        "SELECT replay_run_id, min(site_id), count(*) FROM telemetry_raw "
        "GROUP BY 1 ORDER BY max(ingested_at) DESC LIMIT 1",
        "now-1h",
        "now",
    )
    assert status == 200, runs
    rows = _rows(runs)
    if not rows:
        pytest.skip("telemetry_raw is empty: `make replay` + `make sink` first")
    run, site, _ = rows[0]
    panel = grafana.network_kpis_dashboard()["panels"][0]
    sql = (
        panel["targets"][0]["rawSql"]
        .replace("($cell)", "(SELECT cell_id FROM telemetry_raw WHERE site_id = '$site')")
        .replace("$run", run)
        .replace("$site", site)
    )
    status, result = _query(sql, "2013-10-31T00:00:00Z", "2014-01-02T00:00:00Z")
    assert status == 200, result
    points = _rows(result)
    assert points, "panel query returned no rows for the latest run"
    assert all(v is not None and v >= 0 for _, _, v in points)


@pytest.mark.parametrize("uid", [grafana.DETECTION_UID, grafana.DETECTOR_HEALTH_UID])
def test_detection_dashboards_provisioned_and_their_queries_run(uid: str) -> None:
    status, doc = _api(f"/api/dashboards/uid/{uid}")
    assert status == 200 and doc["meta"]["provisioned"] is True
    for panel in doc["dashboard"]["panels"]:
        for target in panel.get("targets", []):
            sql = target["rawSql"].replace("$run", "no-such-run")
            status, result = _query(sql, "2013-12-01T00:00:00Z", "2013-12-16T00:00:00Z")
            assert status == 200, (panel["title"], result)


def test_dead_detector_rule_provisioned_and_healthy() -> None:
    status, rule = _api(f"/api/v1/provisioning/alert-rules/{grafana.DEAD_DETECTOR_UID}")
    assert status == 200, rule
    assert rule["data"][0]["model"]["rawSql"] == grafana.UNSCORED_MINUTES_SQL
    status, rules = _api("/api/prometheus/grafana/api/v1/rules")
    assert status == 200
    (found,) = [
        r for g in rules["data"]["groups"] for r in g["rules"] if r["name"] == "Dead detector"
    ]
    assert found["health"] in {"ok", "unknown"}, found  # "unknown" before its first evaluation
