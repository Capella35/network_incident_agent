from __future__ import annotations

import json
import re
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from common import grafana, splits
from common.domain import load_domain
from common.domain.loader import KpiSource, Unit

ROOT = Path(__file__).resolve().parents[2]
PROVISIONING = ROOT / "infra" / "grafana" / "provisioning"


@pytest.mark.parametrize("name", sorted(grafana.DASHBOARDS))
def test_committed_dashboards_match_generator(name: str) -> None:
    """Edit kpis.yaml, grafana.yaml or the generator, then `make dashboards`; never the JSON."""
    committed = ROOT / grafana.DASHBOARD_DIR / name
    assert committed.read_text(encoding="utf-8") == grafana.render(grafana.DASHBOARDS[name]())


@pytest.mark.parametrize("name", sorted(grafana.ALERT_RULES))
def test_committed_alert_rules_match_generator(name: str) -> None:
    committed = ROOT / grafana.ALERTING_DIR / name
    assert committed.read_text(encoding="utf-8") == grafana.render_rules(
        grafana.ALERT_RULES[name]()
    )


def test_every_unit_maps_to_grafana() -> None:
    assert set(grafana.GRAFANA_UNIT) == set(Unit)
    assert set(grafana.DATA_LABEL) == set(KpiSource)


def test_kpi_sql_comes_from_the_catalog() -> None:
    domain = load_domain()
    for kpi in domain.kpis.values():
        if kpi.source is KpiSource.REAL:
            assert grafana.kpi_sql_expr(kpi) == " + ".join(f'"{f}"' for f in kpi.milan_fields)
        elif kpi.source is KpiSource.MODELED:
            assert grafana.kpi_sql_expr(kpi) == f'"{kpi.name}"'
        else:
            with pytest.raises(ValueError, match="not stored"):
                grafana.kpi_sql_expr(kpi)


def test_panels_titles_units_and_labels() -> None:
    domain = load_domain()
    dash = grafana.network_kpis_dashboard()
    kpi_panels = [p for p in dash["panels"] if p["type"] == "timeseries"]
    assert len(kpi_panels) == len(grafana.NETWORK_PANEL_KPIS)
    for panel, name in zip(kpi_panels, grafana.NETWORK_PANEL_KPIS, strict=True):
        kpi = domain.kpi(name)
        assert panel["title"] == grafana.kpi_title(kpi)
        assert kpi.description in panel["title"]
        assert panel["fieldConfig"]["defaults"]["unit"] == grafana.GRAFANA_UNIT[kpi.unit]
        sql = panel["targets"][0]["rawSql"]
        assert grafana.kpi_sql_expr(kpi) in sql and "$__timeFilter(interval_start)" in sql
    titles = " ".join(p["title"] for p in kpi_panels)
    assert "MODELED" in titles and "real" in titles  # Rule 9: synthetic parts labeled


def test_unknown_panel_kpi_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(grafana, "NETWORK_PANEL_KPIS", ("no_such_kpi",))
    with pytest.raises(KeyError):
        grafana.network_kpis_dashboard()


def test_footer_attribution_and_synthetic_label() -> None:
    dash = grafana.network_kpis_dashboard()
    footer = dash["panels"][-1]
    assert footer["type"] == "text"
    text = footer["options"]["content"]
    assert "from BigDataChallenge contest" in text and "ODbL" in text  # P3
    # the link text points at the challenge page, and the ODI Trento page is linked too
    assert f"[from BigDataChallenge contest]({grafana.BIGDATACHALLENGE_URL})" in text
    assert f"({grafana.ODI_TRENTO_URL})" in text
    assert "synthetic/modeled" in text


def test_dashboard_opens_on_training_week_and_refreshes() -> None:
    dash = grafana.network_kpis_dashboard()
    start = splits.bounds()[splits.Split.TRAIN][0]
    assert dash["time"]["from"].startswith(start.isoformat())
    assert dash["refresh"] == "5s" and dash["editable"] is False
    names = [v["name"] for v in dash["templating"]["list"]]
    assert names == ["run", "site", "cell"]


def test_provisioned_datasource_matches_generator_and_is_readonly() -> None:
    doc = yaml.safe_load((PROVISIONING / "datasources" / "postgres.yaml").read_text())
    (ds,) = doc["datasources"]
    assert ds["uid"] == grafana.DATASOURCE_UID and ds["type"] == grafana.DATASOURCE["type"]
    assert ds["user"] == "readonly" and ds["editable"] is False
    secret = ds["secureJsonData"]["password"]
    assert re.fullmatch(r"\$\{[A-Z_]+\}", secret) and "READONLY" in secret  # env placeholder


def test_dashboard_provider_points_at_mounted_folder() -> None:
    doc = yaml.safe_load((PROVISIONING / "dashboards" / "netops.yaml").read_text())
    (provider,) = doc["providers"]
    assert provider["options"]["path"] == "/etc/grafana/dashboards"
    assert provider["allowUiUpdates"] is False


def test_main_writes_dashboards_and_rules(tmp_path: Path) -> None:
    args = ["--out-dir", str(tmp_path), "--alerting-dir", str(tmp_path / "alerting")]
    assert grafana.main(args) == 0
    written = json.loads((tmp_path / "network-kpis.json").read_text())
    assert written["uid"] == grafana.NETWORK_KPIS_UID
    assert {p.name for p in tmp_path.glob("*.json")} == set(grafana.DASHBOARDS)
    assert yaml.safe_load((tmp_path / "alerting" / "detector.yaml").read_text())["groups"]


def test_dead_detector_rule_fires_on_waiting_telemetry_from_config() -> None:
    health = grafana.load_health()
    (group,) = grafana.dead_detector_rules()["groups"]
    assert (
        group["folder"] == grafana.FOLDER and group["interval"] == f"{health.alert_every_seconds}s"
    )
    (rule,) = group["rules"]
    assert rule["for"] == f"{health.alert_for_minutes}m" and rule["noDataState"] == "OK"
    query, threshold = (d["model"] for d in rule["data"])
    assert query["rawSql"] == grafana.UNSCORED_MINUTES_SQL
    assert query["datasource"] == grafana.DATASOURCE  # the readonly role (S4)
    assert threshold["conditions"][0]["evaluator"] == {
        "type": "gt",
        "params": [health.dead_detector_minutes],
    }
    assert rule["condition"] == threshold["refId"]
    for table in ("telemetry_raw", "ingested_at", "detector_heartbeats"):
        assert table in grafana.UNSCORED_MINUTES_SQL


def test_detection_dashboard_labels_synthetic_faults_and_reads_only_totals() -> None:
    dash = grafana.detection_dashboard()
    start = splits.bounds()[splits.Split.VALIDATION][0]  # local midnight: 23:00 UTC before
    assert dash["time"]["from"] == f"{start - timedelta(days=1)}T23:00:00.000Z"
    sqls = [t["rawSql"] for p in dash["panels"] for t in p["targets"]]
    assert any("FROM alarms" in q for q in sqls) and any("FROM telemetry_raw" in q for q in sqls)
    quality = [p for p in dash["panels"] if "detection_reports_latest" in p["targets"][0]["rawSql"]]
    assert len(quality) == 3 and all("SYNTHETIC" in p["title"] for p in quality)  # Rule 9
    assert not any("ground_truth" in q or "fault_id" in q for q in sqls)  # Rule 6: totals only


def test_health_dashboard_shows_the_alert_and_its_value() -> None:
    dash = grafana.detector_health_dashboard()
    assert dash["time"] == {"from": "now-1h", "to": "now"}  # wall clock, not 2013
    types = [p["type"] for p in dash["panels"]]
    assert "alertlist" in types
    stat = next(p for p in dash["panels"] if p["type"] == "stat")
    assert stat["targets"][0]["rawSql"] == grafana.UNSCORED_MINUTES_SQL
    assert [p["id"] for p in dash["panels"]] == list(range(1, len(types) + 1))
