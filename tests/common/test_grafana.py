from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from common import grafana, splits
from common.domain import load_domain
from common.domain.loader import KpiSource, Unit

ROOT = Path(__file__).resolve().parents[2]
COMMITTED = ROOT / grafana.DASHBOARD_DIR / "network-kpis.json"
PROVISIONING = ROOT / "infra" / "grafana" / "provisioning"


def test_committed_dashboard_matches_generator() -> None:
    """Edit kpis.yaml or the generator, then run `make dashboards`; never edit the JSON."""
    assert COMMITTED.read_text(encoding="utf-8") == grafana.render(grafana.network_kpis_dashboard())


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


def test_main_writes_dashboards(tmp_path: Path) -> None:
    assert grafana.main(["--out-dir", str(tmp_path)]) == 0
    written = json.loads((tmp_path / "network-kpis.json").read_text())
    assert written["uid"] == grafana.NETWORK_KPIS_UID
