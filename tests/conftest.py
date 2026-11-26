"""Fixtures shared across test packages."""

from __future__ import annotations

from pathlib import Path

import mlflow
import pytest


@pytest.fixture
def local_mlflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A throwaway MLflow file store (mlflow-skinny has no SQL store; the file store is opt-in)."""
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setenv("MLFLOW_DISABLE_AGENT_HINT", "1")
    mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())
