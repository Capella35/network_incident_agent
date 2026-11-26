"""Log a fitted detector to MLflow as a pyfunc model, with its parameters and metrics.

The tracking server comes from `MLFLOW_TRACKING_URI` (the local stack, `.env`; never printed).
The pyfunc input is feature rows (`streaming.features.KEYS` + feature columns) as pandas; the
output is `Detector.score`'s frame. The Spark job (step 8) loads the champion this way. The
model carries the source of the packages it unpickles from (`code_paths`), and the detector
holds its KPI and column lists from fit time, so it scores the same after a config change.
"""

from __future__ import annotations

from importlib.metadata import version
from importlib.resources import files
from typing import Any

import mlflow
import pandas as pd
import polars as pl
from mlflow.pyfunc.model import PythonModel

from models.detectors import Detector

EXPERIMENT = "detector-candidates"
_RUNTIME = ("numpy", "pandas", "polars", "pyarrow", "pydantic", "pyyaml", "scikit-learn")


class DetectorModel(PythonModel):
    def __init__(self, detector: Detector) -> None:
        self.detector = detector

    def predict(
        self, context: Any, model_input: pd.DataFrame, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        return self.detector.score(pl.from_pandas(model_input)).to_pandas()


def start_run(experiment: str, run_name: str) -> mlflow.ActiveRun:
    """A run in `experiment` (created if missing), addressed by ID: unlike `mlflow.set_experiment`
    it leaves no process-wide active experiment behind."""
    found = mlflow.get_experiment_by_name(experiment)
    exp_id = found.experiment_id if found else mlflow.create_experiment(experiment)
    return mlflow.start_run(experiment_id=exp_id, run_name=run_name)


def log_detector(detector: Detector, *, params: dict[str, Any], metrics: dict[str, float]) -> None:
    """Log the detector's parameters, `metrics`, and the pyfunc model into the active MLflow run.
    The caller owns the run (a plain run now; a nested run per Optuna trial in step 6)."""
    mlflow.log_params({**detector.params(), **params})
    mlflow.log_metrics(metrics)
    mlflow.pyfunc.log_model(
        name="model",
        python_model=DetectorModel(detector),
        pip_requirements=[f"{p}=={version(p)}" for p in _RUNTIME],
        code_paths=[str(files(package)) for package in ("common", "streaming", "models")],
    )
