"""Log a fitted detector to MLflow as a pyfunc model, with its parameters and metrics.

The tracking server comes from `MLFLOW_TRACKING_URI` (the local stack, `.env`; never printed).
The pyfunc input is feature rows (`streaming.features.KEYS` + feature columns) as pandas; the
output is `Detector.score`'s frame, plus a boolean `alarm` column when the model was logged with
a threshold (tuned models, step 6): the logged model is then the whole alarm rule, its
`threshold` and `alarm` settings (for `detectors.alarms`) on the unwrapped `DetectorModel`. The
Spark job (step 8) loads the champion this way. The model carries the source of the packages it
unpickles from (`code_paths`: common, streaming, models), so this module imports nothing else
from the repo; the detector holds its KPI and column lists from fit time, so it scores the same
after a config change.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from importlib.metadata import version
from importlib.resources import files
from typing import Any

import mlflow
import pandas as pd
import polars as pl
from mlflow.pyfunc.model import PythonModel

from models.detectors import AlarmParams, Detector, load_config

EXPERIMENT = "detector-candidates"
_RUNTIME = ("numpy", "pandas", "polars", "pyarrow", "pydantic", "pyyaml", "scikit-learn")


class DetectorModel(PythonModel):
    def __init__(
        self, detector: Detector, threshold: float | None = None, alarm: AlarmParams | None = None
    ) -> None:
        self.detector = detector
        self.threshold = threshold
        self.alarm = alarm

    def predict(
        self, context: Any, model_input: pd.DataFrame, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        scored = self.detector.score(pl.from_pandas(model_input))
        if self.threshold is not None:
            scored = scored.with_columns(alarm=(pl.col("score") >= self.threshold).fill_null(False))
        return scored.to_pandas()


@dataclass(frozen=True)
class Tuned:
    """A fitted detector and its complete alarm rule, as a tuning study logged it."""

    detector: Detector
    threshold: float
    alarm: AlarmParams


def load_tuned(run_id: str) -> Tuned:
    """The tuned detector study run `run_id` logged. Raises ValueError if its model has no
    threshold (an untuned candidate run)."""
    model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model").unwrap_python_model()
    if not isinstance(model, DetectorModel) or model.threshold is None or model.alarm is None:
        raise ValueError(f"run {run_id}: no tuned detector with a threshold")
    return Tuned(model.detector, model.threshold, model.alarm)


def start_run(experiment: str, run_name: str, *, nested: bool = False) -> mlflow.ActiveRun:
    """A run in `experiment` (created if missing), addressed by ID: unlike `mlflow.set_experiment`
    it leaves no process-wide active experiment behind. `nested`: a child of the active run
    (it must be in the same experiment)."""
    found = mlflow.get_experiment_by_name(experiment)
    exp_id = found.experiment_id if found else mlflow.create_experiment(experiment)
    return mlflow.start_run(experiment_id=exp_id, run_name=run_name, nested=nested)


def code_version() -> str:
    """The git commit the code runs from, `+dirty` if the working tree has changes or files git
    does not track yet; "unknown" outside a git checkout."""
    try:
        commit = _git("rev-parse", "HEAD")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return commit + ("+dirty" if _git("status", "--porcelain") else "")  # new files count too


def _git(*args: str) -> str:
    out = subprocess.run(["git", *args], capture_output=True, text=True, check=True)  # noqa: S603, S607 - fixed git command
    return out.stdout.strip()


def log_detector(
    detector: Detector,
    *,
    params: dict[str, Any],
    metrics: dict[str, float],
    threshold: float | None = None,
) -> None:
    """Log the detector's parameters, `metrics`, and the pyfunc model into the active MLflow run
    (the caller owns the run). With `threshold`, the model also carries it and the alarm settings
    from `detectors.yaml`, and is the complete alarm rule."""
    mlflow.log_params({**detector.params(), **params})
    mlflow.log_metrics(metrics)
    alarm = None if threshold is None else load_config().alarm
    mlflow.pyfunc.log_model(
        name="model",
        python_model=DetectorModel(detector, threshold, alarm),
        pip_requirements=[f"{p}=={version(p)}" for p in _RUNTIME],
        code_paths=[str(files(package)) for package in ("common", "streaming", "models")],
    )
