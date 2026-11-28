"""Log a fitted detector to MLflow as a pyfunc model, with its parameters and metrics.

The tracking server comes from `MLFLOW_TRACKING_URI` (the local stack, `.env`; never printed).
The pyfunc input is feature rows (`streaming.features.KEYS` + feature columns) as pandas; the
output is `Detector.score`'s frame, plus a boolean `alarm` column when the model was logged with
a threshold (tuned models, step 6): the logged model is then the whole alarm rule, its
`threshold` and `alarm` settings (for `detectors.alarms`) on the unwrapped `DetectorModel`. A
promoted model (step 7) also carries the feature config and the fitted reference its features
are computed from (`features.Spec`), so a promotion or a rollback moves the model, its threshold,
and its features as one unit; the Spark job (step 8) loads the champion with `load_package` and
computes features from that spec before scoring (pyfunc input is features, not telemetry). The
model carries the source of the packages it unpickles from (`code_paths`: common, streaming,
models), so this module imports nothing else from the repo but `streaming.features`; the detector
holds its KPI and column lists from fit time, so it scores the same after a config change.
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
from mlflow import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException
from mlflow.pyfunc.model import PythonModel

from common import splits
from models.detectors import AlarmParams, Detector, load_config
from streaming import features

EXPERIMENT = "detector-candidates"
MODEL = "detector"  # the registered model promotions go to (models.promote)
ALIAS = "champion"
_RUNTIME = ("numpy", "pandas", "polars", "pyarrow", "pydantic", "pyyaml", "scikit-learn")


class DetectorModel(PythonModel):
    spec: features.Spec | None = None  # also what a model pickled before step 7 reads as

    def __init__(
        self,
        detector: Detector,
        threshold: float | None = None,
        alarm: AlarmParams | None = None,
        spec: features.Spec | None = None,
    ) -> None:
        self.detector = detector
        self.threshold = threshold
        self.alarm = alarm
        self.spec = spec

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


def _load_tuned(model_uri: str) -> tuple[Tuned, features.Spec | None]:
    model = mlflow.pyfunc.load_model(model_uri).unwrap_python_model()
    if not isinstance(model, DetectorModel) or model.threshold is None or model.alarm is None:
        raise ValueError(f"{model_uri}: no tuned detector with a threshold")
    return Tuned(model.detector, model.threshold, model.alarm), model.spec


def load_tuned(run_id: str) -> Tuned:
    """The tuned detector study run `run_id` logged. Raises ValueError if its model has no
    threshold (an untuned candidate run)."""
    return _load_tuned(f"runs:/{run_id}/model")[0]


@dataclass(frozen=True)
class Package:
    """A promotable detector: the tuned alarm rule and the features it was tuned on."""

    tuned: Tuned
    spec: features.Spec


def load_package(model_uri: str) -> Package:
    """The package at `model_uri` (e.g. `models:/detector@champion`). Raises ValueError if the
    model there is not a tuned detector packaged with its features."""
    tuned, spec = _load_tuned(model_uri)
    if spec is None:
        raise ValueError(f"{model_uri}: no feature spec (not a promotion package)")
    return Package(tuned, spec)


def champion(client: MlflowClient) -> ModelVersion | None:
    """The version `ALIAS` points to; None before the first promotion. Any other registry error
    is raised: an unreachable server must never read as "no incumbent"."""
    try:
        model = client.get_registered_model(MODEL)
    except MlflowException as exc:
        if exc.error_code == "RESOURCE_DOES_NOT_EXIST":
            return None
        raise
    if ALIAS not in model.aliases:
        return None
    return client.get_model_version_by_alias(MODEL, ALIAS)


@dataclass(frozen=True)
class Champion:
    package: Package
    version: str  # the registered version, so the alias moving later changes nothing loaded


def load_champion(client: MlflowClient) -> Champion:
    """The package the champion alias points to, loaded by version. Raises ValueError if there
    is no champion yet, or (`load_package`) if it is not a package."""
    current = champion(client)
    if current is None:
        raise ValueError(f"no {MODEL}@{ALIAS} yet: promote a candidate first (make promote)")
    return Champion(load_package(f"models:/{MODEL}/{current.version}"), current.version)


def start_run(experiment: str, run_name: str, *, nested: bool = False) -> mlflow.ActiveRun:
    """A run in `experiment` (created if missing), addressed by ID: unlike `mlflow.set_experiment`
    it leaves no process-wide active experiment behind. `nested`: a child of the active run
    (it must be in the same experiment)."""
    found = mlflow.get_experiment_by_name(experiment)
    exp_id = found.experiment_id if found else mlflow.create_experiment(experiment)
    return mlflow.start_run(experiment_id=exp_id, run_name=run_name, nested=nested)


def validation_tags(snapshot: str, fault_schedule: str | None = None) -> dict[str, str]:
    """Provenance tags of a run scored on the validation weeks with SYNTHETIC faults: the data
    snapshot, the code, and (once known) the ground truth's fingerprint."""
    tags = {
        "data_snapshot": snapshot,
        "code_version": code_version(),
        "split": str(splits.Split.VALIDATION),
        "synthetic_faults": "yes",
    }
    if fault_schedule is not None:
        tags["fault_schedule_fingerprint"] = fault_schedule
    return tags


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
    _log_model(DetectorModel(detector, threshold, alarm))


def log_package(tuned: Tuned, spec: features.Spec) -> str:
    """Log the tuned detector and its feature spec as the active run's pyfunc model (the caller
    owns the run), with the feature config as `features.json` for reading; returns the model URI
    to register."""
    mlflow.log_dict(spec.config.model_dump(mode="json"), "features.json")
    return _log_model(DetectorModel(tuned.detector, tuned.threshold, tuned.alarm, spec))


def _log_model(model: DetectorModel) -> str:
    info = mlflow.pyfunc.log_model(
        name="model",
        python_model=model,
        pip_requirements=[f"{p}=={version(p)}" for p in _RUNTIME],
        code_paths=[str(files(package)) for package in ("common", "streaming", "models")],
    )
    uri: str = info.model_uri
    return uri
