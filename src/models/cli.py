"""What the models and evaluation command lines report as a one-line error (exit 1) instead of
a traceback. Kept out of `models.tracking`, which logged models unpickle from: it must not pull
in `producer` (not shipped with a model)."""

from __future__ import annotations

import mlflow

from common.process import OverBudgetError
from producer.replay import ReplayError

RUN_ERRORS = (
    FileNotFoundError,
    ValueError,
    OverBudgetError,
    ReplayError,
    mlflow.exceptions.MlflowException,
)
