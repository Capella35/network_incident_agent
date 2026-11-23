"""Hand-written telemetry messages for tests (never Milan data). One place to build them."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from common import telemetry
from common.telemetry import TelemetryMessage

T0 = datetime(2013, 11, 5, tzinfo=UTC)  # a training day, on the 10-minute grid


def telemetry_message(
    *, run: str = "run-1", cell: int = 1, minutes: int = 0, traffic: float = 1.0
) -> TelemetryMessage:
    """A valid message for `CELL-1-<cell>` at T0 + `minutes`, every traffic field = `traffic`.
    Variants: `.model_copy(update=...)`."""
    return TelemetryMessage(
        replay_run_id=run,
        cell_id=f"CELL-1-{cell}",
        site_id="SITE-1",
        square_id=cell,
        interval_start=T0 + timedelta(minutes=minutes),
        **dict.fromkeys(telemetry.TRAFFIC_FIELDS, traffic),
        data_gap=False,
        load_ratio=0.3,
        latency_ms=25.0 + cell,
        drop_rate=0.002,
    )


def message_grid(run: str, n_cells: int, n_intervals: int) -> list[TelemetryMessage]:
    """`n_cells` x `n_intervals` messages in replay order (interval_start, cell_id); traffic
    differs per cell and interval, so a changed value changes a digest."""
    msgs = [
        telemetry_message(run=run, cell=c, minutes=10 * i, traffic=c + i / 7)
        for i in range(n_intervals)
        for c in range(1, n_cells + 1)
    ]
    return sorted(msgs, key=lambda m: (m.interval_start, m.cell_id))
