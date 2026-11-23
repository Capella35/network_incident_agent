"""Replay against the running Kafka (`make up`), into throwaway topics that are deleted after.

Checks what a mock can't: the idempotent producer really delivers every message once, and each
partition holds its keys in replay order.
"""

from __future__ import annotations

from typing import Any

import pytest

from producer import replay
from tests.factories import message_grid
from tests.integration.conftest import MakeTopics, consume

pytestmark = pytest.mark.integration

PARTITIONS = 6


def test_every_message_once_and_in_order_per_partition(
    make_topics: MakeTopics, producer: Any
) -> None:
    raw, dlq = make_topics(("replay", PARTITIONS), ("replay-dlq", 1))
    msgs = message_grid("it-run", n_cells=12, n_intervals=10)
    reject = replay.Reject(key="square-99|x", reason="square not in the topology", row={})
    summary = replay.send(
        [reject, *msgs],
        producer,
        replay.Summary("it-run"),
        replay.Pacer(speed=0),
        topic=raw,
        dlq_topic=dlq,
    )
    assert (summary.sent, summary.dead_lettered, summary.failed) == (len(msgs), 1, 0)
    assert summary.delivered == len(msgs) + 1

    got = consume(raw, len(msgs))
    keys = [k for _, k, _ in got]
    assert len(keys) == len(msgs) and len(set(keys)) == len(keys)  # no loss, no duplicates
    sent_order = [m.key() for m in msgs]
    for p in range(PARTITIONS):
        in_p = [k for part, k, _ in got if part == p]
        assert in_p == [k for k in sent_order if k in set(in_p)]  # replay order kept
    assert {p for p, _, _ in got} == set(summary.per_partition)
    assert len(consume(dlq, 1)) == 1
