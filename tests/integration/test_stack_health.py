"""Long-running services are stable, not just passing their health checks (guardrail R1).

A health check can pass between crashes: on 2026-11-22 MLflow was OOM-killed every ~50 s
while `up --wait` reported it healthy. These checks read Docker's own counters instead.
Run against the running stack with `make test-integration`; give it a minute after `make up`.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.integration

PROJECT = "netops"  # `name:` in infra/docker-compose.yml
LONG_RUNNING = ("kafka", "postgres", "mlflow", "grafana")


def _inspect(service: str) -> dict[str, object]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker CLI not found")
    out = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
        [docker, "inspect", f"{PROJECT}-{service}-1"],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        pytest.skip(f"{service} container not found; run `make up` first")
    info: dict[str, object] = json.loads(out.stdout)[0]
    return info


@pytest.mark.parametrize("service", LONG_RUNNING)
def test_service_running_without_restarts(service: str) -> None:
    info = _inspect(service)
    state = info["State"]
    assert isinstance(state, dict)
    assert state["Status"] == "running"
    assert not state["OOMKilled"], f"{service} was OOM-killed: raise its limit or trim it"
    assert info["RestartCount"] == 0, f"{service} restarted {info['RestartCount']} times"


@pytest.mark.parametrize("service", LONG_RUNNING)
def test_no_process_oom_killed(service: str) -> None:
    """The cgroup's own counter: also catches a killed child (e.g. a Grafana plugin) that does
    not restart the container. Docker's OOMKilled flag stayed false in both 2026-11-22 incidents.
    """
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("docker CLI not found")
    out = subprocess.run(  # noqa: S603 - fixed argv, no shell, no user input
        [docker, "exec", f"{PROJECT}-{service}-1", "cat", "/sys/fs/cgroup/memory.events"],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        pytest.skip(f"{service}: cgroup v2 memory.events not readable")
    events = dict(line.split() for line in out.stdout.splitlines() if line.strip())
    assert int(events.get("oom_kill", "0")) == 0, f"{service}: processes OOM-killed: {events}"


@pytest.mark.parametrize("service", LONG_RUNNING)
def test_service_has_memory_limit(service: str) -> None:
    host = _inspect(service)["HostConfig"]
    assert isinstance(host, dict)
    assert host["Memory"] > 0, f"{service} has no memory limit (R1)"
