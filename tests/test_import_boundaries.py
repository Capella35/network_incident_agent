"""Which code may import what (Rules 6 and 15), checked on the source, not by convention.

- Rule 15: the fault injector (anything in `producer`) never imports the fault signatures, so
  the evals test the signatures instead of the injector restating them.
- Rule 6: only `evaluation` reads the ground-truth fault log (`evaluation.ground_truth`); the
  injector writes it, and no detector, correlator, classifier, or agent may read it.

Static imports only (`import x`, `from x import y`, relative imports). Dynamic imports
(`importlib`) are not used in this repo and would bypass this check.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"


@dataclass(frozen=True)
class Rule:
    module: str  # the protected module
    allowed_in: tuple[str, ...] = ()  # if set: only these packages may import it
    forbidden_in: tuple[str, ...] = ()  # these packages may never import it
    why: str = ""
    transitive: bool = False  # also through other src modules they import


RULES = (
    # The injector may not reach the signatures even through a helper module.
    Rule("common.domain.signatures", forbidden_in=("producer",), why="Rule 15", transitive=True),
    # Direct only: tuning (models/) calls evaluation/ for its scores and gets numbers back, by
    # design (specs/03-detection.md); what must not happen is reading the log itself.
    Rule("evaluation.ground_truth", allowed_in=("evaluation",), why="Rule 6"),
    # The injector builds the ground truth: only the producer (replay) and evaluation use it;
    # detectors, correlator, classifier, and agent get telemetry via `replay.telemetry_for`,
    # which returns rows and counts, never the faults.
    Rule("producer.injector", allowed_in=("producer", "evaluation"), why="Rule 6"),
)


def module_name(path: Path, src: Path) -> str:
    parts = path.relative_to(src).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def imported_modules(path: Path, src: Path) -> set[str]:
    """Every module a file imports, as dotted names (`from a import b` counts `a` and `a.b`)."""
    here = module_name(path, src).split(".")
    is_package = path.name == "__init__.py"
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative: resolve against this module's package
                base = here if is_package else here[:-1]
                base = base[: len(base) - (node.level - 1)]
                mod = ".".join([*base, *([node.module] if node.module else [])])
            else:
                mod = node.module or ""
            out.add(mod)
            out.update(f"{mod}.{a.name}" for a in node.names)
    return out


def _in(pkg: str, module: str) -> bool:
    return module == pkg or module.startswith(pkg + ".")


def reachable(start: str, graph: dict[str, set[str]]) -> set[str]:
    """Every module `start` imports, directly or through other modules under src/."""
    seen: set[str] = set()
    todo = list(graph.get(start, ()))
    while todo:
        mod = todo.pop()
        if mod not in seen:
            seen.add(mod)
            todo.extend(graph.get(mod, ()))
    return seen


def violations(src: Path, rules: tuple[Rule, ...] = RULES) -> list[str]:
    graph = {module_name(p, src): imported_modules(p, src) for p in sorted(src.rglob("*.py"))}
    found = []
    for importer, direct in graph.items():
        for rule in rules:
            imports = reachable(importer, graph) if rule.transitive else direct
            if not any(_in(rule.module, m) for m in imports) or _in(rule.module, importer):
                continue
            blocked = any(_in(p, importer) for p in rule.forbidden_in) or (
                bool(rule.allowed_in) and not any(_in(p, importer) for p in rule.allowed_in)
            )
            if blocked:
                found.append(f"{importer} imports {rule.module} ({rule.why})")
    return found


def test_repo_respects_import_boundaries() -> None:
    assert violations(SRC) == []


# An import check misses data read by path (yaml.safe_load / pl.read_parquet on a file name),
# so the protected file names may not even appear in source or config outside their owners.
@dataclass(frozen=True)
class NameRule:
    text: str
    owners: tuple[str, ...]  # path prefixes (relative to src/) that may mention it
    why: str


NAME_RULES = (
    # The fault types file and the signatures module themselves may name the signatures file;
    # the injector (in producer/) may not, in code or config.
    NameRule(
        "fault_signatures",
        ("common/domain/faults.yaml", "common/domain/signatures.py"),
        "Rule 15",
    ),
    # Ground truth: read by evaluation/, written by the injector (producer/injector*).
    NameRule("ground_truth", ("evaluation/", "producer/injector"), "Rule 6"),
)


def name_violations(src: Path, rules: tuple[NameRule, ...] = NAME_RULES) -> list[str]:
    found = []
    files = (p for p in src.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    for path in sorted(files):  # every file: code, YAML, JSON, TOML, CSV config ...
        rel = path.relative_to(src).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for rule in rules:
            if rule.text in text and not rel.startswith(rule.owners):
                found.append(f"{rel} mentions {rule.text!r} ({rule.why})")
    return found


def test_protected_names_stay_with_their_owners() -> None:
    assert name_violations(SRC) == []


def test_reading_protected_data_by_path_is_caught(tmp_path: Path) -> None:
    _plant(tmp_path, "producer/injector.py", "yaml.safe_load(DIR / 'fault_signatures.yaml')\n")
    _plant(tmp_path, "producer/injector_config.yaml", "out: ground_truth/validation.parquet\n")
    _plant(tmp_path, "models/detector.py", "pl.read_parquet('ground_truth/x.parquet')\n")
    _plant(tmp_path, "evaluation/report.py", "GT_DIR = 'ground_truth'\n")  # owner: allowed
    _plant(tmp_path, "producer/settings.json", '{"sig": "fault_signatures.yaml"}\n')
    assert name_violations(tmp_path) == [
        "models/detector.py mentions 'ground_truth' (Rule 6)",
        "producer/injector.py mentions 'fault_signatures' (Rule 15)",
        "producer/settings.json mentions 'fault_signatures' (Rule 15)",
    ]


def _plant(src: Path, rel: str, code: str) -> None:
    path = src / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(code, encoding="utf-8")


def test_planted_violations_are_caught(tmp_path: Path) -> None:
    _plant(tmp_path, "producer/injector.py", "from common.domain import signatures\n")
    _plant(tmp_path, "producer/other.py", "import common.domain.signatures as s\n")
    _plant(tmp_path, "models/detector.py", "from evaluation.ground_truth import read\n")
    _plant(tmp_path, "evaluation/report.py", "from evaluation import ground_truth\n")  # allowed
    _plant(tmp_path, "correlation/__init__.py", "")
    _plant(tmp_path, "correlation/rank.py", "from common.domain.signatures import load\n")  # ok
    _plant(tmp_path, "evaluation/__init__.py", "")
    _plant(tmp_path, "evaluation/metrics.py", "from .ground_truth import read\n")  # relative, ok
    _plant(tmp_path, "agent/tools.py", "from ..evaluation import ground_truth\n")
    _plant(tmp_path, "models/tune.py", "from producer.injector import schedule\n")
    _plant(tmp_path, "producer/replay.py", "from producer import injector\n")  # allowed
    assert violations(tmp_path) == [
        "agent.tools imports evaluation.ground_truth (Rule 6)",
        "models.detector imports evaluation.ground_truth (Rule 6)",
        "models.tune imports producer.injector (Rule 6)",
        "producer.injector imports common.domain.signatures (Rule 15)",
        "producer.other imports common.domain.signatures (Rule 15)",
        # transitive: the planted replay reaches the signatures through the planted injector
        "producer.replay imports common.domain.signatures (Rule 15)",
    ]


def test_signatures_reached_through_a_helper_are_caught(tmp_path: Path) -> None:
    _plant(tmp_path, "common/fault_check.py", "from common.domain.signatures import load\n")
    _plant(tmp_path, "producer/injector.py", "import common.fault_check\n")
    _plant(tmp_path, "models/tune.py", "from evaluation import metrics\n")
    _plant(tmp_path, "evaluation/metrics.py", "from evaluation import ground_truth\n")
    assert violations(tmp_path) == [  # tune -> metrics -> ground_truth is allowed (direct only)
        "producer.injector imports common.domain.signatures (Rule 15)",
    ]


def test_relative_imports_resolve(tmp_path: Path) -> None:
    _plant(tmp_path, "pkg/__init__.py", "from .sub import x\n")
    _plant(tmp_path, "pkg/a/b.py", "from ..c import d\nfrom . import e\n")
    assert "pkg.sub" in imported_modules(tmp_path / "pkg/__init__.py", tmp_path)
    assert {"pkg.c", "pkg.c.d", "pkg.a", "pkg.a.e"} <= imported_modules(
        tmp_path / "pkg/a/b.py", tmp_path
    )


def test_logged_model_code_imports_only_what_the_model_ships() -> None:
    """A logged detector unpickles `models.tracking` with only `code_paths` (common, streaming,
    models) on hand, e.g. in the Spark job: nothing it reaches may live elsewhere in src/."""
    graph = {module_name(p, SRC): imported_modules(p, SRC) for p in sorted(SRC.rglob("*.py"))}
    shipped = ("common", "streaming", "models")
    repo = {m.split(".")[0] for m in graph}
    outside = {
        m
        for m in reachable("models.tracking", graph)
        if m.split(".")[0] in repo and not m.startswith(shipped)
    }
    assert outside == set()


def test_detector_job_reaches_neither_ground_truth_nor_injector() -> None:
    """Alarms come only from detector output (Rule 6): the Spark job and what it imports never
    reach `evaluation` (the ground truth) or `producer` (the injector)."""
    graph = {module_name(p, SRC): imported_modules(p, SRC) for p in sorted(SRC.rglob("*.py"))}
    reached = reachable("streaming.job", graph)
    assert "streaming.scoring" in reached
    assert {m for m in reached if m.startswith(("evaluation", "producer"))} == set()
