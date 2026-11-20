# Build Log

One entry per working session, newest at the top. Copy the template below.

---

<!--
## YYYY-MM-DD — Milestone N, step N: <short title>

**What was built:**
-

**Tests run and results:**
-

**Decisions made:** (also add to DECISIONS.md)
-

**Open items:**
-
-->

## 2026-11-20 — Milestone 1, step 4: synthetic topology

**What was built:**
- `entities.yaml`: `id_template` per entity; loader checks each template produces IDs that match the pattern; `EntityType.make_id()` and `Domain.parent_relation()`.
- `src/producer/topology.py` + `topology_config.yaml`: 1 square = 1 Cell, 3-square strips = Sites, seeded k-means (pure Python, deterministic tie-breaking) = AggregationNodes, one Core. Writes `topology_nodes.csv`, `topology_edges.csv`, `topology_meta.json` (`synthetic: true`, config, input and output hashes).
- `make data` now builds the topology after processing; `make topology` rebuilds it alone.

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 53 tests passed; coverage 96%.
- Real grid: 1 Core, 5 AggregationNodes (7, 8, 9, 12, 12 sites), 48 Sites, 144 Cells, 197 edges; rebuild is byte-identical.
- CI for the step 3 push (`e5be730`): both jobs green.

**Decisions made:** (also in DECISIONS.md)
- Topology layout and parameters; ID templates in the domain model.

**Open items:**
- Topology tables get loaded into Postgres in Step 6.

## 2026-11-20 — Milestone 1, step 3: verify + process (`make data`)

**What was built:**
- Branch protection on `main` (required CI checks, no force-push/deletion, linear history; admin bypass).
- `src/producer/milan_files.json`: published file IDs, sizes, MD5s for all 63 Dataverse files (from the public metadata API).
- `src/producer/integrity.py` (MD5 + SHA-256 in one pass, verify size + MD5), `grid.py` (centroids, 12 × 12 subset around the Duomo), `milan.py` (Polars streaming: sum over country codes, empty = 0, gap fill, UTC ms), `manifest.py` (atomic writes, stable keys), `data_prep.py` (CLI; skips up-to-date days; `--force`, `--delete-raw`, `--date`).
- `make data` target. Dependency added: polars 2.1.0.

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 37 tests passed; coverage 95%.
- Real run on 2013-11-01: MD5 matches published (`658c49f8…`); 4,842,625 rows in → 20,736 out (144 squares × 144 intervals), 0 gaps, 0 out-of-day rows; 2.4 s, ~400 MB peak RAM.
- Cross-check: awk sums for square 5061 at the first interval match the parquet values to 6 decimals.
- Reproducibility: forced reprocess gives a byte-identical parquet (SHA-256 `a947728e…`); a plain rerun leaves `data/manifest.json` byte-identical.

**Decisions made:** (also in DECISIONS.md)
- Subset = 144 squares around square 5061. Published checksums committed. Polars + parquet. All-empty source row ≠ data gap. Branch protection settings.

**Open items:**
- Polars 2 gotchas hit: a list passed next to other expressions in `with_columns` is read as a literal (unpack with `*`); `pl.from_epoch(..., "ms")` returns µs (cast explicitly).
- CI warns that `actions/checkout@v4` and `setup-uv@v5` run on deprecated Node 20; bump later.

## 2026-11-19 — Milestone 1, steps 0–2: resource check, tooling, domain model

**What was built:**
- Step 0 (partial): laptop resource check — 8 GB RAM total (~47% free at check), Intel i5-10500, 12 threads, 54 GB disk free; Docker 24.0.2 installed (daemon not running at check). Milan files present in `data/raw/` (checked later the same session: grid MD5 `e64ed3858e97347c14cd049efde8c7d3` matches the published value; 2013-11-01 MD5 `658c49f8e3c7910bd3bb2bafbf62cdce`, 322,874,887 bytes). An earlier note that `data/raw/` was empty came from a truncated directory listing and was wrong.
- Step 1: `pyproject.toml` (Python 3.11, uv, pinned via `uv.lock`), `Makefile` (`setup`, `lint`, `format`, `typecheck`, `test`, `check`, `clean`), `.pre-commit-config.yaml` (gitleaks v8.30.1, ruff, large-file/private-key checks), `.github/workflows/ci.yml` (gitleaks + `make check`), `.claude/settings.json` (deny `.env` reads/edits and edits under `data/`).
- Step 2: `src/common/domain/entities.yaml`, `kpis.yaml`, `loader.py` (pydantic, frozen, cached `load_domain()`).

**Tests run and results:**
- `make check`: ruff clean, mypy strict clean, 17 tests passed, coverage 93%.
- `pre-commit run --all-files`: all hooks passed.
- gitleaks flags a fake Groq-style key (tested in a scratch dir, not the repo).

**Decisions made:** (also in DECISIONS.md)
- Milestone 1 started; plan defaults confirmed. Repo stays public. Subset ~150 squares and Spark behind a Compose profile (8 GB RAM). Tooling choices.

**Open items:**
- Set Docker Desktop memory to ~5 GB.
- Branch protection on `main` (GitHub settings).
- CI not yet seen running on GitHub (needs a push).
