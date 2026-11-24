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

## 2026-11-23 — Milestone 2, step 3: features

**What was built:**
- New package `src/streaming/` (via the `new-package` skill; wheel + mypy lists, tests in `tests/streaming/`).
- `streaming/baseline.py` + `make baseline`: seasonal mean/std per (cell, weekday, hour, Europe/Rome) for every real and MODELED KPI, from the clean training weeks (Rule 7: `fit` refuses any non-training local day); gap rows left out; std floored (`features.yaml`). Writes `data/processed/baseline.parquet` + `.meta.json` (training days, traffic hashes, and the baseline's provenance: feature config, KPI catalog entries, KPI-model params, calibration and topology hashes). `baseline.load` refuses a missing or **stale** baseline.
- `streaming/features.py` + `features.yaml`: causal features per cell and KPI: `_z` (clipped ±20), `_bad` (oriented by the KPI's bad direction), `_z_mean_3`, `_z_mean_6` (time-based windows: a missing interval shortens the window), `_z_delta` (null unless the previous interval exists). No features on gap rows; windows skip them. Imports only `common`, so the Spark job (Step 8) can call it.
- `streaming/features_check.py` + `make features ARGS="--split validation --inject"`: computes a split's features, reports rows, time, peak memory; `--inject` only with validation; writes nothing.
- Shared pieces moved instead of copied: `common.telemetry.INTERVAL`, `LOCAL_TZ`, `add_real_kpis` (were in `producer.injector`, `producer.milan`, `producer.kpi_model`); `data_prep.processed_days` (calibration, baseline, and replay now find processed days one way); `tests/factories.telemetry_frame`. `replay.telemetry_for` now raises `ReplayError` for injector failures, because packages outside `producer` may not import the injector (Rule 6).

**Review (before verification):**
- `/simplify`, 4 reviewers. Applied: the shared constants/helpers above; CLI split out of `features.py` (it pulled the producer and, through `replay`, the injector into the feature code); z computed before the sort; unused per-bucket `n` dropped; `PositiveInt` windows; one time-zone expression; test factory shared. Skipped: per-cell `.over()` and baseline-join optimizations (matter only for Step 8, profile then); a `topology_nodes.csv` constant (pre-existing literal in 4 producer files).
- `/code-review` (medium), 7 findings: **important, fixed:** features silently used a baseline fitted under an older calibration/config (now a provenance check in `baseline.load`). Nits fixed: injector errors escaped as tracebacks; `make features` needed every split day while `make baseline` used the days present; `--split train --inject` allowed; sidecar missed KPI-model params; a test name mangled by my own find-and-replace. Rejected: "`features.yaml` version not checked" (`read_yaml` already rejects any version but 1). The fixes were tested, not re-reviewed by a second `/code-review` pass.

**Tests run and results:**
- `make check`: 361 passed, coverage 90.43%.
- Done when: `make features ARGS="--split validation --inject"` → 14 days + 57 SYNTHETIC faults, 290,304 rows × 30 features, features 0.3 s (whole run ~1 s), peak memory 376–415 MiB. Training weeks: 580,608 rows, 0.7 s, 663 MiB. Baseline: 24,192 buckets = 144 × 7 × 24.
- Injection check: 5,421 rows' features differ between clean and injected validation (injector changed 5,425 cell-intervals).
- `make calibrate` after the `processed_days` refactor: `kpi_calibration.json` byte-identical (sha256 `c11f1bd8…`), so the baseline stays current.

**Found:** a real December **internet level shift**: from 2013-12-03/04, 18/144 cells drop below 60% of their November internet level and stay there (voice unchanged). On clean validation days `traffic_internet_bad > 3` on 10.7% of rows. Recorded as a known risk; handling is an open decision.

**Open items:**
- Decide how to treat the internet level shift before tuning (Step 6); options in `DECISIONS.md`.
- Step 8: per-cell `compute` cost (baseline join per call, `.over()` per call) to profile.
- A direct read of `data/processed/traffic/*.parquet` in an ad-hoc analysis was blocked by Claude Code's auto-mode classifier; the same view through `replay.telemetry_for` was allowed.

## 2026-11-23 — Milestone 2, step 2: fault injector + ground-truth log

**What was built:**
- `src/producer/injector.py` + `injector_config.yaml` (SYNTHETIC, own parameters, never the signatures): seeded schedule per split (type by weight, root at an allowed level with cells under it, onset, duration, severity, magnitudes; no two faults on a shared cell at once); one effect model (traffic × factor before the KPI model, latency × factor after, optional ramp); change events (one before each config push + 2 decoys per fault); `write_ground_truth` → `data/processed/ground_truth/<split>-<seed>.parquet` + `.meta.json` with a schedule **fingerprint**; `make inject`. Test split needs `--final-eval`.
- `replay.telemetry_for(data_dir, days, inject=...)`: the one assembly path for the replay and offline code; returns rows, rejects, fault count, change events, fingerprint (never the faults). `make replay … --inject` regenerates the schedule, refuses if its fingerprint differs from the ground truth, writes the run's change events to Postgres, then sends.
- `topology.Index` (nodes by type, cells under a node, cell maps; one CSV parser) replaces three readers of `topology_nodes.csv`.

**Review (before verification):**
- `/simplify`, 4 reviewers. Applied: one shared `telemetry_for` (offline Steps 3–6 and Spark parity need the same rows); one `inject()` wrapper (no hidden column contract through the KPI model); schedule fingerprint + drift check; `--inject` as a flag; `topology.Index`; reuse of `FrozenModel`, `read_yaml`, `manifest.save`; `_draw` helper, change IDs set at placement, test fixtures reuse `test_kpi_model.traffic` and a real generated topology. Skipped (outside the diff): a generic upsert builder shared with the sink, moving `kpi_model`/`topology_db` onto the shared loaders, integrity checks in the topology loader.
- `/code-review` (medium), 7 findings, all fixed: **`telemetry_for` returned the faults** (ground truth reachable by detector code; now counts only, plus a boundary rule on `producer.injector`); **a config push's change event could be dropped** when it fell just before the replayed day; **`--inject` crashed with a raw traceback** if Postgres was unavailable; roots with no cells (empty fault) or levels with no nodes (crash); a bare KeyError for a cell without a square; the fingerprint was never compared; config errors showed a raw traceback. The name-scan boundary test caught one of my own fixes (`replay.py` mentioning `ground_truth`); renamed. The fixes were tested but not re-reviewed by a second `/code-review` pass.

**Tests run and results:**
- `make check`: 334 passed, coverage 89.62%. `make test-integration`: 25 passed.
- Done when: `make inject ARGS="--split validation"` → 57 faults (site_power 17, congestion 12, cell_outage 11, config_push 9, backhaul 8), 123 change events, fingerprint `62d292f4…`. Applied to all 14 validation days (290,304 rows): exactly the 5,425 cell-intervals in fault windows changed, **0 mismatches** vs multipliers computed independently from the ground-truth file. Spot check: SITE-7 site_power, all 3 cells ×0.038 for exactly 09:00–10:20 UTC on 12-02; 08:50 and 10:30 untouched.
- Live: `make replay ARGS="--start 2013-12-02 --speed 0 --inject --run-id step2-inject-2013-12-02"` → 3 faults, 4 change events in Postgres, schedule matched, 20,736 sent; sink wrote them (SITE-7 window min internet 21.15).

**Open items:**
- M6: test-split faults must use parameters disjoint from training/validation (specs/09-evals.md); today only the seed differs.
- Busy-peak false alarms (load_ratio > 0.9 on 0.23% of rows) still to handle in Steps 3–4.
- Outside-the-diff reuse left for later: shared upsert builder (sink + injector), shared YAML loader for `kpi_model`/`topology`, integrity checks when loading the topology.

## 2026-11-23 — Milestone 2, step 1: fault types in the domain model

**What was built:**
- `src/common/domain/faults.yaml` + `Fault` model: six fault types with root levels (topology entity types), severities, owning team, `needs_change_event`; `Severity` enum that orders by rank (minor < major < critical), not alphabetically; `Fault.is_real` (false only for `no_fault`). `load_domain().fault(name)`.
- Signatures apart: `fault_signatures.yaml` + `common.domain.signatures` (description + named thresholds), validated against the fault types in the same directory.
- `tests/test_import_boundaries.py`: Rule 15 (no `producer` code reaches `common.domain.signatures`, also transitively) and Rule 6 (only `evaluation` imports `evaluation.ground_truth`, direct), plus a scan of every file under `src/` for the names `fault_signatures` / `ground_truth` outside their owners (catches reading by path).
- `change_events` table (`infra/db/002_change_events.sql`, idempotent; writer: INSERT/SELECT/UPDATE, no DELETE; readonly: SELECT) and `make db-migrate` (migration list derived from `infra/db/0*.sql`); `tests/test_db_migrations.py` checks every migration per statement for non-idempotent SQL.
- Loader helpers that replaced repeated code: `_lookup`, `_dupes`, `parse_named`; public `DEFAULT_DIR`, `FrozenModel`, `read_yaml`. Shared `tests/common/conftest.py`.

**Review (new process, before verification):**
- `/simplify`, 4 reviewers. Applied: Severity enum instead of a hand-built list and checks; signatures checked against the same directory; one parse/lookup/duplicate helper each; shared test fixtures; derived migration list; name scan for data read by path. Skipped: banning relative imports in ruff (config), a typed threshold model per fault (deferred to M3's predicate registry; a drift test added instead), dropping `needs_change_event` (the injector uses it in Step 2).
- `/code-review` (medium), 9 findings, all fixed: **Severity compared alphabetically** (critical < minor, so `max()` gave the least severe); a YAML entry's own `name:` silently overrode its key; the migration checker read to end of file (one ON CONFLICT excused every INSERT) and missed ADD COLUMN / UNIQUE INDEX / views; import checks were direct-only; plus a test not reaching its branch, a raw AttributeError on a list-shaped section, duplicate/inconsistent fault fields, and a name scan limited to 3 file types.

**Tests run and results:**
- `make check`: 311 passed, coverage 89.09%. `make test-integration`: 24 passed.
- Done when: 6 fault types load, 5 signatures; boundary tests pass and catch planted violations (import, relative, transitive, by file name in .py/.yaml/.json). `make db-migrate` applied twice to the running stack (second run: "already exists, skipping").

**Open items:**
- M3: one executable predicate per signature (a registry keyed by fault type), so the description stops being the only statement of the logic.
- Step 2 decides where the ground-truth write side lives (the injector, `producer/injector*`, is the name-rule owner).

## 2026-11-23 — Code-quality guardrails + cleanup (before Milestone 2)

**What was built:**
- `CLAUDE.md` "Code quality" section; ruff C90 / ERA / PLR; coverage floor 85%; `milestone-step` review step (`/simplify` + `/code-review` at the end of Build, before verification).
- Cleanup of duplication found in the repo: `common/kafka.py` now holds every Kafka client setting (base, producer, consumer; the sink's DLQ producer no longer borrows the replay's config and `client.id`); `replay.send` split (complexity 11 → under 10) with `Summary.record_delivery`; `sink.LoopSettings` (was 9 parameters); `sink.count_rows`; `tests/factories.py` (one message builder), `tests/fakes.py` (one fake Kafka message and clock, replacing 5 near-duplicate classes), `tests/integration/conftest.py` (throwaway topics, producer, read-back, writer connection, `unique()`).
- Dogfooded the new review step on this change: `/simplify` with 4 parallel reviewers (reuse, simplification, efficiency, altitude). Applied: unused options removed (env injection, traffic callable, `**over`, group parameter), shared fakes, single producer/consumer config, batch-size default defined once, read-back no longer waits a full second per call. Skipped: merging the two fake producers (different behavior), a Topics settings object (only needed for max-args 5), and the rule-file suggestions the user did not pick. Not applied: removing the `make_topics` teardown guard (the admin client rejects an empty delete list).

**Tests run and results:**
- `make check`: 267 passed, coverage 88.6% (floor 85%). `make test-integration`: 24 passed, **12 s** (was 19 s).

**Open items:**
- Optional rule tightening still on the table: ruff ARG/T20, coverage excluding live-stack entry points with a higher floor, trimming `CLAUDE.md`, max-args 5.

## 2026-11-22 — Milestone 2 prep: training weeks 2–4 + validation weeks

**What was built:**
- Downloaded (manual, user, 2026-11-22): 35 daily files, 2013-11-11 → 2013-12-15 (training weeks 2–4 + both validation weeks), 11 GB. No test-week file.
- `make data`: all 35 passed size + MD5 against the published values; 4.36–5.71 M rows each → 20,736 rows per day (144 cells × 144 intervals), 0 gaps, 0 out-of-day rows; ~70 s total. Topology rebuilt byte-identical (nodes SHA-256 `ab6cc146…`, same as in Postgres). Processed days on disk: 2013-11-01 → 12-15 (45 days, 31 MB). Manifest: 46 entries, all MD5 = published.
- `make calibrate` on all 28 training days (2013-11-04 → 12-01; validation days not read): per-cell capacity vs the week-1 calibration: median ratio 1.008 (range 0.79–1.20).

**Tests run and results (MODELED values over all 28 training days, 580,608 rows):**
- `load_ratio`: p50 0.321, **p99 0.800** (design target), max 2.65; 0.23% of rows > 0.9 (week-1 calibration: 0.22%).
- `latency_ms`: p50 29.5, p99 100.9.

**Open items:**
- Raw files deleted with the user's OK: `make data ARGS="--delete-raw"` re-verified all 35, then deleted them; only `data/raw/milano-grid.geojson` remains (3 MB). Free disk 13 → 46 GiB. Hashes stay in the manifest (46 entries, all MD5 = published); 45 processed days on disk.
- `telemetry_raw` rows from earlier runs used the week-1 calibration; any new replay uses the 4-week one. Matters only if old and new runs are compared directly.
- Still for M2: busy peaks with `load_ratio` > 0.9 (0.23% of rows) will look like congestion against the false-alarm budget.

## 2026-11-22 — Milestone 1, step 11: close-out — **Milestone 1 done**

**What was built:**
- P3 attribution, exactly as the publisher requires (Harvard Dataverse terms of use, read from the public dataset metadata API 2026-11-22): "from BigDataChallenge contest" → `http://www.telecomitalia.com/tit/en/bigdatachallenge.html`, plus the ODI node Trento homepage `http://theodi.fbk.eu/`, with the dataset DOI. Added to `README.md` and the Grafana footer (`src/common/grafana.py`, regenerated); wording recorded in `specs/02-telemetry.md`.
- `README.md`: status line (Milestone 1 of 6 done; the rest designed, not built) and a quickstart of the commands that work today, separated from the target ones.
- Checked: the decisions Step 11 lists (subset, Python 3.11, 6 partitions, KPI model) are already in `DECISIONS.md` (2026-11-19 / 2026-11-20); `specs/02-telemetry.md` already shows the subset and the confirmed time split.

**Guardrail checklist (Milestone 1):**

| # | Result | Evidence (2026-11-22) |
|---|---|---|
| P3 | **pass** | Attribution links in README and dashboard footer, as the terms of use word them; no raw or derived data published (only code and the checksum manifest). The write-up and video credits are due at M6. |
| P4 | **pass** | `git ls-files data` → `README.md`, `manifest-template.md`, `manifest.json` only; no data file anywhere in git history; `check-added-large-files` (1 MB) in pre-commit. |
| P5 | **pass** | Milan data is aggregated per grid square; `milan.py` sums away the only finer field (country code); nothing per-person is added or joined. |
| S1 | **pass** | All 5 published ports bound to `127.0.0.1` (Kafka 9092, Postgres 5433, MLflow 5001, Grafana 3000, Spark UI 4040); MLflow's `--host=0.0.0.0` is inside its container only. 4 ports closed on the LAN address from the Mac; Grafana unreachable from a phone. |
| S2 | **pass** | `preflight.sh` refuses placeholder or short passwords (stack would not start); Grafana anonymous access off (401); app roles and the Grafana data source take passwords from the environment, never from files. |
| S3 | **pass** | gitleaks pre-commit hook installed and run on every commit today ("no leaks found"); CI gitleaks job green on the last pushed commit. Steps 7–11 are not pushed yet, so CI has not run on them. |
| S5 | **pass** (note) | Python deps pinned in `uv.lock`; all 7 image tags pinned (no `latest`). Note: GitHub Actions use major tags (`checkout@v4`, `setup-uv@v5`), not commit SHAs, and run on deprecated Node 20. |
| S6 | **pass** | Milan files downloaded manually and only parsed as text after MD5 checks against the published list; no `subprocess`, `eval`/`exec`, archive extraction, or `pickle` anywhere in `src/`. |
| R1 | **pass** | Every container has a memory limit (stack ~2.9 GiB, +1.5 GiB Spark, of Docker's ~5.3 GiB); actual use ~1.4 GiB; 0 restarts and cgroup `oom_kill` 0 on all four services after the MLflow (Step 6) and Grafana (Step 10) fixes. |
| R2 | **pass** | `telemetry.raw` 7 d / 256 MiB per partition, `telemetry.dlq` 7 d / 64 MiB; Kafka data 73 MB on disk after ~0.8 M messages today (the volume's 1.3 GB "size" is sparse pre-allocated index files). |
| R5 | **pass** | 144-square subset (12 × 12 around the Duomo); full stack + a 7-day double replay ran within the laptop's limits. |

**Tests run and results:**
- `make check`: 267 passed, 89% coverage. `make test-integration`: 24 passed. Grafana serves the new footer (provisioned file contains the ODI Trento link).

**Milestone 1 Definition of done:** met in Step 10 (`make replay-check` on training week 1 → MATCH).

**Open items carried to Milestone 2:**
- Push Steps 7–11 and let CI run on them.
- Data: download the remaining training weeks 2–4 (2013-11-11 → 12-01) and the validation weeks (12-02 → 12-15, needed for Optuna tuning), then `make data` + `make calibrate`.
- A cleanup path for check/demo rows in `telemetry_raw` (the writer role cannot delete, by design).
- Pin GitHub Actions to commit SHAs and move off Node 20.
- The guard hook's heredoc false alarm on `.env` text (user-only fix).
- Grafana memory headroom (~120 MiB) as dashboards grow.

## 2026-11-22 — Milestone 1, step 10: reproducibility check (Definition of done)

**What was built:**
- `src/producer/replay_check.py` + `make replay-check`: replays the same days twice (`replay-check-<day>-<tag>-a` / `-b`); records each partition's end offset before and after each run and reads back exactly that range; compares per-partition key sequences and payload SHA-256 (run ID blanked); drains both runs with the normal sink; compares row counts and an ordered MD5 over every `telemetry_raw` column but `replay_run_id` / `ingested_at`, as `readonly`. Exit 0 only on MATCH. Refuses test-week days (Rule 7).
- Grafana memory fix (found while testing): 640m limit, `GOMEMLIMIT=480MiB`, unused plugins disabled. `test_stack_health.py` now also checks the cgroup `oom_kill` counter.

**Tests run and results:**
- **Definition of done:** `make replay-check ARGS="--start 2013-11-04 --end 2013-11-10"`: sent 145,152 | 145,152; read back 145,152 | 145,152; partitions 0–5 (24,085–24,403 keys each) with identical key sequences and payload digests; `telemetry_raw` 145,152 | 145,152 rows, MD5 `8530ca4a789314acefdce32217131f0a` | same. **RESULT: MATCH.** Sink: 290,304 consumed and written, 0 dead-lettered. ~2.5 min.
- Unit (14): read-back window (ignores messages before/after), empty range, timeout, Kafka error; payload normalization; foreign messages ignored; mismatch detection for reordered keys, changed payload, row count, digest, short read-back; digest SQL covers every column but the run ID. Integration (2): two runs read back identically from a throwaway topic; DB digest equal for equal rows inserted in a different order, different after a 1e-9 change (rolled back).
- Fixed during testing: offsets on a just-created topic fail with NOT_LEADER (not flagged retriable by the client); `end_offset` now retries for up to ~10 s.
- **Grafana incident:** `test_stack_health` reported 2, then 3 restarts. Docker events: `oom` → `die exit=137` (at 04:21, 04:32, 04:46 UTC, while the dashboard was in use); `OOMKilled` flag false. Cause: 384m limit; 13 unused data-source plugins + 5 preinstalled apps running (~380 MB of plugin processes); the main server ~580 MB RSS. After the fix: 2 processes, 390 MiB / 1.5% CPU at rest; three back-to-back integration runs: cgroup peak 631 MiB (anon 466, file cache 152), `oom_kill` 0, restarts 0.
- `make check`: 267 passed, 89% coverage (`replay_check.py` orchestration runs only against the live stack). `make test-integration`: 24 passed.

**Decisions made:** (also in DECISIONS.md)
- The replay-check design; Grafana memory settings and disabled plugins.

**Open items:**
- `telemetry_raw` now holds 2 × 145,152 replay-check rows plus the four earlier runs (~370k rows); fine for M1, but there is no cleanup path (the writer role cannot delete, by design). Decide in M2 whether test runs get a separate schema or an admin cleanup target.
- Grafana's anon memory under load (~466 MiB) leaves ~120 MiB below the limit; keep an eye on it when more dashboards arrive (M2–M4).

## 2026-11-22 — Milestone 1, step 9: Grafana panel

**What was built:**
- `src/common/grafana.py` + `make dashboards`: generates `infra/grafana/dashboards/network-kpis.json` from `kpis.yaml`: two time-series panels (*Internet activity (real, scaled)*, *Modeled user-plane latency (MODELED)*), selectors for replay run / site / cells, 5 s refresh, opens on training week 1, footer with the ODbL attribution and the synthetic/modeled label.
- `infra/grafana/provisioning/`: Postgres data source as `readonly` (password from the container environment), dashboard provider (read-only, no UI edits); placeholder `plugins/` and `alerting/` folders so Grafana logs no read errors. Mounted read-only in `docker-compose.yml`.

**Tests run and results:**
- Unit (10): committed JSON equals the generator output; every `Unit` maps to a Grafana unit; KPI SQL from the catalog; titles, units, real/MODELED labels; unknown KPI fails; footer; time window from `splits.yaml`; provisioning files (readonly user, password as an env placeholder). `make check`: 253 passed, 92% coverage.
- Integration (4, through Grafana's API as admin): data source provisioned, read-only, health OK; a write through a panel query is refused (the plugin runs queries in a read-only transaction, SQLSTATE 25006, on top of the role having no grants); dashboard provisioned; the panel's own SQL returns the latest run's rows. `make test-integration`: 18 passed.
- Live check: `make replay ARGS="--start 2013-11-05 --speed 5"` with `make sink` alongside: rows grew ~1,440 per 20 s (10 intervals = 100 data minutes, matching `--speed 5`); the panel-query test passed mid-run against the live run. Two more live runs (2013-11-06, 2013-11-07), each 20,736 sent / written / 0 dead-lettered; the user watched `step9-live-2013-11-07` grow in the browser (screenshot: real daily traffic pattern, modeled latency peaking at ~100–170 ms at busy times).
- Two sinks overlapped for a while (same consumer group): 31,230 + 10,242 = 41,472 = two days exactly; the group split the partitions with nothing lost or duplicated.

**Decisions made:** (also in DECISIONS.md)
- Generated dashboards with a drift test; read-only provisioning; training-week default window; attribution link to the dataset DOI page.

**Open items:**
- The "Replay run" selector keeps the run stored in the page URL; after a new replay, pick it from the dropdown (a reload alone keeps the old one).
- P3 (Step 11): confirm the attribution links (Big Data Challenge page / ODI Trento) for the README and the dashboard footer.
- `telemetry_raw` now holds four runs: `step7-check-2013-11-04`, `step9-live-2013-11-05`, `-06`, `-07` (20,736 rows each).

## 2026-11-22 — Milestone 1, step 8: Postgres sink

**What was built:**
- `src/producer/sink.py` + `make sink`: consumes `telemetry.raw` in batches (≤ 500); validates each message with `common.telemetry`; upserts valid rows into `telemetry_raw` in one transaction as `pipeline_writer`; invalid messages → `telemetry.dlq` with reason and source position; Kafka offsets committed only after the Postgres commit and the DLQ ack. Postgres data errors fall back to row-by-row so only the bad row is dead-lettered. Ctrl-C / SIGTERM finish the current batch; `--idle-exit N` stops when drained.
- Schema: `latency_ms` now must be > 0 (matches the table's CHECK; found while writing the sink).

**Tests run and results:**
- Unit (20): DB commit before offset commit; counts; DLQ contents and source; Postgres-refused rows dead-lettered and the stream moves on; unacknowledged DLQ blocks the offset commit; transport errors not stored; re-delivery idempotent; idle exit, stop request, consumer always closed. `make check`: 243 passed, 92% coverage (`sink.py` 75% in unit tests; the Postgres writer and live `main` are covered by integration tests and the real run).
- Integration (`tests/integration/test_sink_stack.py`, throwaway topics, all writes rolled back): 24 messages each delivered twice + 2 bad → 24 rows, 2 dead letters; row-by-row fallback isolates a row Postgres refuses (23514). `make test-integration`: 14 passed.
- Real run: `make sink ARGS="--idle-exit 10"` on the 2013-11-04 replay → 20,736 consumed, 20,736 written, 42 batches, 0 dead-lettered, ~5 s of work. `telemetry_raw`: 20,736 rows, 144 cells × 144 intervals. Totals vs the parquet, exact: sms_in 872,517.048, sms_out 498,817.592, call_in 570,114.819, call_out 675,271.004, internet 11,519,990.328, 0 gaps; sample row (square 5061, 09:00 UTC) identical.
- Effectively once: second sink run consumed 0; same day replayed again with the same run ID → 41,472 deliveries in total, still 20,736 rows (`ingested_at` refreshed); lag 0 on all 6 partitions; DLQ empty.

**Decisions made:** (also in DECISIONS.md)
- Offset-commit order, batch size, row-by-row fallback, `latency_ms > 0`.

**Open items:**
- The sink runs in the foreground (`make sink`); it becomes a Compose service only if a later step needs it always on.
- DLQ records can be duplicated if the sink crashes between the DLQ write and the offset commit (at-least-once DLQ); acceptable for a review queue.

## 2026-11-22 — Milestone 1, step 7: replay producer → Kafka

**What was built:**
- `src/common/telemetry.py`: the `telemetry.raw` message (pydantic, JSON, `schema_version` 1), key `cell_id|interval_start`, topic names; validation (finite non-negative traffic, domain ID patterns, UTC 10-minute grid, gap ⇔ null modeled values); `check_against_domain()` ties field names to `kpis.yaml` and the Milan fields.
- `src/producer/replay.py` + `make replay`: processed days + SYNTHETIC topology + MODELED KPIs → messages sorted by `(interval_start, cell_id)`; idempotent producer (`acks=all`, ≤ 5 in flight, murmur2); `--speed` / `--max-rate` pacing; invalid rows and unknown squares → `telemetry.dlq` with the reason; test-week days refused unless `--final-eval`.
- `topology.load_cell_sites()`; new dependency `confluent-kafka` 2.16.2.
- Spec: the three TODO message fields settled in `specs/02-telemetry.md`; Step 7 given a "Done when".

**Tests run and results:**
- Unit: 19 schema tests, 20 replay tests (order, site join, DLQ routes, pacing with a fake clock, buffer-full retry, delivery failures, determinism across run IDs, test-week refusal). `make check`: 222 passed, 94% coverage.
- Integration (`tests/integration/test_replay_kafka.py`, throwaway topics): 120 messages + 1 reject through the real broker; every key once, each partition in replay order, reject in the DLQ. `make test-integration`: 12 passed.
- Real run: `make replay ARGS="--start 2013-11-04 --speed 0 --run-id step7-check-2013-11-04"`: 20,736 sent in ~2 s, 0 dead-lettered, 0 failed; partitions 3,446 / 3,460 / 3,434 / 3,411 / 3,484 / 3,501.
- Read back and compared with keys recomputed from the parquet + topology (independent of the replay code): 20,736 read, 0 duplicates, 0 missing, 0 unexpected, every partition in source order; `telemetry.dlq` empty.

**Decisions made:** (also in DECISIONS.md)
- `confluent-kafka`; JSON + pydantic, not Avro; `site_id` carried; key format and partitioner; replay refuses test weeks without `--final-eval`.

**Open items:**
- `telemetry.raw` now holds the 2013-11-04 check run (`replay_run_id` `step7-check-2013-11-04`; kept 7 days by retention). The Step 8 sink will consume it.
- Pacing at the default `--speed 10` (one day ≈ 2.4 min) is unit-tested with a fake clock, not timed on the real broker.

## 2026-11-22 — Milestone 1, step 6: Docker Compose stack (+ repo tooling)

**What was built:**
- Repo tooling (commit `b52962a`, pushed, CI green): Claude Code guard hook `.claude/hooks/guard.py` with 101 tests; skills `milestone-step`, `new-package`; agent `eval-failure-reviewer`; docs reorganized into numbered `specs/00…09` + `specs/milestones/`; handoff moved to `specs/HANDOFF.md`; `evals/cases/` added.
- Step 6 run for the first time: `infra/docker-compose.yml` (Kafka 4.3.2 KRaft, Postgres 18.7, MLflow 3.17.2, Grafana 13.2.6; Spark 4.2.1 under the `spark` profile), preflight password check, DB roles + schema, Kafka topic init, `src/common/db.py`, `src/producer/topology_db.py`, make targets `up` / `down` / `ps` / `logs` / `db-topology` / `test-integration` / `spark-smoke`.
- New `tests/integration/test_stack_health.py`: fails on any restart or OOM kill, and on a missing memory limit.

**Tests run and results:**
- First `make up` failed: the user's `.env` (copied from an older `.env.example`) still had `POSTGRES_PORT=5432`, held by a local Postgres (PID 1373). User set 5433; then all services healthy; `preflight` and `kafka-init` exit 0 and `up --wait` accepts them.
- **MLflow crash loop found:** 8 restarts in ~5 min, exit 137 (cgroup OOM) every ~50 s, pinned at 767.9 / 768 MiB and ~350% CPU, while its health check passed. Cause: a job runner + 7 huey worker processes started by default. Fixed with `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`: steady 408 MiB, idle CPU, 0 restarts over 3 min.
- `make db-topology`: 198 nodes (1 core + 5 agg + 48 sites + 144 cells), 197 edges, SYNTHETIC.
- `make test-integration`: 11 passed (DB roles: writer can't delete telemetry or create tables, readonly can't write; stack stability + memory limits).
- `docker stats`: postgres 91 / 512 MiB, mlflow 417 / 768, grafana 229 / 384, kafka 578 / 1024 (~1.3 GiB total of Docker's 5.3 GiB).
- Topics (R2): `telemetry.raw` 6 partitions, retention 7 d / 256 MB; `telemetry.dlq` 1 partition, 7 d / 64 MB.
- S1: 5433 / 3000 / 5001 / 9092 closed on the LAN address `192.168.254.159` from the Mac (open on 127.0.0.1); Grafana unreachable from a phone on the same Wi-Fi. S2: preflight passed; Grafana returns 401 to anonymous requests. S5: all tags pinned.
- `make spark-smoke`: Pi ≈ 3.133, exit 0.
- `make check`: 183 passed, 95% coverage.

**Decisions made:** (also in DECISIONS.md)
- Postgres host port 5433, MLflow 5001; MLflow server jobs off.

**Open items:**
- The guard hook flags heredoc text that mentions `.env` (it reads heredoc lines as commands), including commit messages. Workaround: write the text with the Edit/Write tools or `git commit -F <file>`. A fix needs the user, since Claude can't edit `.claude/hooks/`.
- `make up` reports "healthy" even when a service is crash-looping; run `make test-integration` about a minute after `make up` to catch that.
- `.env` drifted from `.env.example` once already; a preflight check for missing keys could catch that.

## 2026-11-20 — Milestone 1, step 5 (calibration): training week 1

**What was built:**
- Downloaded (manual): 2013-11-02 → 2013-11-10. `make data`: all 9 files passed size + MD5 against the published values; 4.7–5.9 M rows each → 20,736 rows, 0 gaps; 19 s total, ~630 MB peak RAM. Raw files kept for now.
- `make calibrate` on the 7 training days (2013-11-04 → 2013-11-10).

**Tests run and results (modeled values, all 7 training days, 145,152 rows):**
- Capacity per cell (scaled units): min 177, median 1,533, max 6,045.
- `load_ratio`: p1 0.05, p50 0.33, **p99 0.80** (as designed), max 1.87; 0.22% of rows > 0.9.
- `latency_ms`: p1 20.2, p50 29.9, p99 102, max 1,122.
- `drop_rate`: p50 0.0020, p99 0.0035, max 0.252.
- Daily pattern follows real traffic: median load 0.13 at 03h, 0.57 at 12h (Europe/Rome).

**Open items:**
- **For Milestone 2:** about 0.22% of real rows already exceed load_ratio 0.9 (busy peaks). The `congestion` signature (load > 0.9 with latency/drop up) will fire on these, so they count against the false-alarm budget unless the detector or signature accounts for them.
- `drop_rate` can exceed the configured `max` (0.2) by the noise factor (seen: 0.252). Still a valid ratio; note if a strict cap is wanted.
- 2013-11-02 / 11-03 are in no split (before training); processed but never used for fitting.

## 2026-11-20 — Milestone 1, step 5 (code): modeled KPIs

**What was built:**
- `src/common/splits.yaml` + `splits.py`: the train/validation/test days in one place; `require()` guard.
- `src/producer/kpi_model.py` + `kpi_model.yaml`: real KPIs from the `kpis.yaml` Milan-field mapping; per-cell capacity calibrated on training days only; `load_ratio`, `latency_ms`, `drop_rate` with seeded per-(cell, interval) noise; null modeled values on gap rows. CLI writes `data/processed/kpi_calibration.json` (`modeled: true`, params, train days, input hashes). `make calibrate`.
- `topology.load_cells()`; telemetry schema updated (`load_ratio` added; modeled KPIs nullable on gaps).

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 71 tests passed; coverage 96%.
- `make calibrate` on the real repo refuses to run: only 2013-11-01 is processed, and it isn't a training day (guard works).

**Decisions made:** (also in DECISIONS.md)
- Splits in code; KPI model curves and parameters.

**Open items:**
- Calibrate once training week 1 is downloaded and processed; check the real distributions (expect `load_ratio` p99 ≈ 0.8).

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
