# 02 — Telemetry (data in)

Where the data comes from, how it is processed, which parts are synthetic or modeled, and the message the replay emits. Summary in `PROJECT.md` → The data.

> **Note:** SKAB and MetroPT-3 were the datasets of the original industrial scope. They were **dropped on 2026-11-14** when the project was re-scoped to telecom (see `DECISIONS.md`). They are not used.

## Telecom Italia "Big Data Challenge" — Milan (real traffic)

Checked 2026-11-14 via the Harvard Dataverse API (dataset pages themselves returned empty). Contents verified 2026-11-14 by inspecting `milano-grid.geojson` and `sms-call-internet-mi-2013-11-01.txt` (both MD5s match the published values).

### Activity data
| Item | Value |
|---|---|
| Title | "Telecommunications - SMS, Call, Internet - MI" |
| DOI | [10.7910/DVN/EGZHFV](https://doi.org/10.7910/DVN/EGZHFV) — Harvard Dataverse, author "Telecom Italia", published 2015-05-14 |
| Related paper | Nature Scientific Data, [10.1038/sdata.2015.55](https://www.nature.com/articles/sdata201555) (field descriptions live here) |
| What it is | Telecom activity over Milan, computed from Call Detail Records |
| License | **ODbL 1.0** (see Rules below). No non-commercial clause stated. |
| Access | Files not restricted, but **every download requires a guestbook response** (guestbook 96, "Privacy risk assessment": **email required**; name, institution, position optional). A plain API download returns HTTP 400: "You may not download this file without the required Guestbook response". Checked 2026-11-14. |
| Files | 62 daily files, `sms-call-internet-mi-YYYY-MM-DD.txt`, 2013-11-01 … 2014-01-01, plain text |
| Size | ~277–376 MB per file, **~20.8 GB total** |
| Checksums | MD5 published per file (verified against by the download script; SHA-256 computed and recorded too) |
| Download | Manual, in the browser (guestbook form). Per-file IDs are published (e.g., 2013-11-01 = 2674255) |
| Format | Tab-separated, **no header**, 8 columns in every row. Column order per the paper: `square_id`, `time_interval`, `country_code`, `sms_in`, `sms_out`, `call_in`, `call_out`, `internet`. Observed values fit that order (`square_id` 1–10000; `internet` has by far the largest values) |
| Rows | One row per (square, interval, country code). 2013-11-01: 4,842,625 rows, 246 country codes (`39` = Italy is most common; `0` also frequent) |
| Values | Scaled activity, no physical units; min ~2.3e-6. **Empty fields are common** (20–35% of rows per column) and mean no recorded activity |
| Time | `time_interval` is the interval start in **epoch milliseconds (UTC)**. Intervals are exactly **10 minutes** (144 per day). Each file covers one **Milan local day** (CET, UTC+1 in Nov): the 2013-11-01 file starts at 2013-10-31 23:00 UTC |
| Completeness | 9,994 of 10,000 squares have all 144 intervals on 2013-11-01; the fewest is 135. Missing intervals are real gaps, not faults, and must not be read as outages |
| Caveats | Values are scaled, so only relative changes are meaningful. The 2013-11-01 file is a public holiday in Italy (All Saints' Day); it is outside the proposed training weeks anyway |

### Grid
| Item | Value |
|---|---|
| Title | "Milano Grid" |
| DOI | [10.7910/DVN/QJWLFU](https://doi.org/10.7910/DVN/QJWLFU) |
| File | `milano-grid.geojson`, 3.2 MB, file ID 2670806, MD5 `e64ed3858e97347c14cd049efde8c7d3` |
| License | ODbL 1.0 (same terms) |
| Grid | GeoJSON FeatureCollection, EPSG:4326 (lat/lon), **10,000 square polygons** (100 × 100), each **~235 m × 235 m**. One property, `cellId` (1–10000), which matches the activity files' `square_id` |

### Processing rules (decided 2026-11-14)
- **Sum over country codes** per (square, interval): the project needs total load per cell, not callers' nationality.
- **Empty field = 0 activity** for that measure. A source row whose fields are all empty is recorded zero activity, **not** a data gap.
- **A missing (square, interval) row = 0 activity, flagged as a data gap**, so the detector and evals can tell real data gaps from injected faults.
- Store timestamps in UTC; convert to Europe/Rome only for the seasonal baseline (hour of day, weekday) and for display.

### How it's used (manual download + scripted processing)
Decided 2026-11-14: files are **downloaded manually in the browser** (the guestbook form asks for an email; the user fills it in themselves, so no script sends personal details). Files go into `data/raw/`.

A processing script then handles them **one day at a time**: check MD5 against the published value → filter to the chosen subset of grid squares → write the small processed file → record hashes in the manifest → optionally delete the raw file. Raw files can be downloaded in batches (e.g., a week at a time) to keep disk use low; the processed subset is small.

**Subset (decided 2026-11-20):** the **12 × 12 block of 144 grid squares centred on square 5061**, the square nearest Milan's Duomo (9.1919 E, 45.4641 N). Sized for an 8 GB laptop. Grid IDs run row by row from the south-west corner (`id = row × 100 + col + 1`, verified from the grid file). Written by `make data` to `data/processed/subset.json`; rule in `src/producer/grid.py`.

**Published checksums:** the file IDs, sizes, and MD5s of all 63 files (62 daily + grid), taken from the public Dataverse metadata API (no guestbook needed for metadata), are committed in `src/producer/milan_files.json` and used by `make data` to verify each manual download. Retrieved 2026-11-20.

**Time split** (9 weeks of data; confirmed 2026-11-19, in code since 2026-11-20 as `src/common/splits.yaml`):
- Training: 2013-11-04 → 2013-12-01 (4 weeks)
- Validation: 2013-12-02 → 2013-12-15 (2 weeks)
- Test: 2013-12-16 → 2014-01-01, including the Christmas / New Year holidays as a realistic false-alarm test

## Synthetic / modeled parts (generated, committed as code + parameters)
- **Topology (decided 2026-11-20):** one grid square = one Cell; each row of the 12 × 12 subset is cut west → east into strips of 3 squares, one Site per strip (48 sites, 3 sectors each); sites are grouped into 5 AggregationNodes by seeded k-means on location (7–12 sites each); all hang off `CORE-1`. Parameters in `src/producer/topology_config.yaml` (seed 20131101); generator `src/producer/topology.py`; outputs `data/processed/topology_nodes.csv`, `topology_edges.csv`, and `topology_meta.json` (`"synthetic": true`, config, input hashes). Real networks are not laid out like this.
- **Derived KPIs:** latency, drop rate, etc., modeled from real load.
- **Injected faults:** cell outage, site power failure, backhaul failure, congestion, bad config push (fault types: `01-domain-model.md` §4; the injector does not use the signatures, Rule 15).
- **Noisy reports:** human-style incident descriptions; test set uses unseen templates.

## Rules
1. **Download from the canonical source.** Data comes only from the Harvard Dataverse DOIs above, never from mirrors. Because of the guestbook, the download is a documented manual browser step; verification and processing are scripted. Data is never redistributed in the repo.
2. **Data files only.** Never run code from a dataset repository or archive. Download only the data files.
3. **License (ODbL 1.0) obligations:**
   - **Attribution:** any use of the data must carry a hyperlink reading "from BigDataChallenge contest", linking to the Telecom Italia Big Data Challenge page and to the ODI Trento homepage (or a page referring to that information). This goes in the README, the write-up, and the demo video credits.
   - **Share-alike:** any derived or combined *database* we publish must be published under ODbL. Default: we publish **no** derived data (only code), so this doesn't apply. If that changes, record it in `DECISIONS.md` and publish the data under ODbL.
4. **Hash manifest.** Every downloaded file's SHA-256 hash, source, download date, and license are recorded in the manifest (`data/manifest.json`; template in `data/manifest-template.md`).
5. **Never committed.** Data files are git-ignored.

## Telemetry message (`telemetry.raw`)

Milan-derived fields verified 2026-11-14 against `sms-call-internet-mi-2013-11-01.txt`. One message = one cell (grid square) per 10-minute interval, summed over country codes. All fields settled 2026-11-22 (milestone 1, step 7); code: `src/common/telemetry.py`, producer: `src/producer/replay.py`.

**Encoding:** UTF-8 JSON, one object per message, fields as below; `interval_start` as ISO 8601 UTC (`2013-11-04T23:00:00Z`). JSON rather than Avro: no schema-registry container on an 8 GB laptop; the schema is enforced in code (pydantic) on both ends, and `schema_version` allows evolution.

**Kafka key:** `cell_id|interval_start`, e.g. `CELL-12-2|2013-11-04T23:00:00Z`, which makes replays deterministic and idempotent. Sent in `(interval_start, cell_id)` order by an idempotent producer (`acks=all`, murmur2 partitioner), so a replay of the same days gives every partition of `telemetry.raw` (6 partitions) the same key sequence.

**Validation:** non-negative, finite traffic; IDs match the domain patterns; `interval_start` is UTC and on the 10-minute grid; `data_gap` ⇔ modeled values null (and traffic 0); `drop_rate` ≤ 1. Rows that fail go to `telemetry.dlq` as `{schema_version, replay_run_id, reason, row}`, never silently dropped.

| Field | Type | Meaning |
|---|---|---|
| `cell_id` | string | Cell identifier, `CELL-<site>-<sector>`, mapped from a Milan grid square by the topology generator |
| `square_id` | int | Milan grid square (1–10000), from the grid's `cellId` |
| `interval_start` | timestamp (UTC) | Start of the 10-minute interval (Milan `time_interval`, epoch ms UTC) |
| `site_id` | string | Parent Site of the cell, from the SYNTHETIC topology. Carried in the message (decided 2026-11-22) so the correlator and Spark need no join; `telemetry_raw` stores it too |
| `sms_in` | float | Incoming SMS activity (scaled), summed over country codes; empty = 0 |
| `sms_out` | float | Outgoing SMS activity (scaled), as above |
| `call_in` | float | Incoming call activity (scaled), as above |
| `call_out` | float | Outgoing call activity (scaled), as above |
| `internet` | float | Internet activity (scaled), as above |
| `data_gap` | bool | True if the source had no row for this square and interval (values set to 0) |
| `load_ratio` | float, nullable | Modeled: load ÷ the cell's modeled capacity (training-week q99 × 1.25). Null when `data_gap` |
| `latency_ms` | float, nullable | Modeled from load (queueing curve, seeded noise). Null when `data_gap` |
| `drop_rate` | float, nullable | Modeled from load (logistic rise near saturation, seeded noise). Null when `data_gap` |
| `schema_version` | int | `1`. Bumped on any incompatible change; consumers reject versions they don't know |
| `replay_run_id` | string | Which replay run produced this message (1–64 chars; a new UUID unless `--run-id` is given). Part of `telemetry_raw`'s primary key, so two replays of the same day never collide |
