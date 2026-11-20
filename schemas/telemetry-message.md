# Telemetry message (`telemetry.raw`)

Derived from `README.md` / `PROJECT.md`. Milan-derived fields verified 2026-11-14 against `sms-call-internet-mi-2013-11-01.txt`. One message = one cell (grid square) per 10-minute interval, summed over country codes. Fields still marked **TODO** were inferred.

**Kafka key:** `(cell_id, interval_start)`, which makes replays deterministic and idempotent. Malformed messages go to the dead-letter topic.

| Field | Type | Meaning |
|---|---|---|
| `cell_id` | string | Cell identifier, `CELL-<site>-<sector>`, mapped from a Milan grid square by the topology generator |
| `square_id` | int | Milan grid square (1–10000), from the grid's `cellId` |
| `interval_start` | timestamp (UTC) | Start of the 10-minute interval (Milan `time_interval`, epoch ms UTC) |
| `site_id` | string | TODO (inferred): parent site, from topology; may be joined later instead of carried in the message |
| `sms_in` | float | Incoming SMS activity (scaled), summed over country codes; empty = 0 |
| `sms_out` | float | Outgoing SMS activity (scaled), as above |
| `call_in` | float | Incoming call activity (scaled), as above |
| `call_out` | float | Outgoing call activity (scaled), as above |
| `internet` | float | Internet activity (scaled), as above |
| `data_gap` | bool | True if the source had no row for this square and interval (values set to 0) |
| `load_ratio` | float, nullable | Modeled: load ÷ the cell's modeled capacity (training-week q99 × 1.25). Null when `data_gap` |
| `latency_ms` | float, nullable | Modeled from load (queueing curve, seeded noise). Null when `data_gap` |
| `drop_rate` | float, nullable | Modeled from load (logistic rise near saturation, seeded noise). Null when `data_gap` |
| `schema_version` | int | TODO (inferred): schema version for evolution |
| `replay_run_id` | string | TODO (inferred): which replay run produced this message |
