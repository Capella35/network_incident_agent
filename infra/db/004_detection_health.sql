-- Milestone 2, step 10. What the detection dashboards and the dead-detector alert read.
-- Idempotent: runs on a fresh volume (docker-entrypoint-initdb.d) and via `make db-migrate`.

\set ON_ERROR_STOP on

-- The dead-detector alert (src/common/grafana.py) looks up telemetry ingested after the latest
-- detector heartbeat every minute; without this index that is a scan of all of telemetry_raw.
CREATE INDEX IF NOT EXISTS telemetry_raw_ingested_idx ON telemetry_raw (ingested_at);

-- One row per day checked by `make detect-check` (src/evaluation/detect_check.py): the streamed
-- alarms of that day scored against the SYNTHETIC ground truth. Written only by src/evaluation/,
-- the one reader of the ground truth (Rule 6); it holds totals, never fault labels.
CREATE TABLE IF NOT EXISTS detection_reports (
    replay_run_id    text        NOT NULL,
    day              date        NOT NULL,  -- local day (Europe/Rome), a validation day
    detector_version text        NOT NULL,
    faults           integer     NOT NULL CHECK (faults >= 0),  -- SYNTHETIC faults starting that day
    unobservable     integer     NOT NULL CHECK (unobservable BETWEEN 0 AND faults),  -- all data gaps
    detected         integer     NOT NULL CHECK (detected BETWEEN 0 AND faults - unobservable),
    delay_median     integer,               -- Kaplan-Meier median, intervals; NULL if not reached
    delay_low        integer,               -- its 95% CI; NULL if not reached
    delay_high       integer,
    incidents        integer     NOT NULL CHECK (incidents >= 0),  -- provisional grouping
    false_incidents  integer     NOT NULL CHECK (false_incidents >= 0 AND false_incidents <= incidents),
    parity           boolean     NOT NULL,  -- streamed alarms == offline scoring
    reported_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (replay_run_id, day)
);

COMMENT ON TABLE detection_reports IS
    'Detection quality per checked validation day against SYNTHETIC faults (make detect-check).';

-- What the dashboard shows: the latest check of each day (a day can be checked more than once).
CREATE OR REPLACE VIEW detection_reports_latest AS
    SELECT DISTINCT ON (day) * FROM detection_reports ORDER BY day, reported_at DESC;

-- Inserted once per run and day (ON CONFLICT DO NOTHING); nothing updates or deletes them.
GRANT SELECT, INSERT ON detection_reports TO pipeline_writer;
GRANT SELECT ON detection_reports_latest TO pipeline_writer;
GRANT SELECT ON detection_reports, detection_reports_latest TO readonly;
