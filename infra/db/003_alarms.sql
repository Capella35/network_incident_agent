-- Milestone 2, step 8. Alarms raised by the Spark detector job (src/streaming/job.py), and one
-- heartbeat row per micro-batch (Step 10's dead-detector alert reads them). Alarms come only
-- from detector output, never from ground truth (Rule 6); on replays with injected faults the
-- faults behind them are SYNTHETIC. Message fields: src/common/alarms.py.
-- Idempotent: runs on a fresh volume (docker-entrypoint-initdb.d) and via `make db-migrate`.

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS alarms (
    alarm_id         text             PRIMARY KEY,  -- ALM-<uuid5 of run, cell, interval, detector>
    replay_run_id    text             NOT NULL,
    cell_id          text             NOT NULL,
    site_id          text             NOT NULL,
    interval_start   timestamptz      NOT NULL,
    score            double precision NOT NULL,
    kpis             text[]           NOT NULL CHECK (cardinality(kpis) > 0),
    detector_version text             NOT NULL,  -- registered model version, e.g. detector/1
    stored_at        timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alarms_run_time_idx ON alarms (replay_run_id, interval_start);
CREATE INDEX IF NOT EXISTS alarms_cell_time_idx ON alarms (cell_id, interval_start);

CREATE TABLE IF NOT EXISTS detector_heartbeats (
    job_id           text        NOT NULL,  -- one per job process
    batch_id         bigint      NOT NULL,  -- Spark micro-batch
    detector_version text        NOT NULL,
    rows_scored      integer     NOT NULL CHECK (rows_scored >= 0),
    rows_late        integer     NOT NULL CHECK (rows_late >= 0),  -- too late to score
    alarms           integer     NOT NULL CHECK (alarms >= 0),
    latest_interval  timestamptz,           -- latest interval scored; NULL if none yet
    at               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, batch_id)
);
CREATE INDEX IF NOT EXISTS detector_heartbeats_at_idx ON detector_heartbeats (at);

COMMENT ON TABLE alarms IS
    'Detector alarms (one cell, one interval). Faults behind them on injected replays are SYNTHETIC.';

-- Inserted once each (ON CONFLICT DO NOTHING); nothing updates or deletes them.
GRANT SELECT, INSERT ON alarms, detector_heartbeats TO pipeline_writer;
GRANT SELECT ON alarms, detector_heartbeats TO readonly;
