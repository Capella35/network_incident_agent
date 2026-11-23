-- Milestone 2, step 1. Change events: operational data the system may see (a NOC's change log),
-- NOT ground truth. SYNTHETIC: some precede a config_push fault, most are harmless decoys, so a
-- change alone is never a label. Part of a replay run's timeline, like telemetry_raw: the
-- replay writes them for its run when it injects faults, so they share its replay_run_id
-- (a new UUID per run; a re-run under the same id writes identical rows).
-- Idempotent: runs on a fresh volume (docker-entrypoint-initdb.d) and via `make db-migrate`
-- on an existing one. Every file from 002 on must stay idempotent.

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS change_events (
    replay_run_id text        NOT NULL,
    change_id     text        NOT NULL,  -- CHG-<n> (entities.yaml)
    site_id       text        NOT NULL,  -- one row per site the change was applied to
    applied_at    timestamptz NOT NULL,
    description   text        NOT NULL,
    PRIMARY KEY (replay_run_id, change_id, site_id)
);
CREATE INDEX IF NOT EXISTS change_events_site_time_idx ON change_events (site_id, applied_at);

COMMENT ON TABLE change_events IS
    'SYNTHETIC change log written by the fault injector; operational data, not ground truth.';

-- Written with an upsert, like telemetry; nothing deletes them.
GRANT SELECT, INSERT, UPDATE ON change_events TO pipeline_writer;
GRANT SELECT ON change_events TO readonly;
