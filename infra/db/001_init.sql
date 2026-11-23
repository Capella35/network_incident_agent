-- Milestone 1 schema. Runs once, after 000_roles.sh, on an empty Postgres volume.
-- Column meanings: specs/02-telemetry.md and specs/01-domain-model.md section 2.
-- Entity and KPI names are not checked here; src/common/domain/ is the single source (Rule 15).

\set ON_ERROR_STOP on

-- Only the owner creates objects; application roles get explicit grants below.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Topology (SYNTHETIC; Rule 9) ----------------------------------------------------------

CREATE TABLE topology_nodes (
    node_id   text PRIMARY KEY,
    node_type text NOT NULL,
    parent_id text REFERENCES topology_nodes (node_id) DEFERRABLE INITIALLY DEFERRED,
    lat       double precision NOT NULL,
    lon       double precision NOT NULL,
    square_id integer UNIQUE,  -- Milan grid square; set only for Cells
    CHECK (parent_id IS NULL OR parent_id <> node_id)
);
CREATE INDEX topology_nodes_parent_idx ON topology_nodes (parent_id);

CREATE TABLE topology_edges (
    src      text NOT NULL REFERENCES topology_nodes (node_id) ON DELETE CASCADE,
    dst      text NOT NULL REFERENCES topology_nodes (node_id) ON DELETE CASCADE,
    relation text NOT NULL,
    PRIMARY KEY (src, dst, relation)
);
CREATE INDEX topology_edges_dst_idx ON topology_edges (dst);

-- One row: what was loaded, from which generator config and input hashes.
CREATE TABLE topology_meta (
    id        smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    synthetic boolean NOT NULL CHECK (synthetic),
    meta      jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE topology_nodes IS
    'SYNTHETIC topology: Milan grid squares mapped to cells, sites, aggregation nodes and one core by proximity. Not a real network.';
COMMENT ON TABLE topology_edges IS 'SYNTHETIC topology edges (parent -> child).';

-- Telemetry --------------------------------------------------------------------------------

-- One row per (replay run, cell, 10-minute interval). Traffic fields are REAL (Milan, ODbL);
-- load_ratio, latency_ms and drop_rate are MODELED from load and NULL on data gaps.
-- No FK to topology_nodes: the topology can be regenerated without deleting telemetry.
CREATE TABLE telemetry_raw (
    replay_run_id  text             NOT NULL,
    cell_id        text             NOT NULL,
    interval_start timestamptz      NOT NULL,
    square_id      integer          NOT NULL,
    site_id        text             NOT NULL,
    sms_in         double precision NOT NULL CHECK (sms_in >= 0),
    sms_out        double precision NOT NULL CHECK (sms_out >= 0),
    call_in        double precision NOT NULL CHECK (call_in >= 0),
    call_out       double precision NOT NULL CHECK (call_out >= 0),
    internet       double precision NOT NULL CHECK (internet >= 0),
    data_gap       boolean          NOT NULL,
    load_ratio     double precision CHECK (load_ratio >= 0),
    latency_ms     double precision CHECK (latency_ms > 0),
    drop_rate      double precision CHECK (drop_rate BETWEEN 0 AND 1),
    schema_version smallint         NOT NULL,
    ingested_at    timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (replay_run_id, cell_id, interval_start),
    CHECK (NOT data_gap OR (load_ratio IS NULL AND latency_ms IS NULL AND drop_rate IS NULL))
);
CREATE INDEX telemetry_raw_cell_time_idx ON telemetry_raw (cell_id, interval_start);
CREATE INDEX telemetry_raw_site_time_idx ON telemetry_raw (site_id, interval_start);

COMMENT ON COLUMN telemetry_raw.load_ratio IS 'MODELED: load / modeled cell capacity';
COMMENT ON COLUMN telemetry_raw.latency_ms IS 'MODELED from load';
COMMENT ON COLUMN telemetry_raw.drop_rate IS 'MODELED from load';

-- Grants (least privilege) -------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO pipeline_writer, readonly;

-- Upsert needs INSERT + UPDATE; nothing deletes telemetry.
GRANT SELECT, INSERT, UPDATE ON telemetry_raw TO pipeline_writer;
-- The topology loader replaces the whole topology in one transaction.
GRANT SELECT, INSERT, UPDATE, DELETE ON topology_nodes, topology_edges, topology_meta
    TO pipeline_writer;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;
