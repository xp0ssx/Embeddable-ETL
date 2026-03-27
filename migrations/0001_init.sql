CREATE TABLE IF NOT EXISTS schema_migrations (
    version text primary key,
    applied_at timestamptz NOT NULL default now()
);

CREATE TABLE IF NOT EXISTS runs (
    id bigserial primary key,
    created_at timestamptz NOT NULL default now(),
    finished_at timestamptz NULL,
    status text default 'running',
    total_rows int default 0,
    ok_rows int default 0,
    bad_rows int default 0
);

CREATE TABLE IF NOT EXISTS etl_rows (
    id bigserial primary key,
    run_id bigint NULL references runs(id),
    ingested_at timestamptz default now(),
    source text NOT NULL default 'csv',
    payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS etl_rows_run_id_idx on etl_rows (run_id);

CREATE TABLE IF NOT EXISTS etl_dead_letters (
    id bigserial primary key,
    run_id bigint NULL references runs(id),
    ingested_at timestamptz NOT NULL default now(),
    source text NOT NULL default 'csv',
    raw_line text NOT NULL,
    error text NOT NULL,
    payload jsonb NULL
);
CREATE INDEX IF NOT EXISTS etl_dead_letters_run_id_idx on etl_dead_letters (run_id);