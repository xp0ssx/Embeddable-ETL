CREATE TABLE IF NOT EXISTS pipelines (
	id bigserial PRIMARY KEY,
	name text UNIQUE,
	schema jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);