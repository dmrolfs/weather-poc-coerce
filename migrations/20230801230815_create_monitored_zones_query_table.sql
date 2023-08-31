-- Add migration script here
CREATE TABLE IF NOT EXISTS public.monitored_zones_query(
  projection_id TEXT NOT NULL,
  payload BYTEA NOT NULL,
  created at BIGINT NOT NULL,
  last_updated at BIGINT NOT NULL,
  PRIMARY KEY (projection_id)
);
