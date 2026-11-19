CREATE TABLE IF NOT EXISTS jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    input text NOT NULL,
    fail boolean NOT NULL DEFAULT false,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    attempt bigint NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    lease_until timestamptz,
    CHECK ((status = 'running') = (lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_queued_idx ON jobs (id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_until, id) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS job_effects (
    job_id bigint PRIMARY KEY REFERENCES jobs(id),
    attempt bigint NOT NULL,
    value text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
