-- Read-only audit of all retained jobs and effects, after workers are stopped.
WITH per_job AS (
  SELECT j.id, j.status, j.lease_until,
         count(e.job_id) AS effects,
         count(e.job_id) FILTER (WHERE e.attempt IS DISTINCT FROM j.attempt
                                OR e.value IS DISTINCT FROM j.input) AS wrong_effects
  FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id
  GROUP BY j.id
), states AS (
  SELECT status, count(*) AS n FROM jobs GROUP BY status
)
SELECT json_build_object(
  'jobs', (SELECT count(*) FROM jobs),
  'states', (SELECT json_object_agg(status,n) FROM states),
  'effects', (SELECT count(*) FROM job_effects),
  'nonterminal', (SELECT count(*) FROM jobs WHERE status IN ('queued','running')),
  'leased', (SELECT count(*) FROM jobs WHERE lease_until IS NOT NULL),
  'effect_mismatches', (SELECT count(*) FROM per_job
     WHERE effects <> CASE WHEN status='succeeded' THEN 1 ELSE 0 END OR wrong_effects<>0),
  'orphan_effects', (SELECT count(*) FROM job_effects e LEFT JOIN jobs j ON j.id=e.job_id WHERE j.id IS NULL)
);
