# Running the fault test

Run against a disposable local stack, from the repository root. Stop any
existing worker service so only the harness's workers handle jobs:

```sh
docker compose up -d --build db api
docker compose stop worker
python3 scripts/chaos.py
```

Wait for the API to be healthy first. The harness starts its own eight real
containers, sends each of 10,000 unique keys twice with identical bodies, and
races those requests and the workers against 2,000 cancel requests. Some jobs
have a deterministic failure input. A seeded random slot receives SIGKILL
about every two seconds, followed by restart. In Docker mode it restarts the
same stopped container; each restart still launches a new worker process.
Docker worker logs are cumulative across restarts, so later generation log
files can contain earlier lines. The test requires an
observed kill, a winning cancellation and a reclaimed job, not just a fully
drained queue. It compares client identities with a consistent database
snapshot after workers stop and verifies final states, leases and effect
count/attempt correspondence.

On Unix without Compose, with the actual API already running and its
disposable PostgreSQL database migrated:

```sh
DATABASE_URL='postgres://queue:queue@localhost:5432/queue?sslmode=disable' \
  python3 scripts/chaos.py --mode local --binary ./queue
```

If container management is too slow to sustain the requested injection cadence,
retain the run and measure its actual intervals. A second route uses native
worker processes (the same static executable copied from the built API image),
with PostgreSQL and the API still running in Compose:

```sh
docker compose -p soup-practice stop worker
docker compose -p soup-practice -f compose.yaml -f compose.local.yaml up -d --no-build db api
docker cp soup-practice-api-1:/usr/local/bin/queue ../queue
chmod u+x ../queue
DATABASE_URL='postgres://queue:queue@127.0.0.1:54329/queue?sslmode=disable' \
  python3 scripts/chaos.py --project soup-practice --mode local --sql-via-compose \
  --binary ../queue --jobs 10000 --workers 8 --work-ms 120 --kill-interval 2
```

`compose.local.yaml` exposes only the disposable DB on localhost:54329.
`--sql-via-compose` avoids requiring a host psql installation; ownership and work
run in the eight native Go processes, and only the observation queries use
Compose. Do not run other worker processes alongside this experiment. Compare
`kill_requested` timestamps to report actual injection cadence; `worker_exit`
timestamps also include the time needed to collect container logs.

The default run takes a few minutes. A different seed changes the kill-slot
sequence, not request interleavings. Every invocation creates a timestamped
directory under `evidence/chaos-runs` (or `--output`): raw harness log and actual
worker logs, plus observed HTTP ids and final DB rows when reached. Pass and
fail runs both belong in the hand-in. Counts are the number of invocation
directories, not number of individual kills.

A worker crash test does not reproduce a paused worker returning a stale
result, prove races for every possible interleaving, exercise real external
effects, test API/DB outages or PostgreSQL clock jumps, or prove 100 requests/s
under production traffic. The workload is concurrent, not a controlled
100 rps benchmark. The fault test samples one concrete implementation and
configuration; deterministic race tests and a separate throughput benchmark
serve different purposes.
