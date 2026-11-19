# Soup training queue

Go HTTP API, PostgreSQL queue and independent worker processes. Jobs simulate
training by waiting; a database row is the durable effect. See DESIGN.md for the
design written before the initial code, and RESULTS.md for measured evidence.

## Run

Requirements: Docker Engine with Compose v2, Python 3 for experiments. No host Go
installation is needed. Run all commands from this directory. Use a disposable
database; do not point experiments at production data.

```sh
docker compose -p soup-practice up -d --build --scale worker=8
curl --fail http://127.0.0.1:8080/healthz
docker compose -p soup-practice ps
```

The API binds localhost:8080. PostgreSQL is private to the Compose network. The
default Compose configuration starts four workers; the command above selects
eight. Migration completes before API/workers start. The database volume survives
`docker compose -p soup-practice down`. Adding `--volumes` destroys that disposable
database; export anything needed first. Credentials are local demo credentials.

If your environment uses a trusted TLS inspection CA absent from the builder
image, supply that environment's certificate bundle as a temporary build secret
(never disable TLS verification). This was necessary in the recorded execution:

```sh
docker build --secret id=ca_bundle,src=/etc/ssl/certs/ca-certificates.crt -t soup-queue:local .
docker compose -p soup-practice up -d --no-build --scale worker=8
docker build --secret id=ca_bundle,src=/etc/ssl/certs/ca-certificates.crt \
  --target build -t soup-queue-test:local .
docker run --rm --network soup-practice_default \
  -e TEST_DATABASE_URL='postgres://queue:queue@db:5432/queue?sslmode=disable' \
  soup-queue-test:local sh -c 'go version && go vet ./... && go test -race -count=10 -v .'
```

The host bundle is not embedded into the runtime image. On ordinary networks,
the standard Compose build uses the builder's own trusted CA set.

```sh
curl -i http://127.0.0.1:8080/jobs -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: practice-1' -d '{"input":"training A"}'
curl -s http://127.0.0.1:8080/jobs/1
curl -s 'http://127.0.0.1:8080/jobs?limit=10&after=0'
curl -s -X POST http://127.0.0.1:8080/jobs/1/cancel
```

Use the ID returned by POST, which may differ from 1. Same key and input returns
200 and the same ID; first creation returns 201; different input with the same
key returns 409. `fail: true` simulates terminal failure. Cancellation returns
the current terminal state if completion already won. GET of an unknown ID is
404. Listing uses an exclusive ID cursor and `next_after`; null means no next
page in that statement's snapshot. Concurrent inserts can become visible on
later requests; this is not a stable multi-page snapshot. Inputs are compared
exactly after JSON decoding; only the idempotency key is whitespace-trimmed.

## Tests and experiments

```sh
mkdir -p evidence/my-run
docker compose -p soup-practice --profile test build test
docker compose -p soup-practice --profile test run --rm test \
  go test -race -count=10 -v . > evidence/my-run/go-tests.log 2>&1
python3 -m unittest discover -s scripts -v > evidence/my-run/python-tests.log 2>&1
```

Go tests require a real database and create isolated temporary schemas. They
skip if TEST_DATABASE_URL is absent: a skipped run is not evidence of correctness.
The Compose test service supplies it. Tests cover claims, cancellation races,
idempotency, pagination, expired/reassigned attempts, and effect transaction
rollback. Clock-skew coverage changes the in-memory lease representation, not
the machine clock. The additional blocked-finish test observes a database lock
wait before expiring the stored lease.

```sh
docker compose -p soup-practice stop worker
python3 scripts/chaos.py --project soup-practice --jobs 10000 --workers 8 \
  --work-ms 120 --kill-interval 2 --seed 20261119
```

The harness starts and owns eight actual worker containers. It sends two POSTs
per unique key plus cancellation requests, kills seeded random workers and
restarts them. It requires actual kills and reclaimed jobs. See scripts/CHAOS.md.
All run directories, including failures, must be retained.

For a separate open-loop 100 unique POST/s experiment without process kills:

```sh
docker compose -p soup-practice up -d --scale worker=8 worker
python3 scripts/load.py --project soup-practice --rate 100 --seconds 60
```

To compare worker capacity, stop every other worker first, use
`--scale worker=4` together with `load.py --expected-workers 4`, then compare
with eight. For the slower 120 ms work profile:

```sh
docker compose -p soup-practice -f compose.yaml -f compose.work120.yaml \
  up -d --no-build --no-deps --scale worker=8 worker
python3 scripts/load.py --project soup-practice --expected-workers 8 --rate 100 --seconds 60
```

Use `--no-deps` when changing only worker counts/profiles on an already healthy
stack; this avoids reconfiguring the DB after the optional native-worker port
override. Return to the base worker configuration before comparing default work.
The load driver's preflight health retries happen before scheduling begins.

The load tool preserves per-request service latency and dispatch lag, queue
samples, the final database snapshot and a summary. It uses no retries and
checks every observed job/effect after draining. This all-new-job profile is
not a mixed production workload or a long soak. Its invariant PASS does not
mean backlog was stable: inspect queue-samples.json and RESULTS.md.

## Limits and operational tradeoffs

There is no authentication, tenant isolation, admission control, retention,
heartbeat, metrics endpoint or database HA. Default work is 40 ms, lease 2000 ms;
WORK_MS must be lower than LEASE_MS. Each worker handles one job at a time.
Attempts can execute more than once; the exactly-one guarantee covers only
committed effects in this PostgreSQL transaction, not arbitrary external I/O.
Use an idempotent destination/outbox protocol for external training effects.
Infinite retry after permanent infrastructure faults is possible. A forward DB
clock jump may cause duplicate execution attempts; a backward jump may delay
recovery. Fencing and row locks protect the database effect, not real-world work.
The API pool allows 24 open connections and every process has its own pool;
budget PostgreSQL connections when scaling. The demo uses an unbounded jobs
table and scans may get more expensive as history grows.

## AI contribution and candidate verification

AI assisted with the initial design, Go implementation, tests and Python chaos
harness. In the continuation, AI restored the supplied archive, inspected the
code, added the lock-wait regression test and separate load driver, executed
the experiments in the provided environment and wrote the evidence report.
The bulk race tests, 10,000-job chaos runs and capacity measurements were
AI-operated. No employer code or data was sought or copied.

Vladimir personally ran the short recovery check and reported its PASS output:
worker A was killed with exit code 137, worker B recovered after the lease,
and the final result was `succeeded|2|1` (status, attempt, durable effect count).
The supplied result matches the retained run ending at 15:18:18 UTC on
19 November 2026 in `evidence/recovery-once/soup-demo-d4cd4645/run.log`.
This attributes only that short check to him. The larger experiments and
the 100 requests/s verdict are not presented as his personal verification.

Reported observation: a killed worker left an unfinished first attempt; the
second attempt completed successfully with one committed database effect.
No additional personal conclusions or opinions are attributed to Vladimir.

To reproduce the one-job check on Linux from this directory:

```sh
python3 scripts/check-recovery-once.py
```

For Windows PowerShell with Docker Desktop running, execute or paste
`scripts/recovery-once.ps1` from this directory. The check creates an isolated
Compose project and retains its DB for inspection. The Docker/SQL sequence was
checked on Linux; the PowerShell wrapper was not executed in that environment.
The old recovery summaries hardcode `operator: "AI"`; RESULTS.md explains why
that field is not reliable attribution. Historical logs are kept unchanged.

## Evidence handling for publication

All failed build/test/runner evidence is retained, including the VCS error,
five database connection failures, TLS build failure and shell-runner error.
Six environment/inspection files contain explicit redactions of internal proxy
addresses or host storage/runtime paths. No job state, count, timing, outcome
or failure message was removed to improve the result. Original bytes remain
outside this publication tree; `evidence/publication-redactions.json` records
the affected paths, counts and original/publication SHA-256 values.

The repository contains synthetic training inputs and deliberate local demo
credentials (`queue` / `queue`). No actual account token or private key was
found in the complete publication-tree scan. Private conversation, learning
notes, working diffs containing those notes, executables and caches are
excluded. See `evidence/publication-review.json` for the review scope and limits.
