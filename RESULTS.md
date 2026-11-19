# Execution evidence — 19 November 2026

This report records the completed experiments below. The authoritative
raw results live under `evidence/`, including prior failures from the handoff.
The bulk execution in this report was operated by AI in the supplied
environment. The separate short-check section records Vladimir's reported
personal run without attributing any bulk experiment to him.

## Environment and provenance

The supplied Soup-Go-handoff.zip passed Python's ZIP integrity check and was
extracted into a separate workspace. Its SHA-256 is
`3ba8092af90305720a4939a7579dde0a8884e1b7c34a83a75afdc3d23e7ffad9`.
The existing implementation was continued. No employer code or data was
searched or copied. Nothing was pushed, published, shared or submitted to Soup.

Recorded versions: Go 1.24.13 linux/amd64 inside the build image; PostgreSQL
16.16; Docker Engine 29.9.2; Compose v2.40.3; Python 3.12.14. Docker reports
5 CPUs and 18,882,662,400 bytes of memory. API, DB, load generator and workers
share this one rootless Docker host (vfs storage). These are environment facts,
not production sizing recommendations. Full versions, image IDs/digests and
the native executable hash are in `evidence/run-20261119/`.

The first current Compose build failed because the builder did not trust this
environment's TLS inspection CA. A temporary BuildKit secret supplied the
host's trusted certificate bundle; TLS verification remained enabled. The
runtime image does not contain that bundle. The original failure log is kept.
The native worker executable was copied from the successfully built API image;
the queue implementation was not rewritten for the native experiment.

## Deterministic checks

`go vet ./... && go test -race -count=10 -v .` ran against the live PostgreSQL
service from the builder image. All 60 cases passed (6 tests repeated 10 times);
no race-detector finding occurred. Raw output: `evidence/run-20261119/go-tests.log`.

The checks cover one winner among eight simultaneous claimants, SKIP LOCKED
under READ COMMITTED, expired/reassigned attempts, both cancellation/completion
orders plus a concurrent race, sixteen duplicate HTTP POSTs with one insertion,
payload conflicts and pagination. An injected effect-insert error must roll
back the terminal transition. The additional lock-wait test observes a blocked
finish query, expires its lease while another transaction owns the row lock,
then releases the lock and verifies rejection followed by successful reclaim.

Clock-skew coverage changes the worker's in-memory lease representation by
plus/minus 24 hours. SQL uses the database clock and ignores those values.
Actual OS/DB clock steps were not injected. This is narrower than a clock-jump
test and does not establish behavior during failover or arbitrary time changes.

Python harness control tests also passed. Earlier handoff logs remain intact:
the first Go build encountered VCS stamping failure; the subsequent build/vet
passed, but all five original DB tests failed before connecting to PostgreSQL.
Those failures are not reclassified as successes by the new executions.

## Fault experiments

Three full chaos invocations completed; all three passed their final invariant checks.

| Run | Mode / seed | SIGKILL | Kill gap median [min, max], s | Reclaimed jobs | Succeeded / failed / cancelled | Result |
| --- | --- | ---: | --- | ---: | --- | --- |
| C1 | Docker / 20261119 | 20 | 7.75 [4.95, 36.39] | 18 | 7919 / 84 / 1997 | PASS |
| C2 | Docker / 20261120 | 65 | 2.18 [1.97, 8.17] | 61 | 7918 / 84 / 1998 | PASS |
| C3 | native / 20261121 | 96 | 2.00 [1.87, 2.19] | 82 | 7917 / 83 / 2000 | PASS |

Each run ended with zero queued/running rows, zero leases, zero orphan effects and zero
effect count/metadata mismatches. Effect counts were respectively 7,919, 7,918 and
7,917, exactly the succeeded counts. Client errors and HTTP retries were zero.
C1 intervals are exit-log gaps (no request markers existed yet); C2/C3 use
kill-request markers. C1 is retained as a cadence shortfall, despite invariant PASS.

Raw logs, client ID maps, generation logs and final snapshots:

- C1: [20261119T140551Z-0ecf6071a5074219b29960684efb4f7b](evidence/chaos-runs/20261119T140551Z-0ecf6071a5074219b29960684efb4f7b/run.log)
- C2: [20261119T141042Z-ff62570c370f43f28430b36658238187](evidence/chaos-runs/20261119T141042Z-ff62570c370f43f28430b36658238187/run.log)
- C3: [20261119T141521Z-3ef0a6a487134bdbb7b936670c03f93e](evidence/chaos-runs/20261119T141521Z-3ef0a6a487134bdbb7b936670c03f93e/run.log)

Each full run uses 10,000 unique keys, 20,000 concurrent POST attempts (two per
key), 2,000 cancellation requests, eight actual worker processes and simulated
120 ms work with a 2 s lease. Seeds choose kill slots; scheduling interleavings
remain nondeterministic. SQL snapshots and client ID maps are preserved.
Verification compares exact expected key/ID sets and inputs, terminal states,
absence of leases, effect count, output and attempt token, and orphan effects.

A passed final snapshot means no observed lost jobs, nonterminal rows, extra
effects or state/effect mismatches for these finite runs. It is not a proof
over every interleaving. First-run container replacement was too slow for the
requested two-second fault cadence. The second run restarts containers, and
the third uses native processes so direct SIGKILL and restart can sustain the
target cadence. All raw runs are retained; container logs from restarted
containers are cumulative across generations.

The first container run overlapped compilation of race-instrumented tests and
is not a performance measurement. Tests finished before the later experiments.
Opening the optional localhost DB port for native workers recreated the DB
container. Health checks temporarily returned 503 while stale API pool
connections were discarded; workload began only after a successful health
check. A later configuration change before the first load run also recreated
the DB; the load driver's readiness retries completed before scheduling began.
These setup events are logged and are not claimed as a controlled DB outage
test. DB/volume durability across a crash or failover was not established.

## Separate 100 requests/s experiments

Three independent load invocations ran for 60 s each (6,000 new jobs each).
All 18,000 POSTs returned 201 with no retries, and all final job/effect checks passed.

| Run | Workers / work | Completed requests/s | API p95 / p99, ms | Dispatch lag p99, ms | Pending after input | Additional drain, s |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| L1 | 4 / 40 ms | 99.94 | 50.3 / 231.4 | 280.4 | 2751 | 51.6 |
| L2 | 8 / 40 ms | 99.90 | 103.7 / 270.6 | 155.2 | 1431 | 17.2 |
| L3 | 8 / 120 ms | 99.85 | 83.2 / 265.6 | 284.3 | 2902 | 64.5 |

Pending counts were observed at 60.56, 60.72 and 60.77 seconds respectively.
Dispatch lag maxima were 495.1, 286.4 and 447.5 ms: arrivals were not perfectly
evenly spaced, despite the open-loop target and aggregate rate near 100/s.
The shared host/load generator and periodic snapshot queries are measurement
limitations. These short runs demonstrate backlog growth, not a long soak or
a mixed production capacity certification. Raw timing files allow reanalysis.

Raw run directories:

- L1: [20261119T141853Z-d26cf6468c1a4cbbb50f9ec8ba0e8699](evidence/load-runs/20261119T141853Z-d26cf6468c1a4cbbb50f9ec8ba0e8699/summary.json)
- L2: [20261119T142103Z-245b3d8dcf9641768a9e1f1864d410a3](evidence/load-runs/20261119T142103Z-245b3d8dcf9641768a9e1f1864d410a3/summary.json)
- L3: [20261119T142237Z-7167709b875d4579850f98e64bc03ca5](evidence/load-runs/20261119T142237Z-7167709b875d4579850f98e64bc03ca5/summary.json)

The load generator schedules requests at 100/s independently of response
completion, with up to 64 client threads and no automatic retry. It preserves
every request's scheduled/start/end times, response status and body, dispatch
lag, periodic queue counts and DB snapshots immediately after ingestion and
after drain. Reported latency is HTTP enqueue latency, not job completion
latency. Scheduling lag is reported separately to expose generator saturation.
All requests create unique successful jobs; GET/list/duplicate/cancel traffic
is not included in these capacity measurements.

The database history is retained throughout: the four-worker run starts after
30,000 terminal chaos jobs, the eight-worker run after 36,000 terminal jobs,
and the 120 ms run after 42,000 terminal jobs. The latter two baselines are
captured explicitly. These are successive measurements on a growing queue
table, not an isolated same-history A/B comparison. A captured EXPLAIN during
the first load run chose a primary-key scan with a status/lease filter; that
plan makes accumulated terminal history relevant. Its share of measured cost
was not profiled, so the results are not attributed solely to worker count.

## Verdict and limits

**DON'T SHIP the claim of sustained 100 new training jobs/s with this
implementation/configuration on the measured environment.** The API accepted
the scheduled POST load, but execution capacity lagged: four default 40 ms
workers left 2,751 nonterminal jobs after ingestion, and even eight left 1,431.
They required another 51.6 s and 17.2 s to drain respectively. Raising worker
count helped in these successive measurements, but did not eliminate backlog.
There is no admission/backpressure limit to bound queue growth during sustained
overload. The 120 ms profile is further constrained even ideally: eight serial
workers can execute at most 66.7 such jobs/s before SQL overhead.

The crash/retry and database-effect safety checks passed, which supports the
design as a practice implementation. It does not establish the claimed service
capacity or a production SLO. To revisit the verdict, measure/profile claim
cost on retained history, size workers against actual training cost, add
admission control and queue-depth/age monitoring, define acceptable response
and completion latency, then repeat a long, mixed-workload soak and controlled
DB-failure tests on the target hardware. A read/duplicate-heavy 100 HTTP req/s
profile could impose a different execution rate; these load runs deliberately
use the demanding all-new-job case and do not settle every possible mix.

This demo has no admission limit, retry cap/dead-letter policy, heartbeat for
long jobs, production authentication, retention, metrics/alerts or HA. Safety
of committed effects relies on the single PostgreSQL transaction and unique
effect key. Arbitrary external training effects need their own idempotency or
outbox protocol; repeated execution attempts are expected after worker loss.

The measured experiments are short, local and use simulated work. They do not
cover a prolonged soak, production hardware/network contention, a growing
multi-day history, real training costs, database crashes/failover, network
partitions, paused live workers returning later in a distributed system, or
arbitrary external side effects. Deterministic tests cover stale result
fencing; SIGKILL cannot reproduce a live paused worker returning a result.

The AI contribution and the scope of candidate verification are separated in
README.md and the short-check section below.


## Final audit and retained failures

A separate read-only SQL audit after all workers stopped found 48,000 jobs:
41,754 succeeded, 251 failed, 5,995 cancelled, and exactly 41,754 effects.
There were zero nonterminal jobs, leases, effect mismatches or orphan effects.
See `evidence/final-audit.sql` and `evidence/final-audit.json`.

Counts: 3 full chaos invocations (all invariant PASS, C1 cadence shortfall),
3 load invocations (all final invariant PASS, all capacity profiles grew backlog),
1 new Go test invocation with 60 passed cases, and 4 Python control-test
invocations with 2 passed cases each. The original Go invocation with five
DB-connection failures, original VCS build failure and current TLS build failure
remain in their raw logs.

The continuation shell runner exited 2 with an unterminated-quote parser error
**after** the workloads and worker cleanup had completed. Its file was edited
while it was running, which likely caused the parser error. This was an
orchestration mistake, not a passed runner. `remaining-console.log` is retained.
The final runner file passes `sh -n`; the completed workload summaries and the
independent final SQL audit above establish their actual outcomes. The workload
experiments were not rerun solely to replace this failed orchestration log.

The application queue/API/schema implementation remains the supplied version.
Changes add build CA handling, a lock-wait regression, harness restart/cadence
observation, the separate load driver, evidence summaries and documentation.
The publication snapshot includes updated attribution/documentation and
explicit metadata redactions; the Go queue/API/schema behavior is unchanged.
API/DB and final load-worker stdout captures cover the surviving containers;
earlier DB/Compose worker stdout was not fully archived before recreation.
The raw harness logs, client observations, request timings and DB result
snapshots for every experiment are retained.

## Short recovery checks and candidate verification

Two additional isolated one-job checks have raw evidence under
`evidence/recovery-once/`. These jobs are in separate disposable databases and
are not included in the earlier 48,000-job audit or bulk experiment counts.
The first (`soup-demo-af0ab9dc`) was an AI-operated preparation check.

Vladimir personally ran the second check and supplied its result. The retained
`soup-demo-d4cd4645/run.log` matches his reported output exactly:

```text
2026-11-19T15:18:18.231144+00:00 +42.13s RESULT PASS: A killed (137); B reclaimed after old lease; succeeded|2|1; {"job_id":1,"attempt":2,"value":"practice","created_at":"2026-11-19T15:18:13.05858+00:00"}
```

Observed outcome in that evidence: A exited from SIGKILL with code 137; after
B started, the job initially remained `running|1|0`; following expiration of
the old lease it became `succeeded|2|1`. The single effect uses attempt 2 and
was created at 15:18:13.058580 UTC, after the old deadline at
15:18:12.031082 UTC. This supports the short recovery scenario only. No other
run, personal interpretation or capacity conclusion is attributed to him.

Both historical `summary.json` files say `operator: "AI"` because the helper
unconditionally wrote that literal. They remain untouched as raw evidence;
that field cannot identify who invoked the script. The publication helper now
writes `operator: "not_recorded"` rather than inferring a human identity.

For publication, internal proxy addresses and host storage/runtime paths in
six metadata files were replaced with explicit `[REDACTED: ...]` markers.
All job records, result counts, timing evidence and failed runs remain.
`evidence/publication-redactions.json` records byte hashes and edit categories;
originals remain separately with the owner. The earlier raw result archives
were not overwritten.
