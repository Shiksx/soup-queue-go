# Design: a recoverable training queue
*Initial design, written before implementation.*

Use Go's HTTP server and PostgreSQL as the one source of truth. PostgreSQL gives row-level concurrency, durable transactions, constraints and a common clock across workers; with many writers, SQLite's single-writer model is less attractive. The payload is an input string and a flag to simulate a deterministic training failure. A durable effects row represents an output, not a real training run.

**Enqueue.** A unique idempotency key plus an equality check on normalized inputs makes identical POSTs return the same job; reuse with different inputs is a conflict. Identity and status live in one jobs table. Use an ID cursor for pagination; this is traversal, not a cross-request snapshot.

**Claim and reclaim.** A short SQL statement locks one eligible queued job or expired running job with `FOR UPDATE SKIP LOCKED`. In the same statement it increments a monotonic attempt token and sets a deadline from the database clock. Work runs without the row lock. The simple demo uses bounded work shorter than the lease; real long training needs heartbeat/lease renewals, deadlines, and a retry policy. A worker that disappears is detected indirectly when its lease expires. A new claimant changes its attempt token; the old worker must no longer commit.

**Finish or cancel.** The worker locks its job by ID and may finish only if it is still running, the token matches and the lease is valid on the DB clock after locking. A successful terminal update and insertion of the uniquely keyed effect share one transaction. Failure is terminal with no effect. Cancel conditionally transitions queued or running rows to cancelled and clears the lease; finish and cancel serialize on the same row, so whichever gets there first wins, and a terminal result is never overwritten. The demo may continue computing briefly after cancellation, but it cannot persist an effect.

**Guarantee.** Execution attempts can be repeated, notably after a kill. There is exactly one committed simulated database effect per succeeded job and no effect for any other final state; we cannot promise exactly-once arbitrary external I/O. PostgreSQL's default READ COMMITTED is sufficient here because of row locks, guarded updates and unique constraints, not because reads alone prevent races. A shared DB clock removes differences across worker clocks, but DB clock jumps, database failover and permanent faults remain risks.

## Post-implementation validation notes (19 November 2026)

The initial design above is retained. "Normalized inputs" means the decoded Go
struct: JSON whitespace and omitted `fail:false` are equivalent; the input
string itself is compared exactly. Only key whitespace is trimmed.

Validation uses a real PostgreSQL instance at READ COMMITTED. The blocked-finish
regression waits until PostgreSQL reports the query blocked on a row lock,
expires the lease while holding that lock, then releases it and verifies that
the stale finish cannot commit. This covers the race where a lease check made
before waiting would become stale. A separate test verifies that an effect
insert failure rolls back the terminal transition.

Worker wall-clock timestamps are not passed to eligibility or finish SQL.
Changing the in-memory lease time by +/-24 hours cannot bypass DB validation.
Go's local timers govern simulated work, retries and operation deadlines; they
do not grant ownership. We did not step OS/DB clocks, exercise failover, or
simulate arbitrary external effects. A backward DB clock jump can lengthen a
lease and a forward jump can increase attempts; a stalled/permanently broken
database defeats liveness. The measured run cannot establish liveness under
all failures. See RESULTS.md for observed recovery and throughput limits.

At 100 newly enqueued jobs/s, worker service capacity matters independently of
HTTP acceptance latency. Four 40 ms workers have an ideal ceiling of 100 jobs/s
before any SQL/network overhead; eight 120 ms workers have an ideal ceiling of
66.7 jobs/s. A production decision therefore needs arrival mix, real job cost,
queue age SLO, admission/backpressure and sufficient worker capacity. The
independent load driver records queue depth as well as API latency.
