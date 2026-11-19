#!/usr/bin/env python3
"""Exercise a disposable Soup queue; keep one unedited log directory per run."""

import argparse
import collections
import concurrent.futures
import datetime
import http.client
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


TERMINAL = {"succeeded", "failed", "cancelled"}
EXPECTED = TERMINAL | {"queued", "running"}
LOCK = threading.Lock()


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def proc(args, timeout=25):
    completed = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if completed.returncode != 0:
        # Avoid echoing credentials that may be present in a local psql DSN.
        raise RuntimeError(f"{args[0]} exit={completed.returncode}: {completed.stderr.strip()}")
    return completed.stdout


class Run:
    def __init__(self, args):
        self.args = args
        self.prefix = "chaos-" + uuid.uuid4().hex + "-"
        self.logdir = Path(args.output) / (
            datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "-" + self.prefix[6:-1]
        )
        self.logdir.mkdir(parents=True, exist_ok=False)
        self.log_file = (self.logdir / "run.log").open("x", encoding="utf-8", buffering=1)
        self.started = time.monotonic()
        self.stop = threading.Event()
        self.workers = {}
        self.restartable = {}
        self.generation = collections.Counter()
        self.kills = 0
        self.http_retries = 0
        self.worker_error = None
        self.compose = ["docker", "compose"] + (
            ["--project-name", args.project] if args.project else []
        )
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def log(self, line):
        with LOCK:
            msg = f"{utcnow()} +{time.monotonic() - self.started:8.2f}s {line}"
            print(msg, flush=True)
            print(msg, file=self.log_file, flush=True)

    def sql(self, query):
        flags = ["-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", query]
        if self.args.mode == "docker" or getattr(self.args, "sql_via_compose", False):
            # Use the same Postgres credentials already configured inside the service.
            cmd = self.compose + [
                "exec", "-T", self.args.db_service, "sh", "-c",
                'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"',
                "psql",
            ] + flags
        else:
            cmd = ["psql", os.environ["DATABASE_URL"]] + flags
        return json.loads(proc(cmd))

    def start_worker(self, slot):
        self.generation[slot] += 1
        generation = self.generation[slot]
        logpath = self.logdir / f"worker-{slot:02d}-{generation:03d}.log"
        if self.args.mode == "docker":
            name = self.restartable.pop(slot, None)
            if name is not None:
                self.workers[slot] = (name, logpath)
                proc(["docker", "start", name], timeout=90)
                self.log(f"worker_start slot={slot} generation={generation} process={name} restarted=true")
                return
            name = self.prefix + f"w{slot}-{generation}"
            # Track before launch, so cleanup still tries the unique name after a timeout.
            self.workers[slot] = (name, logpath)
            proc(self.compose + [
                "run", "-d", "--no-deps", "-T", "--name", name,
                "-e", f"WORK_MS={self.args.work_ms}",
                self.args.worker_service,
            ], timeout=90)
            marker = name
        else:
            env = dict(os.environ, WORK_MS=str(self.args.work_ms))
            with logpath.open("xb", buffering=0) as stream:
                child = subprocess.Popen(
                    [self.args.binary, "worker"], env=env, stdout=stream,
                    stderr=subprocess.STDOUT,
                )
            self.workers[slot] = (child, logpath)
            marker = child.pid
        self.log(f"worker_start slot={slot} generation={generation} process={marker}")

    def stop_worker(self, slot, kill):
        child, logpath = self.workers[slot]
        if self.args.mode == "docker":
            before = json.loads(proc(["docker", "inspect", "--format", "{{json .State}}", child]))
            if kill:
                self.log(f"kill_requested slot={slot} process={child}")
                proc(["docker", "kill", "--signal=KILL", child])
            else:
                proc(["docker", "stop", "--time=2", child])
            state = json.loads(proc(["docker", "inspect", "--format", "{{json .State}}", child]))
            # docker logs writes container stderr to its own stderr.
            with logpath.open("xb") as stream:
                subprocess.run(["docker", "logs", "--timestamps", child], stdout=stream,
                               stderr=subprocess.STDOUT, timeout=25, check=True)
            if kill:
                self.restartable[slot] = child
            else:
                proc(["docker", "rm", child])
            exit_code = state["ExitCode"]
            was_alive = before["Running"]
        else:
            was_alive = child.poll() is None
            if kill and not was_alive:
                raise RuntimeError(f"worker slot {slot} exited before deliberate SIGKILL")
            if kill:
                self.log(f"kill_requested slot={slot} process={child.pid}")
            child.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                exit_code = child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if kill:
                    raise
                child.kill()
                exit_code = child.wait(timeout=5)
        del self.workers[slot]
        self.log(f"worker_exit slot={slot} deliberate_kill={kill} exit={exit_code}")
        if not was_alive:
            raise RuntimeError(f"worker slot {slot} exited before planned termination")
        if kill and exit_code != (137 if self.args.mode == "docker" else -signal.SIGKILL):
            raise RuntimeError(f"worker slot {slot} did not die from SIGKILL")
        return exit_code

    def disrupt(self):
        chooser = random.Random(self.args.seed)
        target_time = time.monotonic() + self.args.kill_interval
        try:
            while not self.stop.wait(max(0, target_time - time.monotonic())):
                slot = chooser.randrange(self.args.workers)
                self.stop_worker(slot, kill=True)
                self.kills += 1
                self.start_worker(slot)
                # No catch-up burst if container restart was slower than the interval.
                target_time = max(target_time + self.args.kill_interval,
                                  time.monotonic() + self.args.kill_interval / 2)
        except Exception as exc:
            self.worker_error = exc
            self.log(f"worker_failure {exc!r}")
            self.stop.set()

    def request(self, path, payload=None, key=None):
        headers = {"Accept": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.args.url + path, data=data, method="POST", headers=headers
        )
        for attempt in range(3):
            try:
                with self.http.open(req, timeout=10) as resp:
                    return resp.status, json.load(resp)
            except urllib.error.HTTPError as exc:
                with exc:
                    body = exc.read(2048).decode("utf-8", errors="replace")
                if exc.code < 500 or attempt == 2:
                    raise RuntimeError(f"HTTP {exc.code} {path}: {body}") from exc
                reason = f"HTTP {exc.code}: {body}"
            except (OSError, http.client.HTTPException) as exc:
                if attempt == 2:
                    raise
                reason = repr(exc)
            with LOCK:
                self.http_retries += 1
            self.log(f"http_retry path={path} key={key} attempt={attempt + 1} cause={reason}")
            time.sleep(0.05 * (attempt + 1))

    def create(self, index, primary):
        if self.stop.is_set():
            raise RuntimeError("worker disruption failed; aborting outstanding clients")
        key = self.prefix + str(index)
        should_fail = index % 97 == 0
        code, body = self.request("/jobs", {"input": f"training-{index}", "fail": should_fail}, key)
        if code not in {200, 201} or body.get("status") not in EXPECTED:
            raise RuntimeError(f"bad create contract for {key}: {code}, {body!r}")
        raw_id = body["id"]
        if not isinstance(raw_id, (int, str)) or isinstance(raw_id, bool) or not str(raw_id):
            raise RuntimeError(f"bad job id for {key}: {raw_id!r}")
        job_id = str(raw_id)
        cancelled = False
        if primary and index % 5 == 0:
            path = f"/jobs/{urllib.parse.quote(job_id, safe='')}/cancel"
            code, response = self.request(path)
            if code != 200 or str(response.get("id")) != job_id or response.get("status") not in TERMINAL:
                raise RuntimeError(f"bad cancel contract for {key}: {code}, {response!r}")
            cancelled = response["status"] == "cancelled"
        return index, primary, job_id, cancelled

    def traffic(self):
        ids = {}
        cancelled = set()
        errors = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.clients) as pool:
            # Alternate ordering so neither request is always first; both share the payload.
            futures = {
                pool.submit(self.create, i, primary): (i, primary)
                for i in range(self.args.jobs)
                for primary in ((True, False) if i % 2 else (False, True))
            }
            for future in concurrent.futures.as_completed(futures):
                i, is_primary = futures[future]
                try:
                    index, primary, job_id, did_cancel = future.result()
                    if index in ids and ids[index] != job_id:
                        raise RuntimeError(f"same key returned two IDs: {ids[index]} / {job_id}")
                    ids[index] = job_id
                    if did_cancel:
                        cancelled.add(index)
                except Exception as exc:
                    errors += 1
                    if errors <= 25:
                        self.log(f"client_failure index={i} primary={is_primary} error={exc!r}")
        self.log(f"clients_done keys_seen={len(ids)} requests={len(futures)} "
                 f"errors={errors} http_retries={self.http_retries} cancel_wins={len(cancelled)}")
        (self.logdir / "client-observations.json").write_text(
            json.dumps({"ids": ids, "cancel_wins": sorted(cancelled), "errors": errors},
                       sort_keys=True) + "\n", encoding="utf-8"
        )
        if errors or len(ids) != self.args.jobs or len(set(ids.values())) != self.args.jobs:
            raise RuntimeError("HTTP client failed or returned inconsistent IDs")
        return ids, cancelled

    def snapshot(self):
        # UUID-generated prefix contains no quotes or LIKE metacharacters.
        # One statement means jobs and effects come from the same MVCC snapshot.
        return self.sql(f"""
WITH counts AS (
    SELECT j.idempotency_key AS key, j.id::text AS id, j.status, j.input, j.fail,
           j.attempt, j.lease_until IS NOT NULL AS leased,
           count(e.job_id) AS effects,
           count(e.job_id) FILTER (WHERE e.attempt IS DISTINCT FROM j.attempt
                                     OR e.value IS DISTINCT FROM j.input) AS bad_effects
    FROM jobs j
    LEFT JOIN job_effects e ON e.job_id = j.id
    WHERE j.idempotency_key LIKE '{self.prefix}%'
    GROUP BY j.id, j.idempotency_key, j.status, j.input, j.fail, j.attempt, j.lease_until
)
SELECT json_build_object(
    'jobs', coalesce((SELECT json_agg(counts ORDER BY key) FROM counts), '[]'::json),
    'orphan_effects', (SELECT count(*) FROM job_effects e
                       LEFT JOIN jobs j ON e.job_id = j.id WHERE j.id IS NULL)
);
""")

    def wait_for_drain(self):
        deadline = time.monotonic() + self.args.drain_timeout
        next_log = time.monotonic()
        while True:
            if self.worker_error is not None:
                raise RuntimeError("worker disruption failed") from self.worker_error
            snapshot = self.snapshot()
            stats = collections.Counter(row["status"] for row in snapshot["jobs"])
            pending = stats["queued"] + stats["running"]
            if time.monotonic() >= next_log or pending == 0:
                self.log(f"db_progress status={dict(stats)} kills={self.kills}")
                next_log = time.monotonic() + 10
            if pending == 0 or time.monotonic() >= deadline:
                return snapshot
            time.sleep(1)

    def verify(self, snapshot, ids, cancelled):
        expected = {self.prefix + str(index): (index, job_id) for index, job_id in ids.items()}
        actual = {row["key"]: row for row in snapshot["jobs"]}
        violations = []
        if len(actual) != len(snapshot["jobs"]):
            violations.append("duplicate key in database")
        if actual.keys() != expected.keys():
            violations.append(f"key sets differ, missing={len(expected.keys() - actual.keys())} "
                              f"unexpected={len(actual.keys() - expected.keys())}")
        if snapshot["orphan_effects"]:
            violations.append(f"orphan effects={snapshot['orphan_effects']}")
        for key in sorted(actual.keys() & expected.keys()):
            row = actual[key]
            index, known_id = expected[key]
            expected_status = "failed" if index % 97 == 0 else "succeeded"
            if index in cancelled:
                expected_status = "cancelled"
            if (row["status"] != expected_status or row["id"] != known_id or row["leased"]
                    or row["input"] != f"training-{index}" or row["fail"] != (index % 97 == 0)
                    or row["effects"] != int(row["status"] == "succeeded") or row["bad_effects"]
                    or (row["status"] in {"succeeded", "failed"} and row["attempt"] < 1)):
                violations.append(f"{key}: expected={expected_status} id={known_id}; row={row!r}")
        recovered = sum(row["attempt"] > 1 for row in actual.values())
        self.log(f"assertion_summary observed={len(actual)} expected={len(expected)} worker_kills={self.kills} "
                 f"reclaimed_jobs={recovered} effects={sum(row['effects'] for row in actual.values())}")
        if not cancelled:
            violations.append("no cancellation won; scenario was not exercised")
        if not self.kills:
            violations.append("no worker was deliberately killed; scenario was not exercised")
        if not recovered:
            violations.append("no job was reclaimed; did not observe a kill during a claim")
        for line in violations[:25]:
            self.log(f"invariant_failure {line}")
        if violations:
            raise RuntimeError(f"{len(violations)} invariant/coverage assertion(s) failed")

    def run(self):
        self.log(f"config prefix={self.prefix} mode={self.args.mode} jobs={self.args.jobs} "
                 f"workers={self.args.workers} clients={self.args.clients} work_ms={self.args.work_ms} "
                 f"seed={self.args.seed} kill_interval={self.args.kill_interval} "
                 f"drain_timeout={self.args.drain_timeout} output={self.logdir}")
        disruptor = None
        success = False
        try:
            if self.args.mode == "docker":
                running = proc(self.compose + [
                    "ps", "--quiet", "--status", "running", self.args.worker_service
                ]).strip()
                if running:
                    raise RuntimeError("stop the Compose worker service first; the harness owns its workers")
            else:
                if os.name != "posix":
                    raise RuntimeError("local mode requires Unix signals; use Docker mode")
                if not os.environ.get("DATABASE_URL"):
                    raise RuntimeError("DATABASE_URL is required in local mode")
            pending = self.sql("SELECT to_json(count(*)) FROM jobs WHERE status IN ('queued', 'running');")
            if pending != 0:
                raise RuntimeError(f"disposable database already has {pending} nonterminal jobs")
            for slot in range(self.args.workers):
                self.start_worker(slot)
            disruptor = threading.Thread(target=self.disrupt, name="worker-disruptor")
            disruptor.start()
            ids, cancelled = self.traffic()
            snapshot = self.wait_for_drain()
            self.stop.set()
            disruptor.join()
            if self.worker_error is not None:
                raise RuntimeError("worker disruption failed") from self.worker_error
            # Catch any late commits after the first observed drained state.
            for slot in sorted(self.workers):
                self.stop_worker(slot, kill=False)
            snapshot = self.snapshot()
            (self.logdir / "final-db-snapshot.json").write_text(
                json.dumps(snapshot, indent=2) + "\n", encoding="utf-8"
            )
            self.verify(snapshot, ids, cancelled)
            success = True
        except Exception as exc:
            self.log(f"error {exc!r}")
        finally:
            self.stop.set()
            if disruptor is not None:
                disruptor.join()
            for slot in sorted(self.workers):
                try:
                    self.stop_worker(slot, kill=False)
                except Exception as exc:
                    self.log(f"cleanup_failure slot={slot} error={exc!r}")
                    success = False
            for slot, name in sorted(self.restartable.items()):
                try:
                    proc(["docker", "rm", name])
                except Exception as exc:
                    self.log(f"cleanup_failure stopped_slot={slot} error={exc!r}")
                    success = False
            self.log("RESULT PASS" if success else "RESULT FAIL")
            self.log_file.close()
        return 0 if success else 1


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["docker", "local"], default="docker")
    parser.add_argument("--sql-via-compose", action="store_true",
                        help="local worker processes with SQL snapshots via the Compose db service")
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--work-ms", type=int, default=120)
    parser.add_argument("--kill-interval", type=float, default=2.0)
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument("--seed", type=int, default=20261119)
    parser.add_argument("--output", default="evidence/chaos-runs")
    parser.add_argument("--db-service", default="db")
    parser.add_argument("--worker-service", default="worker")
    parser.add_argument("--project", help="Compose project name, if not using the default")
    parser.add_argument("--binary", default="./queue", help="worker executable in local mode")
    args = parser.parse_args()
    args.url = args.url.rstrip("/")
    url = urllib.parse.urlsplit(args.url)
    if (url.scheme != "http" or url.hostname not in {"127.0.0.1", "::1", "localhost"}
            or url.username or url.path or url.query or url.fragment):
        parser.error("--url must be a local disposable HTTP queue, e.g. http://127.0.0.1:8080")
    if not (1 <= args.jobs <= 100_000 and 1 <= args.workers <= 32 and 1 <= args.clients <= 128
            and args.work_ms > 0 and args.kill_interval > 0 and args.drain_timeout > 0):
        parser.error("invalid jobs/workers/clients counts, or nonpositive interval/duration")
    return args


if __name__ == "__main__":
    sys.exit(Run(parse_args()).run())
