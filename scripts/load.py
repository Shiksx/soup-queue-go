#!/usr/bin/env python3
"""Open-loop unique POST load; raw request timings and queue snapshots retained.

Run only against the disposable Compose stack. No automatic request retries.
"""
import argparse
import concurrent.futures
import json
import math
import threading
import time
import urllib.request
from chaos import Run, proc


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project', default='soup-practice')
    p.add_argument('--url', default='http://127.0.0.1:8080')
    p.add_argument('--rate', type=int, default=100)
    p.add_argument('--seconds', type=int, default=60)
    p.add_argument('--output', default='evidence/load-runs')
    p.add_argument('--drain-timeout', type=int, default=300)
    p.add_argument('--expected-workers', type=int, default=8)
    a = p.parse_args()
    if a.rate <= 0 or a.seconds <= 0 or a.url != 'http://127.0.0.1:8080':
        p.error('positive rate/duration and disposable localhost URL required')
    a.mode, a.db_service = 'docker', 'db'
    r = Run(a)
    ready_deadline = time.monotonic()+60
    while True:
        try:
            with r.http.open(a.url+'/healthz', timeout=5) as response:
                if response.status == 200:
                    break
        except Exception as exc:
            r.log(f'preflight_health_retry {exc!r}')
        if time.monotonic() >= ready_deadline:
            r.log('ERROR API not ready before load')
            r.log_file.close()
            return 1
        time.sleep(1)
    workers = proc(r.compose + ['ps', '-q', '--status', 'running', 'worker']).split()
    if len(workers) != a.expected_workers:
        r.log(f'ERROR expected {a.expected_workers} workers, found {len(workers)}')
        r.log_file.close()
        return 1
    metadata = json.loads(proc(['docker', 'inspect'] + workers))
    (r.logdir/'worker-config.json').write_text(json.dumps([
        {'id': w['Id'], 'image': w['Image'], 'state': w['State'],
         'env': w['Config']['Env']} for w in metadata], indent=2)+'\n')
    baseline = r.sql("SELECT json_build_object('jobs',count(*),'nonterminal',count(*) FILTER (WHERE status IN ('queued','running'))) FROM jobs;")
    (r.logdir/'baseline.json').write_text(json.dumps(baseline)+'\n')
    rows, samples = [], []
    stop = threading.Event()
    start = time.monotonic()
    def sample():
        while not stop.is_set():
            try:
                snap = r.snapshot()
                counts = {}
                for row in snap['jobs']:
                    counts[row['status']] = counts.get(row['status'], 0) + 1
                samples.append({'elapsed': time.monotonic()-start, 'counts': counts})
            except Exception as e:
                samples.append({'elapsed': time.monotonic()-start, 'error': str(e)})
            stop.wait(5)
    def request(i, scheduled):
        begun = time.monotonic()
        result = {'i': i, 'scheduled': scheduled-start, 'start': begun-start}
        req = urllib.request.Request(a.url+'/jobs', method='POST',
            data=json.dumps({'input': 'load-'+str(i)}).encode(),
            headers={'Idempotency-Key': r.prefix+str(i), 'Content-Type': 'application/json'})
        try:
            with r.http.open(req, timeout=10) as response:
                result.update(code=response.status, body=json.load(response))
        except Exception as e:
            result.update(error=str(e))
        result['end'] = time.monotonic()-start
        return result
    observer = threading.Thread(target=sample)
    observer.start()
    r.log(f'load rate={a.rate} duration={a.seconds} unique_posts={a.rate*a.seconds} no_retries=true')
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            futures = []
            for i in range(a.rate*a.seconds):
                scheduled = start+i/a.rate
                time.sleep(max(0, scheduled-time.monotonic()))
                futures.append(pool.submit(request, i, scheduled))
            rows = [f.result() for f in futures]
        load_end = time.monotonic()-start
        deadline = time.monotonic()+a.drain_timeout
        ingest_snapshot = None
        while True:
            snap = r.snapshot()
            pending = sum(j['status'] in {'queued','running'} for j in snap['jobs'])
            if ingest_snapshot is None:
                ingest_snapshot = snap
                pending_at_load_end = pending
                ingest_snapshot_elapsed = time.monotonic()-start
                (r.logdir/'ingest-end-db-snapshot.json').write_text(json.dumps(snap)+'\n')
            if not pending or time.monotonic() > deadline:
                break
            time.sleep(1)
        errors = sum(x.get('code') != 201 for x in rows)
        expected = {str(x['body']['id']) for x in rows if x.get('code') == 201}
        bad = sum(j['status'] != 'succeeded' or j['effects'] != 1 or j['bad_effects'] or j['leased']
                  or j['input'] != 'load-'+j['key'][len(r.prefix):] for j in snap['jobs'])
        passed = not errors and not bad and len(expected) == a.rate*a.seconds and expected == {j['id'] for j in snap['jobs']} and not snap['orphan_effects']
        latency = sorted((x['end']-x['start'])*1000 for x in rows)
        lag = sorted((x['start']-x['scheduled'])*1000 for x in rows)
        def percentile(xs, q):
            return xs[min(len(xs)-1, math.ceil(len(xs)*q)-1)]
        summary = dict(rate=a.rate, seconds=a.seconds, requests=len(rows), errors=errors,
            workers=len(workers),
            completed_rps=len(rows)/load_end, response_p50_ms=percentile(latency,.5),
            response_p95_ms=percentile(latency,.95), response_p99_ms=percentile(latency,.99),
            dispatch_lag_p99_ms=percentile(lag,.99), dispatch_lag_max_ms=max(lag),
            pending_at_load_end=pending_at_load_end, ingest_snapshot_elapsed=ingest_snapshot_elapsed,
            drain_seconds=time.monotonic()-start-load_end, jobs=len(snap['jobs']),
            bad_jobs=bad, invariants_pass=passed)
        (r.logdir/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
        (r.logdir/'final-db-snapshot.json').write_text(json.dumps(snap)+'\n')
        r.log(json.dumps(summary))
        return 0 if passed else 1
    finally:
        stop.set()
        observer.join()
        (r.logdir/'requests.jsonl').write_text(''.join(json.dumps(x)+'\n' for x in rows))
        (r.logdir/'queue-samples.json').write_text(json.dumps(samples, indent=2)+'\n')
        r.log_file.close()


if __name__ == '__main__':
    raise SystemExit(main())
