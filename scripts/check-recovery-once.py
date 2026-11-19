#!/usr/bin/env python3
"""Check the Docker/SQL recovery recipe on isolated data; no production data."""
import datetime
import json
from pathlib import Path
import subprocess
import time
import uuid

project = 'soup-demo-' + uuid.uuid4().hex[:8]
folder = Path('evidence/recovery-once') / project
folder.mkdir(parents=True)
stream = (folder/'run.log').open('x', buffering=1)
started = time.monotonic()

def log(message):
    line = f'{datetime.datetime.now(datetime.timezone.utc).isoformat()} +{time.monotonic()-started:.2f}s {message}'
    print(line, flush=True)
    print(line, file=stream)

def run(*args):
    log('COMMAND ' + repr(args))
    result = subprocess.run(args, capture_output=True, text=True, timeout=180)
    if result.stdout.strip(): log(result.stdout.strip())
    if result.stderr.strip(): log(result.stderr.strip())
    if result.returncode: raise RuntimeError(f'exit {result.returncode}')
    return result.stdout.strip()

def dc(*args): return run('docker', 'compose', '-p', project, *args)
def sql(query): return dc('exec', '-T', 'db', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'queue', '-d', 'queue', '-c', query)

state_query = "SELECT j.status,j.attempt,count(e.job_id) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id GROUP BY j.id;"
try:
    dc('build', 'worker')
    dc('up', '-d', '--wait', 'db')
    dc('run', '--rm', '--no-deps', 'migrate')
    sql("INSERT INTO jobs(idempotency_key,input) VALUES ('one-recovery','practice');")
    dc('run', '-d', '--no-deps', '--name', project+'-a', '-e', 'WORK_MS=15000', '-e', 'LEASE_MS=20000', 'worker')
    deadline = time.monotonic()+30
    while sql("SELECT status FROM jobs WHERE idempotency_key='one-recovery';") != 'running':
        if time.monotonic() > deadline: raise RuntimeError('A did not claim')
        time.sleep(.2)
    assert sql(state_query) == 'running|1|0'
    old_lease = sql("SELECT lease_until FROM jobs WHERE idempotency_key='one-recovery';")
    run('docker', 'kill', '--signal', 'KILL', project+'-a')
    assert run('docker', 'inspect', '--format', '{{.State.ExitCode}}', project+'-a') == '137'
    dc('run', '-d', '--no-deps', '--name', project+'-b', '-e', 'WORK_MS=1000', '-e', 'LEASE_MS=20000', 'worker')
    # Observation immediately after B starts: old lease should still fence it.
    before_expiry = sql(state_query)
    log('BEFORE EXPIRY ' + before_expiry)
    assert before_expiry == 'running|1|0'
    time.sleep(22)
    result = sql(state_query)
    assert result == 'succeeded|2|1', result
    assert sql(f"SELECT created_at >= '{old_lease}'::timestamptz FROM job_effects;") == 't'
    effect = sql('SELECT row_to_json(e) FROM job_effects e;')
    log('RESULT PASS: A killed (137); B reclaimed after old lease; succeeded|2|1; ' + effect)
    # The process cannot infer the identity of the person who invoked it.
    (folder/'summary.json').write_text(json.dumps(dict(project=project, old_lease=old_lease, result=result, effect=json.loads(effect), operator='not_recorded', powershell_executed=False), indent=2)+'\n')
finally:
    # Stop this run's two uniquely named containers; retain DB for inspection.
    for suffix in ['a','b']:
        subprocess.run(['docker', 'logs', '--timestamps', project+'-'+suffix], stdout=(folder/f'worker-{suffix}.log').open('w'), stderr=subprocess.STDOUT)
    subprocess.run(['docker', 'stop', project+'-b'], stdout=stream, stderr=subprocess.STDOUT)
    stream.close()
