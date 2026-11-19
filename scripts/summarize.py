#!/usr/bin/env python3
"""Summarize preserved experiments, including incomplete and failed runs."""
import collections
import json
from pathlib import Path
import re
import statistics


def main():
    result = {'chaos': [], 'load': []}
    for folder in sorted(Path('evidence/chaos-runs').glob('*')):
        log = (folder/'run.log').read_text()
        entry = {'directory': str(folder), 'result': 'INCOMPLETE',
                 'config': log.splitlines()[0]}
        matches = re.findall(r'RESULT (PASS|FAIL)', log)
        if matches:
            entry['result'] = matches[-1]
        kills = [line for line in log.splitlines() if 'deliberate_kill=True' in line]
        entry['kills'] = len(kills)
        markers = [line for line in log.splitlines() if 'kill_requested ' in line] or kills
        ts = [float(re.search(r'\+\s*([\d.]+)s', line)[1]) for line in markers]
        gaps = [b-a for a,b in zip(ts, ts[1:])]
        if gaps:
            entry['kill_interval_seconds'] = dict(min=min(gaps), median=statistics.median(gaps), max=max(gaps))
        client = folder/'client-observations.json'
        if client.exists():
            obs = json.loads(client.read_text())
            entry.update(client_keys=len(obs['ids']), client_errors=obs['errors'], cancel_wins=len(obs['cancel_wins']))
        snapshot = folder/'final-db-snapshot.json'
        if snapshot.exists():
            snap = json.loads(snapshot.read_text())
            jobs = snap['jobs']
            entry.update(jobs=len(jobs), states=dict(collections.Counter(j['status'] for j in jobs)),
                effects=sum(j['effects'] for j in jobs), reclaimed=sum(j['attempt']>1 for j in jobs),
                max_attempt=max((j['attempt'] for j in jobs), default=0),
                bad_effect_rows=sum(j['effects'] != int(j['status']=='succeeded') or j['bad_effects'] for j in jobs),
                leased_rows=sum(j['leased'] for j in jobs), orphan_effects=snap['orphan_effects'])
        result['chaos'].append(entry)
    for folder in sorted(Path('evidence/load-runs').glob('*')):
        summary = folder/'summary.json'
        result['load'].append({'directory': str(folder), **(json.loads(summary.read_text()) if summary.exists() else {'result': 'INCOMPLETE'})})
    Path('evidence/results-index.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
