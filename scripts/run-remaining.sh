#!/bin/sh
# Corrected continuation runner. See RESULTS.md for the recorded runner failure.
set -eu
out=evidence/sequence-$(date -u +%Y%m%dT%H%M%SZ)-$$
mkdir -p "$out"
docker compose -p soup-practice stop worker > "$out/initial-stop.log" 2>&1
docker compose -p soup-practice -f compose.yaml -f compose.local.yaml up -d --no-build db api > "$out/local-db-port.log" 2>&1
curl --fail --retry 10 --retry-delay 1 http://127.0.0.1:8080/healthz > "$out/health-before-native.log" 2>&1
docker cp soup-practice-api-1:/usr/local/bin/queue ../queue
chmod u+x ../queue
DATABASE_URL='postgres://queue:queue@127.0.0.1:54329/queue?sslmode=disable' \
  python3 scripts/chaos.py --project soup-practice --mode local --sql-via-compose \
  --binary ../queue --jobs 10000 --workers 8 --work-ms 120 --kill-interval 2 \
  --seed 20261121 > "$out/chaos-console-03.log" 2>&1
docker compose -p soup-practice up -d --no-build --no-deps --scale worker=4 worker > "$out/load-four-start.log" 2>&1
python3 scripts/load.py --expected-workers 4 --rate 100 --seconds 60 > "$out/load-four-console.log" 2>&1
docker compose -p soup-practice up -d --no-build --no-deps --scale worker=8 worker > "$out/load-eight-start.log" 2>&1
python3 scripts/load.py --expected-workers 8 --rate 100 --seconds 60 > "$out/load-eight-console.log" 2>&1
docker compose -p soup-practice -f compose.yaml -f compose.work120.yaml up -d --no-build --no-deps --scale worker=8 worker > "$out/load-slow-start.log" 2>&1
python3 scripts/load.py --expected-workers 8 --rate 100 --seconds 60 > "$out/load-slow-console.log" 2>&1
docker compose -p soup-practice stop worker > "$out/final-stop-workers.log" 2>&1
