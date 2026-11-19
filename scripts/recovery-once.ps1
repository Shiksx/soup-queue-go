# Run from soup-queue in Windows PowerShell, with Docker Desktop running.
$ErrorActionPreference = 'Stop'
$p = 'soup-demo-' + [guid]::NewGuid().ToString('N').Substring(0,8)
function dc { & docker compose -p $p @args; if ($LASTEXITCODE -ne 0) { throw 'Compose failed' } }
function sql($q) { dc exec -T db psql -X -qAt -v ON_ERROR_STOP=1 -U queue -d queue -c $q }
Write-Host "Isolated project: $p (keep this name to inspect it later)"
dc build worker
dc up -d --wait db
dc run --rm --no-deps migrate
sql "INSERT INTO jobs(idempotency_key,input) VALUES ('one-recovery','practice');"
dc run -d --no-deps --name "$p-a" -e WORK_MS=15000 -e LEASE_MS=20000 worker
$until = [datetime]::UtcNow.AddSeconds(30)
do {
    $state = sql "SELECT status FROM jobs WHERE idempotency_key='one-recovery';"
    if ($state -eq 'running') { break }
    if ([datetime]::UtcNow -gt $until) { throw 'Worker A did not claim the job' }
    Start-Sleep -Milliseconds 200
} while ($true)
Write-Host 'BEFORE KILL (expected: running|1|0):'
sql "SELECT j.status,j.attempt,count(e.job_id) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id GROUP BY j.id;"
& docker kill --signal KILL "$p-a"
if ($LASTEXITCODE -ne 0) { throw 'SIGKILL failed' }
Write-Host 'Worker A exit code (expected: 137):'
& docker inspect --format '{{.State.ExitCode}}' "$p-a"
dc run -d --no-deps --name "$p-b" -e WORK_MS=1000 -e LEASE_MS=20000 worker
Write-Host 'B is running; before old lease expiry the row should still be running|1|0:'
sql "SELECT j.status,j.attempt,count(e.job_id) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id GROUP BY j.id;"
Write-Host 'Worker B started. Waiting for the old 20-second lease to expire...'
Start-Sleep -Seconds 22
$result = sql "SELECT j.status,j.attempt,count(e.job_id) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id GROUP BY j.id;"
Write-Host "AFTER RECOVERY: $result"
if ($result -ne 'succeeded|2|1') { throw 'Expected succeeded|2|1; retain containers for inspection' }
Write-Host 'PASS: succeeded, attempt 2, exactly one effect. Inspect the row below:'
sql "SELECT row_to_json(e) FROM job_effects e;"
& docker stop "$p-b"
# Keep the isolated database and stopped containers for your own inspection.
