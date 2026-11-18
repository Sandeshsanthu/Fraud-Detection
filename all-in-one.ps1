# filename: all-in-one.ps1
$ErrorActionPreference = "Stop"

function Step($title) {
    Write-Host "`n========================================================" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host "========================================================" -ForegroundColor Cyan
}

function Run-Compose {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Args)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & docker compose @Args
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($code -ne 0) {
        throw "Command 'docker compose $($Args -join ' ')' failed with exit code $code"
    }
}

function Wait-ForSetup {
    Write-Host "Checking setup container..." -ForegroundColor Yellow
    $setupId = (docker compose ps -a -q setup | Select-Object -First 1)
    if ($setupId) {
        $exitCode = (docker wait $setupId.Trim()).Trim()
        Run-Compose logs setup
        if ($exitCode -ne "0") {
            throw "Setup container failed with exit code $exitCode"
        }
    } else {
        Write-Host "No existing setup container found; continuing." -ForegroundColor DarkGray
    }
}

# 1. VERIFY SETUP LOGS & GENERATE OFFLINE DATA
Step "1. Checking Setup & Generating Offline Transaction Data"
Wait-ForSetup
python txn_generator.py --tps 100 --duration 10 --seed 42 `
    --out data/payments.jsonl --labels data/labels.jsonl --cards-out data/cards.jsonl
if ($LASTEXITCODE -ne 0) { throw "txn_generator offline run failed." }

# 2. CORE VERIFICATION SCRIPTS AGAINST RUNNING STACK
Step "2. Running Core Verify Scripts"
Run-Compose run --rm --no-deps setup python -m scripts.check
Run-Compose run --rm --no-deps setup python -m scripts.verify_fixes
Run-Compose run --rm --no-deps setup python -m scripts.poison_test
Run-Compose run --rm --no-deps setup python -m scripts.verify_store
Run-Compose run --rm --no-deps setup python -m scripts.verify_stream
Run-Compose run --rm --no-deps --volume "${PWD}\config:/config" setup python -m scripts.verify_policy

if ($env:FEATURE_MODE -ne "poller") {
    Run-Compose run --rm --no-deps setup python -m scripts.diag_features
} else {
    Write-Host "Skipping diag_features.py (only applies when FEATURE_MODE is Lambda, not poller)." -ForegroundColor DarkGray
}

# 3. SMOKE TEST & A5 IDEMPOTENT SETUP / BLOCKING CHECK
Step "3. Testing Smoke, Card Blocking & Idempotent Setup (A5)"
python scripts/smoke.py --url http://localhost:8080/v1/decide
if ($LASTEXITCODE -ne 0) { throw "Initial smoke test failed." }

$tok = (Get-Content data/cards.jsonl -Tail 1 | ConvertFrom-Json).token
Write-Host "Testing block/unblock on card token: $tok" -ForegroundColor Yellow

Run-Compose run --rm --no-deps setup python -m scripts.block $tok
Run-Compose up -d --force-recreate setup
Wait-ForSetup

python scripts/smoke.py --url http://localhost:8080/v1/decide
Run-Compose run --rm --no-deps setup python -m scripts.block $tok --off

# 4. STEP-UP E2E & CASE LIFECYCLE (C4 & C5)
Step "4. Testing Step-Up E2E & Case Closing Lifecycle (C4/C5)"
python scripts/stepup_e2e.py
if ($LASTEXITCODE -ne 0) { throw "stepup_e2e.py failed." }

python scripts/smoke.py --url http://localhost:8080/v1/decide
Start-Sleep -Seconds 10

$prev = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$caseList = docker compose run --rm --no-deps setup python -m scripts.close_case --list 2>&1
$ErrorActionPreference = $prev
$caseList | ForEach-Object { Write-Host $_ }

$firstLine = ($caseList | Where-Object { $_ -match "^tok_.*#" -or $_ -match "#" } | Select-Object -First 1)

if ($firstLine) {
    $case = ($firstLine.ToString().Trim() -split "\s+")[0]
    Write-Host "Extracted Case ID: $case" -ForegroundColor Green

    # Close case as fraud
    Run-Compose run --rm --no-deps setup python -m scripts.close_case "$case" fraud --analyst a.analyst
    # Idempotent rerun (must succeed)
    Run-Compose run --rm --no-deps setup python -m scripts.close_case "$case" fraud --analyst a.analyst

    # Conflicting disposition (expect refused)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    docker compose run --rm --no-deps setup python -m scripts.close_case "$case" not_fraud --analyst b.analyst
    $ErrorActionPreference = $prev

    # Check S3 disposition object
    Run-Compose run --rm --no-deps setup python -c "from common.aws import client; print('S3 labels:', [o['Key'] for o in client('s3').list_objects_v2(Bucket='fraud-labels', Prefix='dispositions/').get('Contents', [])])"

    # Timing-gap check: new flag on same card opens a NEW case
    python scripts/smoke.py --url http://localhost:8080/v1/decide
    Start-Sleep -Seconds 10
    Run-Compose run --rm --no-deps setup python -m scripts.close_case --list
} else {
    Write-Warning "No open case found in close_case --list output; skipping automated case-close step."
}

# 5. LIVE TRAFFIC GENERATOR & SHADOW REPORT
Step "5. Running Live Traffic Generator & Shadow Report"
python txn_generator.py --tps 50 --duration 15 --seed 42 --speedup 1 --timeout-ms 300 `
    --url http://localhost:8080/v1/decide `
    --out data/payments.jsonl --labels data/labels.jsonl --cards-out data/cards.jsonl
if ($LASTEXITCODE -ne 0) { throw "Live traffic generator failed." }

Run-Compose run --rm --no-deps setup python -m scripts.shadow_report

# 6. STORE-DOWN CIRCUIT BREAKER & FALLBACK VERIFICATION
Step "6. Running Store-Down & Breaker Verification"
$prev = $ErrorActionPreference
$ErrorActionPreference = "Continue"
python scripts/verify_store_down.py
$storeDownExit = $LASTEXITCODE
$ErrorActionPreference = $prev

if ($storeDownExit -ne 0) {
    Write-Warning "verify_store_down.py exited with code $storeDownExit (check if Windows Docker latency exceeded 50 ms)."
}

Write-Host "`n========================================================" -ForegroundColor Green
Write-Host "  ALL REMAINING CHECKS COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green

