# E1 — node failure and rolling restart on a real multi-node cluster.
#
# What it proves: a worker that is SIGKILLed mid-turn (and a worker restarted
# while the queue is busy) neither loses work nor produces a duplicate reply.
# Redis Streams keeps the unacked entries in the PEL, XAUTOCLAIM hands them to a
# surviving consumer, the idempotency lease lets the turn be re-run, and the
# outbox's per-message marker keeps the second run from replying twice.
#
# It brings up the role-split nodes (backend-admin + N backend-worker) against
# the SAME MySQL/Redis as the running stack and drives load through the existing
# backend's /chat. The IM gateway is deliberately not started (one bot ⇔ one
# connection; see docs/部署与运维手册.md §3.3).
#
# Usage: pwsh -File scripts/faults/node-failure.ps1 [-Messages 20] [-Workers 2] [-Keep]
param(
  [string]$Base = "http://127.0.0.1:8080",
  [string]$Tenant = "t-demo",
  [string]$Agent = "agent001",
  [int]$Messages = 20,
  [int]$Workers = 2,
  [int]$DrainBudgetSeconds = 300,
  [switch]$Keep
)

$ErrorActionPreference = "Stop"
$root = (Get-Location).Path
$composeFiles = @("-f", "$root/deployments/docker-compose.yml", "-f", "$root/deployments/docker-compose.prod.yml")
$failures = @()
$runID = [guid]::NewGuid().ToString("N").Substring(0, 8)
$prefix = "fault-node-$runID"

function Say([string]$m) { Write-Host $m }
function Compose { docker compose @composeFiles --project-directory "$root/deployments" @args }
function GroupState {
  # Pending count comes from XPENDING (a single number), lag from XINFO GROUPS
  # (flat key/value lines). Reading both from XINFO and pairing was fragile: a
  # transient exec failure parsed as "0" and made the drain look finished while
  # a message was still being recovered.
  $pendRaw = (docker exec deployments-redis-1 redis-cli XPENDING stream:inbound workers 2>$null | Select-Object -First 1)
  $pending = 0
  if ($pendRaw -match '^\d+$') { $pending = [int]$pendRaw }
  $lines = @(docker exec deployments-redis-1 redis-cli XINFO GROUPS stream:inbound 2>$null)
  $map = @{}
  for ($i = 0; $i + 1 -lt $lines.Count; $i += 2) { $map[[string]$lines[$i]] = [string]$lines[$i + 1] }
  $lag = 0
  if ($map['lag'] -match '^\d+$') { $lag = [int]$map['lag'] }
  return [pscustomobject]@{ Pending = $pending; Lag = $lag; Raw = ("pending=$pendRaw lag=$($map['lag'])") }
}

# Wait-Answered is the real assertion: every published message must end up with
# exactly one recorded reply. It waits on the OUTCOME, not on a proxy — the PEL
# emptying does not prove anything was processed (the defect this drill found was
# exactly "acked, PEL empty, no reply"), and recovery is allowed to take up to
# the idempotency lease (90s) plus a reclaim window when a node dies mid-turn.
function Wait-Answered([string]$prefix, [int]$want, [int]$budgetSeconds) {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $budgetSeconds) {
    $n = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id LIKE '$prefix-%' AND role='ASSISTANT'")
    if ($n -ge $want) { return [pscustomobject]@{ Count = $n; Seconds = ((Get-Date) - $start).TotalSeconds } }
    Start-Sleep -Seconds 2
  }
  $n = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id LIKE '$prefix-%' AND role='ASSISTANT'")
  return [pscustomobject]@{ Count = $n; Seconds = ((Get-Date) - $start).TotalSeconds }
}
function Sql([string]$q) {
  (docker exec deployments-mysql-1 mysql -uroot -ptrpc123 -N -B -D trpc_agent_service -e $q 2>$null) -join "`n"
}

# --- cluster up -------------------------------------------------------------
Say "== bring up role-split nodes (admin + $Workers worker) against the live datastores =="
$env:TRPC_JWT_SECRET = if ($env:TRPC_JWT_SECRET) { $env:TRPC_JWT_SECRET } else { "fault-drill-jwt-secret" }
$env:TRPC_SECRET_MASTER_KEY = if ($env:TRPC_SECRET_MASTER_KEY) { $env:TRPC_SECRET_MASTER_KEY } else { "dev-master-key-change-me" }
$env:ADMIN_PASSWORD = if ($env:ADMIN_PASSWORD) { $env:ADMIN_PASSWORD } else { "admin123" }
$env:WORKER_REPLICAS = "$Workers"
Compose up -d backend-admin backend-worker | Out-Null
Compose ps --format "{{.Name}} {{.Service}} {{.Status}}"
$before = GroupState
Say "consumer group before: $($before.Raw)"

# --- load ------------------------------------------------------------------
Say ""
Say "== publish $Messages messages (sessions $prefix-*) =="
$env:GOMODCACHE = Join-Path $root ".mc8"; $env:GOCACHE = Join-Path $root ".gc14"; $env:GOPROXY = "https://goproxy.cn,direct"
$exe = Join-Path $env:TEMP "trpc-loadtest.exe"
go build -o $exe ./cmd/loadtest | Out-Null

# The loadtest client generates its own session ids, so drive the API directly
# here: the drill needs to know the session ids in order to count replies.
$token = (curl.exe -s -X POST "$Base/auth/login" -H "Content-Type: application/json" `
  -d (ConvertTo-Json @{ user_id = "admin"; password = "admin123" } -Compress) | ConvertFrom-Json).token
$sessions = @()
$msgIDs = @()
for ($i = 1; $i -le $Messages; $i++) {
  $sid = "$prefix-$i"
  $sessions += $sid
  $body = ConvertTo-Json @{ tenant_id = $Tenant; agent_id = $Agent; session_id = $sid; text = "node drill $i" } -Compress
  # One request: append the status code to the body and split it back off, so
  # the drill never publishes twice. (`-w` with a newline separator is not
  # reliable through PowerShell argument quoting.)
  $raw = (curl.exe -s -w "%{http_code}" -X POST "$Base/chat" -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d $body) -join ''
  $code = $raw.Substring($raw.Length - 3)
  $jsonText = $raw.Substring(0, $raw.Length - 3)
  if ($code -ne "202") { $failures += "publish $sid returned $code"; continue }
  $msgIDs += ($jsonText | ConvertFrom-Json).message_id
}
Say "published $($sessions.Count) messages (session prefix $prefix), captured $($msgIDs.Count) message ids"

# Give the consumers a moment to take work in flight, then kill one of them.
Start-Sleep -Seconds 2
$mid = GroupState
Say "in flight at kill time: $($mid.Raw)"

Say ""
Say "== kill a worker mid-turn (SIGKILL, no graceful shutdown) =="
$victim = (docker ps --filter "name=backend-worker" --format "{{.Names}}" | Select-Object -First 1)
docker update --restart=no $victim | Out-Null   # keep it dead so the kill is observable
docker kill $victim | Out-Null
Say "killed: $victim"
$afterKill = GroupState
Say "immediately after the kill: $($afterKill.Raw)"
if ($afterKill.Pending -eq 0 -and $afterKill.Lag -eq 0) {
  Say "note: nothing was in flight at that instant (the queue had already drained)"
}

Say ""
Say "== rolling restart of another worker while the queue is busy =="
$other = (docker ps --filter "name=backend-worker" --format "{{.Names}}" | Where-Object { $_ -ne $victim } | Select-Object -First 1)
if ($other) {
  docker restart $other | Out-Null
  Say "restarted: $other"
} else {
  Say "no second worker running (Workers=1?)"
}

# --- recover and verify ----------------------------------------------------
Say ""
Say "== wait for every message to be answered (budget ${DrainBudgetSeconds}s) =="
$answered = Wait-Answered $prefix $Messages $DrainBudgetSeconds
$final = GroupState
Say ("answered {0}/{1} after {2:N0}s   bus: {3}" -f $answered.Count, $Messages, $answered.Seconds, $final.Raw)
Say "  (a message the dead node had claimed is recovered after the idempotency lease expires: 90s + a reclaim window)"

$assistant = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id LIKE '$prefix-%' AND role='ASSISTANT'")
$user = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id LIKE '$prefix-%' AND role='USER'")
$dupes = Sql "SELECT session_id, role, COUNT(*) c FROM chat_messages WHERE session_id LIKE '$prefix-%' GROUP BY session_id, role HAVING c > 1"
$distinct = [int](Sql "SELECT COUNT(DISTINCT session_id) FROM chat_messages WHERE session_id LIKE '$prefix-%' AND role='ASSISTANT'")
# The durable "handled" record: idempotency_keys is written in the SAME
# transaction as the outbox reply event, so this count is the real "no work was
# lost" criterion. The conversation ledger is a best-effort projection of the
# same turn (see docs/部署与运维手册.md §2.4) and is reported separately.
$idList = ($msgIDs | ForEach-Object { "'$_'" }) -join ','
$durable = 0
if ($idList) { $durable = [int](Sql "SELECT COUNT(*) FROM idempotency_keys WHERE msg_key IN ($idList)") }
Say ""
Say "== verification =="
Say "durable handled records (idempotency_keys) = $durable of $Messages   <- the no-loss criterion"
Say "ledger rows: user=$user assistant=$assistant distinct sessions=$distinct (want $Messages)"
if ($durable -ne $Messages) { $failures += "durable handled records = $durable, want $Messages (work lost)" }
if ($assistant -ne $Messages) { $failures += "assistant ledger rows = $assistant, want $Messages" }
if ($user -ne $Messages) { $failures += "user ledger rows = $user, want $Messages" }
if ($distinct -ne $Messages) { $failures += "sessions answered = $distinct, want $Messages" }
if ($dupes) { $failures += "duplicate rows: $dupes" } else { Say "duplicate rows: none" }

Say ""
Say "== restarted worker rejoined the group =="
Compose ps --format "{{.Name}} {{.Service}} {{.Status}}"
$rejoined = GroupState
Say "consumer group after: $($rejoined.Raw)"

Say ""
Say "== worker logs for this run (before the containers go away) =="
foreach ($c in @(docker ps -a --filter "name=backend-worker" --format "{{.Names}}")) {
  $lines = docker logs --since 10m $c 2>&1 | Select-String -Pattern "$prefix|already completed|no text|requeue|no agent bound|idempotency"
  Say "--- $c : $($lines.Count) matching line(s)"
  $lines | Select-Object -Last 20 | ForEach-Object { Say "    $($_.Line.Trim())" }
}
$mainLines = docker logs --since 10m deployments-backend-1 2>&1 | Select-String -Pattern "$prefix|already completed|no text"
Say "--- deployments-backend-1 : $($mainLines.Count) matching line(s)"
$mainLines | Select-Object -Last 20 | ForEach-Object { Say "    $($_.Line.Trim())" }

if (-not $Keep) {
  Say ""
  Say "== tear down the drill nodes (the live gateway node is untouched) =="
  Compose rm -sf backend-admin backend-worker | Out-Null
}

Say ""
if ($failures.Count -eq 0) {
  Say "RESULT: PASS (work survived a SIGKILL and a rolling restart; exactly one reply per message)"
  exit 0
}
Say "RESULT: FAIL"
$failures | ForEach-Object { Say "  - $_" }
exit 1
