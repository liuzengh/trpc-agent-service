# E3 — short MySQL outage on a live node.
#
# What it proves: the platform does not confuse "the database is gone" with
# "the message is bad". Messages stay on the bus, the turn keeps running while
# its session state lives in Redis, the reply's durable record cannot be written
# during the outage, and the delivery is therefore retried — after MySQL returns
# the user gets the reply, exactly once.
#
# It also documents two honest boundaries: MySQL is NOT part of the runtime
# health probe (only the startup wait), and audit/ledger/usage writes are
# best-effort by design, so they can be missing for turns completed during the
# outage.
#
# Usage: pwsh -File scripts/faults/mysql-outage.ps1 [-DownSeconds 25]
param(
  [string]$Base = "http://127.0.0.1:8080",
  [string]$Tenant = "t-demo",
  [string]$Agent = "agent001",
  [string]$MySQLContainer = "deployments-mysql-1",
  [int]$DownSeconds = 25,
  [int]$DrainBudgetSeconds = 300
)

$ErrorActionPreference = "Stop"
$failures = @()
$runID = [guid]::NewGuid().ToString("N").Substring(0, 8)
$session = "fault-mysql-$runID"

function Say([string]$m) { Write-Host $m }
function Sql([string]$q) {
  (docker exec $MySQLContainer mysql -uroot -ptrpc123 -N -B -D trpc_agent_service -e $q 2>$null) -join "`n"
}
function GroupState {
  $lines = @(docker exec deployments-redis-1 redis-cli XINFO GROUPS stream:inbound)
  $map = @{}
  for ($i = 0; $i + 1 -lt $lines.Count; $i += 2) { $map[[string]$lines[$i]] = [string]$lines[$i + 1] }
  return [pscustomobject]@{ Pending = [int]$map['pending']; Lag = [int]$map['lag'] }
}

Say "== baseline =="
Say "healthz: $(curl.exe -s $Base/healthz)"
$token = (curl.exe -s -X POST "$Base/auth/login" -H "Content-Type: application/json" `
  -d (ConvertTo-Json @{ user_id = "admin"; password = "admin123" } -Compress) | ConvertFrom-Json).token
if (-not $token) { $failures += "login failed before the outage" }

Say ""
Say "== stop $MySQLContainer for ${DownSeconds}s =="
docker stop $MySQLContainer | Out-Null
$t0 = Get-Date

# Ingress during the outage: the bus is Redis, so accepting the message is
# correct -- refusing it would be a lie in the other direction.
$body = ConvertTo-Json @{ tenant_id = $Tenant; agent_id = $Agent; session_id = $session; text = "mysql outage drill" } -Compress
$chat = (curl.exe -s -o NUL -w "%{http_code}" -X POST "$Base/chat" -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d $body).Trim()
Say "POST /chat during the outage: $chat (202 = accepted onto the bus, as designed)"
if ($chat -ne "202") { $failures += "POST /chat during the MySQL outage returned $chat, want 202" }

$health = (curl.exe -s -o NUL -w "%{http_code}" $Base/healthz).Trim()
$healthBody = curl.exe -s $Base/healthz
Say "GET /healthz during the outage: $health $healthBody"
Say "  (MySQL is not part of the runtime probe: only the startup wait uses it, see docs/部署与运维手册.md §2.4)"

$admin = (curl.exe -s -o NUL -w "%{http_code}" "$Base/tenants" -H "Authorization: Bearer $token").Trim()
Say "GET /tenants during the outage: $admin (5xx expected: the management plane reads MySQL)"
if ($admin -notmatch '^5') { $failures += "GET /tenants returned $admin during the outage, want a 5xx" }

$mid = GroupState
Say "bus state: pending=$($mid.Pending) lag=$($mid.Lag) (the message is waiting, not lost)"

while (((Get-Date) - $t0).TotalSeconds -lt $DownSeconds) { Start-Sleep -Seconds 2 }

Say ""
Say "== start $MySQLContainer =="
docker start $MySQLContainer | Out-Null
$up = Get-Date

# Wait for the message to be processed: the reply only becomes durable once the
# outbox write succeeds again.
$answered = $false
while (((Get-Date) - $up).TotalSeconds -lt $DrainBudgetSeconds) {
  Start-Sleep -Seconds 3
  $n = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$session' AND role='ASSISTANT'")
  if ($n -ge 1) { $answered = $true; break }
}
Say "reply recorded after $([int]((Get-Date) - $up).TotalSeconds)s: $answered"
if (-not $answered) { $failures += "no reply was recorded within ${DrainBudgetSeconds}s after MySQL returned" }

Say ""
Say "== verification =="
$user = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$session' AND role='USER'")
$assistant = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$session' AND role='ASSISTANT'")
Say "chat_messages: USER=$user ASSISTANT=$assistant (want 1/1: retried, not duplicated)"
if ($user -ne 1) { $failures += "USER rows = $user, want 1" }
if ($assistant -ne 1) { $failures += "ASSISTANT rows = $assistant, want 1" }

Say ""
Say "== worker log lines mentioning this session/run =="
docker logs --since 6m deployments-backend-1 2>&1 |
  Select-String -Pattern $runID | Select-Object -Last 6 | ForEach-Object { Say $_.Line.Trim() }

Say ""
if ($failures.Count -eq 0) {
  Say "RESULT: PASS (accepted during the outage, no loss, exactly one reply after recovery)"
  exit 0
}
Say "RESULT: FAIL"
$failures | ForEach-Object { Say "  - $_" }
exit 1
