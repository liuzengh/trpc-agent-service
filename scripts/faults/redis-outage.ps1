# E4 — Redis outage on a live node (reproducible fault drill).
#
# What it proves: a short Redis outage neither kills the node nor loses work.
# The consumer and the IM outbound follower must reconnect on their own (the
# defect this drill was written for: both loops returned on the first failed
# XREAD and never came back, while /healthz kept answering 200 and publishing
# kept accepting messages), the probe must report 503 while they are detached,
# clear once they are reading again, and a turn sent after recovery must
# complete -- all without restarting the process.
#
# Usage: pwsh -File scripts/faults/redis-outage.ps1 [-DownSeconds 25]
# Exit code 0 = every invariant held.
param(
  [string]$Base = "http://127.0.0.1:8080",
  [string]$BackendContainer = "deployments-backend-1",
  [string]$RedisContainer = "deployments-redis-1",
  [string]$Tenant = "t-demo",
  [string]$Agent = "agent001",
  [int]$DownSeconds = 25,
  [int]$RecoveryBudgetSeconds = 120
)

$ErrorActionPreference = "Stop"
$failures = @()

function Say([string]$msg) { Write-Host $msg }
function Probe([string]$path) { (curl.exe -s -o NUL -w "%{http_code}" "$Base$path").Trim() }
function ProbeBody([string]$path) { (curl.exe -s "$Base$path").Trim() }

function BuildLoadtest {
  $env:GOMODCACHE = Join-Path (Get-Location) ".mc8"
  $env:GOCACHE = Join-Path (Get-Location) ".gc14"
  $env:GOPROXY = "https://goproxy.cn,direct"
  $exe = Join-Path $env:TEMP "trpc-loadtest.exe"
  go build -o $exe ./cmd/loadtest | Out-Null
  return $exe
}

# A turn is the end-to-end signal: POST /chat is accepted, then the assistant row
# shows up in the conversation ledger, which is written only after the reply is
# durable.
function RunTurn([string]$exe, [int]$timeoutSeconds) {
  $out = & $exe -base $Base -tenant $Tenant -agent $Agent -mode turn -n 1 -c 1 -timeout "${timeoutSeconds}s" 2>&1
  $text = ($out | Out-String)
  $ok = $text -match "ok=1 failed=0"
  return [pscustomobject]@{ Ok = $ok; Text = $text.Trim() }
}

$lt = BuildLoadtest

$before = docker inspect $BackendContainer --format "{{.State.StartedAt}} {{.State.Pid}}"
Say "backend before outage : $before"

Say ""
Say "== 1. baseline (dependencies healthy) =="
Say "healthz               : $(Probe '/healthz') $(ProbeBody '/healthz')"
$baseline = RunTurn $lt 180
Say "turn                  : $($baseline.Text -replace "`r?`n", ' | ')"
if (-not $baseline.Ok) { $failures += "baseline turn failed" }

Say ""
Say "== 2. stopping $RedisContainer for ${DownSeconds}s =="
docker stop $RedisContainer | Out-Null
$downStart = Get-Date

# Sample the probe while the dependency is gone. The consumer needs
# consumeDegradeAfter consecutive failures before it reports, so the first
# samples are expected to be 200.
$sawDegraded = $false
$degradedBody = ""
while ((Get-Date) -lt $downStart.AddSeconds($DownSeconds)) {
  $code = Probe "/healthz"
  if ($code -eq "503") { $sawDegraded = $true; $degradedBody = ProbeBody "/healthz" }
  Say ("  t+{0,3:N0}s healthz={1}" -f ((Get-Date) - $downStart).TotalSeconds, $code)
  Start-Sleep -Seconds 2
}
if ($sawDegraded) {
  Say "degraded reason       : $degradedBody"
} else {
  $failures += "healthz never reported 503 during the outage"
}

# Ingress with the bus gone: the platform must fail the request rather than
# accept a message it cannot publish anywhere.
$token = (curl.exe -s -X POST "$Base/auth/login" -H "Content-Type: application/json" `
  -d (ConvertTo-Json @{ user_id = "admin"; password = "admin123" } -Compress) | ConvertFrom-Json).token
$body = ConvertTo-Json @{ tenant_id = $Tenant; agent_id = $Agent; session_id = "fault-redis-$([guid]::NewGuid().ToString('N').Substring(0,8))"; text = "ping" } -Compress
$during = (curl.exe -s -o NUL -w "%{http_code}" -X POST "$Base/chat" -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d $body).Trim()
Say "POST /chat during     : $during (expected: not 202 -- the message must not be accepted)"

Say ""
Say "== 3. starting $RedisContainer, waiting for self-healing =="
docker start $RedisContainer | Out-Null
$upStart = Get-Date
$healed = $false
while (((Get-Date) - $upStart).TotalSeconds -lt $RecoveryBudgetSeconds) {
  Start-Sleep -Seconds 3
  $code = Probe "/healthz"
  Say ("  t+{0,3:N0}s healthz={1} {2}" -f ((Get-Date) - $upStart).TotalSeconds, $code, (ProbeBody '/healthz'))
  if ($code -eq "200") { $healed = $true; break }
}
if (-not $healed) { $failures += "healthz never returned to 200 after Redis came back" }

Say ""
Say "== 4. work after recovery (no restart) =="
$after = RunTurn $lt 180
Say "turn                  : $($after.Text -replace "`r?`n", ' | ')"
if (-not $after.Ok) { $failures += "turn after recovery failed" }

$afterInfo = docker inspect $BackendContainer --format "{{.State.StartedAt}} {{.State.Pid}}"
Say "backend after outage  : $afterInfo"
if ($afterInfo -ne $before) { $failures += "the backend process was restarted ($before -> $afterInfo)" }

Say ""
if ($failures.Count -eq 0) {
  Say "RESULT: PASS (reconnected, reported 503 while detached, recovered, processed work)"
  exit 0
}
Say "RESULT: FAIL"
$failures | ForEach-Object { Say "  - $_" }
exit 1
