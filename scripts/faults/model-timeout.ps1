# E5 — model/upstream timeout on a live node.
#
# What it proves: a hung upstream cannot hold a turn (or a worker slot) forever,
# and the failure is classified rather than silently swallowed. Guards under test:
# the per-call HTTP timeout (llm.DefaultHTTPTimeout, 120s) and the per-turn hard
# timeout (worker.runTimeout, 300s).
#
# The upstream is a TCP listener that accepts the connection and never answers
# (busybox nc), because that is what "the model endpoint hangs" actually looks
# like. A black-holed address is a DIFFERENT failure: the OS gives up on the TCP
# handshake first (~68s, no route) and the platform correctly classifies it as
# run_error, not timeout — the drill's first run proved exactly that, so use
# -Blackhole to reproduce it.
#
# Usage: pwsh -File scripts/faults/model-timeout.ps1 [-WaitSeconds 240] [-Keep]
#        pwsh -File scripts/faults/model-timeout.ps1 -Blackhole http://10.255.255.1:9/v1 -Expect run_error
param(
  [string]$Base = "http://127.0.0.1:8080",
  [string]$Tenant = "t-demo",
  [string]$Blackhole = "",
  [string]$Expect = "timeout",
  [int]$WaitSeconds = 240,
  [switch]$Keep
)

$ErrorActionPreference = "Stop"
$failures = @()
$run = [guid]::NewGuid().ToString("N").Substring(0, 8)
$epID = "drill-timeout-ep-$run"
$agentID = "drill-timeout-agent-$run"
$session = "drill-timeout-$run"
$stallName = "drill-stall-$run"
$network = "deployments_default"

function Say([string]$m) { Write-Host $m }
function Api([string]$method, [string]$path, [string]$body = "") {
  $args = @("-s", "-X", $method, "$Base$path", "-H", "Authorization: Bearer $script:token", "-H", "Content-Type: application/json")
  if ($body) { $args += @("-d", $body) }
  return (curl.exe @args) -join ''
}
function Sql([string]$q) {
  (docker exec deployments-mysql-1 mysql -uroot -ptrpc123 -N -B -D trpc_agent_service -e $q 2>$null) -join "`n"
}

$script:token = (curl.exe -s -X POST "$Base/auth/login" -H "Content-Type: application/json" `
  -d (ConvertTo-Json @{ user_id = "admin"; password = "admin123" } -Compress) | ConvertFrom-Json).token

if ($Blackhole) {
  $upstream = $Blackhole
  Say "== upstream: $upstream (black-holed address) =="
} else {
  Say "== start a stalling upstream: accepts the connection, never answers =="
  docker rm -f $stallName 2>$null | Out-Null
  # A raw socket server that accepts and then does nothing. busybox `nc -lk`
  # looks like it would do this, but it closes the connection after reading the
  # request, which fails fast as a connection error (run_error) instead of
  # exercising the read timeout — the first two runs of this drill proved that.
  $py = "import socket`n" +
        "s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)`n" +
        "s.bind(('0.0.0.0',8080)); s.listen(64)`n" +
        "while True:`n" +
        "    c,_=s.accept()`n"
  docker run -d --name $stallName --network $network python:3.12-alpine python -c $py | Out-Null
  Start-Sleep -Seconds 3
  $upstream = "http://${stallName}:8080/v1"
  $reachable = (docker exec deployments-backend-1 sh -c "nc -z -w 2 $stallName 8080 && echo yes || echo no") -join ''
  Say "   $stallName accepting on 8080 inside $network (reachable from the backend: $reachable)"
  Say "   the client writes its request, gets no response, and must give up at the 120s HTTP guard"
}

Say ""
Say "== create a temporary endpoint pointing at it =="
$ep = ConvertTo-Json @{
  id = $epID; scope = "tenant"; tenant_id = $Tenant; name = "drill hanging endpoint $run";
  provider = "openai"; base_url = $upstream; model_name = "hangs-forever"; api_key = "drill-key"
} -Compress
$epResp = Api POST "/endpoints" $ep
Say "   endpoint create: $($epResp.Substring(0, [Math]::Min(120, $epResp.Length)))"

Say "== create and publish a temporary agent on it =="
$ag = ConvertTo-Json @{ id = $agentID; tenant_id = $Tenant; name = "drill timeout agent" } -Compress
Say "   agent create: $((Api POST '/agents' $ag).Substring(0, [Math]::Min(120, 200)))"
$profile = ConvertTo-Json @{ system_prompt = "answer briefly"; endpoint_id = $epID } -Compress
$pub = Api POST "/agents/$agentID/publish" $profile
Say "   publish: $($pub.Substring(0, [Math]::Min(160, $pub.Length)))"

Say ""
Say "== send one message and wait for the turn to be classified =="
$msg = ConvertTo-Json @{ tenant_id = $Tenant; agent_id = $agentID; session_id = $session; text = "hello" } -Compress
$send = (curl.exe -s -w "%{http_code}" -X POST "$Base/chat" -H "Authorization: Bearer $script:token" -H "Content-Type: application/json" -d $msg) -join ''
Say "POST /chat: $($send.Substring($send.Length - 3)) (202 = accepted; the timeout happens in the worker)"

$start = Get-Date
$decision = ""
$errorType = ""
$elapsed = 0
while (((Get-Date) - $start).TotalSeconds -lt $WaitSeconds) {
  Start-Sleep -Seconds 5
  $row = Sql "SELECT decision, error_type FROM audit_logs WHERE session_id='$session' ORDER BY id DESC LIMIT 1"
  if ($row) {
    $parts = $row -split "`t"
    $decision = $parts[0]
    if ($parts.Count -gt 1) { $errorType = $parts[1] }
    if ($decision -eq "failed") { $elapsed = ((Get-Date) - $start).TotalSeconds; break }
  }
}
Say ("turn outcome after {0:N0}s: decision={1} error_type={2} (want failed/{3})" -f $elapsed, $decision, $errorType, $Expect)
if ($decision -ne "failed") { $failures += "no failed audit row within ${WaitSeconds}s (decision=$decision)" }
if ($errorType -ne $Expect) { $failures += "error_type = '$errorType', want '$Expect'" }
if ($elapsed -gt 200) { $failures += "the failure took ${elapsed}s, want it bounded by the 120s HTTP / 300s turn guard" }

Say ""
Say "== stop the retry loop (this drill does not want to fill the DLQ) =="
$raw = docker exec deployments-redis-1 redis-cli XRANGE stream:inbound - +
$entryID = ""
for ($i = 0; $i -lt $raw.Count; $i++) {
  if ($raw[$i] -eq $session) { for ($j = $i; $j -ge 0; $j--) { if ($raw[$j] -match '^\d+-\d+$') { $entryID = $raw[$j]; break } }; break }
}
if ($entryID) {
  docker exec deployments-redis-1 redis-cli XACK stream:inbound workers $entryID | Out-Null
  docker exec deployments-redis-1 redis-cli XDEL stream:inbound $entryID | Out-Null
  Say "   acked+deleted stream entry $entryID"
} else {
  Say "   stream entry not found (already retried away)"
}

Say ""
Say "== the node is still healthy and serves other traffic =="
Say "healthz: $(curl.exe -s $Base/healthz)"
$okAgent = "agent001"
$okSession = "drill-timeout-after-$run"
$okMsg = ConvertTo-Json @{ tenant_id = $Tenant; agent_id = $okAgent; session_id = $okSession; text = "are you alive" } -Compress
$okSend = (curl.exe -s -w "%{http_code}" -X POST "$Base/chat" -H "Authorization: Bearer $script:token" -H "Content-Type: application/json" -d $okMsg) -join ''
$okCode = $okSend.Substring($okSend.Length - 3)
Say "POST /chat on the normal agent: $okCode"
$recovered = $false
$t2 = Get-Date
while (((Get-Date) - $t2).TotalSeconds -lt 60) {
  Start-Sleep -Seconds 3
  $n = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$okSession' AND role='ASSISTANT'")
  if ($n -ge 1) { $recovered = $true; break }
}
Say "normal turn answered afterwards: $recovered"
if (-not $recovered) { $failures += "a normal turn did not complete after the timeout" }

if (-not $Keep) {
  Say ""
  Say "== clean up the temporary assets =="
  Api DELETE "/agents/$agentID" | Out-Null
  Api DELETE "/endpoints/$epID" | Out-Null
  Say "   deleted $agentID and $epID (soft delete; the credential reference goes with the endpoint)"
  if (-not $Blackhole) {
    docker rm -f $stallName | Out-Null
    Say "   removed the stalling upstream container $stallName"
  }
}

Say ""
if ($failures.Count -eq 0) {
  Say "RESULT: PASS (upstream hang was bounded, classified as a timeout, and the node kept serving)"
  exit 0
}
Say "RESULT: FAIL"
$failures | ForEach-Object { Say "  - $_" }
exit 1
