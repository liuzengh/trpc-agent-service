# E2 — duplicate IM callback (platform redelivery) on a live node.
#
# What it proves: the same provider message delivered twice produces exactly one
# turn and one reply. IM platforms re-push a callback when they do not get an
# answer in time (the WeCom aibot SDK does not ack on its own), so this is not a
# hypothetical: it was observed as three identical replies in the audit log.
#
# The drill injects the SAME envelope id twice into stream:inbound, which is what
# the gateway publishes for one provider callback (Message.ID =
# "<channel>:<platformMsgID>"). Redis fast-path idempotency (SetNX lease) plus
# the MySQL outbox marker are what make the second delivery a no-op.
#
# Usage: pwsh -File scripts/faults/im-redelivery.ps1 [-Agent agent001]
param(
  [string]$Tenant = "t-demo",
  [string]$Agent = "agent001",
  [string]$Channel = "admin",
  [int]$DrainBudgetSeconds = 180
)

$ErrorActionPreference = "Stop"
$failures = @()
$runID = [guid]::NewGuid().ToString("N").Substring(0, 8)
$session = "fault-idem-$runID"
$msgID = "$Channel`:$runID"

function Say([string]$m) { Write-Host $m }
function Sql([string]$q) {
  (docker exec deployments-mysql-1 mysql -uroot -ptrpc123 -N -B -D trpc_agent_service -e $q 2>$null) -join "`n"
}
function GroupState {
  $lines = @(docker exec deployments-redis-1 redis-cli XINFO GROUPS stream:inbound)
  $map = @{}
  for ($i = 0; $i + 1 -lt $lines.Count; $i += 2) { $map[[string]$lines[$i]] = [string]$lines[$i + 1] }
  return [pscustomobject]@{ Pending = [int]$map['pending']; Lag = [int]$map['lag'] }
}
function Wait-Drain {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $DrainBudgetSeconds) {
    $st = GroupState
    if ($st.Pending -eq 0 -and $st.Lag -eq 0) { return $true }
    Start-Sleep -Seconds 1
  }
  return $false
}

Say "== inject the same provider message twice (id=$msgID, session=$session) =="
$content = '{"role":"user","content":"idempotency drill"}'
$fields = @(
  "id", $msgID,
  "tenant_id", $Tenant,
  "agent_id", $Agent,
  "session_id", $session,
  "channel", $Channel,
  "user_id", "drill-user",
  "content", $content
)
# Two separate stream entries with the same envelope id: exactly the shape of a
# provider re-push. XADD with explicit field/value pairs.
$e1 = (docker exec deployments-redis-1 redis-cli XADD stream:inbound '*' @fields).Trim()
$e2 = (docker exec deployments-redis-1 redis-cli XADD stream:inbound '*' @fields).Trim()
Say "stream entries: $e1 / $e2"

Say ""
Say "== wait for the consumer to drain =="
$drained = Wait-Drain
Say "drained=$drained"
if (-not $drained) { $failures += "queue did not drain" }
Start-Sleep -Seconds 2   # let the ledger write settle

Say ""
Say "== verification =="
$user = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$session' AND role='USER'")
$assistant = [int](Sql "SELECT COUNT(*) FROM chat_messages WHERE session_id='$session' AND role='ASSISTANT'")
$executed = [int](Sql "SELECT COUNT(*) FROM audit_logs WHERE session_id='$session' AND decision='executed'")
# The durable handled record is written in the same transaction as the reply, so
# it must exist exactly once even though the envelope was delivered twice.
$durable = [int](Sql "SELECT COUNT(*) FROM idempotency_keys WHERE msg_key='$msgID'")
$outbox = [int](Sql "SELECT COUNT(*) FROM outbox_events WHERE payload LIKE '%$msgID%'")
Say "chat_messages: USER=$user ASSISTANT=$assistant   audit executed=$executed   idempotency_keys=$durable   outbox_events=$outbox"
Say "(want USER=1 ASSISTANT=1 executed=1 durable=1 -- the second delivery must be a no-op)"

if ($user -ne 1) { $failures += "USER rows = $user, want 1" }
if ($assistant -ne 1) { $failures += "ASSISTANT rows = $assistant, want 1 (duplicate replies)" }
if ($executed -ne 1) { $failures += "audit executed rows = $executed, want 1 (turn ran more than once)" }
if ($durable -ne 1) { $failures += "idempotency_keys rows = $durable, want 1 (durable marker written twice)" }

Say ""
Say "== recent worker log lines for this message =="
docker logs --since 3m deployments-backend-1 2>&1 |
  Select-String -Pattern $runID | Select-Object -Last 5 | ForEach-Object { Say $_.Line.Trim() }

Say ""
if ($failures.Count -eq 0) {
  Say "RESULT: PASS (a re-pushed callback produced exactly one turn and one reply)"
  exit 0
}
Say "RESULT: FAIL"
$failures | ForEach-Object { Say "  - $_" }
exit 1
