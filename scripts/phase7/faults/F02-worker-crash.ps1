. (Join-Path $PSScriptRoot '..\assert.ps1')
$message=''
try {
  $deadline=(Get-Date).AddSeconds(45)
  $idle=$false
  do {
    try {
      $nodes=@(Invoke-P7Admin '/api/v1/admin/nodes')
      $worker1=$nodes | Where-Object { $_.node_id -eq 'phase7-worker-1' }
      $worker2=$nodes | Where-Object { $_.node_id -eq 'phase7-worker-2' }
      $idle=($null -ne $worker1 -and $null -ne $worker2 -and $worker1.state -eq 'ready' -and $worker2.state -eq 'ready' -and [int]$worker1.inflight -eq 0 -and [int]$worker2.inflight -eq 0)
    } catch { $idle=$false }
    if (-not $idle) { Start-Sleep -Seconds 1 }
  } while (-not $idle -and (Get-Date) -lt $deadline)
  if (-not $idle) { throw 'workers were not both ready and idle before F02' }
  $deadline=(Get-Date).AddSeconds(45)
  $leaderReady=$false
  do {
    try { $leaderReady=((Invoke-P7Admin '/api/v1/admin/reconciler').is_leader -eq $true) } catch { $leaderReady=$false }
    if (-not $leaderReady) { Start-Sleep -Seconds 1 }
  } while (-not $leaderReady -and (Get-Date) -lt $deadline)
  if (-not $leaderReady) { throw 'reconciler leader was not ready before F02' }
  Set-P7Mock mock-model '/control/reset'; Set-P7Mock mock-model '/control/block?enabled=true'
  for ($i=0; $i -lt 20; $i++) {
    $candidate=Submit-P7Message 'demo-redis' 'worker takeover probe'
    $task=Wait-P7Task 'demo-redis' $candidate @('queued','processing') 20
    if ($task.node_id -eq 'phase7-worker-1') { $message=$candidate; break }
    # A candidate assigned to worker-2 occupies its single slot while the
    # model is blocked. Finish it before probing the next rendezvous result.
    Set-P7Mock mock-model '/control/block?enabled=false'
    Wait-P7Task 'demo-redis' $candidate @('succeeded') 30 | Out-Null
    Set-P7Mock mock-model '/control/block?enabled=true'
  }
  if (-not $message) { throw 'could not assign a probe to worker-1' }
  $deadline=(Get-Date).AddSeconds(20); do { $stats=Get-P7MockStats mock-model; if ($stats.active -gt 0) { break }; Start-Sleep -Milliseconds 250 } while ((Get-Date) -lt $deadline)
  if ($stats.active -lt 1) { throw 'worker-1 never entered the model call' }
  Invoke-P7Compose -CommandArgs @('kill','worker-1')
  Set-P7Mock mock-model '/control/block?enabled=false'
  $offlineDeadline=(Get-Date).AddSeconds(20)
  $offline=$false
  do {
    try { $nodes=@(Invoke-P7Admin '/api/v1/admin/nodes'); $worker1=$nodes | Where-Object { $_.node_id -eq 'phase7-worker-1' }; $offline=($null -eq $worker1 -or $worker1.state -eq 'offline') } catch { $offline=$false }
    if (-not $offline) { Start-Sleep -Seconds 1 }
  } while (-not $offline -and (Get-Date) -lt $offlineDeadline)
  $task=Wait-P7Task 'demo-redis' $message @('succeeded') 90
  if ($task.attempt -ne 2 -or $task.node_id -ne 'phase7-worker-2') { throw "unexpected takeover result: $($task | ConvertTo-Json -Compress)" }
} finally {
  Set-P7Mock mock-model '/control/reset'
  Invoke-P7Compose -CommandArgs @('up','-d','worker-1')
  Wait-P7Http 'http://127.0.0.1:18081/readyz' 200 60
  Save-P7Evidence 'F02-worker-crash'
}
