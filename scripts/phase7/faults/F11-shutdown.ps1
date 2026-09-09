. (Join-Path $PSScriptRoot '..\assert.ps1')
$service=''
try {
  $deadline=(Get-Date).AddSeconds(45)
  $ready=$false
  do {
    try {
      $nodes=@(Invoke-P7Admin '/api/v1/admin/nodes')
      $worker1=$nodes | Where-Object { $_.node_id -eq 'phase7-worker-1' }
      $worker2=$nodes | Where-Object { $_.node_id -eq 'phase7-worker-2' }
      $leader=((Invoke-P7Admin '/api/v1/admin/reconciler').is_leader -eq $true)
      $ready=($leader -and $null -ne $worker1 -and $null -ne $worker2 -and $worker1.state -eq 'ready' -and $worker2.state -eq 'ready' -and [int]$worker1.inflight -eq 0 -and [int]$worker2.inflight -eq 0)
    } catch { $ready=$false }
    if (-not $ready) { Start-Sleep -Seconds 1 }
  } while (-not $ready -and (Get-Date) -lt $deadline)
  if (-not $ready) { throw 'control plane and both workers were not ready/idle before F11' }
  Set-P7Mock mock-model '/control/reset'; Set-P7Mock mock-model '/control/block?enabled=true'
  $message=Submit-P7Message 'demo-redis' 'shutdown probe'
  $task=Wait-P7Task 'demo-redis' $message @('processing') 30
  $service=if ($task.node_id -eq 'phase7-worker-1') {'worker-1'} else {'worker-2'}
  $deadline=(Get-Date).AddSeconds(20); do { $stats=Get-P7MockStats mock-model; if ($stats.active -gt 0) { break }; Start-Sleep -Milliseconds 250 } while ((Get-Date) -lt $deadline)
  if ($stats.active -lt 1) { throw 'worker did not enter the in-flight model call' }
  Invoke-P7Compose -CommandArgs @('stop','--timeout','15',$service)
  Set-P7Mock mock-model '/control/block?enabled=false'
  $final=Wait-P7Task 'demo-redis' $message @('succeeded') 90
  if ($final.attempt -lt 2) { throw "shutdown did not retry the in-flight task: $($final | ConvertTo-Json -Compress)" }
} finally {
  Set-P7Mock mock-model '/control/reset'
  if ($service) {
    Invoke-P7Compose -CommandArgs @('up','-d',$service)
    $port=if($service -eq 'worker-1'){18081}else{18082}
    Wait-P7Http ("http://127.0.0.1:$port/readyz") 200 60
  }
  Save-P7Evidence 'F11-shutdown'
}
