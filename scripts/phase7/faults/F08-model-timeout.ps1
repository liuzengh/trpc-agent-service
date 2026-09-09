. (Join-Path $PSScriptRoot '..\assert.ps1')
try {
  Set-P7Mock mock-model '/control/reset'; Set-P7Mock mock-model '/control/delay?ms=6000'
  $message=Submit-P7Message 'demo-redis' 'timeout probe'
  $task=Wait-P7Task 'demo-redis' $message @('failed_terminal') 90
  $stats=Get-P7MockStats mock-model
  if ($task.error_code -ne 'model_timeout' -or $stats.requests -ne 3) { throw "timeout assertion failed: task=$($task | ConvertTo-Json -Compress) stats=$($stats | ConvertTo-Json -Compress)" }
} finally {
  Set-P7Mock mock-model '/control/reset'
  $recovery=Submit-P7Message 'demo-redis' 'timeout recovery probe'
  Wait-P7Task 'demo-redis' $recovery @('succeeded') 60 | Out-Null
  Save-P7Evidence 'F08-model-timeout'
}
