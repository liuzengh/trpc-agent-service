. (Join-Path $PSScriptRoot '..\assert.ps1')
try {
  Invoke-P7Compose -CommandArgs @('stop','postgres')
  $sql=Submit-P7Message 'demo-postgres' 'postgres isolation probe'
  $redis=Submit-P7Message 'demo-redis' 'redis isolation probe'
  $sqlTask=Wait-P7Task 'demo-postgres' $sql @('failed_terminal') 90
  if ($sqlTask.error_code -ne 'sql_unavailable') { throw "unexpected PostgreSQL error: $($sqlTask.error_code)" }
  Wait-P7Task 'demo-redis' $redis @('succeeded') 60 | Out-Null
} finally {
  Invoke-P7Compose -CommandArgs @('start','postgres')
  Start-Sleep -Seconds 3
  $recovery=Submit-P7Message 'demo-postgres' 'postgres recovery probe'
  Wait-P7Task 'demo-postgres' $recovery @('succeeded') 90 | Out-Null
  Save-P7Evidence 'F06-postgres'
}
