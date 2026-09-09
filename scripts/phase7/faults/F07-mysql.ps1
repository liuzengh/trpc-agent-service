. (Join-Path $PSScriptRoot '..\assert.ps1')
try {
  Invoke-P7Compose -CommandArgs @('stop','mysql')
  $sql=Submit-P7Message 'demo-mysql' 'mysql isolation probe'
  $redis=Submit-P7Message 'demo-redis' 'redis isolation probe'
  $sqlTask=Wait-P7Task 'demo-mysql' $sql @('failed_terminal') 90
  if ($sqlTask.error_code -ne 'sql_unavailable') { throw "unexpected MySQL error: $($sqlTask.error_code)" }
  Wait-P7Task 'demo-redis' $redis @('succeeded') 60 | Out-Null
} finally {
  Invoke-P7Compose -CommandArgs @('start','mysql')
  Start-Sleep -Seconds 5
  $recovery=Submit-P7Message 'demo-mysql' 'mysql recovery probe'
  Wait-P7Task 'demo-mysql' $recovery @('succeeded') 90 | Out-Null
  Save-P7Evidence 'F07-mysql'
}
