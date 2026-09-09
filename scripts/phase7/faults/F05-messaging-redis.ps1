. (Join-Path $PSScriptRoot '..\assert.ps1')
$message=''
try {
  Set-P7Mock mock-model '/control/block?enabled=true'
  $message=Submit-P7Message 'demo-redis' 'messaging durability probe'
  Wait-P7Task 'demo-redis' $message @('queued','processing') 30 | Out-Null
  Invoke-P7Compose -CommandArgs @('stop','messaging-redis')
  Wait-P7Http 'http://127.0.0.1:18080/readyz' 503 30
  Test-P7SubmitError 'demo-redis' 'not_ready'
} finally {
  Invoke-P7Compose -CommandArgs @('start','messaging-redis')
  Set-P7Mock mock-model '/control/block?enabled=false'
  if ($message) { Wait-P7Task 'demo-redis' $message @('succeeded') 90 | Out-Null }
  Save-P7Evidence 'F05-messaging-redis'
}
