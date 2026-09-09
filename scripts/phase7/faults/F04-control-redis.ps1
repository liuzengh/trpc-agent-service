. (Join-Path $PSScriptRoot '..\assert.ps1')
try {
  Invoke-P7Compose -CommandArgs @('stop','control-redis')
  Wait-P7Http 'http://127.0.0.1:18080/readyz' 503 30
  Test-P7SubmitError 'demo-redis' 'not_ready'
  Test-P7AdminError '/api/v1/admin/nodes' 'control_plane_unavailable'
} finally {
  Invoke-P7Compose -CommandArgs @('start','control-redis')
  Wait-P7Http 'http://127.0.0.1:18080/readyz' 200 60
  Save-P7Evidence 'F04-control-redis'
}
