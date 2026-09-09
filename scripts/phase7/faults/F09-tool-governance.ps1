. (Join-Path $PSScriptRoot '..\assert.ps1')
$conversation='phase7-tool-'+[guid]::NewGuid().ToString('N')
$originalPolicy=$null
try {
  $originalPolicy=Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy'
  $restrictedAllow=@($originalPolicy.tool_allowlist | Where-Object { $_ -ne 'phase6.dangerous_echo' })
  $restrictedDangerous=@($originalPolicy.dangerous_tools | Where-Object { $_ -ne 'phase6.dangerous_echo' })
  if ($restrictedAllow.Count -ne @($originalPolicy.tool_allowlist).Count -or $restrictedDangerous.Count -ne @($originalPolicy.dangerous_tools).Count) {
    $restrictedBody=@{actor_allowlist_hashes=@($originalPolicy.actor_allowlist_hashes);tool_allowlist=$restrictedAllow;dangerous_tools=$restrictedDangerous;redaction_patterns=@($originalPolicy.redaction_patterns);expected_revision=$originalPolicy.revision}
    Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy' 'PUT' $restrictedBody | Out-Null
    Start-Sleep -Seconds 2
  }
  Set-P7Mock mock-model '/control/reset'; Set-P7Mock mock-model '/control/tool-call?enabled=true'
  $rejected=Submit-P7Message 'demo-redis' 'run dangerous tool' -ConversationID $conversation
  $rejectedTask=Wait-P7Task 'demo-redis' $rejected @('failed_terminal') 60
  if ($rejectedTask.error_code -ne 'tool_rejected') { throw "expected tool_rejected, got $($rejectedTask.error_code)" }
  $policy=Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy'
  $allow=@($policy.tool_allowlist | Where-Object { $_ -ne 'phase6.dangerous_echo' }) + 'phase6.dangerous_echo'
  $body=@{actor_allowlist_hashes=@($policy.actor_allowlist_hashes);tool_allowlist=$allow;dangerous_tools=@('phase6.dangerous_echo');redaction_patterns=@($policy.redaction_patterns);expected_revision=$policy.revision}
  Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy' 'PUT' $body | Out-Null
  $nonce=''
  for ($i=0; $i -lt 5 -and -not $nonce; $i++) {
    $request=Submit-P7Message 'demo-redis' 'run dangerous tool' -ConversationID $conversation
    $task=Wait-P7Task 'demo-redis' $request @('succeeded','failed_terminal') 60
    if ($task.state -eq 'succeeded') { $snapshot=Get-P7WebSnapshot 'demo-redis' $request; if ($snapshot.text -match 'confirm ([A-Fa-f0-9]+)') { $nonce=$Matches[1] } }
    Start-Sleep -Seconds 1
  }
  if (-not $nonce) { throw 'confirmation nonce was not returned in the Agent reply' }
  $confirm=Submit-P7Message 'demo-redis' ("confirm $nonce") -ConversationID $conversation
  Wait-P7Task 'demo-redis' $confirm @('succeeded') 60 | Out-Null
  $approved=Submit-P7Message 'demo-redis' 'run dangerous tool' -ConversationID $conversation
  Wait-P7Task 'demo-redis' $approved @('succeeded') 60 | Out-Null
  $reuse=Submit-P7Message 'demo-redis' ("confirm $nonce") -ConversationID $conversation
  $reuseTask=Wait-P7Task 'demo-redis' $reuse @('failed_terminal') 60
  if ($reuseTask.error_code -ne 'confirmation_denied') { throw "old nonce reuse was not denied: $($reuseTask.error_code)" }
} finally {
  try { Set-P7Mock mock-model '/control/reset' } finally {
    try {
      if ($null -ne $originalPolicy) {
        $currentPolicy=Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy'
        $restoreBody=@{actor_allowlist_hashes=@($originalPolicy.actor_allowlist_hashes);tool_allowlist=@($originalPolicy.tool_allowlist);dangerous_tools=@($originalPolicy.dangerous_tools);redaction_patterns=@($originalPolicy.redaction_patterns);expected_revision=$currentPolicy.revision}
        Invoke-P7Admin '/api/v1/admin/tenants/tenant-redis/policy' 'PUT' $restoreBody | Out-Null
      }
    } finally { Save-P7Evidence 'F09-tool-governance' }
  }
}
