. (Join-Path $PSScriptRoot '..\assert.ps1')
$id=[int]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() % 100000000)
try {
  Set-P7Mock mock-telegram '/control/reset'; Set-P7Mock mock-telegram '/control/send-status?code=500'
  $update=@{update_id=$id;message=@{message_id=$id;date=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds();chat=@{id=7002;type='private'};from=@{id=8002;is_bot=$false;first_name='phase7'};text='outbound retry probe'}} | ConvertTo-Json -Depth 8 -Compress
  Invoke-P7Mock mock-telegram '/control/inject' $update | Out-Null
  $task=Wait-P7Task 'telegram-mock' ([string]$id) @('succeeded') 60
  $deadline=(Get-Date).AddSeconds(30); do { try { $outbound=Invoke-P7Admin "/api/v1/admin/outbound/$($task.task_id)"; if ($outbound.state -eq 'retry_wait') { break } } catch {}; Start-Sleep -Milliseconds 250 } while ((Get-Date) -lt $deadline)
  if ($outbound.error_code -ne 'send_failed') { throw "outbound did not enter send_failed: $($outbound | ConvertTo-Json -Compress)" }
  $before=(Get-P7MockStats mock-telegram).sent
  Set-P7Mock mock-telegram '/control/send-status?code=200'
  $deadline=(Get-Date).AddSeconds(30); do { $outbound=Invoke-P7Admin "/api/v1/admin/outbound/$($task.task_id)"; if ($outbound.state -eq 'succeeded') { break }; Start-Sleep -Milliseconds 250 } while ((Get-Date) -lt $deadline)
  $after=(Get-P7MockStats mock-telegram).sent
  if ($outbound.state -ne 'succeeded' -or ($after-$before) -ne 1) { throw "outbound recovery was not exactly once: before=$before after=$after" }
} finally { Set-P7Mock mock-telegram '/control/reset'; Save-P7Evidence 'F10-im-send' }
