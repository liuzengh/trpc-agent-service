. (Join-Path $PSScriptRoot '..\assert.ps1')
$id = [int]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() % 100000000)
try {
  Set-P7Mock mock-telegram '/control/reset'
  $update = @{update_id=$id;message=@{message_id=$id;date=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds();chat=@{id=7001;type='private'};from=@{id=8001;is_bot=$false;first_name='phase7'};text='duplicate probe'}} | ConvertTo-Json -Depth 8 -Compress
  Invoke-P7Mock mock-telegram '/control/repeat' $update | Out-Null
  $task = Wait-P7Task 'telegram-mock' ([string]$id) @('succeeded') 90
  $deadline=(Get-Date).AddSeconds(30); do { $stats=Get-P7MockStats mock-telegram; if ($stats.sent -eq 1) { break }; Start-Sleep -Milliseconds 500 } while ((Get-Date) -lt $deadline)
  if ($stats.sent -ne 1 -or $task.attempt -ne 1) { throw "duplicate was not collapsed: $($stats | ConvertTo-Json -Compress) task=$($task | ConvertTo-Json -Compress)" }
} finally { Save-P7Evidence 'F01-duplicate' }
