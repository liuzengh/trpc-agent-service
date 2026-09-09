param([string]$Url,[int]$Status=200,[int]$TimeoutSeconds=60)
$ErrorActionPreference = 'Stop'
$script:Phase7Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$script:Phase7EnvFile = Join-Path $script:Phase7Root 'compose\.env'
$script:Phase7Evidence = Join-Path $script:Phase7Root 'phase7-evidence'

function Get-P7EnvValue([string]$Name) {
  $value = [Environment]::GetEnvironmentVariable($Name)
  if (-not [string]::IsNullOrWhiteSpace($value)) { return $value }
  if (Test-Path $script:Phase7EnvFile) {
    $line = Get-Content -LiteralPath $script:Phase7EnvFile | Where-Object { $_ -match ('^' + [regex]::Escape($Name) + '=') } | Select-Object -Last 1
    if ($line) { return ($line -split '=',2)[1] }
  }
  return ''
}

function Invoke-P7Compose([string[]]$CommandArgs,[switch]$Obs,[switch]$HA) {
  $args = @('--project-name','trpc-phase7','--env-file','compose/.env','-f','compose/docker-compose.yml')
  if ($Obs) { $args += @('-f','compose/docker-compose.obs.yml') }
  if ($HA) { $args += @('-f','compose/docker-compose.ha.yml') }
  $args += $CommandArgs
  Push-Location $script:Phase7Root
  try { & docker compose @args; if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($CommandArgs -join ' ')" } } finally { Pop-Location }
}

function Wait-P7Http([string]$TargetUrl,[int]$Expected=200,[int]$Seconds=60,[hashtable]$Headers=@{}) {
  $deadline = (Get-Date).AddSeconds($Seconds)
  do {
    $actual = 0
    try { $response = Invoke-WebRequest -UseBasicParsing -Uri $TargetUrl -Method Get -Headers $Headers; $actual = [int]$response.StatusCode }
    catch { if ($_.Exception.Response) { $actual = [int]$_.Exception.Response.StatusCode } }
    if ($actual -eq $Expected) { return }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $deadline)
  throw "Expected HTTP $Expected from $TargetUrl; last status was $actual"
}

function Invoke-P7Admin([string]$Path,[string]$Method='GET',$Body=$null,[string]$BaseUrl='http://127.0.0.1:18080') {
  $token = Get-P7EnvValue 'PHASE7_ADMIN_TOKEN'
  $headers = @{Authorization="Bearer $token"}
  $params = @{Uri=($BaseUrl + $Path);Method=$Method;Headers=$headers;UseBasicParsing=$true}
  if ($null -ne $Body) { $params.ContentType='application/json'; $params.Body=($Body | ConvertTo-Json -Depth 20 -Compress) }
  $response = Invoke-WebRequest @params
  return ($response.Content | ConvertFrom-Json)
}

function Get-P7ErrorPayload($ErrorRecord) {
  $body = $ErrorRecord.ErrorDetails.Message
  if ([string]::IsNullOrWhiteSpace($body) -and $ErrorRecord.Exception.Response) {
    $reader = New-Object System.IO.StreamReader($ErrorRecord.Exception.Response.GetResponseStream())
    $body = $reader.ReadToEnd()
  }
  if ([string]::IsNullOrWhiteSpace($body)) { throw 'HTTP error response body was empty' }
  return ($body | ConvertFrom-Json)
}

function Test-P7AdminError([string]$Path,[string]$ExpectedCode) {
  $token=Get-P7EnvValue 'PHASE7_ADMIN_TOKEN'
  try { Invoke-WebRequest -UseBasicParsing -Uri ('http://127.0.0.1:18080'+$Path) -Headers @{Authorization="Bearer $token"} | Out-Null; throw "Admin unexpectedly succeeded; expected $ExpectedCode" }
  catch {
    if (-not $_.Exception.Response) { throw }
    $payload=Get-P7ErrorPayload $_
    if ($payload.code -ne $ExpectedCode) { throw "expected $ExpectedCode, got $($payload.code)" }
  }
}

function Submit-P7Message([string]$BindingID='demo-redis',[string]$Text='phase7 fault probe',[string]$MessageID='',[string]$ConversationID='') {
	if (-not $MessageID) { $MessageID = 'phase7-' + [guid]::NewGuid().ToString('N') }
	if (-not $ConversationID) { $ConversationID = 'conversation-' + $MessageID }
	$body = @{binding_id=$BindingID;message_id=$MessageID;external_user_id='phase7-user';conversation_id=$ConversationID;text=$Text} | ConvertTo-Json -Compress
  Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18080/api/v1/web/messages' -ContentType 'application/json' -Body $body | Out-Null
  return $MessageID
}

function Test-P7SubmitError([string]$BindingID,[string]$ExpectedCode) {
  $message='phase7-'+[guid]::NewGuid().ToString('N')
  $body=@{binding_id=$BindingID;message_id=$message;external_user_id='phase7-user';conversation_id=('conversation-'+$message);text='failure probe'} | ConvertTo-Json -Compress
  try { Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://127.0.0.1:18080/api/v1/web/messages' -ContentType 'application/json' -Body $body | Out-Null; throw "request unexpectedly succeeded; expected $ExpectedCode" }
  catch {
    if (-not $_.Exception.Response) { throw }
    $payload=Get-P7ErrorPayload $_
    if ($payload.code -ne $ExpectedCode) { throw "expected $ExpectedCode, got $($payload.code)" }
  }
}

function Get-P7WebSnapshot([string]$BindingID,[string]$MessageID) {
  return Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:18080/api/v1/web/messages/$MessageID`?binding_id=$BindingID"
}

function Get-P7Task([string]$BindingID,[string]$MessageID,[string]$BaseUrl='http://127.0.0.1:18080') {
  return Invoke-P7Admin "/api/v1/admin/tasks/$BindingID/$MessageID" -BaseUrl $BaseUrl
}

function Wait-P7Task([string]$BindingID,[string]$MessageID,[string[]]$States,[int]$Seconds=90,[string]$BaseUrl='http://127.0.0.1:18080') {
  $deadline=(Get-Date).AddSeconds($Seconds); $last=$null
  do { try { $last=Get-P7Task $BindingID $MessageID $BaseUrl; if ($States -contains $last.state) { return $last } } catch {}; Start-Sleep -Milliseconds 500 } while ((Get-Date) -lt $deadline)
  throw "Task $BindingID/$MessageID did not reach [$($States -join ',')]; last=$($last | ConvertTo-Json -Compress)"
}

function Invoke-P7Mock([ValidateSet('mock-model','mock-telegram')][string]$Service,[string]$Path,[string]$Body='') {
  $port = if ($Service -eq 'mock-model') { 8090 } else { 8091 }
	if ($Body -and $Service -eq 'mock-telegram') {
		$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Body)).TrimEnd('=').Replace('+','-').Replace('/','_')
		$separator = if ($Path.Contains('?')) { '&' } else { '?' }
		$Path = $Path + $separator + 'body_b64=' + $encoded
	}
	$wget = @('-qO-', "http://127.0.0.1:$port$Path")
	if ($Body -and $Service -ne 'mock-telegram') { $wget = @('-qO-','--header=Content-Type: application/json','--post-data', $Body, "http://127.0.0.1:$port$Path") }
	$commandArgs = @('exec','-T',$Service,'wget') + $wget
	$output = Invoke-P7Compose -CommandArgs $commandArgs
  return $output
}

function Get-P7MockStats([ValidateSet('mock-model','mock-telegram')][string]$Service) {
  $port = if ($Service -eq 'mock-model') { 8090 } else { 8091 }
  Push-Location $script:Phase7Root
  try {
    $output = & docker compose --project-name trpc-phase7 --env-file compose/.env -f compose/docker-compose.yml exec -T $Service wget -qO- "http://127.0.0.1:$port/control/stats"
    if ($LASTEXITCODE -ne 0) { throw "could not query $Service stats" }
    return ($output | ConvertFrom-Json)
  } finally { Pop-Location }
}

function Set-P7Mock([ValidateSet('mock-model','mock-telegram')][string]$Service,[string]$Path) {
  $port = if ($Service -eq 'mock-model') { 8090 } else { 8091 }
	Invoke-P7Compose -CommandArgs @('exec','-T',$Service,'wget','-qO-',"http://127.0.0.1:$port$Path") | Out-Null
}

function Save-P7Evidence([string]$Name) {
  New-Item -ItemType Directory -Force -Path $script:Phase7Evidence | Out-Null
  $path = Join-Path $script:Phase7Evidence ($Name + '.log')
  Push-Location $script:Phase7Root
  try { & docker compose --project-name trpc-phase7 --env-file compose/.env -f compose/docker-compose.yml logs --no-color 2>&1 | Set-Content -LiteralPath $path -Encoding utf8 } finally { Pop-Location }
}

if ($Url) { Wait-P7Http -TargetUrl $Url -Expected $Status -Seconds $TimeoutSeconds }
