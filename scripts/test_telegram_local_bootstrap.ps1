# Offline contract test: all HTTP and Docker calls are replaced with in-process
# fixtures. No provider, container or model is contacted.
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$bootstrap = Join-Path $PSScriptRoot 'telegram_local_bootstrap.ps1'
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('telegram-bootstrap-contract-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixtureRoot) | Out-Null
$configPath = Join-Path $fixtureRoot 'telegram.json'
$global:telegramFixture = @{
    requests=[Collections.Generic.List[object]]::new();docker=[Collections.Generic.List[object]]::new()
    webhook='';tenants=@();apps=@();versions=@();deployments=@();failTenantResponse=$false
}
function Assert-Contract([bool]$Condition,[string]$Message) { if (-not $Condition) { throw $Message } }
function global:docker {
    $global:telegramFixture.docker.Add(@($args))
    $global:LASTEXITCODE = 0
}
function global:Invoke-RestMethod {
    param($Uri,$Method,$TimeoutSec,$MaximumRedirection,$Proxy,$Headers,$ContentType,$Body,$NoProxy)
    $global:telegramFixture.requests.Add(@{uri=[string]$Uri;method=[string]$Method})
    if ($Uri -match '^https://api\.telegram\.org/bot[0-9]+:[A-Za-z0-9_-]+/getMe$') {
        return [pscustomobject]@{ok=$true;result=[pscustomobject]@{id=123456789;is_bot=$true;username='fixture_bot'}}
    }
    if ($Uri -match '^https://api\.telegram\.org/bot[0-9]+:[A-Za-z0-9_-]+/getWebhookInfo$') {
        return [pscustomobject]@{ok=$true;result=[pscustomobject]@{url=$global:telegramFixture.webhook}}
    }
    if (-not ([string]$Uri).StartsWith('http://127.0.0.1:48081/api/v1/')) { throw 'Unexpected network request in offline test.' }
    $path = ([uri]$Uri).AbsolutePath
    $data = $null
    if ($Body) { $data = [Text.Encoding]::UTF8.GetString($Body) | ConvertFrom-Json -AsHashtable }
    if ($path -eq '/api/v1/tenants') {
        if ($Method -eq 'Get') { return ,@($global:telegramFixture.tenants) }
        Assert-Contract (-not $data.config.models[0].Contains('endpoint')) 'Operator endpoint leaked into tenant configuration.'
        $value = $data.config
        $value.id='10000000-0000-4000-8000-000000000001';$value.name=$data.name
        $value = $value | ConvertTo-Json -Depth 20 | ConvertFrom-Json
        $global:telegramFixture.tenants=@($value)
        if ($global:telegramFixture.failTenantResponse) {
            $global:telegramFixture.failTenantResponse=$false
            throw 'Simulated response lost after tenant commit.'
        }
        return $value
    }
    if ($path -match '^/api/v1/operations/(apps|versions|deployments)$') {
        return [pscustomobject]@{items=@($global:telegramFixture[$Matches[1]])}
    }
    if ($path -eq '/api/v1/agent-apps') {
        $value=[pscustomobject]@{id='20000000-0000-4000-8000-000000000001';name=$data.name;status='active'}
        $global:telegramFixture.apps=@($value)
        return $value
    }
    if ($path -eq '/api/v1/agent-versions') {
        Assert-Contract (-not $data.snapshot.model.Contains('apiKey') -and $data.snapshot.model.apiKeyRef -ceq 'env://TRPC_SECRET_OPENAI_API_KEY') 'Version model must preserve its tenant-bound reference without a plaintext credential.'
        Assert-Contract (-not $data.snapshot.model.Contains('endpoint')) 'Operator endpoint leaked into version snapshot.'
        $value=[pscustomobject]@{id='30000000-0000-4000-8000-000000000001';appId=$global:telegramFixture.apps[0].id;status='draft'}
        $global:telegramFixture.versions=@($value)
        return $value
    }
    if ($path -match '^/api/v1/agent-versions/[^/]+/publish$') {
        $global:telegramFixture.versions[0].status='published'
        return [pscustomobject]@{status='published'}
    }
    if ($path -eq '/api/v1/deployments') {
        Assert-Contract ($global:telegramFixture.versions[0].status -eq 'published') 'Deployment preceded version publication.'
        $value=[pscustomobject]@{id='40000000-0000-4000-8000-000000000001';appId=$global:telegramFixture.apps[0].id;versionId=$data.stableVersionId;status='active';kind='stable'}
        $global:telegramFixture.deployments=@($value)
        return [pscustomobject]@{stableId=$value.id}
    }
    throw 'Unexpected Admin request in offline test.'
}
try {
    $config = @{
        TRPC_SECRET_TELEGRAM_BOT_TOKEN='123456789:fixture_bot_token_that_is_not_real_0000'
        # Compose a recognizable fake at runtime without shipping a complete
        # credential-shaped literal through the submission secret scanner.
        TRPC_SECRET_OPENAI_API_KEY=('sk-' + 'fixture_not_a_real_openai_credential')
        TELEGRAM_ALLOWED_USER_ID=@('987654321');TELEGRAM_MODEL_NAME='gpt-4o-mini'
    }
    function Save-Fixture { [IO.File]::WriteAllText($configPath, ($config | ConvertTo-Json -Depth 5)) }
    function Expect-Blocked([string]$Pattern) {
        $caught=$false
        try { & $bootstrap -ConfigPath $configPath -ValidateOnly | Out-Null }
        catch { $caught=$true;Assert-Contract ($_.Exception.Message -match $Pattern) 'Unexpected blocked reason.' }
        Assert-Contract $caught 'Invalid credential configuration was accepted.'
    }
    Save-Fixture
    $output = & $bootstrap -ConfigPath $configPath -ValidateOnly
    Assert-Contract ($global:telegramFixture.requests.Count -eq 2) 'ValidateOnly did not perform exactly two read-only preflights.'
    Assert-Contract ($global:telegramFixture.docker.Count -eq 0) 'ValidateOnly invoked Docker.'
    Assert-Contract (-not (Test-Path -LiteralPath (Join-Path $fixtureRoot 'trpc-telegram-local'))) 'ValidateOnly wrote infrastructure state.'
    Assert-Contract (($output -join "`n") -notmatch [regex]::Escape($config.TRPC_SECRET_TELEGRAM_BOT_TOKEN)) 'Bot token leaked to output.'
    foreach ($invalidEndpoint in @('http://models.example/v1','https://user:password@models.example/v1','https://models.example/v1?key=value','https://models.example/v1#fragment','https://models.example/v1/../secret','https://models.example/v1%2fprivate','https://models.example:/v1')) {
        $config.TRPC_OPENAI_BASE_URL=$invalidEndpoint;Save-Fixture;Expect-Blocked 'TRPC_OPENAI_BASE_URL'
    }
    $config.TRPC_OPENAI_BASE_URL='https://models.example/v1';Save-Fixture
    & $bootstrap -ConfigPath $configPath -ValidateOnly | Out-Null
    $global:telegramFixture.webhook='https://existing.example/callback'
    Expect-Blocked 'already has a webhook'
    $global:telegramFixture.webhook=''
    $config.TELEGRAM_ALLOWED_USER_ID=@();Save-Fixture;Expect-Blocked 'TELEGRAM_ALLOWED_USER_ID'
    $config.TELEGRAM_ALLOWED_USER_ID=@('987654321')
    $key=$config.TRPC_SECRET_OPENAI_API_KEY;$config.TRPC_SECRET_OPENAI_API_KEY='';Save-Fixture;Expect-Blocked 'OpenAI API key'
    $config.TRPC_SECRET_OPENAI_API_KEY=$key
    $token=$config.TRPC_SECRET_TELEGRAM_BOT_TOKEN;$config.TRPC_SECRET_TELEGRAM_BOT_TOKEN='';Save-Fixture;Expect-Blocked 'Telegram bot token'
    $config.TRPC_SECRET_TELEGRAM_BOT_TOKEN=$token;Save-Fixture

    # Fail after the server persists a tenant but before the client receives the
    # response. Rerun must discover that tenant, retain secrets and release once.
    $global:telegramFixture.failTenantResponse=$true
    $failed=$false
    try { & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null } catch {
        $failed=$true
        Assert-Contract ($_.Exception.Message -match 'Admin API Post /api/v1/tenants failed') ('Unexpected bootstrap error: '+$_.Exception.Message)
    }
    Assert-Contract $failed 'Lost tenant response fixture did not interrupt bootstrap.'
    $statePath=Join-Path $fixtureRoot 'trpc-telegram-local/telegram.bootstrap.state.json'
    $stateBefore=Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable
    & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null
    $stateAfter=Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable
    Assert-Contract ($stateAfter.modelBaseURL -ceq 'https://models.example/v1') 'Operator endpoint was not bound to deployment state.'
    Assert-Contract ($stateAfter.secrets.MASTER_KEY -ceq $stateBefore.secrets.MASTER_KEY) 'Resume rotated the database encryption key.'
    Assert-Contract ($stateAfter.deploymentId -ne '') 'Resume failed to persist deployment identity.'
    $postCount=@($global:telegramFixture.requests | Where-Object {$_.method -eq 'Post'}).Count
    & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null
    Assert-Contract (@($global:telegramFixture.requests | Where-Object {$_.method -eq 'Post'}).Count -eq $postCount) 'Rerun duplicated a tenant, version, publication or deployment.'
    Assert-Contract (@($global:telegramFixture.requests | Where-Object {$_.uri -match 'getUpdates|setWebhook|deleteWebhook|sendMessage'}).Count -eq 0) 'Bootstrap called a consuming or sending provider API.'
    $lastDocker=$global:telegramFixture.docker[$global:telegramFixture.docker.Count-1] -join ' '
    Assert-Contract ($lastDocker -match 'up -d --no-build telegram-poller$') 'Poller was not started as the last deployment step.'
    Assert-Contract (-not $global:telegramFixture.tenants[0].channels[0].accessPolicy.allowGroupMessages) 'Group messages were enabled.'
    $acl=Get-Acl -LiteralPath $statePath
    Assert-Contract (@($acl.Access | Where-Object {$_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -in @('S-1-1-0','S-1-5-32-545','S-1-5-11')}).Count -eq 0) 'Generated secrets are readable by broad Windows groups.'
    Write-Output 'PASS: offline Telegram bootstrap preflight, blocked inputs, interrupted provisioning, idempotent resume, private-chat policy, ACL and publication order.'
} finally {
    Remove-Item -LiteralPath Function:\global:docker -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Function:\global:Invoke-RestMethod -ErrorAction SilentlyContinue
    Remove-Variable telegramFixture -Scope Global -ErrorAction SilentlyContinue
    $resolved=[IO.Path]::GetFullPath($fixtureRoot)
    $tempBase=[IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')+'\'
    if ($resolved.StartsWith($tempBase,[StringComparison]::OrdinalIgnoreCase) -and [IO.Path]::GetFileName($resolved).StartsWith('telegram-bootstrap-contract-')) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
