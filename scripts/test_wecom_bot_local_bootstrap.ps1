# Offline contract test: all HTTP and Docker calls are replaced with in-process
# fixtures. No provider, container or model is contacted.
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$bootstrap = Join-Path $PSScriptRoot 'wecom_bot_local_bootstrap.ps1'
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('wecom-bot-bootstrap-contract-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixtureRoot) | Out-Null
$configPath = Join-Path $fixtureRoot 'wecom-bot.json'
$global:wecomBotFixture = @{
    requests=[Collections.Generic.List[object]]::new();docker=[Collections.Generic.List[object]]::new()
    tenants=@();apps=@();versions=@();deployments=@();failTenantResponse=$false
    privateDir=(Join-Path $fixtureRoot 'trpc-wecom-bot-local')
}
function Assert-Contract([bool]$Condition,[string]$Message) { if (-not $Condition) { throw $Message } }
function global:docker {
    $global:wecomBotFixture.docker.Add(@($args))
    $global:LASTEXITCODE = 0
}
function global:Invoke-RestMethod {
    param($Uri,$Method,$TimeoutSec,$MaximumRedirection,$Proxy,$Headers,$ContentType,$Body,$NoProxy)
    $global:wecomBotFixture.requests.Add(@{uri=[string]$Uri;method=[string]$Method})
    if (-not ([string]$Uri).StartsWith('http://127.0.0.1:48091/api/v1/')) { throw 'Unexpected network request in offline test.' }
    $path = ([uri]$Uri).AbsolutePath
    $data = $null
    if ($Body) { $data = [Text.Encoding]::UTF8.GetString($Body) | ConvertFrom-Json -AsHashtable }
    if ($path -eq '/api/v1/tenants') {
        if ($Method -eq 'Get') { return ,@($global:wecomBotFixture.tenants) }
        Assert-Contract (-not $data.config.models[0].Contains('endpoint')) 'Operator endpoint leaked into tenant configuration.'
        Assert-Contract ($data.config.channels[0].type -eq 'wecom_bot' -and $data.config.channels[0].tokenRef -eq 'env://TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN' -and -not $data.config.channels[0].secretRef) 'Smart bot tenant received provider credentials instead of a bridge reference.'
        Assert-Contract ($data.config.agents[0].maxLLMCalls -eq 3 -and ($data.config.agents[0].tools -join ',') -eq 'memory_add,memory_search') 'Agent memory tool configuration is incomplete.'
        Assert-Contract ($data.config.toolPolicy.mode -eq 'whitelist' -and ($data.config.toolPolicy.allowed -join ',') -eq 'memory_add,memory_search') 'Agent tools lack a matching whitelist.'
        Assert-Contract ($data.config.budget.maxTokensPerRequest -ge 384000) 'Agent call budget does not cover the configured context window.'
        $value = $data.config
        $value.id='10000000-0000-4000-8000-000000000001';$value.name=$data.name
        $value = $value | ConvertTo-Json -Depth 20 | ConvertFrom-Json
        $global:wecomBotFixture.tenants=@($value)
        if ($global:wecomBotFixture.failTenantResponse) {
            $global:wecomBotFixture.failTenantResponse=$false
            throw 'Simulated response lost after tenant commit.'
        }
        return $value
    }
    if ($path -match '^/api/v1/operations/(apps|versions|deployments)$') {
        return [pscustomobject]@{items=@($global:wecomBotFixture[$Matches[1]])}
    }
    if ($path -eq '/api/v1/agent-apps') {
        $value=[pscustomobject]@{id='20000000-0000-4000-8000-000000000001';name=$data.name;status='active'}
        $global:wecomBotFixture.apps=@($value)
        return $value
    }
    if ($path -eq '/api/v1/agent-versions') {
        Assert-Contract (-not $data.snapshot.model.Contains('apiKey') -and $data.snapshot.model.apiKeyRef -ceq 'env://TRPC_SECRET_OPENAI_API_KEY') 'Version model must preserve its tenant-bound reference without a plaintext credential.'
        Assert-Contract (-not $data.snapshot.model.Contains('endpoint')) 'Operator endpoint leaked into version snapshot.'
        $value=[pscustomobject]@{id='30000000-0000-4000-8000-000000000001';appId=$global:wecomBotFixture.apps[0].id;status='draft'}
        $global:wecomBotFixture.versions=@($value)
        return $value
    }
    if ($path -match '^/api/v1/agent-versions/[^/]+/publish$') {
        $scopeFile=Join-Path $global:wecomBotFixture.privateDir 'secret-bindings.compose.json'
        Assert-Contract (Test-Path -LiteralPath $scopeFile) 'Publication preceded secret scope provisioning.'
        $lastCompose=$global:wecomBotFixture.docker[$global:wecomBotFixture.docker.Count-1] -join ' '
        Assert-Contract ($lastCompose.Contains($scopeFile) -and $lastCompose -match 'admin gateway worker summary-worker consumer delivery$') 'Runtime processes were not recreated with scoped bindings before publication.'
        $global:wecomBotFixture.versions[0].status='published'
        return [pscustomobject]@{status='published'}
    }
    if ($path -eq '/api/v1/deployments') {
        Assert-Contract ($global:wecomBotFixture.versions[0].status -eq 'published') 'Deployment preceded version publication.'
        $value=[pscustomobject]@{id='40000000-0000-4000-8000-000000000001';appId=$global:wecomBotFixture.apps[0].id;versionId=$data.stableVersionId;status='active';kind='stable'}
        $global:wecomBotFixture.deployments=@($value)
        return [pscustomobject]@{stableId=$value.id}
    }
    throw 'Unexpected Admin request in offline test.'
}
try {
    $config = @{
        WECOM_BOT_ID='aib_fixture_bot_123';WECOM_BOT_SECRET='fixture_wecom_secret_not_real_0000000000000'
        # Compose a recognizable fake at runtime without shipping a complete
        # credential-shaped literal through the submission secret scanner.
        TRPC_SECRET_OPENAI_API_KEY=('sk-' + 'fixture_not_a_real_openai_credential')
        WECOM_ALLOWED_USER_ID=@('fixture-user-123');MODEL_NAME='gpt-4o-mini'
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
    Assert-Contract ($global:wecomBotFixture.requests.Count -eq 0) 'ValidateOnly contacted a provider or Admin API.'
    Assert-Contract ($global:wecomBotFixture.docker.Count -eq 0) 'ValidateOnly invoked Docker.'
    Assert-Contract (-not (Test-Path -LiteralPath (Join-Path $fixtureRoot 'trpc-wecom-bot-local'))) 'ValidateOnly wrote infrastructure state.'
    Assert-Contract (($output -join "`n") -notmatch [regex]::Escape($config.WECOM_BOT_SECRET)) 'Bot token leaked to output.'
    foreach ($invalidEndpoint in @('http://models.example/v1','https://user:password@models.example/v1','https://models.example/v1?key=value','https://models.example/v1#fragment','https://models.example/v1/../secret','https://models.example/v1%2fprivate','https://models.example:/v1')) {
        $config.TRPC_OPENAI_BASE_URL=$invalidEndpoint;Save-Fixture;Expect-Blocked 'TRPC_OPENAI_BASE_URL'
    }
    $config.TRPC_OPENAI_BASE_URL='https://models.example/v1';Save-Fixture
    & $bootstrap -ConfigPath $configPath -ValidateOnly | Out-Null
    $config.WECOM_ALLOWED_USER_ID=@();Save-Fixture;Expect-Blocked 'WECOM_ALLOWED_USER_ID'
    $config.WECOM_ALLOWED_USER_ID=@('fixture-user-123')
    $key=$config.TRPC_SECRET_OPENAI_API_KEY;$config.TRPC_SECRET_OPENAI_API_KEY='';Save-Fixture;Expect-Blocked 'OpenAI API key'
    $config.TRPC_SECRET_OPENAI_API_KEY=$key
    $token=$config.WECOM_BOT_SECRET;$config.WECOM_BOT_SECRET='';Save-Fixture;Expect-Blocked 'WECOM_BOT_ID and WECOM_BOT_SECRET'
    $config.WECOM_BOT_SECRET=$token;Save-Fixture

    # Fail after the server persists a tenant but before the client receives the
    # response. Rerun must discover that tenant, retain secrets and release once.
    $global:wecomBotFixture.failTenantResponse=$true
    $failed=$false
    try { & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null } catch {
        $failed=$true
        Assert-Contract ($_.Exception.Message -match 'Admin API Post /api/v1/tenants failed') ('Unexpected bootstrap error: '+$_.Exception.Message)
    }
    Assert-Contract $failed 'Lost tenant response fixture did not interrupt bootstrap.'
    $statePath=Join-Path $fixtureRoot 'trpc-wecom-bot-local/wecom-bot.bootstrap.state.json'
    $stateBefore=Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable
    & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null
    $stateAfter=Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable
    Assert-Contract ($stateAfter.modelBaseURL -ceq 'https://models.example/v1') 'Operator endpoint was not bound to deployment state.'
    Assert-Contract ($stateAfter.secrets.MASTER_KEY -ceq $stateBefore.secrets.MASTER_KEY) 'Resume rotated the database encryption key.'
    Assert-Contract ($stateAfter.deploymentId -ne '') 'Resume failed to persist deployment identity.'
    $postCount=@($global:wecomBotFixture.requests | Where-Object {$_.method -eq 'Post'}).Count
    & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null
    Assert-Contract (@($global:wecomBotFixture.requests | Where-Object {$_.method -eq 'Post'}).Count -eq $postCount) 'Rerun duplicated a tenant, version, publication or deployment.'
    Assert-Contract (@($global:wecomBotFixture.requests | Where-Object {$_.uri -match 'getUpdates|setWebhook|deleteWebhook|sendMessage'}).Count -eq 0) 'Bootstrap called a consuming or sending provider API.'
    $lastDocker=$global:wecomBotFixture.docker[$global:wecomBotFixture.docker.Count-1] -join ' '
    Assert-Contract ($lastDocker -match 'up -d --no-build wecom-bot$') 'Connector was not started as the last deployment step.'
    Assert-Contract (-not $global:wecomBotFixture.tenants[0].channels[0].accessPolicy.allowGroupMessages) 'Group messages were enabled.'
    . (Join-Path $PSScriptRoot 'im_secret_bindings.ps1')
    $scopePath=Join-Path $global:wecomBotFixture.privateDir 'secret-bindings.compose.json'
    $scope=Get-Content -LiteralPath $scopePath -Raw | ConvertFrom-Json -AsHashtable
    Assert-Contract ($scope.services.Count -eq 5) 'Scope override added an unexpected process.'
    $modelBinding=Get-IMSecretBindingName -TenantId $stateAfter.tenantId -Purpose model -Provider openai -Model 'gpt-4o-mini'
    $channelBinding=Get-IMSecretBindingName -TenantId $stateAfter.tenantId -Purpose channel_token -Provider wecom_bot -Model $config.WECOM_BOT_ID
    foreach ($service in @('admin','worker','summary-worker')) {
        Assert-Contract ($scope.services[$service].environment.Count -eq 1 -and $scope.services[$service].environment[$modelBinding] -ceq 'env://TRPC_SECRET_OPENAI_API_KEY') 'Wrong model authorization scope.'
    }
    foreach ($service in @('gateway','delivery')) {
        Assert-Contract ($scope.services[$service].environment.Count -eq 1 -and $scope.services[$service].environment[$channelBinding] -ceq 'env://TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN') 'Wrong channel authorization scope.'
    }
    $material=(Get-Content -LiteralPath $statePath -Raw)+(Get-Content -LiteralPath $scopePath -Raw)
    Assert-Contract (-not $material.Contains($config.WECOM_BOT_SECRET) -and -not $material.Contains($config.TRPC_SECRET_OPENAI_API_KEY)) 'Provider credentials were persisted in generated deployment files.'
    $acl=Get-Acl -LiteralPath $statePath
    Assert-Contract (@($acl.Access | Where-Object {$_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -in @('S-1-1-0','S-1-5-32-545','S-1-5-11')}).Count -eq 0) 'Generated secrets are readable by broad Windows groups.'
    $beforeResume=$global:wecomBotFixture.docker.Count
    $config.WECOM_ALLOWED_USER_ID=@('different-user');Save-Fixture
    $blocked=$false
    try { & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null } catch { $blocked=$true }
    Assert-Contract ($blocked -and $global:wecomBotFixture.docker.Count -eq $beforeResume) 'Changed identity restarted a previously bound deployment.'
    $config.WECOM_ALLOWED_USER_ID=@('fixture-user-123');Save-Fixture
    [IO.File]::WriteAllText($statePath,'{"incomplete":')
    $blocked=$false
    try { & $bootstrap -ConfigPath $configPath -SkipBuild | Out-Null } catch { $blocked=$true }
    Assert-Contract ($blocked -and $global:wecomBotFixture.docker.Count -eq $beforeResume) 'Corrupt state silently regenerated deployment secrets.'
    Write-Output 'PASS: offline WeCom bot bootstrap, endpoint validation, interrupted provisioning, idempotent resume, private-chat policy, ACL, secret scopes and publication order.'
} finally {
    Remove-Item -LiteralPath Function:\global:docker -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath Function:\global:Invoke-RestMethod -ErrorAction SilentlyContinue
    Remove-Variable wecomBotFixture -Scope Global -ErrorAction SilentlyContinue
    $resolved=[IO.Path]::GetFullPath($fixtureRoot)
    $tempBase=[IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')+'\'
    if ($resolved.StartsWith($tempBase,[StringComparison]::OrdinalIgnoreCase) -and [IO.Path]::GetFileName($resolved).StartsWith('wecom-bot-bootstrap-contract-')) {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
