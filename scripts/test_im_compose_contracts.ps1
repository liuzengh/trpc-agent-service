# Render actual Compose configuration with fixture values. `compose config`
# is local parsing: this script never contacts an Engine or an IM provider.
[CmdletBinding()]
param([string]$TestRoot = [IO.Path]::GetTempPath())
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$dockerCommand=Get-Command docker -CommandType Application -ErrorAction Stop | Select-Object -First 1
$fixture=Join-Path ([IO.Path]::GetFullPath($TestRoot)) ('im-compose-contract-'+[Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixture) | Out-Null
$saved=@{}
$values=@{
    POSTGRES_PASSWORD='fixture-postgres';MASTER_KEY=('a'*64);SERVICE_AUTH_SECRET=('b'*64)
    ADMIN_API_TOKEN=('c'*64);AUDIT_IDENTITY_HMAC_KEY=('d'*64);METRICS_AUTH_TOKEN=('e'*64);GRAFANA_PASSWORD='fixture-grafana'
    TRPC_SECRET_OPENAI_API_KEY='fixture_model_key_not_real';TRPC_OPENAI_BASE_URL='https://models.example/v1'
    TRPC_SECRET_TELEGRAM_BOT_TOKEN='123456789:fixture_bot_token_that_is_not_real_0000';TRPC_SECRET_TELEGRAM_WEBHOOK=('f'*64)
    TELEGRAM_EXPECTED_BOT_ID='123456789';TELEGRAM_WEBHOOK_ROUTE_KEY='fixture-telegram-route'
    WECOM_BOT_ID='aib_fixture';WECOM_BOT_SECRET='fixture_bot_secret_not_real';TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN=('1'*64);WECOM_BOT_WEBHOOK_ROUTE_KEY='fixture-wecom-route'
    ADMIN_HOST_PORT='48091';GATEWAY_HOST_PORT='48090';POSTGRES_HOST_PORT='64542';OTEL_GRPC_HOST_PORT='64337';OTEL_HTTP_HOST_PORT='64338'
    TELEGRAM_ADMIN_PORT='48081';TELEGRAM_GATEWAY_PORT='48080';TELEGRAM_POSTGRES_PORT='64532';TELEGRAM_OTLP_GRPC_PORT='64327';TELEGRAM_OTLP_HTTP_PORT='64328'
    CONTAINER_HTTPS_PROXY='';DATA_PLANE_PROFILES='[]';MCP_PROFILES='[]'
}
function Assert-Compose([bool]$Condition,[string]$Message) { if (-not $Condition) { throw $Message } }
try {
    foreach ($entry in $values.GetEnumerator()) {
        $saved[$entry.Key]=[Environment]::GetEnvironmentVariable($entry.Key,'Process')
        [Environment]::SetEnvironmentVariable($entry.Key,[string]$entry.Value,'Process')
    }
    . (Join-Path $PSScriptRoot 'im_secret_bindings.ps1')
    $emptyEnv=Join-Path $fixture 'empty.env'
    [IO.File]::WriteAllText($emptyEnv,'')
    foreach ($channelType in @('telegram','wecom_bot')) {
        $profile=if ($channelType -eq 'telegram') {'telegram'} else {'wecom-bot'}
        $connector=if ($channelType -eq 'telegram') {'telegram-poller'} else {'wecom-bot'}
        if ($channelType -eq 'wecom_bot') {
            # Match the smart-bot bootstrap, which clears unrelated IM values.
            $env:TRPC_SECRET_TELEGRAM_BOT_TOKEN=''
            $env:TRPC_SECRET_TELEGRAM_WEBHOOK=''
        }
        $account=if ($channelType -eq 'telegram') {'123456789'} else {'aib_fixture'}
        $tokenRef=if ($channelType -eq 'telegram') {'env://TRPC_SECRET_TELEGRAM_BOT_TOKEN'} else {'env://TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN'}
        $secretRef=if ($channelType -eq 'telegram') {'env://TRPC_SECRET_TELEGRAM_WEBHOOK'} else {''}
        $definition=New-IMSecretBindingsOverride -TenantId 'fixture-tenant' -ModelProvider openai -ModelName 'gpt-4o-mini' -ModelSecretRef 'env://TRPC_SECRET_OPENAI_API_KEY' -ChannelType $channelType -ChannelAccountId $account -ChannelTokenRef $tokenRef -ChannelSecretRef $secretRef
        $scopePath=Join-Path $fixture ($channelType+'.json')
        Write-IMSecretBindingsOverride -Path $scopePath -Definition $definition | Out-Null
        $overlay=Join-Path $repo ('deploy/docker-compose.'+$profile+'.yml')
        $arguments=@('compose','--project-name',('trpc-im-contract-'+$profile),'--env-file',$emptyEnv,'-f',(Join-Path $repo 'deploy/docker-compose.yml'),'-f',$overlay,'-f',$scopePath,'--profile',$profile,'config','--format','json')
        $raw=@(& $dockerCommand.Source @arguments 2> (Join-Path $fixture ($profile+'.stderr')))
        if ($LASTEXITCODE -ne 0) { throw "Offline Compose config failed for $channelType; no container was started." }
        $rendered=($raw -join "`n") | ConvertFrom-Json -AsHashtable
        $serviceConfig=$rendered.services[$connector]
        Assert-Compose ($null -ne $serviceConfig) "$channelType connector is absent from the merged configuration."
        $tmpfs=@($serviceConfig['tmpfs'])
        Assert-Compose ($tmpfs.Count -eq 1 -and $tmpfs[0] -ceq '/tmp:size=16m,mode=1777') "$channelType connector tmpfs must be one complete mount string, including its comma-separated options."
        Assert-Compose ($serviceConfig['read_only'] -eq $true -and $serviceConfig['user'] -ceq '65532:65532') "$channelType connector lost its read-only non-root runtime."
        $capDrop=@($serviceConfig['cap_drop'])
        Assert-Compose ($capDrop.Count -eq 1 -and $capDrop[0] -ceq 'ALL') "$channelType connector must drop all capabilities."
        $securityOptions=@($serviceConfig['security_opt'])
        Assert-Compose ($securityOptions.Count -eq 1 -and $securityOptions[0] -ceq 'no-new-privileges:true') "$channelType connector lost its privilege-escalation restriction."
        Assert-Compose (@($serviceConfig['ports'] | Where-Object { $null -ne $_ }).Count -eq 0) "$channelType connector must not publish a host port."
        $dependencies=@(if ($channelType -eq 'telegram') {'gateway';'consumer';'delivery'} else {'gateway'})
        Assert-Compose ($serviceConfig.depends_on.Count -eq $dependencies.Count) "$channelType connector dependencies differ from its deployment contract."
        foreach ($dependency in $dependencies) {
            Assert-Compose ($serviceConfig.depends_on[$dependency]['condition'] -ceq 'service_healthy' -and $serviceConfig.depends_on[$dependency]['required'] -eq $true) "$channelType connector must wait for healthy $dependency."
        }
        $mounts=@($serviceConfig['volumes'])
        $stateVolume=if ($channelType -eq 'telegram') {'telegram_state'} else {'wecom_bot_state'}
        Assert-Compose ($mounts.Count -eq 1 -and $mounts[0]['type'] -ceq 'volume' -and $mounts[0]['source'] -ceq $stateVolume -and $mounts[0]['target'] -ceq '/state') "$channelType connector must persist its checkpoint in the dedicated state volume."
        $restart=if ($channelType -eq 'telegram') {'unless-stopped'} else {'no'}
        Assert-Compose ($serviceConfig['restart'] -ceq $restart) "$channelType connector restart policy differs from its ownership contract."
        $modelBinding=Get-IMSecretBindingName -TenantId 'fixture-tenant' -Purpose model -Provider openai -Model 'gpt-4o-mini'
        foreach ($entry in $rendered.services.GetEnumerator()) {
            $environment=$entry.Value['environment']
            if ($null -eq $environment) { $environment=@{} }
            $name=$entry.Key
            $needsKey=$name -in @('worker','summary-worker')
            $needsEndpoint=$name -in @('worker','summary-worker')
            Assert-Compose ([bool]$environment['TRPC_SECRET_OPENAI_API_KEY'] -eq $needsKey) "$channelType/$name model credential scope is incomplete or excessive."
            Assert-Compose ([bool]$environment['TRPC_OPENAI_BASE_URL'] -eq $needsEndpoint) "$channelType/$name operator endpoint scope is incorrect."
            if ($needsKey) {
                Assert-Compose ($environment['TRPC_SECRET_OPENAI_API_KEY'] -ceq $values.TRPC_SECRET_OPENAI_API_KEY -and $environment[$modelBinding] -ceq 'env://TRPC_SECRET_OPENAI_API_KEY') "$channelType/$name requires both model credential and exact tenant binding."
            }
            if ($needsEndpoint) { Assert-Compose ($environment['TRPC_OPENAI_BASE_URL'] -ceq $values.TRPC_OPENAI_BASE_URL) "$channelType/$name operator endpoint value differs." }
            if ($name -eq 'admin') { Assert-Compose ($environment[$modelBinding] -ceq 'env://TRPC_SECRET_OPENAI_API_KEY') "$channelType/Admin must authorize the model reference without receiving its value." }
            if ($channelType -eq 'wecom_bot') {
                Assert-Compose ([bool]$environment['WECOM_BOT_SECRET'] -eq ($name -eq 'wecom-bot')) "$name received the wrong smart bot credential scope."
                $needsBridge=$name -in @('gateway','delivery','wecom-bot')
                Assert-Compose ([bool]$environment['TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN'] -eq $needsBridge) "$name received the wrong smart bot bridge credential scope."
                if ($needsBridge) { Assert-Compose ($environment['TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN'] -ceq $values.TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN) "$name bridge credential was changed by Compose merging." }
                Assert-Compose (-not $environment['TRPC_SECRET_TELEGRAM_BOT_TOKEN'] -and -not $environment['TRPC_SECRET_TELEGRAM_WEBHOOK']) "$name inherited unrelated Telegram credentials."
            } else {
                $needsTelegram=$name -in @('gateway','delivery','telegram-poller')
                Assert-Compose ([bool]$environment['TRPC_SECRET_TELEGRAM_BOT_TOKEN'] -eq $needsTelegram -and [bool]$environment['TRPC_SECRET_TELEGRAM_WEBHOOK'] -eq $needsTelegram) "$name received the wrong Telegram credential scope."
                if ($needsTelegram) { Assert-Compose ($environment['TRPC_SECRET_TELEGRAM_BOT_TOKEN'] -ceq $values.TRPC_SECRET_TELEGRAM_BOT_TOKEN -and $environment['TRPC_SECRET_TELEGRAM_WEBHOOK'] -ceq $values.TRPC_SECRET_TELEGRAM_WEBHOOK) "$name Telegram credential values were changed by Compose merging." }
            }
            foreach ($port in @($entry.Value['ports'])) {
                if ($null -ne $port) { Assert-Compose ($port.host_ip -ceq '127.0.0.1') "$channelType/$name exposes a non-loopback port." }
            }
        }
        foreach ($service in @('gateway','delivery')) {
            foreach ($binding in $definition.services[$service].environment.GetEnumerator()) {
                Assert-Compose ($rendered.services[$service].environment[$binding.Key] -ceq $binding.Value) "$channelType/$service lost its channel authorization during Compose merge."
            }
        }
        Write-Output "PASS: actual offline Compose merge for $channelType; single tmpfs mount, non-root read-only runtime, capabilities, state volume, health dependencies, restart policy, credential scopes and loopback ports verified."
    }
} finally {
    foreach ($entry in $saved.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key,$entry.Value,'Process') }
    $full=[IO.Path]::GetFullPath($fixture)
    $allowed=[IO.Path]::GetFullPath($TestRoot).TrimEnd('\')+'\'
    if ($full.StartsWith($allowed,[StringComparison]::OrdinalIgnoreCase) -and [IO.Path]::GetFileName($full).StartsWith('im-compose-contract-')) {
        Remove-Item -LiteralPath $full -Recurse -Force
    }
}
