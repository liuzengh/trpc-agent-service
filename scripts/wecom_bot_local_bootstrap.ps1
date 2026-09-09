# Requires PowerShell 7 on Windows. Validation never opens a bot connection.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ConfigPath,
    [ValidatePattern('^trpc-wecom-bot-[a-z0-9][a-z0-9-]{0,40}$')][string]$ProjectName = 'trpc-wecom-bot-local',
    [ValidateRange(1024,65535)][int]$AdminPort = 48091,
    [ValidateRange(1024,65535)][int]$GatewayPort = 48090,
    [ValidateRange(1024,65535)][int]$PostgresPort = 64542,
    [ValidateRange(1024,65535)][int]$OtelGrpcPort = 64337,
    [ValidateRange(1024,65535)][int]$OtelHttpPort = 64338,
    [switch]$ValidateOnly,
    [switch]$SkipBuild
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'im_local_config.ps1')
$inputPath=[IO.Path]::GetFullPath($ConfigPath)
$config=Read-IMLocalConfig $inputPath
$botId=[string]$config['WECOM_BOT_ID']
$botSecret=[string]$config['WECOM_BOT_SECRET']
if ($botId -cnotmatch '^[A-Za-z0-9_-]{1,128}\z' -or $botSecret -cnotmatch '^[A-Za-z0-9_-]{16,4096}\z') {
    throw 'Blocked: WECOM_BOT_ID and WECOM_BOT_SECRET must come from the smart bot long-connection settings.'
}
$allowedUsers=@($config['WECOM_ALLOWED_USER_ID'] | ForEach-Object { [string]$_ } | Sort-Object -Unique)
if ($allowedUsers.Count -eq 0 -or @($allowedUsers | Where-Object { $_ -cnotmatch '^[A-Za-z0-9_.@-]{1,256}\z' }).Count -gt 0) {
    throw 'Blocked: WECOM_ALLOWED_USER_ID must contain the exact user ID from a paired official callback.'
}
$modelKey=[string]$config['TRPC_SECRET_OPENAI_API_KEY']
Assert-IMModelKey $modelKey
$modelBaseURL=[string]$config['TRPC_OPENAI_BASE_URL']
Assert-IMModelBaseURL $modelBaseURL
Assert-IMProxy ([string]$config['CONTAINER_HTTPS_PROXY'])
$modelName=[string]$config['MODEL_NAME']
if (-not $modelName) { $modelName='gpt-4o-mini' }
# Match the current immutable model catalog. Runtime admission remains final.
$contextWindows=@{'gpt-4o-mini'=128000;'gpt-4'=8192}
if (-not $contextWindows.ContainsKey($modelName)) { throw 'MODEL_NAME is not in the installed operator-approved model catalog.' }
$systemPrompt=[string]$config['SYSTEM_PROMPT']
if (-not $systemPrompt) { $systemPrompt='请用简洁、准确的中文回答。用户明确要求记住或回忆信息时，可以使用记忆工具；不得编造工具结果或声称执行过未执行的操作。' }
if ([Text.Encoding]::UTF8.GetByteCount($systemPrompt) -gt 16384 -or $systemPrompt.Contains([char]0)) { throw 'SYSTEM_PROMPT is invalid or exceeds 16 KiB.' }
$ports=@($AdminPort,$GatewayPort,$PostgresPort,$OtelGrpcPort,$OtelHttpPort)
if (@($ports | Select-Object -Unique).Count -ne $ports.Count) { throw 'Published ports must be distinct.' }
if ($ValidateOnly) {
    Write-Output 'Configuration valid. Offline validation only: no WebSocket, provider API, Docker, model call or state write was performed.'
    return
}
$arguments=@{
    InputPath=$inputPath;ProjectName=$ProjectName;ChannelType='wecom_bot';BotId=$botId
    ModelKey=$modelKey;ModelName=$modelName;ModelBaseURL=$modelBaseURL;AllowedUsers=$allowedUsers
    SystemPrompt=$systemPrompt;MaxLLMCalls=3;AgentTools=@('memory_add','memory_search');Reservation=([int64]$contextWindows[$modelName]*3)
    ProviderEnvironment=@{WECOM_BOT_ID=$botId;WECOM_BOT_SECRET=$botSecret};Config=$config
    AdminPort=$AdminPort;GatewayPort=$GatewayPort;PostgresPort=$PostgresPort;OtelGrpcPort=$OtelGrpcPort;OtelHttpPort=$OtelHttpPort
    SkipBuild=$SkipBuild
}
& (Join-Path $PSScriptRoot 'im_local_deploy.ps1') @arguments
