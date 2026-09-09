[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')]
    [string]$Namespace = 'agent-platform',
    [ValidatePattern('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')]
    [string]$DataPlaneNamespace = 'agent-data-plane',
    [ValidateSet('bootstrap', 'compatible')]
    [string]$SchemaClass = 'bootstrap',
    [string]$ReleaseDirectory = (Join-Path $PSScriptRoot '..\deploy\kubernetes\releases\platform-k3d-20260905'),
    [string]$ExpectedContext = 'k3d-agent-platform',
    [string]$OtelCAFromNamespace = 'agent-platform',
    [ValidatePattern('^[^\r\n\x00]{1,256}$')]
    [string]$MeshEvidence = 'k3d-agent-platform-linkerd-edge-20260905',
    [Parameter(Mandatory)][ValidatePattern('^sha256:[a-f0-9]{64}$')]
    [string]$GatewayDigest,
    [ValidatePattern('^[0-9]+[smh]$')]
    [string]$RolloutTimeout = '12m'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$releaseRoot = [IO.Path]::GetFullPath($ReleaseDirectory)
$releaseFiles = @(
    'migration.yaml',
    'profiles.yaml',
    'availability-policies.yaml',
    'worker.yaml',
    'summary.yaml',
    'pipeline.yaml',
    'admin.yaml',
    'gateway.yaml'
)
$applicationDeployments = @(
    'agent-gateway',
    'agent-consumer',
    'agent-worker',
    'agent-summary-worker',
    'agent-delivery',
    'agent-admin'
)

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [switch]$Quiet
    )
    if ($Quiet) {
        & kubectl @Arguments | Out-Null
    } else {
        & kubectl @Arguments
    }
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed: $($Arguments -join ' ')"
    }
}

function Test-KubernetesResource {
    param([Parameter(Mandatory)][string[]]$Arguments)
    & kubectl @Arguments *> $null
    return $LASTEXITCODE -eq 0
}

function Apply-KubernetesObject {
    param([Parameter(Mandatory)][object]$Object)
    $json = $Object | ConvertTo-Json -Depth 20 -Compress
    $json | & kubectl apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw 'kubectl failed while applying an in-memory object'
    }
}

function New-RandomHex {
    param([ValidateRange(16, 128)][int]$Bytes = 32)
    return [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes($Bytes)).ToLowerInvariant()
}

function Ensure-Secret {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][hashtable]$StringData
    )
    if (Test-KubernetesResource -Arguments @('-n', $Namespace, 'get', 'secret', $Name)) {
        $secret = (& kubectl -n $Namespace get secret $Name -o json | ConvertFrom-Json)
        if ($LASTEXITCODE -ne 0) { throw "could not inspect existing Secret $Name" }
        foreach ($key in $StringData.Keys) {
            if (-not $secret.data.PSObject.Properties[$key]) {
                throw "existing Secret $Name is missing required key $key"
            }
        }
        Write-Output "Secret already provisioned: $Name"
        return
    }
    $object = [ordered]@{
        apiVersion = 'v1'
        kind = 'Secret'
        metadata = [ordered]@{name = $Name; namespace = $Namespace}
        type = 'Opaque'
        stringData = $StringData
    }
    Apply-KubernetesObject -Object $object
}

function Get-SecretText {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Key
    )
    $encoded = [string]::Join('', @(& kubectl -n $Namespace get secret $Name -o "jsonpath={.data.$Key}")).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($encoded)) {
        throw "could not read required key $Key from existing Secret $Name"
    }
    return [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encoded))
}

foreach ($command in 'kubectl', 'docker', 'go') {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "required command is missing: $command"
    }
}

$context = [string]::Join('', @(& kubectl config current-context)).Trim()
if ($LASTEXITCODE -ne 0 -or $context -ne $ExpectedContext) {
    throw "refusing to mutate Kubernetes context '$context'; expected '$ExpectedContext'"
}
& docker version --format '{{.Server.Version}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker Engine is unavailable' }

# Render and verify all release inputs before the first cluster mutation.
& (Join-Path $PSScriptRoot 'render_k3d_release.ps1') `
    -OutputDirectory $releaseRoot `
    -SchemaClass $SchemaClass `
    -MeshEvidence $MeshEvidence `
    -GatewayDigest $GatewayDigest
foreach ($file in $releaseFiles + @('network-policies.yaml', 'k3d-validation-prerequisites.yaml')) {
    if (-not (Test-Path -LiteralPath (Join-Path $releaseRoot $file))) {
        throw "rendered release is missing $file"
    }
}

$verifyArgs = @('run', './cmd/releaseverify')
foreach ($file in $releaseFiles) {
    $verifyArgs += @('--manifest', (Join-Path $releaseRoot $file))
}
$verifyArgs += @(
    '--network-policy', (Join-Path $releaseRoot 'network-policies.yaml'),
    '--schema-class', $SchemaClass
)
$oldToolchain = [Environment]::GetEnvironmentVariable('GOTOOLCHAIN', 'Process')
try {
    [Environment]::SetEnvironmentVariable('GOTOOLCHAIN', 'local', 'Process')
    Push-Location $repoRoot
    try {
        & go @verifyArgs
        if ($LASTEXITCODE -ne 0) { throw 'release verification failed' }
    } finally {
        Pop-Location
    }
} finally {
    [Environment]::SetEnvironmentVariable('GOTOOLCHAIN', $oldToolchain, 'Process')
}

$namespaceObject = [ordered]@{
    apiVersion = 'v1'
    kind = 'Namespace'
    metadata = [ordered]@{
        name = $Namespace
        labels = [ordered]@{
            'pod-security.kubernetes.io/enforce' = 'restricted'
            'pod-security.kubernetes.io/audit' = 'restricted'
            'pod-security.kubernetes.io/warn' = 'restricted'
        }
        annotations = [ordered]@{
            'linkerd.io/inject' = 'enabled'
            'config.linkerd.io/default-inbound-policy' = 'all-authenticated'
        }
    }
}
Apply-KubernetesObject -Object $namespaceObject

$dataPlaneNamespaceObject = [ordered]@{
    apiVersion = 'v1'
    kind = 'Namespace'
    metadata = [ordered]@{
        name = $DataPlaneNamespace
        labels = [ordered]@{
            'agent-platform-access' = 'data-plane'
            'pod-security.kubernetes.io/enforce' = 'restricted'
        }
    }
}
Apply-KubernetesObject -Object $dataPlaneNamespaceObject

foreach ($access in 'public-ingress', 'observability', 'admin-ingress', 'egress-gateway', 'data-plane') {
    $matches = [string]::Join('', @(& kubectl get namespace -l "agent-platform-access=$access" -o name)).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($matches)) {
        throw "no namespace is labelled agent-platform-access=$access"
    }
}

if ($SchemaClass -eq 'bootstrap') {
    foreach ($deployment in $applicationDeployments) {
        if (Test-KubernetesResource -Arguments @('-n', $Namespace, 'get', 'deployment', $deployment)) {
            throw "bootstrap refused because Deployment $deployment already exists in $Namespace"
        }
    }
}

$storageSecrets = 'postgres-auth', 'redis-auth', 'db-credentials', 'redis-credentials', 'tenant-storage-credentials'
$existingStorageSecrets = @($storageSecrets | Where-Object {
    Test-KubernetesResource -Arguments @('-n', $Namespace, 'get', 'secret', $_)
})
if ($existingStorageSecrets.Count -ne 0 -and $existingStorageSecrets.Count -ne $storageSecrets.Count) {
    throw "partial storage Secret set found in $Namespace; refusing to rotate credentials underneath retained PVCs"
}
if ($existingStorageSecrets.Count -eq $storageSecrets.Count) {
    $postgresPassword = Get-SecretText -Name 'postgres-auth' -Key 'password'
    $redisPassword = Get-SecretText -Name 'redis-auth' -Key 'password'
} else {
    $postgresPassword = New-RandomHex -Bytes 24
    $redisPassword = New-RandomHex -Bytes 24
}
$databaseURL = "postgres://agent:${postgresPassword}@postgres:5432/trpc_agent?sslmode=disable"
$redisURL = "redis://:${redisPassword}@redis:6379/0"

Ensure-Secret -Name 'postgres-auth' -StringData @{password = $postgresPassword}
Ensure-Secret -Name 'redis-auth' -StringData @{password = $redisPassword}
Ensure-Secret -Name 'db-credentials' -StringData @{url = $databaseURL}
Ensure-Secret -Name 'redis-credentials' -StringData @{url = $redisURL}
Ensure-Secret -Name 'tenant-storage-credentials' -StringData @{
    'postgres-url' = $databaseURL
    'redis-url' = $redisURL
}
Ensure-Secret -Name 'master-key' -StringData @{key = (New-RandomHex)}
Ensure-Secret -Name 'service-auth' -StringData @{key = (New-RandomHex)}
Ensure-Secret -Name 'audit-identity' -StringData @{key = (New-RandomHex)}
Ensure-Secret -Name 'metrics-auth' -StringData @{token = (New-RandomHex)}
Ensure-Secret -Name 'admin-api-token' -StringData @{token = (New-RandomHex)}
Ensure-Secret -Name 'runtime-data-plane-credentials' -StringData @{
    'qdrant-api-key' = (New-RandomHex)
    'embedding-api-key' = (New-RandomHex)
    's3-access-key' = (New-RandomHex -Bytes 16)
    's3-secret-key' = (New-RandomHex)
}

# Only the public CA certificate is copied. No private key or application
# credential is read, printed, written to disk, or moved across namespaces.
$caEncoded = [string]::Join('', @(& kubectl -n $OtelCAFromNamespace get secret otel-collector-tls -o 'jsonpath={.data.ca\.crt}')).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($caEncoded)) {
    throw "OTEL CA certificate was not found in namespace $OtelCAFromNamespace"
}
$otelCAObject = [ordered]@{
    apiVersion = 'v1'
    kind = 'Secret'
    metadata = [ordered]@{name = 'otel-collector-tls'; namespace = $Namespace}
    type = 'Opaque'
    data = [ordered]@{'ca.crt' = $caEncoded}
}
Apply-KubernetesObject -Object $otelCAObject
$caEncoded = $null

Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'k3d-validation-prerequisites.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'rollout', 'status', "--timeout=$RolloutTimeout", 'statefulset/postgres')
Invoke-Kubectl -Arguments @('-n', $Namespace, 'rollout', 'status', "--timeout=$RolloutTimeout", 'statefulset/redis')

Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'network-policies.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'profiles.yaml'))

if (Test-KubernetesResource -Arguments @('-n', $Namespace, 'get', 'job', 'agent-migrate')) {
    $active = [string]::Join('', @(& kubectl -n $Namespace get job agent-migrate -o 'jsonpath={.status.active}')).Trim()
    $failed = [string]::Join('', @(& kubectl -n $Namespace get job agent-migrate -o 'jsonpath={.status.failed}')).Trim()
    if (($active -ne '' -and $active -ne '0') -or ($failed -ne '' -and $failed -ne '0')) {
        throw 'existing migration Job is active or failed; inspect it before another rollout'
    }
    Invoke-Kubectl -Arguments @('-n', $Namespace, 'delete', 'job', 'agent-migrate', '--wait=true')
}
Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'migration.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'wait', '--for=condition=complete', '--timeout=32m', 'job/agent-migrate')

Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'availability-policies.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'worker.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'summary.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'admin.yaml'))
foreach ($deployment in 'agent-worker', 'agent-summary-worker', 'agent-admin') {
    Invoke-Kubectl -Arguments @('-n', $Namespace, 'rollout', 'status', "--timeout=$RolloutTimeout", "deployment/$deployment")
}

Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'pipeline.yaml'))
foreach ($deployment in 'agent-consumer', 'agent-delivery') {
    Invoke-Kubectl -Arguments @('-n', $Namespace, 'rollout', 'status', "--timeout=$RolloutTimeout", "deployment/$deployment")
}

Invoke-Kubectl -Arguments @('-n', $Namespace, 'apply', '-f', (Join-Path $releaseRoot 'gateway.yaml'))
Invoke-Kubectl -Arguments @('-n', $Namespace, 'rollout', 'status', "--timeout=$RolloutTimeout", 'deployment/agent-gateway')

$pods = (& kubectl -n $Namespace get pods -o json | ConvertFrom-Json).items
if ($LASTEXITCODE -ne 0) { throw 'could not inspect released Pods' }
$applicationPods = @($pods | Where-Object { $_.metadata.labels.app -like 'agent-*' -and $_.metadata.labels.app -ne 'agent-migrate' })
foreach ($pod in $applicationPods) {
    $containerNames = @($pod.spec.containers | ForEach-Object name)
    # Linkerd can use Kubernetes native sidecars (restartPolicy: Always).
    if ($pod.spec.PSObject.Properties['initContainers']) {
        $containerNames += @($pod.spec.initContainers | Where-Object {
            $_.PSObject.Properties['restartPolicy'] -and $_.restartPolicy -eq 'Always'
        } | ForEach-Object name)
    }
    if ($containerNames -notcontains 'linkerd-proxy') {
        throw "Pod $($pod.metadata.name) is missing the Linkerd proxy"
    }
    foreach ($status in $pod.status.containerStatuses) {
        if (-not $status.ready) {
            throw "Pod $($pod.metadata.name) container $($status.name) is not ready"
        }
    }
    $readyCondition = @($pod.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' })
    if ($readyCondition.Count -ne 1) {
        throw "Pod $($pod.metadata.name) is not Ready (including native sidecars)"
    }
}

Write-Output "K3d application release is ready: namespace=$Namespace schema=$SchemaClass"
Invoke-Kubectl -Arguments @('-n', $Namespace, 'get', 'pods', '-o', 'wide')
Invoke-Kubectl -Arguments @('-n', $Namespace, 'get', 'hpa')
