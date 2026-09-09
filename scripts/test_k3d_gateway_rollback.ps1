[CmdletBinding()]
param(
    [string]$Namespace = 'agent-platform',
    [string]$ExpectedContext = 'k3d-agent-platform',
    [ValidatePattern('^[^\s]+@sha256:[a-f0-9]{64}$')]
    [string]$PreviousImage = 'trpc-v13-registry:5000/v13/gateway@sha256:aecc6b7adf5407862dcb90a6792e4504cb0747a31e856a307bbabc4f0385763c'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ((kubectl config current-context) -ne $ExpectedContext -or $Namespace -ne 'agent-platform') {
    throw 'this drill is restricted to the dedicated local application namespace'
}
$deployment = kubectl -n $Namespace get deployment agent-gateway -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'could not read Gateway deployment' }
$originalImage = ($deployment.spec.template.spec.containers | Where-Object name -eq 'gateway').image
$originalRevision = $deployment.metadata.annotations.'deployment.kubernetes.io/revision'
if ($originalImage -notmatch '/gateway@sha256:[a-f0-9]{64}$') { throw 'expected a digest-pinned Gateway image before this drill' }

function Assert-GatewayReady {
    param([string]$ExpectedImage)
    kubectl -n $Namespace rollout status deployment/agent-gateway --timeout=180s
    if ($LASTEXITCODE -ne 0) { throw 'Gateway rollout did not become ready' }
    $current = kubectl -n $Namespace get deployment agent-gateway -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'could not reread Gateway deployment' }
    $image = ($current.spec.template.spec.containers | Where-Object name -eq 'gateway').image
    if ($image -ne $ExpectedImage -or $current.status.availableReplicas -lt $current.spec.replicas) { throw 'Gateway post-state image/replicas mismatch' }
    Write-Output "PASS: Gateway Ready replicas=$($current.status.availableReplicas) image=$image"
}

try {
    kubectl -n $Namespace set image deployment/agent-gateway "gateway=$PreviousImage"
    if ($LASTEXITCODE -ne 0) { throw 'could not deploy previous Gateway image' }
    Assert-GatewayReady $PreviousImage
} finally {
    # Always restore the exact prior Pod template, not a mutable image tag. The
    # database schema is intentionally not rolled back in this image-only drill.
    kubectl -n $Namespace rollout undo deployment/agent-gateway "--to-revision=$originalRevision"
    if ($LASTEXITCODE -ne 0) { throw 'RESTORATION REQUIRED: Gateway rollback failed' }
    Assert-GatewayReady $originalImage
}
Write-Output 'PASS: two real Gateway digests rolled out and original application revision restored; this is not a full-platform upgrade or database rollback certification.'
