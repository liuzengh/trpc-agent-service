[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')]
    [string]$Namespace = 'agent-platform',
    [string]$ExpectedContext = 'k3d-agent-platform'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ((kubectl config current-context) -ne $ExpectedContext) { throw 'unexpected Kubernetes context' }
$suffix = [DateTime]::UtcNow.ToString('yyyyMMddHHmmss')
$probeNames = @()
# Reuse the already-pinned Alpine database image for its wget client. No database
# process, password, volume, or provider credential is used by these probes.
$image = 'trpc-v13-registry:5000/v13/postgres@sha256:68c8f729caca8638396647002948c0ab753fda787cbee9e887f7166169c6b87e'
try {
    foreach ($mode in @('authenticated', 'plaintext')) {
        $name = "platform-mesh-$mode-$suffix"
        $inject = if ($mode -eq 'authenticated') { 'enabled' } else { 'disabled' }
        $pod = @{
            apiVersion = 'v1'; kind = 'Pod'
            metadata = @{
                name = $name; namespace = $Namespace
                labels = @{app = 'agent-consumer'; 'validation.trpc.io/probe' = 'mesh'}
                annotations = @{'linkerd.io/inject' = $inject}
            }
            spec = @{
                restartPolicy = 'Never'
                securityContext = @{runAsNonRoot = $true; runAsUser = 65532; runAsGroup = 65532; seccompProfile = @{type = 'RuntimeDefault'}}
                containers = @(@{
                    name = 'probe'; image = $image; imagePullPolicy = 'IfNotPresent'
                    command = @('/bin/sh', '-c', 'sleep 600')
                    securityContext = @{allowPrivilegeEscalation = $false; readOnlyRootFilesystem = $true; capabilities = @{drop = @('ALL')}}
                    resources = @{requests = @{cpu = '5m'; memory = '8Mi'}; limits = @{cpu = '100m'; memory = '32Mi'}}
                })
            }
        }
        $pod | ConvertTo-Json -Depth 15 | kubectl apply -f -
        if ($LASTEXITCODE -ne 0) { throw "probe creation failed: $mode" }
        $probeNames += $name
        kubectl -n $Namespace wait --for=condition=Ready --timeout=90s "pod/$name"
        if ($LASTEXITCODE -ne 0) { throw "probe startup failed: $mode" }
        # Linkerd permits Kubernetes health probes without mTLS. Check the
        # protected route: a meshed request reaches the HMAC gate (401), while
        # the otherwise-identical plaintext request must fail at the mesh (403).
        $result = @(& kubectl -n $Namespace exec $name -c probe -- wget -S -O /dev/null -T 8 http://agent-worker:9090/process 2>&1)
        $code = $LASTEXITCODE
        $signal = [string]::Join(' ', $result)
        if ($mode -eq 'authenticated') {
            if ($code -eq 0 -or $signal -notmatch 'HTTP/1\.[01] 401') { throw 'meshed probe did not reach the HMAC gate (expected 401)' }
            Write-Output 'PASS: meshed Consumer-labelled probe -> Worker HMAC gate HTTP 401'
        } else {
            if ($code -eq 0 -or $signal -notmatch 'HTTP/1\.[01] 403') { throw 'plaintext probe was not rejected by mesh (expected 403)' }
            Write-Output 'PASS: plaintext Consumer-labelled probe -> mesh HTTP 403'
        }
    }
    Write-Output 'Scope: local K3d namespace all-authenticated policy; not per-service identity authorization or production certification.'
} finally {
    foreach ($name in $probeNames) {
        kubectl -n $Namespace delete pod $name --ignore-not-found --grace-period=5 --wait=false | Out-Null
    }
}
