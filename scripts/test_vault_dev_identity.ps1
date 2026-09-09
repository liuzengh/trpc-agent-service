[CmdletBinding()]
param([string]$ExpectedContext = 'k3d-agent-platform')

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ((kubectl config current-context) -ne $ExpectedContext) { throw 'unexpected Kubernetes context' }
$namespace = 'vault-platform-validation'
$mount = 'platform-validation'
$run = [DateTime]::UtcNow.ToString('yyyyMMddHHmmss')
$names = @()
$image = 'hashicorp/vault@sha256:5be49781ecf78bfe775c5309c6a4d9f4e9e040b6c885c99eb2b12fb69855e1a2'

function Apply-Object([object]$Object) {
    $Object | ConvertTo-Json -Depth 20 | kubectl apply -f -
    if ($LASTEXITCODE -ne 0) { throw 'could not provision local Vault identity fixture' }
}

# Root authentication stays inside the existing disposable dev Vault process.
# Never read or print it on the host, and never use this script with real Vault.
$setup = @'
set -eu
test -n "${VAULT_DEV_ROOT_TOKEN_ID:-}" || exit 90
export VAULT_TOKEN="$VAULT_DEV_ROOT_TOKEN_ID"
vault auth list -format=json | grep -q '"platform-validation/"' || vault auth enable -path=platform-validation kubernetes >/dev/null
vault write auth/platform-validation/config kubernetes_host=https://kubernetes.default.svc kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt >/dev/null
printf '%s\n' 'path "secret/data/platform-validation/allowed" { capabilities = ["read"] }' | vault policy write platform-validation-reader - >/dev/null
vault write auth/platform-validation/role/reader bound_service_account_names=vault-client bound_service_account_namespaces=vault-platform-validation audience=vault policies=platform-validation-reader ttl=2m >/dev/null
vault kv put secret/platform-validation/allowed value=validation-only-not-a-production-secret >/dev/null
echo VAULT_DEV_FIXTURE_READY
'@
$setup | kubectl -n vault exec -i vault-0 -- sh -s
if ($LASTEXITCODE -ne 0) { throw 'local dev Vault fixture setup failed' }
Apply-Object @{apiVersion='v1';kind='Namespace';metadata=@{name=$namespace;labels=@{'pod-security.kubernetes.io/enforce'='restricted'};annotations=@{'linkerd.io/inject'='disabled'}}}
foreach ($sa in @('vault-client','vault-denied')) {
    Apply-Object @{apiVersion='v1';kind='ServiceAccount';metadata=@{name=$sa;namespace=$namespace};automountServiceAccountToken=$false}
}

$allowScript = @'
set -eu
export VAULT_TOKEN="$(vault write -field=token auth/platform-validation/login role=reader jwt=@/identity/token)"
test "$(vault kv get -field=value secret/platform-validation/allowed)" = validation-only-not-a-production-secret
echo PASS_AUTHORIZED_WORKLOAD_READ
if vault kv get secret/platform-validation/forbidden >/tmp/denial 2>&1; then exit 71; fi
grep -q 'Code: 403' /tmp/denial
echo PASS_FORBIDDEN_PATH_DENIED
vault token revoke -self >/dev/null
'@
$denyScript = @'
set -eu
if vault write auth/platform-validation/login role=reader jwt=@/identity/token >/tmp/denial 2>&1; then exit 72; fi
grep -q 'Code: 403' /tmp/denial
echo PASS_WRONG_SERVICE_ACCOUNT_DENIED
'@
try {
    foreach ($mode in @('allowed','denied')) {
        $sa = if ($mode -eq 'allowed') { 'vault-client' } else { 'vault-denied' }
        $body = if ($mode -eq 'allowed') { $allowScript } else { $denyScript }
        $name = "vault-platform-$mode-$run"
        Apply-Object @{
            apiVersion='v1';kind='Pod';metadata=@{name=$name;namespace=$namespace;annotations=@{'linkerd.io/inject'='disabled'}}
            spec=@{
                serviceAccountName=$sa;automountServiceAccountToken=$false;restartPolicy='Never'
                securityContext=@{runAsNonRoot=$true;runAsUser=100;runAsGroup=1000;fsGroup=1000;seccompProfile=@{type='RuntimeDefault'}}
                containers=@(@{
                    name='test';image=$image;imagePullPolicy='IfNotPresent';command=@('/bin/sh','-c',$body)
                    env=@(@{name='VAULT_ADDR';value='http://vault.vault.svc:8200'})
                    securityContext=@{allowPrivilegeEscalation=$false;readOnlyRootFilesystem=$true;capabilities=@{drop=@('ALL')}}
                    resources=@{requests=@{cpu='10m';memory='32Mi'};limits=@{cpu='250m';memory='128Mi'}}
                    volumeMounts=@(@{name='identity';mountPath='/identity';readOnly=$true},@{name='tmp';mountPath='/tmp'})
                })
                volumes=@(@{name='identity';projected=@{sources=@(@{serviceAccountToken=@{audience='vault';expirationSeconds=600;path='token'}})}},@{name='tmp';emptyDir=@{}})
            }
        }
        $names += $name
        $deadline = [DateTime]::UtcNow.AddSeconds(90)
        do {
            $pod = kubectl -n $namespace get pod $name -o json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0) { throw 'could not inspect Vault identity probe' }
            if ($pod.status.phase -in @('Succeeded','Failed')) { break }
            Start-Sleep -Seconds 2
        } while ([DateTime]::UtcNow -lt $deadline)
        if ($pod.status.phase -ne 'Succeeded') { throw "Vault $mode identity probe did not succeed" }
        $lines = @(kubectl -n $namespace logs $name -c test)
        if ($LASTEXITCODE -ne 0) { throw 'could not read Vault probe verdict' }
        $expected = if ($mode -eq 'allowed') { @('PASS_AUTHORIZED_WORKLOAD_READ','PASS_FORBIDDEN_PATH_DENIED') } else { @('PASS_WRONG_SERVICE_ACCOUNT_DENIED') }
        foreach ($verdict in $expected) {
            if ($lines -notcontains $verdict) { throw 'missing Vault probe verdict' }
            Write-Output $verdict
        }
    }
    Write-Output 'Scope: local dev-mode Vault + projected Kubernetes identity only; not HA, auto-unseal, cloud KMS or an application SecretResolver integration.'
} finally {
    foreach ($name in $names) { kubectl -n $namespace delete pod $name --ignore-not-found --wait=false | Out-Null }
}
