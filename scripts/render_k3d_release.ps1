[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot '..\deploy\kubernetes\releases\platform-k3d-20260905'),
    [ValidateSet('bootstrap', 'compatible')]
    [string]$SchemaClass = 'bootstrap',
    [ValidatePattern('^[^\r\n\x00]{1,256}$')]
    [string]$MeshEvidence = 'k3d-agent-platform-linkerd-edge-20260905',
    [Parameter(Mandatory)][ValidatePattern('^sha256:[a-f0-9]{64}$')]
    [string]$GatewayDigest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$templateRoot = Join-Path $repoRoot 'deploy\kubernetes'
$outputRoot = [IO.Path]::GetFullPath($OutputDirectory)

function Replace-Exact {
    param(
        [Parameter(Mandatory)][string]$Content,
        [Parameter(Mandatory)][string]$Old,
        [Parameter(Mandatory)][string]$New,
        [Parameter(Mandatory)][int]$ExpectedCount,
        [Parameter(Mandatory)][string]$Label
    )
    $count = [regex]::Matches($Content, [regex]::Escape($Old)).Count
    if ($count -ne $ExpectedCount) {
        throw "render invariant failed for ${Label}: expected ${ExpectedCount} match(es), found $count"
    }
    return $Content.Replace($Old, $New)
}

$releaseFiles = [ordered]@{
    'migration.yaml'             = 'migration-job.yaml'
    'profiles.yaml'              = 'runtime-data-plane-config.yaml'
    'availability-policies.yaml' = 'availability-policies.yaml'
    'worker.yaml'                = 'worker-deployment.yaml'
    'summary.yaml'               = 'summary-worker-deployment.yaml'
    'pipeline.yaml'              = 'pipeline-deployments.yaml'
    'admin.yaml'                 = 'admin-deployment.yaml'
    'gateway.yaml'               = 'gateway-deployment.yaml'
}

$imagesByOutput = @{
    'migration.yaml' = @{
        'trpc-agent-migrate:0.1.0' = 'trpc-v13-registry:5000/platform/migrate@sha256:8a3822391bec617daed6b93f91ed02e579c35b591a471673456afe635e818b3b'
    }
    'worker.yaml' = @{
        'trpc-agent-worker:0.1.0' = 'trpc-v13-registry:5000/platform/worker@sha256:858c68472ebdb57f00ac0d89f6ce74b083aee328b7489234879e807c1b2b49ea'
    }
    'summary.yaml' = @{
        'trpc-agent-summary-worker:0.1.0' = 'trpc-v13-registry:5000/platform/summary-worker@sha256:e0a9cf4c7a15423264c06fe46afa7df1bbea080e5832a8cd45ce4787cab82d5b'
    }
    'pipeline.yaml' = @{
        'trpc-agent-consumer:0.1.0' = 'trpc-v13-registry:5000/platform/consumer@sha256:9b4eadd4f2c6e61f170c4caa71c9485a5026fa3cc373bd2751dad627a657f201'
        'trpc-agent-delivery:0.1.0' = 'trpc-v13-registry:5000/platform/delivery@sha256:b40e67600fb1410ea6ac121458ff48c563e467ee45d10ff404a6c4f2ba04a307'
    }
    'admin.yaml' = @{
        'trpc-agent-admin:0.1.0' = 'trpc-v13-registry:5000/platform/admin@sha256:3703fc2379a4daba90650d59c23d889ee436fd5860562b5b8f648b759840d3e6'
    }
    'gateway.yaml' = @{
        'trpc-agent-gateway:0.1.0' = "trpc-v13-registry:5000/platform/gateway@$GatewayDigest"
    }
}

$secureProfiles = '[{"id":"tenant-postgres","backend":"postgres","connectionEnv":"TENANT_POSTGRES_DSN"},{"id":"tenant-redis","backend":"redis","connectionEnv":"TENANT_REDIS_URL"}]'
$labProfiles = '[{"id":"tenant-postgres","backend":"postgres","connectionEnv":"TENANT_POSTGRES_DSN","allowInsecure":true},{"id":"tenant-redis","backend":"redis","connectionEnv":"TENANT_REDIS_URL","allowInsecure":true}]'
$longDatabaseBinding = "        - name: DATABASE_URL`n          valueFrom:`n            secretKeyRef:`n              name: db-credentials`n              key: url"
$shortDatabaseBinding = "        - name: DATABASE_URL`n          valueFrom:`n            secretKeyRef: {name: db-credentials, key: url}"
$inlineDatabaseBinding = '        - {name: DATABASE_URL, valueFrom: {secretKeyRef: {name: db-credentials, key: url}}}'
$insecureDatabaseEnv = '        - {name: DATABASE_ALLOW_INSECURE, value: "true"}'

[IO.Directory]::CreateDirectory($outputRoot) | Out-Null

foreach ($entry in $releaseFiles.GetEnumerator()) {
    $outputName = $entry.Key
    $sourcePath = Join-Path $templateRoot $entry.Value
    if (-not (Test-Path -LiteralPath $sourcePath)) {
        throw "release template is missing: $sourcePath"
    }
    $content = [IO.File]::ReadAllText($sourcePath, [Text.Encoding]::UTF8).Replace("`r`n", "`n")

    if ($imagesByOutput.ContainsKey($outputName)) {
        foreach ($image in $imagesByOutput[$outputName].GetEnumerator()) {
            $content = Replace-Exact -Content $content -Old ("image: " + $image.Key) `
                -New ("image: " + $image.Value + "`n        imagePullPolicy: IfNotPresent") `
                -ExpectedCount 1 -Label "$outputName image $($image.Key)"
        }
    }

    if ($content.Contains($secureProfiles)) {
        $expectedProfileCount = if ($outputName -eq 'pipeline.yaml') { 2 } else { 1 }
        $content = Replace-Exact -Content $content -Old $secureProfiles -New $labProfiles `
            -ExpectedCount $expectedProfileCount -Label "$outputName storage profile security mode"
    }

    switch ($outputName) {
        'migration.yaml' {
            $content = Replace-Exact -Content $content -Old 'agent.trpc.io/schema-class: RENDER_REQUIRED' `
                -New "agent.trpc.io/schema-class: $SchemaClass" -ExpectedCount 1 -Label 'migration schema class'
            $content = Replace-Exact -Content $content -Old $inlineDatabaseBinding `
                -New ($inlineDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 1 -Label 'migration local database mode'
        }
        'worker.yaml' {
            $content = Replace-Exact -Content $content -Old $longDatabaseBinding `
                -New ($longDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 1 -Label 'worker local database mode'
        }
        'summary.yaml' {
            $content = Replace-Exact -Content $content -Old $shortDatabaseBinding `
                -New ($shortDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 1 -Label 'summary local database mode'
        }
        'pipeline.yaml' {
            $content = Replace-Exact -Content $content -Old $inlineDatabaseBinding `
                -New ($inlineDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 2 -Label 'pipeline local database mode'
            $consumerMetadata = "metadata:`n  name: agent-consumer`nspec:"
            $consumerMetadataRendered = "metadata:`n  name: agent-consumer`n  annotations:`n    agent.trpc.io/mesh-mtls-evidence: $MeshEvidence`n    agent.trpc.io/validation-scope: local-k3d-only`nspec:"
            $content = Replace-Exact -Content $content -Old $consumerMetadata -New $consumerMetadataRendered `
                -ExpectedCount 1 -Label 'consumer mesh evidence'
            $content = Replace-Exact -Content $content -Old '{name: WORKER_MESH_MTLS_ASSERTED, value: "false"}' `
                -New '{name: WORKER_MESH_MTLS_ASSERTED, value: "true"}' -ExpectedCount 1 -Label 'consumer mesh assertion'
        }
        'admin.yaml' {
            $content = Replace-Exact -Content $content -Old $longDatabaseBinding `
                -New ($longDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 1 -Label 'admin local database mode'
        }
        'gateway.yaml' {
            $content = Replace-Exact -Content $content -Old $longDatabaseBinding `
                -New ($longDatabaseBinding + "`n" + $insecureDatabaseEnv) -ExpectedCount 1 -Label 'gateway local database mode'
        }
    }

    $outputPath = Join-Path $outputRoot $outputName
    [IO.File]::WriteAllText($outputPath, $content, [Text.UTF8Encoding]::new($false))
}

foreach ($supportFile in 'network-policies.yaml', 'k3d-validation-prerequisites.yaml') {
    $sourcePath = Join-Path $templateRoot $supportFile
    $content = [IO.File]::ReadAllText($sourcePath, [Text.Encoding]::UTF8).Replace("`r`n", "`n")
    [IO.File]::WriteAllText((Join-Path $outputRoot $supportFile), $content, [Text.UTF8Encoding]::new($false))
}

$readme = @'
# Enterprise Multi-Tenant Agent Platform K3d validation release

Generated from the checked-in Kubernetes baselines by `scripts/render_k3d_release.ps1`.

- Scope: local three-node K3d evidence only; this is not a production certification.
- Schema class: `__SCHEMA_CLASS__`.
- Images: immutable digests in the in-cluster repository; the recorded registry path is retained as historical lab evidence.
- Transport: Linkerd mesh mode, bound to evidence `__MESH_EVIDENCE__`.
- Storage: isolated in-namespace PostgreSQL and Redis with explicit local-only insecure transport flags.
- Secrets: generated at runtime by `scripts/run_k3d_validation.ps1`; no credential values are stored here.

The eight release inputs are `migration.yaml`, `profiles.yaml`,
`availability-policies.yaml`, `worker.yaml`, `summary.yaml`, `pipeline.yaml`,
`admin.yaml`, and `gateway.yaml`. `network-policies.yaml` is the reviewed policy
input. `k3d-validation-prerequisites.yaml` is local test infrastructure and is
not passed to `releaseverify`.
'@
$readme = $readme.Replace('__SCHEMA_CLASS__', $SchemaClass).Replace('__MESH_EVIDENCE__', $MeshEvidence)
[IO.File]::WriteAllText((Join-Path $outputRoot 'README.md'), $readme.Replace("`r`n", "`n"), [Text.UTF8Encoding]::new($false))

$checksumTargets = Get-ChildItem -LiteralPath $outputRoot -File |
    Where-Object { $_.Name -ne 'SHA256SUMS.txt' } |
    Sort-Object Name
$checksumLines = foreach ($file in $checksumTargets) {
    $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    "$hash  $($file.Name)"
}
[IO.File]::WriteAllLines((Join-Path $outputRoot 'SHA256SUMS.txt'), $checksumLines, [Text.UTF8Encoding]::new($false))

Write-Output "Rendered K3d release: $outputRoot"
Write-Output "SchemaClass=$SchemaClass"
Write-Output "GatewayDigest=$GatewayDigest"
