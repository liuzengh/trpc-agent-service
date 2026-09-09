[CmdletBinding()]
param([string]$TestRoot = [IO.Path]::GetTempPath())

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Join-Path ([IO.Path]::GetFullPath($TestRoot)) ('package-test-' + [Guid]::NewGuid().ToString('N'))
$repo = Join-Path $root 'source'
$workspace = Join-Path $root 'workspace'
$output = Join-Path $workspace 'outputs'
$script = Join-Path $PSScriptRoot 'package_all_materials.ps1'
$encoding = [Text.UTF8Encoding]::new($false)

function Write-Fixture([string]$Relative, [string]$Content) {
    $path = Join-Path $root $Relative
    New-Item -ItemType Directory -Path (Split-Path -Parent $path) -Force | Out-Null
    [IO.File]::WriteAllText($path, $Content, $encoding)
}

function Expect-Failure([scriptblock]$Action, [string]$Message) {
    $failed = $false
    try { & $Action | Out-Null } catch { $failed = $true }
    if (-not $failed) { throw $Message }
}

foreach ($path in @('go.mod','README.md','LICENSE','docs/JUDGE_QUICKSTART.md','docs/COMPETITION_SUBMISSION.md','docs/ACCEPTANCE_EVIDENCE.md','cmd/demo/main.go','pkg/example_test.go','migrations/001.sql','deploy/kubernetes/gateway.yaml','.github/workflows/ci.yml','.env.example','scripts/check.sh')) {
    Write-Fixture "source/$path" "fixture $path"
}
# Package the real offline provider fixture through the actual secret scanner.
# Synthetic text-only package inputs cannot detect credential-shaped literals
# accidentally introduced in an integration/bootstrap contract test.
$bootstrapContracts = [ordered]@{}
foreach ($name in @('test_telegram_local_bootstrap.ps1', 'test_wecom_bot_local_bootstrap.ps1')) {
    $bootstrapContracts[$name] = [IO.File]::ReadAllText((Join-Path $PSScriptRoot $name))
    Write-Fixture "source/scripts/$name" $bootstrapContracts[$name]
}
foreach ($path in @('docs/archive/old.md','archive/old.md','deploy/kubernetes/releases/old/gateway.yaml','deploy/kubernetes/k3d-validation-prerequisites.yaml','scripts/test_local_capacity.ps1','scripts/render_k3d_release.ps1','scripts/test_vault_dev_identity.ps1','.env','.env.private','.git/config','runtime/db/data','outputs/old.md')) {
    Write-Fixture "source/$path" 'excluded fixture'
}
Write-Fixture 'workspace/outputs/validation-old.log' 'historical output must not be selected'
Write-Fixture 'current-validation.log' 'fixture validation=PASS'
Write-Fixture 'source/pkg/tenant/secret_leakage_test.go' ('sk-' + 'secret-test-key-12345678901234567890')
$portableShell = "#!/bin/sh`nset -eu`nprintf 'fixture-shell=PASS\n'`n"
Write-Fixture 'source/scripts/check.sh' $portableShell
$evidence = Join-Path $root 'current-validation.log'
& $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output -EvidenceFiles @($evidence) | Out-Null
$zipPath = Join-Path $output 'enterprise-multi-tenant-agent-platform-20260905.zip'
$firstHash = (Get-FileHash -LiteralPath $zipPath).Hash
$zip = [IO.Compression.ZipFile]::OpenRead($zipPath)
try {
    $names = @($zip.Entries | ForEach-Object { $_.FullName })
    foreach ($required in @('/platform-source/.github/workflows/ci.yml','/platform-source/.env.example','/platform-source/LICENSE','/platform-source/migrations/001.sql','/platform-source/cmd/demo/main.go','/verification-evidence/current-validation.log')) {
        if (-not @($names | Where-Object { $_.EndsWith($required) }).Count) { throw "missing required file: $required" }
    }
    if (@($names | Where-Object { $_ -match '/archive/|/releases/|k3d|test_local_capacity|test_vault_dev_identity|/\.git/|/runtime/|/outputs/|validation-old|/\.env(?:\.(?!example)[^/]+)?$' }).Count) {
        throw 'excluded material appeared in fixture archive'
    }
    $shellEntry = @($zip.Entries | Where-Object { $_.FullName.EndsWith('/scripts/check.sh') })[0]
    if (($shellEntry.ExternalAttributes -shr 16 -band 511) -ne 493) { throw 'shell executable mode was not recorded' }
    $shellReader = [IO.StreamReader]::new($shellEntry.Open())
    try { $archivedShell = $shellReader.ReadToEnd() } finally { $shellReader.Dispose() }
    if ($archivedShell -cne $portableShell) { throw 'packaging changed the shell source bytes' }
    foreach ($contract in $bootstrapContracts.GetEnumerator()) {
        $bootstrapEntry = @($zip.Entries | Where-Object { $_.FullName.EndsWith('/scripts/' + $contract.Key) })[0]
        $bootstrapReader = [IO.StreamReader]::new($bootstrapEntry.Open())
        try { $archivedBootstrap = $bootstrapReader.ReadToEnd() } finally { $bootstrapReader.Dispose() }
        if ($archivedBootstrap -cne $contract.Value) { throw 'packaging changed or omitted the real bootstrap contract fixture' }
    }
} finally { $zip.Dispose() }
if (Get-Command tar -ErrorAction SilentlyContinue) {
    $listing = @(tar -tvf $zipPath)
    if ($LASTEXITCODE -ne 0 -or -not @($listing | Where-Object { $_ -match '^-rwxr-xr-x\s.*?/scripts/check\.sh$' }).Count) {
        throw 'ZIP reader did not recognize Unix shell executable permissions'
    }
}

$invalidShells = [ordered]@{
    crlf = $portableShell.Replace("`n", "`r`n")
    mixed = "#!/usr/bin/env bash`nset -euo pipefail`r`nprintf 'mixed'`n"
    bare_cr = "#!/usr/bin/env bash`nprintf 'invalid`rbody'`n"
    utf8_bom = ([char]0xFEFF + $portableShell)
}
foreach ($case in $invalidShells.GetEnumerator()) {
    Write-Fixture 'source/scripts/check.sh' $case.Value
    $invalidHash = (Get-FileHash -LiteralPath (Join-Path $repo 'scripts/check.sh')).Hash
    Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output } "packaging accepted $($case.Key) shell encoding"
    if ((Get-FileHash -LiteralPath $zipPath).Hash -ne $firstHash) { throw 'invalid shell encoding modified the previous submission' }
    if ((Get-FileHash -LiteralPath (Join-Path $repo 'scripts/check.sh')).Hash -ne $invalidHash) { throw 'packaging silently rewrote invalid source encoding' }
}
Write-Fixture 'source/scripts/check.sh' $portableShell

Write-Fixture 'source/credential.txt' ('sk-' + ('A' * 32))
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output } 'secret scan accepted a fixture credential'
if ((Get-FileHash -LiteralPath $zipPath).Hash -ne $firstHash) { throw 'failed packaging modified the previous submission' }
Write-Fixture 'source/credential.txt' 'sanitized fixture'
Write-Fixture 'source/pkg/tenant/secret_leakage_test.go' ('sk-' + ('B' * 32))
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output } 'fake-key exception bypassed scanning the whole test file'
if ((Get-FileHash -LiteralPath $zipPath).Hash -ne $firstHash) { throw 'failed test-file scan modified the previous submission' }
Write-Fixture 'source/pkg/tenant/secret_leakage_test.go' ('sk-' + 'secret-test-key-12345678901234567890')
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $repo -OutputRoot $output } 'accepted a workspace inside source'
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot (Join-Path $repo 'output') } 'accepted output inside source'
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $workspace } 'accepted output containing staging and backups'
Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output -EvidenceFiles @($evidence,$evidence) } 'accepted duplicate evidence names'

& $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output -EvidenceFiles @($evidence) | Out-Null
$backups = @(Get-ChildItem -LiteralPath (Join-Path $workspace 'work/package-backups') -File)
if ($backups.Count -ne 1 -or (Get-FileHash -LiteralPath $backups[0].FullName).Hash -ne $firstHash) { throw 'previous submission backup did not preserve its original contents' }
if ((Get-FileHash -LiteralPath $zipPath).Hash -eq $firstHash) { throw 'new verified submission did not replace the prior archive' }
$currentHash = (Get-FileHash -LiteralPath $zipPath).Hash
foreach ($invalidDate in @('2026096','../20260906','20260230')) {
    Expect-Failure { & $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $output -PackageDate $invalidDate } "accepted invalid package date: $invalidDate"
    if ((Get-FileHash -LiteralPath $zipPath).Hash -ne $currentHash) { throw 'invalid package date modified the previous submission' }
}
$datedOutput = Join-Path $workspace 'dated-output'
& $script -RepoRoot $repo -WorkspaceRoot $workspace -OutputRoot $datedOutput -PackageDate '20260906' -EvidenceFiles @($evidence) | Out-Null
$datedName = 'enterprise-multi-tenant-agent-platform-20260906'
$datedZipPath = Join-Path $datedOutput ($datedName + '.zip')
if (-not (Test-Path -LiteralPath (Join-Path $datedOutput 'PACKAGE_BUILD_RESULT_20260906.txt') -PathType Leaf)) { throw 'build result did not use the requested package date' }
$datedZip = [IO.Compression.ZipFile]::OpenRead($datedZipPath)
try {
    foreach ($required in @('README.md','PACKAGE_INVENTORY_20260906.md','SHA256SUMS_20260906.txt')) {
        if ($null -eq $datedZip.GetEntry("$datedName/$required")) { throw "dated package is missing $required" }
    }
    if (@($datedZip.Entries | Where-Object { $_.FullName -match '20260905' }).Count) { throw 'dated package retained a different date in its entry names' }
} finally { $datedZip.Dispose() }
Write-Output "package fixture checks=PASS; fixtures=$root"
