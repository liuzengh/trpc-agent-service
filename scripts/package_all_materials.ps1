[CmdletBinding()]
param(
    [string]$RepoRoot,
    [Parameter(Mandatory)][string]$WorkspaceRoot,
    [Parameter(Mandatory)][string]$OutputRoot,
    [ValidatePattern('^[0-9]{8}$')][string]$PackageDate = '20260905',
    [string[]]$EvidenceFiles = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$parsedDate = [DateTime]::MinValue
if (-not [DateTime]::TryParseExact($PackageDate, 'yyyyMMdd', [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::None, [ref]$parsedDate)) {
    throw 'PackageDate must be a valid calendar date in yyyyMMdd format'
}
if ([string]::IsNullOrWhiteSpace($RepoRoot)) { $RepoRoot = Join-Path $PSScriptRoot '..' }
$repo = [IO.Path]::GetFullPath($RepoRoot).TrimEnd('\','/')
$workspace = [IO.Path]::GetFullPath($WorkspaceRoot).TrimEnd('\','/')
$output = [IO.Path]::GetFullPath($OutputRoot).TrimEnd('\','/')
$packageName = "enterprise-multi-tenant-agent-platform-$PackageDate"
$inventoryName = "PACKAGE_INVENTORY_$PackageDate.md"
$checksumsName = "SHA256SUMS_$PackageDate.txt"
$encoding = [Text.UTF8Encoding]::new($false)

function Test-Within([string]$Path, [string]$Root) {
    return $Path.Equals($Root, [StringComparison]::OrdinalIgnoreCase) -or
        $Path.StartsWith($Root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-NoReparseAncestor([string]$Path) {
    $current = $Path
    while (-not [string]::IsNullOrWhiteSpace($current)) {
        if (Test-Path -LiteralPath $current) {
            if (((Get-Item -LiteralPath $current -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "package paths cannot traverse a symbolic link or junction: $current"
            }
        }
        $current = [IO.Path]::GetDirectoryName($current)
    }
}

foreach ($path in @($repo, $workspace, $output)) {
    if ($path -eq [IO.Path]::GetPathRoot($path).TrimEnd('\','/')) { throw 'package paths must be directories below the filesystem root' }
    Assert-NoReparseAncestor $path
}
if (-not (Test-Path -LiteralPath (Join-Path $repo 'go.mod') -PathType Leaf)) { throw 'platform source root is not valid' }
if ((Test-Within $workspace $repo) -or (Test-Within $output $repo)) {
    throw 'WorkspaceRoot and OutputRoot must be outside RepoRoot to prevent recursive packaging'
}
$workRoot = Join-Path $workspace 'work'
$backupRoot = Join-Path $workRoot 'package-backups'
Assert-NoReparseAncestor $workRoot
Assert-NoReparseAncestor $backupRoot
if ((Test-Within $workRoot $output) -or (Test-Within $output $workRoot)) {
    throw 'OutputRoot must be separate from WorkspaceRoot/work; staging and backups are not submission materials'
}
foreach ($requiredPath in @('README.md','LICENSE','docs/JUDGE_QUICKSTART.md','docs/COMPETITION_SUBMISSION.md','docs/ACCEPTANCE_EVIDENCE.md')) {
    if (-not (Test-Path -LiteralPath (Join-Path $repo $requiredPath) -PathType Leaf)) { throw "required submission entry is missing: $requiredPath" }
}
New-Item -ItemType Directory -Force -Path $workRoot,$output | Out-Null
$buildRoot = Join-Path $workRoot ('package-build-' + [Guid]::NewGuid().ToString('N'))
$staging = Join-Path $buildRoot $packageName
$candidateZip = Join-Path $buildRoot "$packageName.zip"
$unpack = Join-Path $buildRoot 'fresh-extraction'
$zipPath = Join-Path $output "$packageName.zip"
New-Item -ItemType Directory -Path $staging -Force | Out-Null

$included = [Collections.Generic.List[string]]::new()
$blockedDirectories = @('.git','.idea','.vscode','node_modules','runtime','volumes','cache','caches','tmp','temp','logs','bin','dist','coverage','archive','outputs')
$blockedExtensions = @('.log','.out','.db','.sqlite','.sqlite3','.pem','.key','.p12','.pfx','.crt','.cer','.der','.exe','.dll','.so','.zip','.7z','.tar','.gz','.tgz','.bz2','.xz')
$excludedPaths = @('docs/archive/*','archive/*','deploy/kubernetes/releases/*','deploy/kubernetes/k3d-validation-prerequisites.yaml','scripts/test_local_capacity.ps1','scripts/*k3d*','scripts/test_vault_dev_identity.ps1')

function Test-SkippedPath([string]$Relative, [IO.FileSystemInfo]$Item) {
    foreach ($part in ($Relative -split '[/\\]')) {
        if ($blockedDirectories -contains $part.ToLowerInvariant()) { return $true }
    }
    foreach ($pattern in $excludedPaths) { if ($Relative -like $pattern) { return $true } }
    if ($Item.Name -eq '.env' -or ($Item.Name -like '.env.*' -and $Item.Name -ne '.env.example')) { return $true }
    if ($Item.Name -in @('.DS_Store','.audit-errors.txt')) { return $true }
    if ($Item -is [IO.FileInfo] -and $blockedExtensions -contains $Item.Extension.ToLowerInvariant()) { return $true }
    return $false
}

function Copy-SourceDirectory([string]$Directory) {
    foreach ($item in Get-ChildItem -LiteralPath $Directory -Force) {
        $relative = $item.FullName.Substring($repo.Length).TrimStart('\','/').Replace('\','/')
        if (Test-SkippedPath $relative $item) { continue }
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "source contains a symbolic link or junction; review it before packaging: $relative"
        }
        if ($item.PSIsContainer) { Copy-SourceDirectory $item.FullName; continue }
        $target = Join-Path $staging ("platform-source/$relative")
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
        Copy-Item -LiteralPath $item.FullName -Destination $target
        $included.Add("platform-source/$relative")
    }
}

function Assert-PortableShellScripts([string]$Root) {
    foreach ($file in Get-ChildItem -LiteralPath $Root -Recurse -File -Force | Where-Object { $_.Extension.ToLowerInvariant() -eq '.sh' }) {
        $relative = $file.FullName.Substring($Root.Length).TrimStart('\','/').Replace('\','/')
        $bytes = [IO.File]::ReadAllBytes($file.FullName)
        if ([Array]::IndexOf($bytes, [byte]13) -ge 0) { throw "shell entrypoint must use LF line endings: $relative" }
        if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
            throw "shell entrypoint must not have a UTF-8 BOM: $relative"
        }
    }
}

Copy-SourceDirectory $repo
Assert-PortableShellScripts $staging
$evidenceNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
foreach ($evidencePath in $EvidenceFiles) {
    $resolvedEvidence = [IO.Path]::GetFullPath($evidencePath)
    Assert-NoReparseAncestor $resolvedEvidence
    if (-not (Test-Path -LiteralPath $resolvedEvidence -PathType Leaf)) { throw "requested evidence file is missing: $resolvedEvidence" }
    $file = Get-Item -LiteralPath $resolvedEvidence
    if ($file.Extension.ToLowerInvariant() -notin @('.log','.txt','.json','.md')) { throw 'evidence must be a text log, JSON or Markdown file' }
    if (-not $evidenceNames.Add($file.Name)) { throw "duplicate evidence filename: $($file.Name)" }
    $relative = "verification-evidence/$($file.Name)"
    $target = Join-Path $staging $relative
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
    Copy-Item -LiteralPath $resolvedEvidence -Destination $target
    $included.Add($relative)
}

$entryReadme = @(
    '# Enterprise Multi-Tenant Agent Platform', '',
    '- Start: [Project README](platform-source/README.md).',
    '- Run: [Judge Quickstart](platform-source/docs/JUDGE_QUICKSTART.md).',
    '- Design and scope: [Competition Submission](platform-source/docs/COMPETITION_SUBMISSION.md).',
    '- Verification: [Acceptance Evidence](platform-source/docs/ACCEPTANCE_EVIDENCE.md).',
    "- File inventory: [$inventoryName]($inventoryName).",
    "- SHA-256 checksums: [$checksumsName]($checksumsName)."
)
if ($EvidenceFiles.Count -gt 0) { $entryReadme += '- Validation logs: `verification-evidence/`.' }
[IO.File]::WriteAllLines((Join-Path $staging 'README.md'), $entryReadme, $encoding)
$inventory = @(
    '# Package Inventory', '',
    'Project: Enterprise Multi-Tenant Agent Platform',
    "Packaged: $([DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ'))",
    "Source and evidence files: $($included.Count)", '',
    'The source tree contains code, tests, migrations, deployment templates, CI, licenses and current documentation.',
    'Runtime data, credentials, generated deployment snapshots and archived material are excluded.', '',
    '## Files', ''
)
$inventory += @($included | Sort-Object | ForEach-Object { '- `' + $_ + '`' })
[IO.File]::WriteAllLines((Join-Path $staging $inventoryName), $inventory, $encoding)

# Allow only the exact fake keys asserted by the redaction regression test.
$secretRegex = '-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----|sk-(proj-)?[A-Za-z0-9_-]{24,}|gh[pousr]_[A-Za-z0-9]{30,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{20,}'
$fakeKeys = @(('sk-' + 'secret-test-key-12345678901234567890'),('sk-' + 'very-secret-api-key-should-not-leak'))
foreach ($file in Get-ChildItem -LiteralPath $staging -Recurse -File -Force) {
    $relative = $file.FullName.Substring($staging.Length).TrimStart('\','/').Replace('\','/')
    foreach ($match in [regex]::Matches([IO.File]::ReadAllText($file.FullName), $secretRegex)) {
        if ($relative -eq 'platform-source/pkg/tenant/secret_leakage_test.go' -and $fakeKeys -contains $match.Value) { continue }
        throw "secret-signature scan failed: $relative"
    }
}

$sumLines = [Collections.Generic.List[string]]::new()
foreach ($file in Get-ChildItem -LiteralPath $staging -Recurse -File -Force | Sort-Object FullName) {
    $relative = $file.FullName.Substring($staging.Length).TrimStart('\','/').Replace('\','/')
    $sumLines.Add("$((Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant())  $relative")
}
[IO.File]::WriteAllLines((Join-Path $staging $checksumsName), $sumLines, $encoding)

Add-Type -AssemblyName System.IO.Compression.FileSystem
[IO.Compression.ZipFile]::CreateFromDirectory($staging, $candidateZip, [IO.Compression.CompressionLevel]::Optimal, $true)
$zip = [IO.Compression.ZipFile]::Open($candidateZip, [IO.Compression.ZipArchiveMode]::Update)
try {
    foreach ($entry in $zip.Entries) {
        if ($entry.FullName.EndsWith('/')) { continue }
        $mode = 33188
        if ($entry.FullName.EndsWith('.sh', [StringComparison]::OrdinalIgnoreCase)) { $mode = 33261 }
        $entry.ExternalAttributes = $mode -shl 16
    }
} finally { $zip.Dispose() }
# Windows ZIP writers mark the archive as DOS-origin; readers then ignore Unix
# executable bits. Set the central-directory creator platform without changing payloads.
$stream = [IO.File]::Open($candidateZip, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite)
$reader = [IO.BinaryReader]::new($stream, $encoding, $true)
$writer = [IO.BinaryWriter]::new($stream, $encoding, $true)
try {
    if ($stream.Length -ge [uint32]::MaxValue) { throw 'submission ZIP exceeds the supported 4 GiB limit' }
    $stream.Position = $stream.Length - 22
    if ($reader.ReadUInt32() -ne 0x06054b50) { throw 'unexpected ZIP end-of-directory record' }
    if ($reader.ReadUInt32() -ne 0) { throw 'multi-disk ZIP is unsupported' }
    $diskEntries = $reader.ReadUInt16()
    $entryCount = $reader.ReadUInt16()
    if ($entryCount -eq [uint16]::MaxValue -or $entryCount -ne $diskEntries) { throw 'ZIP64 or multi-disk directory is unsupported' }
    $directorySize = $reader.ReadUInt32()
    $directoryOffset = $reader.ReadUInt32()
    if ([long]$directoryOffset + $directorySize -ne $stream.Length - 22) { throw 'unexpected ZIP central-directory bounds' }
    $position = [long]$directoryOffset
    for ($index = 0; $index -lt $entryCount; $index++) {
        $stream.Position = $position
        if ($reader.ReadUInt32() -ne 0x02014b50) { throw 'invalid ZIP central-directory entry' }
        $stream.Position = $position + 5
        $writer.Write([byte]3)
        $stream.Position = $position + 28
        $nameLength = $reader.ReadUInt16()
        $extraLength = $reader.ReadUInt16()
        $commentLength = $reader.ReadUInt16()
        $position += 46 + $nameLength + $extraLength + $commentLength
    }
    if ($position -ne $stream.Length - 22) { throw 'ZIP central-directory entry count mismatch' }
} finally { $writer.Dispose(); $reader.Dispose(); $stream.Dispose() }
[IO.Compression.ZipFile]::ExtractToDirectory($candidateZip, $unpack)
$unpackedRoot = Join-Path $unpack $packageName
Assert-PortableShellScripts $unpackedRoot
$expected = @(Get-ChildItem -LiteralPath $staging -Recurse -File -Force | ForEach-Object { $_.FullName.Substring($staging.Length).TrimStart('\','/').Replace('\','/') } | Sort-Object)
$actual = @(Get-ChildItem -LiteralPath $unpackedRoot -Recurse -File -Force | ForEach-Object { $_.FullName.Substring($unpackedRoot.Length).TrimStart('\','/').Replace('\','/') } | Sort-Object)
if (@(Compare-Object $expected $actual).Count -ne 0) { throw 'fresh extraction file inventory differs from staging' }
foreach ($relative in $expected) {
    $before = Get-FileHash -LiteralPath (Join-Path $staging $relative) -Algorithm SHA256
    $after = Get-FileHash -LiteralPath (Join-Path $unpackedRoot $relative) -Algorithm SHA256
    if ($before.Hash -ne $after.Hash) { throw "fresh extraction hash mismatch: $relative" }
}
$zipHash = (Get-FileHash -LiteralPath $candidateZip -Algorithm SHA256).Hash.ToLowerInvariant()

# The existing submission stays intact until all verification gates pass.
$backupPath = $null
if (Test-Path -LiteralPath $zipPath) {
    if (-not (Test-Path -LiteralPath $zipPath -PathType Leaf)) { throw 'the destination ZIP path is not a file' }
    Assert-NoReparseAncestor $zipPath
    New-Item -ItemType Directory -Force -Path $backupRoot | Out-Null
    $backupPath = Join-Path $backupRoot ("$packageName-" + [Guid]::NewGuid().ToString('N') + '.zip')
    Copy-Item -LiteralPath $zipPath -Destination $backupPath
    if ((Get-FileHash -LiteralPath $zipPath).Hash -ne (Get-FileHash -LiteralPath $backupPath).Hash) { throw 'previous submission backup failed verification' }
}
try {
    Copy-Item -LiteralPath $candidateZip -Destination $zipPath -Force
    if ((Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $zipHash) { throw 'published ZIP hash mismatch' }
} catch {
    if ($null -ne $backupPath) { Copy-Item -LiteralPath $backupPath -Destination $zipPath -Force }
    throw
}
$verification = @(
    "package=$packageName", "zip_sha256=$zipHash", "files=$($expected.Count)",
    'fresh_extract=PASS', 'secret_signature_scan=PASS', 'shell_executable_modes=0755', 'shell_line_endings=LF',
    'real_env_files=EXCLUDED', 'runtime_data=EXCLUDED', 'archived_material=EXCLUDED',
    "evidence_files=$($EvidenceFiles.Count)", "zip=$zipPath"
)
[IO.File]::WriteAllLines((Join-Path $output "PACKAGE_BUILD_RESULT_$PackageDate.txt"), $verification, $encoding)
Write-Output ($verification -join [Environment]::NewLine)
