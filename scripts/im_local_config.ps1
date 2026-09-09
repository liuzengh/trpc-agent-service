# Shared local-only validation. Runtime model transport performs DNS and IP
# enforcement; this helper never resolves a host or contacts an IM provider.
function Read-IMLocalConfig([string]$Path) {
    $root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    $full = [IO.Path]::GetFullPath($Path)
    if ($full.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Credential JSON must be stored outside the source repository.'
    }
    if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { throw 'Blocked: local credential JSON is missing.' }
    try { $value = Get-Content -LiteralPath $full -Raw | ConvertFrom-Json -AsHashtable }
    catch { throw 'Credential JSON could not be parsed.' }
    if ($value -isnot [Collections.IDictionary]) { throw 'Credential JSON must be an object.' }
    return $value
}

function Assert-IMModelBaseURL([string]$Value) {
    if ($Value -eq '') { return }
    $uri = $null
    if ($Value -match '[\s\x00-\x1f\x7f\\%?#]' -or
        -not [Uri]::TryCreate($Value, [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -cne 'https' -or -not $uri.Host -or $uri.UserInfo -or
        $uri.Query -or $uri.Fragment -or $uri.Port -lt 1 -or $uri.Port -gt 65535 -or
        $Value -notmatch '^https://[^/]+(?:/[A-Za-z0-9._~/-]*)?\z') {
        throw 'TRPC_OPENAI_BASE_URL must be an HTTPS base URL without credentials, query, fragment or encoded path.'
    }
    $authority = ($Value -replace '^https://', '').Split('/')[0]
    $rawPath = $Value.Substring(('https://' + $authority).Length)
    if ($authority.EndsWith(':') -or $rawPath.Contains('//') -or @($rawPath.Split('/') | Where-Object { $_ -in @('.', '..') }).Count -gt 0) {
        throw 'TRPC_OPENAI_BASE_URL contains an invalid authority or path.'
    }
}

function Assert-IMProxy([string]$Value) {
    if ($Value -eq '') { return }
    $uri = $null
    if ($Value -match '[\s\x00-\x1f\x7f\\?#]' -or
        -not [Uri]::TryCreate($Value, [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -notin @('http','https') -or -not $uri.Host -or $uri.UserInfo -or
        $uri.Query -or $uri.Fragment -or $uri.AbsolutePath -ne '/' -or $uri.Port -lt 1 -or $uri.Port -gt 65535) {
        throw 'Proxy must be an HTTP(S) origin without credentials, a path or query.'
    }
}

function Assert-IMModelKey([string]$Value) {
    if ($Value.Length -lt 16 -or $Value.Length -gt 8192 -or $Value -match '[^\x21-\x7e]') {
        throw 'Blocked: an OpenAI API key is required; use a nonempty printable credential without whitespace.'
    }
}
