$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$projectName = "trpc-agent-service-quickstart"
$composeArgs = @(
    "--project-name", $projectName,
    "--env-file", ".env.example",
    "--file", "compose.yaml",
    "--file", "compose.deployment-e2e.yaml",
    "--file", "compose.quickstart.yaml"
)

function Invoke-QuickstartCompose {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]] $CommandArgs
    )

    & docker compose @composeArgs @CommandArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

Push-Location $repoRoot
try {
    Invoke-QuickstartCompose config --quiet
    try {
        Invoke-QuickstartCompose down --volumes --remove-orphans
    } catch {
        # A first run may not have a Compose project to remove yet.
    }

    Invoke-QuickstartCompose up -d --build --wait postgres redis qdrant gateway worker-1 worker-2
    Invoke-QuickstartCompose up --build --abort-on-container-exit --exit-code-from quickstart quickstart

    Write-Host "Golden Path PASSED"
} finally {
    try {
        & docker compose @composeArgs down --volumes --remove-orphans *> $null
    } catch {
        # Cleanup must not mask the Golden Path exit code.
    }
    Pop-Location
}
