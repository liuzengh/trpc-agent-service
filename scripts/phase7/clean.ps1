param([switch]$Volumes)
& (Join-Path $PSScriptRoot 'down.ps1') -Volumes:$Volumes
if ($Volumes) { Write-Host 'Phase 7 named volumes removed.' }
