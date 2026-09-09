$ErrorActionPreference='Stop'
for ($pass=1; $pass -le 2; $pass++) {
  Get-ChildItem -LiteralPath $PSScriptRoot -Filter 'F*.ps1' | Sort-Object Name | ForEach-Object {
    Write-Host "Pass $pass running $($_.Name)"
    & $_.FullName
  }
}
Write-Host 'Phase 7 fault matrix passed twice.'
