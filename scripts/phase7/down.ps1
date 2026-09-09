param([switch]$Volumes)
$ErrorActionPreference='Stop'; $root=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path; Push-Location $root
try {
  $args=@('--project-name','trpc-phase7','--env-file','compose/.env','--profile','full','--profile','light','--profile','obs','--profile','ha','-f','compose/docker-compose.yml','-f','compose/docker-compose.obs.yml','-f','compose/docker-compose.ha.yml','down','--remove-orphans')
  if ($Volumes) {$args += '-v'}
  docker compose @args
  if ($LASTEXITCODE -ne 0) { throw 'docker compose down failed' }
} finally { Pop-Location }
