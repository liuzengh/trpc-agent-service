# Phase 7 Compose Runbook

Run `scripts/phase7/up.ps1 -Mode full` for the default topology. The first run
copies `compose/.env.example` to `compose/.env` and stops until values are
reviewed. `light` starts only Redis-backed tenant traffic; `obs` and `ha` add
local acceptance services. Every command uses project name `trpc-phase7`.

Use `sql-init -kind all` once and `sql-ready -kind all` on subsequent starts.
Never use `down -v` while testing persistence. `down.ps1` leaves named volumes
intact unless `-Volumes` is explicitly supplied.

For a minimal real WeCom + DeepSeek run, set `IDENTITY_SECRET` and either the
`PHASE7_*` credentials or the existing compatible `PHASE5_*` credentials, then
run `scripts/phase7/real-wecom.ps1 -Action Start`. The dedicated Compose project
contains only Redis, Gateway and Worker. Use `-Action Status` to inspect it and
`-Action Stop` to stop it without deleting its volume. Credential values remain
in process/container environment and are never written to a generated file.

For a minimal real Feishu + DeepSeek run, set `IDENTITY_SECRET`,
`PHASE7_FEISHU_APP_ID`, `PHASE7_FEISHU_APP_SECRET`, and either
`PHASE7_REAL_MODEL_KEY` or a compatible existing model-key variable. Run
`scripts/phase7/real-feishu.ps1 -Action Start`. This uses the independent
Compose project `trpc-phase7-real-feishu`, Gateway port `19080`, Worker port
`19081`, and a separate Redis volume, so it can run beside the real WeCom
project. Feishu uses the official long connection and therefore needs no
public callback URL. Before starting, enable bot capability, subscribe to
`im.message.receive_v1` using long connection, grant `im:message:send_as_bot`,
publish the app version, and include the test user in its availability scope.

Both real-IM scripts read credentials from the current PowerShell Process
environment. After setting or changing Windows User environment variables,
open a new PowerShell session or explicitly reload each changed variable into
Process before starting. An existing Process value takes precedence over the
compatible fallback variables. The Feishu script prints only the selected App
ID; check it against the intended app in the developer console. Never print
or copy App Secret or model-key values into files or logs. A successful
`/readyz` response alone does not prove that the intended App ID is connected.
