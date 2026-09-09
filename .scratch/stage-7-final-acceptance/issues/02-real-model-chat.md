# 02: Real Model Chat

**What to build:** A tenant administrator can publish a Deployment Version that
references the server-owned `default-openai` Model Provider Profile, then obtain
an OpenAI-compatible streaming model response through Chat Workspace.

**Blocked by:** 01: Single Gateway Durable Restart.

**Status:** resolved

- [x] Deployment Version persists provider, model, prompt, and generation
  configuration but never stores credentials.
- [x] AgentFactory constructs the Framework Runtime agent from the published
  immutable Deployment Version and the server-owned provider profile.
- [x] When all three `.env.local` values are present, an explicit live smoke
  uses `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL` without printing
  or persisting their values.
- [x] Development mode has a deterministic fallback when live model settings
  are absent; production startup fails when required model settings are absent.
- [x] Automated tests use a local OpenAI-compatible fixture and verify the
  existing SSE contract through the public chat workflow.
- [x] The ticket documents and runs its own model-chat acceptance command.

## Comments

自动化链路由 `./scripts/stage7-acceptance.sh` 验证；真实模型链路由
`./scripts/stage7-live-model-smoke.sh` 验证，命令不会输出或持久化凭据。
