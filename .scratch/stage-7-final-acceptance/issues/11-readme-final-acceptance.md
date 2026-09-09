# 11: README Final Acceptance

**What to build:** A reviewer can run one documented Stage 7 gate, trace every
README acceptance requirement to evidence, inspect the Chinese deliverables,
and conclude the project without a follow-on implementation stage.

**Blocked by:** 02: Real Model Chat; 09: Bounded Shutdown And Recovery; 10:
Chinese Design Deliverables.

**Status:** resolved

- [x] Chinese `docs/acceptance.md` maps every README requirement and deliverable
  to an automated assertion, document section, or explicitly justified design.
- [x] The gate covers two Gateways, Gateway restart, cross-Gateway fencing,
  remote Worker Tool confirmation, Governance API outage, Tenant isolation, and
  request-correlated recovery assertions.
- [x] Automated model tests use a local OpenAI-compatible fixture; a separately
  invoked live smoke reads `.env.local` without displaying or storing secrets.
- [x] CI publishes evidence tied to commit SHA and Control Plane schema version;
  stale generated result JSON is not committed as proof.
- [x] Formatting, unit, integration, race, lint, build, frontend, end-to-end, and
  documentation gates pass from a clean checkout using documented commands.
- [x] Stage 7 handoff records final status and known non-blocking limitations;

## Comments

README 最终验收由 `./scripts/stage7-acceptance.sh` 完成，真实模型 smoke 独立通过；
已知非阻塞限制集中记录在 `docs/acceptance.md`，没有 Stage 8。
  there is no Stage 8 or deferred acceptance-stage placeholder.
