# 07: Stage 2 Data Management Console

**What to build:** Extend the existing progressive Management Console with backend selection and health, Session/Event inspection, Memory/Summary status, and migration job creation, progress, and result views. The frontend uses the backend as the source of truth and supports the expected loading, empty, error, and authorization states.

**Blocked by:** 05: Tenant Backend Routing And Cross-Node Consistency; 06: Repeatable Redis-To-SQL Migration

**Status:** resolved

- [x] Authorized users can view backend selection and health for the active Tenant; unauthorized mutations are rejected by the backend.
- [x] Users can inspect ordered Session Events, materialized Session state, Summary, and Memory without storing business data in browser storage.
- [x] Users can create migration jobs and observe dry-run/active/completed/failed progress and validation results.
- [x] Loading, empty, unavailable, error, and stale-response states are visible and do not overlap on desktop or mobile layouts.
- [x] Frontend unit/component, API contract, typecheck/build, and Playwright tests cover the new data-management workflows.

## Answer

Extended the Stage 1 frontend with backend health/selection, Session/Event inspection, Memory/Summary status, dry-run/execute migration progress/results, role controls, stale-response protection, and desktop/mobile coverage.
