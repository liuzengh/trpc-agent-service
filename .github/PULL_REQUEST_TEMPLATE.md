<!-- Suggested title: package: lowercase summary -->
<!-- Multiple primary packages: {package/a, package/b}: lowercase summary -->

<!-- Use "Fixes #123" when this PR completes an issue; otherwise use "Updates #123". -->

## What changed

<!-- Summarize the implementation and link the issue or design proposal. -->

## Why

<!-- Explain the problem and why this approach is appropriate. -->

## Scope and compatibility

- API or configuration changes:
- Data/schema or migration changes:
- Deployment and rollout considerations:
- Backward compatibility and failure behavior:

## Testing

- [ ] `./scripts/format.sh --check`
- [ ] `./scripts/lint.sh`
- [ ] `./scripts/build.sh`
- [ ] `go test ./...` or `./scripts/coverage.sh`
- [ ] Unit tests added or updated
- [ ] Integration or end-to-end tests run where relevant
- [ ] Race tests run for concurrency-sensitive packages
- [ ] Manual verification performed (describe below)
- [ ] git diff --check passes

Test details:

## Security and tenant isolation

- [ ] Authorization and tenant boundaries were reviewed
- [ ] Logs and errors do not expose secrets or user/customer data
- [ ] External integrations and tool permissions were reviewed

## Notes for reviewers

<!-- Call out risky areas, follow-up work, migrations, or decisions needing review. -->
