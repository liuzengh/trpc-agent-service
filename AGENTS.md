# AGENTS.md

## Goal

Implement requirements as the **smallest complete end-to-end system**.

Always prioritize:

```text
Requirements
→ System correctness and invariants
→ Framework capabilities
→ Existing implementation
```

Existing code is a reference, not a requirement.

------

## Do

- Understand the required external behavior before modifying code.
- Implement the complete required path, not an isolated local change.
- Prefer the smallest change that fully satisfies the requirement.
- Reuse existing project or framework capabilities when their semantics match.
- Keep each responsibility owned by one clear layer.
- Preserve security, isolation, idempotency, transaction and concurrency guarantees.
- Remove code that becomes obsolete after a design is replaced.
- Test important system contracts and real failure boundaries.
- Validate the complete affected path before finishing.

------

## Do Not

- Do not design from the current implementation backward.
- Do not preserve legacy APIs, configs, defaults or internal behavior unless compatibility is explicitly required.
- Do not keep old and new implementations in parallel without a real requirement.
- Do not add abstractions, interfaces, state machines, workers or tables for hypothetical future needs.
- Do not duplicate validation or business rules across multiple layers.
- Do not introduce wrappers that only forward calls without owning a real responsibility.
- Do not expand the task into unrelated refactoring.
- Do not use TODOs, stubs or fake-success paths for required functionality.
- Do not treat existing code, tests or callers as proof that a design is necessary.

------

## Complete-Flow Principle

For every requirement, identify the minimum complete path needed to make the behavior real.

Typical paths may involve:

```text
Ingress
→ Identity / Scope
→ Business Logic
→ Persistence / Transaction
→ Execution
→ Result / Side Effect
```

Not every feature needs every layer.

Modify only the layers required by the feature, but ensure all required layers are connected and executable.

------

## Responsibility Principle

Keep one authoritative owner for each rule.

- Transport/Adapter: protocol parsing, authentication, transport behavior.
- Service/Domain: business rules, authorization and state transitions.
- Repository: persistence, queries and transactions.
- Worker: work execution, concurrency, cancellation and completion.
- Runtime: framework/runtime resource construction and lifecycle.

Do not repeat the same responsibility in multiple layers.

------

## Required Invariants

Never weaken:

- Tenant isolation
- Application isolation
- Session isolation
- Trusted identity and scope derivation
- Idempotency
- Ordering
- Multi-node concurrency safety
- Configuration version consistency
- Transaction boundaries
- Outbox consistency where required
- Secret and credential protection
- Cancellation and resource ownership
- External protocol authentication and integrity checks

------

## Design Principle

Prefer simple concrete implementations.

Introduce a new abstraction only when it represents a real independent responsibility shared by actual use cases.

Do not build infrastructure merely because it may be useful later.

When replacing a design, remove the superseded path unless backward compatibility is an explicit requirement.

------

## Testing

Test contracts, not implementation details.

Prioritize tests for:

- security;
- isolation;
- idempotency;
- transactions;
- concurrency;
- ordering;
- cancellation;
- external protocols;
- real backend behavior.

Avoid tests that only verify trivial helpers, getters, constructors, wrappers or implementation details unless they protect an important contract.

------

## Completion

Before declaring a task complete:

1. confirm the requested behavior works end to end;
2. confirm required invariants still hold;
3. confirm no unnecessary parallel or legacy path remains;
4. confirm no dead code was introduced or left behind;
5. run:

```text
go build ./...
go test ./...
go vet ./...
git diff --check
```

Prefer a smaller complete implementation over a larger flexible design.
