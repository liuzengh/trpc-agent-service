# How to Contribute

Thank you for your interest in contributing to trpc-agent-service.

We welcome bug fixes, features, tests, documentation, examples, design
improvements, and other contributions. Read this guide before submitting a
change so that proposals, implementations, and reviews can proceed
consistently.

## Before contributing code

### Start with the issue tracker

Except for trivial changes, contributions should be associated with an existing
issue or a newly opened issue.

Starting with an issue allows maintainers and contributors to agree on the
problem, scope, acceptance criteria, and high-level design before implementation
begins. It also helps prevent duplicate work and avoids moving architectural
discussions into code review.

Use the repository's issue templates when reporting a bug, requesting a
feature, or proposing a design. Do not begin implementation while important
design or scope questions remain unresolved.

### Report bugs with enough context

A bug report should answer the following questions:

1. Which commit or version of trpc-agent-service are you using?
2. Which Go version, operating system, and processor architecture are you using?
3. Which deployment mode, storage backend, and IM integration are involved?
4. What did you do?
5. What did you expect to happen?
6. What happened instead?

Include the relevant output of `go env` when environment details may affect the
problem. Remove credentials, tokens, personal data, and customer content from
logs, traces, configuration, and examples.

### Discuss significant changes first

Significant features, framework abstractions, public API changes, persistence
changes, protocol changes, tenant-isolation changes, and migration behavior
should be discussed before implementation.

High-level design should be settled in an issue or design proposal. Code review
should verify and refine an agreed design, not become the first place where the
design is considered.

## Contributing code

Follow [GitHub flow](https://docs.github.com/en/get-started/using-github/github-flow)
and submit changes through a pull request. Keep each pull request focused on one
coherent outcome and avoid unrelated refactoring.

### Language

Pull request titles and descriptions must be written in English.

Source-code comments and Go documentation comments must also be written in
English. Exceptions include translated documentation and test data that
intentionally verifies localized behavior.

Issues and review discussions may use another language when that makes
communication more effective.

### Pull request title

Every pull request title must identify the primary affected package or
repository area and summarize the result of the change.

Prefer the following form for a change with one primary package:

```text
package: lowercase summary
```

When multiple packages are equally affected, enclose the package list in braces
and separate package names with a comma and a space:

```text
{package/a, package/b}: lowercase summary
```

Use one primary package whenever possible. Do not add documentation, tests, or
examples to the package list when they only support the primary implementation
change. For a change that does not belong to a Go package, use the narrowest
repository area or subsystem that owns the change, such as `docs`, `ci`, or
`scripts`.

The summary should:

- begin with a lowercase letter;
- describe the result rather than the implementation process;
- be concise but meaningful without reading the diff; and
- complete the sentence "This change modifies trpc-agent-service to _____."

Use an ASCII colon followed by one space. A generic change type, issue number,
tool name, or bracketed tag does not replace the affected package name.

### Pull request description

Keep the description concise. The diff is the source of truth; the description
should provide the context that code alone cannot.

Use the pull request template to explain:

- **What changed**: the outcome and its user or developer impact.
- **Why**: the problem and any non-obvious design rationale.
- **Scope and compatibility**: public API, configuration, data, migration,
  deployment, tenant-isolation, and failure-behavior implications.
- **Testing**: the validation that was actually performed.
- **Notes for reviewers**: optional risks or design decisions that deserve
  focused review.

Do not restate the implementation, enumerate every changed file or symbol, or
leave template instructions unchanged. Call out public API, compatibility,
migration, security, or tenant-isolation concerns when they are not obvious from
the diff.

The pull request title and description form durable project history regardless
of the merge method. Keep them synchronized with the final outcome.

### Public API and platform design

Treat every public API and externally observable platform behavior as a
long-lived compatibility commitment.

A public API change includes adding, removing, renaming, or changing:

- an exported type, function, method, interface, field, constant, or variable;
- an option, callback, plugin, guardrail, adapter, or sentinel error contract;
- default, zero-value, nil, ownership, concurrency, or lifecycle behavior;
- serialized fields, database schemas, persistence formats, or protocols; or
- event ordering, streaming, cancellation, retry, idempotency, or other
  observable behavior.

Before adding a public API:

- search for an existing API or extension point that supports the use case;
- verify that the concept belongs to the selected package and abstraction layer;
- prefer reusing tRPC-Agent-Go capabilities over duplicating framework behavior;
- keep implementation-specific concepts in their owning packages unless their
  semantics are genuinely shared across implementations;
- avoid parallel entry points with substantially overlapping responsibilities;
- prefer the smallest surface that supports external consumers; and
- consider how the API can evolve without duplicate types, methods, or
  incompatible renames.

Every exported symbol must have meaningful Godoc that explains its contract.
Documentation that only restates the declaration is insufficient.

Review exported names for semantic accuracy, discoverability, package fit,
abstraction boundaries, and future evolution. Unexported naming and local
refactoring preferences are not public API concerns unless they are misleading
or likely to cause incorrect behavior.

### Tenant isolation and security

Tenant identity must come from an authenticated and trusted binding. Do not
accept a tenant ID directly from untrusted headers, message payloads, tool
arguments, or user input.

All tenant-owned storage operations must carry an explicit `tenant_id` boundary.
Namespaced string IDs are collision protection, not a replacement for storage
isolation or authorization.

Do not place model keys, IM tokens, database credentials, customer content, or
other secrets in source code, tests, logs, traces, error messages, issue text, or
pull request descriptions. Preserve the distinction between compliance audit
events and sampled telemetry.

Changes involving authentication, authorization, tools, external integrations,
storage routing, or tenant configuration must include focused security and
tenant-boundary review.

### Go conventions

Follow [Effective Go](https://go.dev/doc/effective_go) and the
[Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), while
preserving established project APIs when compatibility requires it.

- Format code with `gofmt` by running `./scripts/format.sh`.
- Use short, lowercase, single-word package names. Avoid underscores, mixed
  capitals, and names that repeat their package when qualified.
- Use MixedCaps for Go names and spell common initialisms consistently, such as
  `ID`, `URL`, `HTTP`, and `API`.
- Make exported names read naturally with their package qualifier. Avoid stutter
  and redundant prefixes such as `pkg.PkgType`.
- Prefer small interfaces that describe behavior required by consumers. Do not
  introduce an interface only to anticipate hypothetical implementations or to
  make a concrete type easier to mock.
- Prefer returning concrete types from constructors. Add a constructor when it
  establishes invariants or improves usability; otherwise, make the zero value
  useful when practical.
- Pass `context.Context` as the first parameter when an operation needs it. Do
  not store a context in a struct unless the type explicitly represents that
  context's lifetime.
- Keep error strings lowercase and without trailing punctuation. Wrap errors
  with `%w` when callers need the cause, and expose sentinel or typed errors only
  when callers have a stable reason to inspect them.
- Make goroutine, channel, resource, cancellation, and shutdown ownership
  explicit. Background work must have a bounded way to stop.
- Write doc comments for exported declarations as complete sentences beginning
  with the declared name, and document behavior that callers must understand.

Do not turn idiomatic guidance into subjective churn. Match surrounding code
when several forms are valid, and do not refactor established public APIs only
to satisfy a style preference.

### Code quality and validation

Before submitting or updating a pull request:

- run `./scripts/format.sh --check`;
- run `./scripts/lint.sh`;
- run `./scripts/build.sh`;
- run `go test ./...` or `./scripts/coverage.sh` for root-module changes;
- run race tests for concurrency-sensitive packages, for example
  `go test -race ./trpcservice/tenant/...`;
- add targeted tests for new behavior, boundary conditions, and regressions;
- update documentation and examples when public behavior changes; and
- verify that tests do not depend on credentials, external services,
  machine-specific paths, or unstable timing unless explicitly required.

Tests should validate externally observable behavior rather than merely execute
new code. Assertions should be strong enough to fail when the intended contract
is broken.

Report validation commands in a portable form. Do not include local absolute
paths, machine-specific cache directories, credentials, or developer-specific
environment configuration in pull request descriptions.

### Referencing issues

Use `Fixes #123` when the pull request completely resolves an issue.

Use `Updates #123` when the pull request contributes to an issue but does not
fully resolve it.

For an issue in another repository, use the full repository reference:

```text
Fixes owner/repository#123
```

### Updating a pull request

Push additional commits to the pull request branch when addressing feedback.
Both incremental commits and rebasing with a force-push are accepted when they
do not disrupt other contributors. Keep the pull request description
synchronized with the final outcome and important review context.

When using stacked pull requests, state the stack and base branch explicitly.
Merge from the bottom of the stack upward, then retarget and revalidate each
remaining pull request so that every final diff is reviewable against `main`.

## Attribution

This guide is adapted from the
[tRPC-Agent-Go contribution guide](https://github.com/trpc-group/trpc-agent-go/blob/main/CONTRIBUTING.md)
for this repository's platform scope, scripts, and security model.
