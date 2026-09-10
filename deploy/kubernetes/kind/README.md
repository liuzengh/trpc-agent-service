# Kind acceptance environment

This directory contains disposable, CI-only dependencies and overlays. It does
not weaken production endpoint validation: the application talks to the mock
OpenAI sidecar over loopback, while a separate Service verifies that the same
mock image is deployable through Kubernetes service discovery.

Run the complete gate from the repository root:

```bash
scripts/kind-e2e.sh
```

The script is idempotent for the `trpc-agent` Kind cluster. Evidence is written
to `deploy/kubernetes/kind/evidence/`.

For a local re-run after the images have already been loaded, use
`KIND_SKIP_BUILD=1 scripts/kind-e2e.sh`. CI never enables this shortcut.
