# New managed Worker workspace seccomp profile

Source: https://github.com/moby/profiles/blob/61eaf32614c7c71b60bd8927d3e6a4ffc8ff1f31/seccomp/default.json

Pinned upstream commit: `61eaf32614c7c71b60bd8927d3e6a4ffc8ff1f31`.
Retrieved 2026-09-09 via git from the official Moby repository. Base SHA-256:
`536529b665dd0972c37bfb569f5d4ac8a53592e7b00752bc39ff063ca9864c74`.

The JSON preserves every upstream default field/rule and appends exactly one
SCMP_ACT_ALLOW rule for `clone`, `unshare`, `mount`, `umount2`, `pivot_root`.
These allow Bubblewrap to construct managed user/PID/network/mount namespaces.
No `setns` addition, no privileged container, and no added Linux capabilities.
The explicit clone rule removes upstream clone argument filtering for this new
Worker: it is a syscall permission change, not a claim of zero broadening.

Apply only to the newly approved managed workspace Worker. The separately
approved `systempaths=unconfined` exception is also required on the tested Docker
Desktop runtime for SDK v1.11.2 proc mounting; this JSON does not imply or apply
that option. Existing deployments are not changed by adding this file.

The Worker must run non-root with cap_drop ALL, read-only root, a private writable
workspace, SDK NetworkRestricted/InheritNone, denied secret and proc paths, and
explicit V1 workspace Attempt exclusivity. Run workspaceadapter/test-linux.sh
against the exact target runtime image before enabling workspace capabilities.
