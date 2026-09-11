# Managed workspace Attempt adapter (explicit Linux V1)

`Open(ctx, workroot) (*Attempt, error)` holds a process-wide workspace execution
permit until `Close`. Cancellation interrupts permit acquisition. Only Runs with
workspace capability acquire it: ordinary model/Memory/Summary Runs are not
serialized by this package. The root belongs exclusively to this Worker process;
never share it between Worker replicas or other writers. An unknown existing
entry fails closed. A failed cleanup poisons future workspace opens in this
process, rather than letting a subsequent Attempt inspect retained files.

`Executor()` returns the SDK CodeExecutor wrapper; `ExecTool()` and
`SaveArtifactTool()` return the real SDK tools with lifecycle guards. The SDK
Engine's ProgramRunner is exposed without its optional interactive method set:
execution stays synchronous, without inaccessible long-lived tool sessions.
Artifact state deltas are forwarded. The caller must drain its Run, call Close,
and check its error. Close cancels outstanding calls, joins them, removes private
files, and only then releases the permit. It never releases merely because a
network timeout was exceeded while a callee is still accessing the workspace.

The wrapper is not a new code executor, scheduler, artifact store or attachment
transport. Filesystem/execution/schema/save semantics remain SDK v1.11.2. Artifact
saving uses the caller's Invocation ArtifactService and trusted Session scope.
No service is created here. Published capability selection and provider name
mapping belong to the caller. Direct access to Engine is the SDK assembly seam,
not an independent public request execution route; callers use the guarded tools
or ExecuteCode and do not retain raw engine handles past Attempt.Close.

## Exact deployment prerequisites

- Linux runtime image with `/bin/bash` and `bwrap`; fixed PATH `/usr/bin:/bin`.
- Non-root Worker, read-only container root, exclusive writable `/workspace`.
- Existing deny targets `/run`, `/config`, `/credentials`, `/proc`, `/sys`,
  `/root`, `/home`, `/tmp`, `/app`. Secrets/config must stay within those denied
  locations, not new readable mounts elsewhere. SDK denies unavailable external
  targets as errors; the image must create the empty conventional directories.
- SDK Managed profile, restricted network, environment inheritance None;
  IncludeOnly PATH/HOME. SDK itself sets private HOME/TMP/workspace variables.
- Approved deployment exception: default Moby seccomp plus
  `clone,unshare,mount,umount2,pivot_root`; `cap_drop: ALL`, no privileged mode.
  Docker Desktop tested additionally needs `systempaths=unconfined` to let SDK
  mount a new proc. The SDK profile then denies `/proc` to commands entirely.
  These are explicit new-Worker exceptions, not changes to an existing stack.

Linux SDK starts with read-only `/`, not a root read allowlist. The explicit
workspace exclusivity and full cleanup are therefore part of the confidentiality
contract, not merely a performance option. A parallel workspace scheduler cannot
be enabled by deleting the permit. Runtime mounts must preserve the above secret
placement contract.

## Verification

```sh
go test -race -count=3 ./services/agent-worker/internal/execution/adapter/outbound/workspaceadapter
./services/agent-worker/internal/execution/adapter/outbound/workspaceadapter/test-linux.sh IMAGE SECCOMP_JSON
```

The first command runs lifecycle unit tests on the host; Linux integration tests
skip unless explicitly enabled. The second compiles tests for the Docker host,
creates only disposable containers with synthetic mounted secret canaries, and
runs real SDK tests three times. It verifies forbidden paths, process-root/env
access, environment filtering, network denial against a proven live control
listener, artifact bytes, cancelled exclusive wait, cross-tenant prior workspace
absence, dirty-root rejection, active-call join and no detached child process.
Artifact persistence in these tests is SDK inmemory, not formal S3/PG acceptance.
