package recovery

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// DockerClientRunner executes the pg_dump/pg_restore calls inside an
// operator-supplied PostgreSQL client image (tag+digest pinned, e.g.
// postgres:16-alpine@sha256:...). It exists for operator hosts whose PATH
// client does not match the server major: the archive must always be
// produced and consumed by a matching, pinned client.
//
// The image reference is the only operator-controlled input; arguments stay
// tool-constructed, the container joins the host network so loopback mapped
// databases stay reachable, and directories are mounted at identical paths.
// Child stderr is discarded: only exit classification leaves the runner.
type DockerClientRunner struct {
	Image string
}

// Run implements ClientRunner.
func (r DockerClientRunner) Run(ctx context.Context, tool ClientTool, args []string, env []string, mounts []Mount, stdout io.Writer) error {
	image := strings.TrimSpace(r.Image)
	if image == "" || strings.Contains(image, " ") {
		return fmt.Errorf("%w: client image reference invalid", ErrInvalidConfig)
	}
	dockerArgs := []string{"run", "--rm", "--network", "host"}
	// Run the client as the invoking user so archive files on the bind
	// mount stay owned (and fsync-able/chmod-able) by the host operator.
	if runtime.GOOS != "windows" {
		dockerArgs = append(dockerArgs, "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()))
	}
	// The 0600 PGPASSFILE lives in its own 0700 temp directory; expose it to
	// the container read-only at the identical path.
	for _, item := range env {
		if value, ok := strings.CutPrefix(item, "PGPASSFILE="); ok {
			dir := filepath.Dir(value)
			dockerArgs = append(dockerArgs, "-v", fmt.Sprintf("%s:%s:ro", dir, dir))
		}
	}
	for _, mount := range mounts {
		mode := "rw"
		if mount.ReadOnly {
			mode = "ro"
		}
		dockerArgs = append(dockerArgs, "-v", fmt.Sprintf("%s:%s:%s", mount.Host, mount.Container, mode))
	}
	for _, item := range env {
		dockerArgs = append(dockerArgs, "-e", item)
	}
	dockerArgs = append(dockerArgs, image, string(tool))
	dockerArgs = append(dockerArgs, args...)
	cmd := exec.CommandContext(ctx, "docker", dockerArgs...)
	cmd.Stdout = stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: %s", ErrTimeoutOrCancelled, tool)
		}
		return classifyClientError(err, tool)
	}
	return nil
}
