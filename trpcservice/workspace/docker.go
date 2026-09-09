// Package workspace executes bounded scripts in disposable Docker sandboxes.
// Local temporary directories contain only CLI state, never an execution fallback.
package workspace

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const ownerLabel = "trpc-agent.sandbox-owner"

// Config is deployment-owned; no tenant input becomes a Docker option.
type Config struct {
	Image       string
	Socket      string
	Timeout     time.Duration
	MemoryMiB   int
	Concurrency int
	MaxOutput   int
}

// Request contains an authorized script and a JSON input file, not host paths.
type Request struct {
	TenantID, AppID, UserID, SessionID, RequestID string
	Script                                        []byte
	Input                                         json.RawMessage
}

// Result describes completed guest execution. Output is untrusted user data.
type Result struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exit_code"`
	DurationMS int64  `json:"duration_ms"`
}

// Executor is the restricted execution capability consumed by Skill tools.
type Executor interface {
	Execute(context.Context, Request) (Result, error)
}

// Failure exposes a safe category without Docker stderr, paths or credentials.
type Failure struct{ Kind string }

func (e *Failure) Error() string { return "sandbox " + e.Kind }

// ErrorCode never includes command output or host details. Only disabled is
// known to precede execution; other failures may have an unknown outcome.
func (e *Failure) ErrorCode() string {
	if e.Kind == "disabled" {
		return "sandbox_unavailable"
	}
	return "sandbox_execution_failed"
}

// Docker runs fresh, unprivileged, networkless containers using a pinned image ID.
type Docker struct {
	config          Config
	binary, imageID string
	slots           chan struct{}
}

// NewDocker resolves an already-local image to an immutable ID. It never pulls.
func NewDocker(ctx context.Context, cfg Config) (*Docker, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MemoryMiB == 0 {
		cfg.MemoryMiB = 128
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	if cfg.MaxOutput == 0 {
		cfg.MaxOutput = 64 << 10
	}
	if cfg.Socket == "" {
		cfg.Socket = "/var/run/docker.sock"
	}
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@-]{0,255}$`).MatchString(cfg.Image) || !filepath.IsAbs(cfg.Socket) || strings.ContainsAny(cfg.Socket, "\r\n\x00") || cfg.Timeout < time.Second || cfg.Timeout > time.Minute || cfg.MemoryMiB < 32 || cfg.MemoryMiB > 512 || cfg.Concurrency < 1 || cfg.Concurrency > 16 || cfg.MaxOutput < 1024 || cfg.MaxOutput > 1<<20 {
		return nil, &Failure{"configuration invalid"}
	}
	binary, err := exec.LookPath("docker")
	if err != nil {
		return nil, &Failure{"docker unavailable"}
	}
	d := &Docker{config: cfg, binary: binary, slots: make(chan struct{}, cfg.Concurrency)}
	dir, err := os.MkdirTemp("", "trpc-sandbox-probe-")
	if err != nil {
		return nil, &Failure{"local state unavailable"}
	}
	defer func() { _ = os.RemoveAll(dir) }()
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := d.command(probe, dir, "image", "inspect", "--format", "{{.Id}}", cfg.Image)
	raw, err := cmd.Output()
	if err != nil {
		return nil, &Failure{"local image unavailable"}
	}
	d.imageID = strings.TrimSpace(string(raw))
	if !regexp.MustCompile(`^sha256:[a-f0-9]{64}$`).MatchString(d.imageID) {
		return nil, &Failure{"image identity invalid"}
	}
	return d, nil
}

func (d *Docker) command(ctx context.Context, dir string, args ...string) *exec.Cmd {
	base := []string{"--config", dir, "--host", "unix://" + d.config.Socket}
	cmd := exec.CommandContext(ctx, d.binary, append(base, args...)...)
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "LC_ALL=C"}
	cmd.Dir = dir
	cmd.WaitDelay = time.Second
	return cmd
}

// Ready inspects the daemon and the already pinned local image. It never
// creates a container, executes a Skill or pulls an image.
func (d *Docker) Ready(ctx context.Context) error {
	if d == nil {
		return &Failure{"disabled"}
	}
	dir, err := os.MkdirTemp("", "trpc-sandbox-health-")
	if err != nil {
		return &Failure{"local state unavailable"}
	}
	defer func() { _ = os.RemoveAll(dir) }()
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, err := d.command(probe, dir, "image", "inspect", "--format", "{{.Id}}", d.imageID).Output()
	if err != nil || strings.TrimSpace(string(raw)) != d.imageID {
		return &Failure{"daemon or pinned image unavailable"}
	}
	return nil
}

func payload(r Request) ([]byte, error) {
	if r.TenantID == "" || r.AppID == "" || r.UserID == "" || r.SessionID == "" || r.RequestID == "" || len(r.Script) == 0 || len(r.Script) > 64<<10 || len(r.Input) > 32<<10 || !json.Valid(r.Input) {
		return nil, &Failure{"request invalid"}
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, item := range []struct {
		name string
		body []byte
	}{{"run.sh", r.Script}, {"input.json", r.Input}} {
		if err := tw.WriteHeader(&tar.Header{Name: item.name, Mode: 0400, Size: int64(len(item.body)), Uid: 65532, Gid: 65532}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(item.body); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Docker) createArgs(name, nonce, scope string) []string {
	seconds := strconv.Itoa(int(d.config.Timeout.Seconds()))
	// The guest timeout survives a worker crash. --rm removes exited containers.
	guest := "/bin/busybox tar -xf - -C /workspace && exec /bin/busybox timeout -s KILL " + seconds + " /bin/sh /workspace/run.sh"
	return []string{"create", "--pull=never", "--rm", "--interactive", "--name", name,
		"--label", ownerLabel + "=" + nonce, "--label", "trpc-agent.sandbox-scope=" + scope,
		"--network=none", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
		"--user=65532:65532", "--pids-limit=32", "--memory=" + strconv.Itoa(d.config.MemoryMiB) + "m", "--memory-swap=" + strconv.Itoa(d.config.MemoryMiB) + "m", "--cpus=0.5",
		"--ulimit=nofile=64:64", "--log-driver=none", "--stop-timeout=1",
		"--tmpfs=/workspace:rw,noexec,nosuid,nodev,size=16777216,mode=0700,uid=65532,gid=65532",
		"--workdir=/workspace", "--entrypoint=/bin/sh", d.imageID, "-c", guest}
}

func (d *Docker) cleanup(dir, name, nonce string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Inspect only this request's random name; never prune by shared labels.
	cmd := d.command(ctx, dir, "container", "ls", "-aq", "--filter", "name=^/"+name+"$", "--filter", "label="+ownerLabel+"="+nonce)
	raw, err := cmd.Output()
	if err != nil {
		return &Failure{"cleanup unconfirmed"}
	}
	if strings.TrimSpace(string(raw)) == "" {
		return nil
	}
	id := strings.TrimSpace(string(raw))
	if !regexp.MustCompile(`^[a-f0-9]{12,64}$`).MatchString(id) {
		return &Failure{"cleanup identity invalid"}
	}
	if err := d.command(ctx, dir, "rm", "--force", id).Run(); err != nil {
		return &Failure{"cleanup unconfirmed"}
	}
	return nil
}

type limitedOutput struct {
	mu             sync.Mutex
	remaining      int
	exceeded       bool
	cancel         context.CancelFunc
	stdout, stderr bytes.Buffer
}
type outputSide struct {
	all    *limitedOutput
	stderr bool
}

func (s outputSide) Write(p []byte) (int, error) {
	o := s.all
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(p)
	take := min(n, o.remaining)
	if s.stderr {
		_, _ = o.stderr.Write(p[:take])
	} else {
		_, _ = o.stdout.Write(p[:take])
	}
	o.remaining -= take
	if take < n {
		o.exceeded = true
		o.cancel()
	}
	return n, nil
}

// Execute accepts no mounts, network access, environment injection or host commands.
func (d *Docker) Execute(parent context.Context, r Request) (result Result, err error) {
	parent, span := otel.Tracer("trpc-agent-service/sandbox").Start(parent, "sandbox.execute")
	span.SetAttributes(attribute.String("tenant.id", r.TenantID), attribute.String("app.id", r.AppID), attribute.String("request.id", r.RequestID))
	result.ExitCode = -1
	defer func() {
		span.SetAttributes(attribute.Int("sandbox.exit_code", result.ExitCode))
		if err != nil {
			span.SetStatus(codes.Error, "sandbox execution failed")
		}
		span.End()
	}()
	if parent.Err() != nil {
		return result, &Failure{"canceled"}
	}
	data, err := payload(r)
	if err != nil {
		return result, err
	}
	select {
	case d.slots <- struct{}{}:
		defer func() { <-d.slots }()
	case <-parent.Done():
		return result, &Failure{"canceled"}
	}
	dir, err := os.MkdirTemp("", "trpc-sandbox-run-")
	if err != nil {
		return result, &Failure{"local state unavailable"}
	}
	defer func() { _ = os.RemoveAll(dir) }()
	key := make([]byte, 16)
	if _, err = rand.Read(key); err != nil {
		return result, &Failure{"identity unavailable"}
	}
	nonce := hex.EncodeToString(key)
	name := "trpc-sandbox-" + nonce
	scope, _ := json.Marshal([]string{r.TenantID, r.AppID, r.UserID, r.SessionID})
	hash := sha256.Sum256(scope)
	defer func() {
		if cleanupErr := d.cleanup(dir, name, nonce); cleanupErr != nil {
			err = cleanupErr
		}
	}()
	ctx, cancel := context.WithTimeout(parent, d.config.Timeout+5*time.Second)
	defer cancel()
	started := time.Now()
	defer func() { result.DurationMS = time.Since(started).Milliseconds() }()
	if e := d.command(ctx, dir, d.createArgs(name, nonce, hex.EncodeToString(hash[:]))...).Run(); e != nil {
		return result, &Failure{"create failed"}
	}
	output := &limitedOutput{remaining: d.config.MaxOutput, cancel: cancel}
	cmd := d.command(ctx, dir, "start", "--attach", "--interactive", name)
	cmd.Stdin = bytes.NewReader(data)
	cmd.Stdout = outputSide{all: output}
	cmd.Stderr = outputSide{all: output, stderr: true}
	execErr := cmd.Run()
	result.Stdout = output.stdout.String()
	result.Stderr = output.stderr.String()
	if output.exceeded {
		return result, &Failure{"output limit"}
	}
	if parent.Err() != nil {
		return result, &Failure{"canceled"}
	}
	if ctx.Err() != nil {
		return result, &Failure{"timeout"}
	}
	if execErr != nil {
		var exit *exec.ExitError
		if errors.As(execErr, &exit) {
			result.ExitCode = exit.ExitCode()
		} else {
			result.ExitCode = -1
		}
		if result.ExitCode == 137 || result.ExitCode == 143 {
			return result, &Failure{"timeout or resource limit"}
		}
		return result, &Failure{"execution failed"}
	}
	result.ExitCode = 0
	return result, nil
}

var _ Executor = (*Docker)(nil)
var _ io.Writer = outputSide{}
