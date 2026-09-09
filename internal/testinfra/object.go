package testinfra

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const minioImage = "minio/minio:RELEASE.2024-06-13T22-53-53Z"

type ObjectLab struct {
	t       *testing.T
	owner   string
	prefix  string
	network string
	volume  string
	object  string
	access  string
	secret  string
	port    int
	bucket  bool
	cleaned bool
}

func NewObjectLab(t *testing.T, owner string) *ObjectLab {
	t.Helper()
	owner = strings.ToLower(strings.Trim(owner, " -_"))
	if owner == "" {
		owner = "unknown"
	}
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)
	prefix := "p009-object-" + owner + "-" + stamp
	lab := &ObjectLab{t: t, owner: owner, prefix: prefix, access: "p009access" + stamp[:8], secret: "p009secret" + stamp}
	t.Cleanup(lab.Cleanup)
	return lab
}

func (l *ObjectLab) command(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (l *ObjectLab) mustCommand(stage string, ctx context.Context, args ...string) string {
	value, err := l.command(ctx, args...)
	if err != nil {
		l.t.Fatalf("object gate failed stage=%s category=command_failed", stage)
	}
	return value
}

func (l *ObjectLab) commandWithEnv(ctx context.Context, env []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (l *ObjectLab) Start(ctx context.Context) {
	l.t.Helper()
	if _, err := l.command(ctx, "version"); err != nil {
		l.t.Skipf("object gate unavailable stage=resource_create category=docker_unavailable")
	}
	var err error
	l.network, err = l.command(ctx, "network", "create", "--label", "p009.object.owner="+l.owner, "--label", "p009.object.run="+l.prefix, l.prefix+"-net")
	if err != nil {
		l.t.Fatalf("object gate failed stage=resource_create category=network_create_failed")
	}
	l.volume, err = l.command(ctx, "volume", "create", "--label", "p009.object.owner="+l.owner, "--label", "p009.object.run="+l.prefix, l.prefix+"-data")
	if err != nil {
		l.t.Fatalf("object gate failed stage=resource_create category=volume_create_failed")
	}
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		l.t.Fatalf("object gate failed stage=endpoint_reachability category=loopback_port_allocation_failed")
	}
	l.port = listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	args := []string{"run", "-d", "--name", l.prefix + "-object", "--label", "p009.object.owner=" + l.owner, "--label", "p009.object.run=" + l.prefix, "--network", l.network, "--network-alias", "minio", "-p", fmt.Sprintf("127.0.0.1:%d:9000", l.port), "-v", l.volume + ":/data", "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", minioImage, "server", "/data", "--address", ":9000", "--console-address", ":9001"}
	l.object, err = l.commandWithEnv(ctx, []string{"MINIO_ROOT_USER=" + l.access, "MINIO_ROOT_PASSWORD=" + l.secret}, args...)
	if err != nil {
		l.t.Fatalf("object gate failed stage=container_start category=container_create_failed")
	}
	l.AssertOwnership(ctx)
	l.WaitHealthy(ctx)
}

func (l *ObjectLab) Endpoint(ctx context.Context) string {
	if l.port == 0 {
		value, err := l.command(ctx, "port", l.object, "9000/tcp")
		if err != nil {
			l.t.Fatalf("object gate failed stage=endpoint_reachability category=host_port_lookup_failed")
		}
		parts := strings.Split(value, ":")
		var parseErr error
		l.port, parseErr = strconv.Atoi(parts[len(parts)-1])
		if parseErr != nil || l.port < 1 {
			l.t.Fatalf("object gate failed stage=endpoint_reachability category=host_port_invalid")
		}
	}
	return fmt.Sprintf("http://127.0.0.1:%d", l.port)
}

func (l *ObjectLab) Network() string      { return l.network }
func (l *ObjectLab) NetworkAlias() string { return "minio" }
func (l *ObjectLab) ObjectID() string     { return l.object }
func (l *ObjectLab) VolumeID() string     { return l.volume }
func (l *ObjectLab) AccessKey() string    { return l.access }
func (l *ObjectLab) SecretKey() string    { return l.secret }
func (l *ObjectLab) Bucket() string       { return l.prefix }
func (l *ObjectLab) Port(ctx context.Context) int {
	_ = l.Endpoint(ctx)
	return l.port
}
func (l *ObjectLab) HostPortPublished() bool { return l.port > 0 }

func (l *ObjectLab) WaitHealthy(ctx context.Context) {
	readyCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	stage, category := "container_start", "container_not_running"
	backoff := 50 * time.Millisecond
	for {
		if running, err := l.command(readyCtx, "inspect", "--format", "{{.State.Running}}", l.object); err != nil || running != "true" {
			stage, category = "container_start", "container_not_running"
		} else if ok := l.probeHealth(readyCtx, "/minio/health/live"); !ok {
			stage, category = "health_live", "health_endpoint_unavailable"
		} else if ok := l.probeHealth(readyCtx, "/minio/health/ready"); !ok {
			stage, category = "health_ready", "health_endpoint_unavailable"
		} else if err := l.probeS3(readyCtx); err != nil {
			stage, category = "s3_probe", "authenticated_probe_failed"
		} else {
			return
		}
		if readyCtx.Err() != nil {
			if errorsIsDeadline(readyCtx.Err()) {
				category = "readiness_deadline"
			} else {
				category = "context_canceled"
			}
			l.t.Fatalf("object readiness failed stage=%s category=%s", stage, category)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-readyCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			category = "readiness_deadline"
			l.t.Fatalf("object readiness failed stage=%s category=%s", stage, category)
		case <-timer.C:
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (l *ObjectLab) probeHealth(ctx context.Context, path string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.Endpoint(ctx)+path, nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	return response.StatusCode == http.StatusOK
}

func (l *ObjectLab) client(ctx context.Context) (*minio.Client, error) {
	endpoint, err := url.Parse(l.Endpoint(ctx))
	if err != nil || endpoint.Host == "" {
		return nil, err
	}
	return minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(l.access, l.secret, ""), Secure: endpoint.Scheme == "https", Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
}

func (l *ObjectLab) probeS3(ctx context.Context) error {
	client, err := l.client(ctx)
	if err != nil {
		return err
	}
	_, err = client.ListBuckets(ctx)
	return err
}

func (l *ObjectLab) CreateBucket(ctx context.Context) {
	client, err := l.client(ctx)
	if err != nil {
		l.t.Fatalf("object gate failed stage=resource_create category=credential_injection_failed")
	}
	if err := client.MakeBucket(ctx, l.Bucket(), minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		response := minio.ToErrorResponse(err)
		if response.Code != "BucketAlreadyOwnedByYou" {
			l.t.Fatalf("object gate failed stage=resource_create category=bucket_create_failed")
		}
	}
	l.bucket = true
}

func (l *ObjectLab) RemoveBucket(ctx context.Context) {
	if !l.bucket {
		return
	}
	client, err := l.client(ctx)
	if err != nil {
		l.t.Errorf("object cleanup failed stage=cleanup_bucket category=client_unavailable")
		return
	}
	if err := client.RemoveBucket(ctx, l.Bucket()); err != nil && !isMissingBucket(err) {
		l.t.Errorf("object cleanup failed stage=cleanup_bucket category=bucket_remove_failed")
		return
	}
	l.bucket = false
}

func (l *ObjectLab) Restart(ctx context.Context) {
	if _, err := l.command(ctx, "restart", l.object); err != nil {
		l.t.Fatalf("object gate failed stage=container_start category=restart_failed")
	}
	l.AssertOwnership(ctx)
	l.WaitHealthy(ctx)
}

func (l *ObjectLab) Stop(ctx context.Context) {
	if _, err := l.command(ctx, "stop", l.object); err != nil {
		l.t.Fatalf("object gate failed stage=container_start category=stop_failed")
	}
}

func (l *ObjectLab) AssertOwnership(ctx context.Context) {
	for _, resource := range []struct {
		kind   string
		id     string
		labels string
	}{{"network", l.network, ".Labels"}, {"volume", l.volume, ".Labels"}, {"container", l.object, ".Config.Labels"}} {
		owner, ownerErr := l.command(ctx, "inspect", "--format", "{{index "+resource.labels+" \"p009.object.owner\"}}", resource.id)
		run, runErr := l.command(ctx, "inspect", "--format", "{{index "+resource.labels+" \"p009.object.run\"}}", resource.id)
		if ownerErr != nil || runErr != nil || owner != l.owner || run != l.prefix {
			l.t.Fatalf("object gate failed stage=ownership_verify category=%s_ownership_mismatch", resource.kind)
		}
	}
}

func (l *ObjectLab) RemainingResources(ctx context.Context) bool {
	for _, args := range [][]string{
		{"ps", "-aq", "--filter", "label=p009.object.owner=" + l.owner, "--filter", "label=p009.object.run=" + l.prefix},
		{"volume", "ls", "-q", "--filter", "label=p009.object.owner=" + l.owner, "--filter", "label=p009.object.run=" + l.prefix},
		{"network", "ls", "-q", "--filter", "label=p009.object.owner=" + l.owner, "--filter", "label=p009.object.run=" + l.prefix},
	} {
		value, err := l.command(ctx, args...)
		if err != nil || value != "" {
			return true
		}
	}
	return false
}

func (l *ObjectLab) Cleanup() {
	if l.cleaned {
		return
	}
	l.cleaned = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	failures := make([]string, 0, 4)
	if l.bucket {
		if err := l.emptyBucket(ctx); err != nil {
			failures = append(failures, "cleanup_bucket")
		}
	}
	for _, resource := range []struct {
		stage string
		id    string
		args  []string
	}{{"cleanup_container", l.object, []string{"rm", "-f"}}, {"cleanup_volume", l.volume, []string{"volume", "rm"}}, {"cleanup_network", l.network, []string{"network", "rm"}}} {
		stage, id, args := resource.stage, resource.id, resource.args
		if id == "" {
			continue
		}
		if _, err := l.command(ctx, append(args, id)...); err != nil {
			failures = append(failures, stage)
		}
	}
	if len(failures) > 0 {
		l.t.Errorf("object cleanup failed stage=cleanup category=resource_cleanup_failed phases=%s", strings.Join(failures, ","))
	}
}

func (l *ObjectLab) emptyBucket(ctx context.Context) error {
	client, err := l.client(ctx)
	if err != nil {
		return err
	}
	found, err := client.BucketExists(ctx, l.Bucket())
	if err != nil || !found {
		return err
	}
	for object := range client.ListObjects(ctx, l.Bucket(), minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			return object.Err
		}
		if err := client.RemoveObject(ctx, l.Bucket(), object.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	return client.RemoveBucket(ctx, l.Bucket())
}

func isMissingBucket(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchBucket" || code == "NotFound"
}

func errorsIsDeadline(err error) bool {
	return err == context.DeadlineExceeded
}
