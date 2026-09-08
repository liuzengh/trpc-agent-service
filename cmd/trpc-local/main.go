// trpc-local provides read-only development diagnostics and identity-checked
// management of the workspace Agent process. It never starts IM/model services.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/localrun"
	redis "github.com/redis/go-redis/v9"
)

func main() {
	code, err := run(os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}
func run(args []string, out io.Writer) (int, error) {
	if len(args) == 0 {
		return 2, errors.New("use status, running, record-pid, stop or wait-ready")
	}
	flags := flag.NewFlagSet("trpc-local", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	rootFlag := flags.String("root", ".", "workspace root")
	envFile := flags.String("env-file", ".env", "private dotenv file")
	pid := flags.Int("pid", 0, "newly started Agent PID")
	timeout := flags.Duration("timeout", 20*time.Second, "bounded wait")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return 2, errors.New("invalid local command arguments")
	}
	if *timeout <= 0 || *timeout > time.Minute {
		return 2, errors.New("timeout must be between zero and one minute")
	}
	root, err := filepath.Abs(*rootFlag)
	if err != nil {
		return 2, errors.New("invalid workspace root")
	}
	manager := localrun.Manager{Root: root, Executable: filepath.Join(root, "bin", "trpc-service")}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch args[0] {
	case "running":
		process, err := manager.Running()
		if errors.Is(err, localrun.ErrNotRunning) {
			return 3, nil
		}
		if err != nil {
			return 4, localrun.ErrIdentity
		}
		fmt.Fprintf(out, "Agent running: pid=%d\n", process.PID)
		return 0, nil
	case "record-pid":
		if err := manager.RecordStarted(ctx, *pid); err != nil {
			return 1, err
		}
		return 0, nil
	case "stop":
		if err := manager.Stop(ctx); err != nil {
			if errors.Is(err, localrun.ErrNotRunning) {
				fmt.Fprintln(out, "Agent is not running")
				return 0, nil
			}
			return 1, err
		}
		fmt.Fprintln(out, "Agent exited; verified PID records removed")
		return 0, nil
	case "status", "wait-ready":
	default:
		return 2, errors.New("unknown local command")
	}
	path := *envFile
	if path != "" && !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	if _, err := config.LoadDotEnv(path); err != nil {
		return 2, errors.New("cannot load local configuration; details withheld")
	}
	address := os.Getenv("TRPC_AGENT_ADDR")
	if address == "" {
		address = ":8080"
	}
	base, err := localBase(address)
	if err != nil {
		return 2, err
	}
	if args[0] == "wait-ready" {
		roles, err := config.ParseRole(os.Getenv("TRPC_AGENT_ROLE"))
		if err != nil {
			return 2, errors.New("invalid role")
		}
		if !roles.Gateway && !roles.Admin {
			_, err := manager.Running()
			if err != nil {
				return 1, err
			}
			fmt.Fprintln(out, "non-HTTP role running; no readiness endpoint")
			return 0, nil
		}
		for {
			if _, err := manager.Running(); err != nil {
				return 1, errors.New("Agent exited or PID identity changed during startup")
			}
			if state := httpProbe(ctx, base+"/readyz", nil); state.State == "ok" {
				fmt.Fprintln(out, "Agent readiness passed")
				return 0, nil
			}
			select {
			case <-ctx.Done():
				return 1, errors.New("Agent did not become ready; process retained for diagnosis")
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return status(ctx, root, base, manager, out)
}

func localBase(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "", errors.New("TRPC_AGENT_ADDR must be host:port")
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return "", errors.New("local diagnostics require a loopback or wildcard Agent address")
		}
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

type check struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail"`
}

type composeService struct{ Service, State, Health string }

func parseComposeServices(raw []byte) []composeService {
	var result []composeService
	if len(raw) > 1<<20 {
		return nil
	}
	if json.Unmarshal(raw, &result) == nil {
		return result
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	for len(result) < 256 {
		var service composeService
		if err := decoder.Decode(&service); err != nil {
			break
		}
		result = append(result, service)
	}
	return result
}

func httpProbe(ctx context.Context, endpoint string, headers map[string]string) check {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return check{State: "down", Detail: "invalid endpoint configuration"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return check{State: "down", Detail: "invalid endpoint configuration"}
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return check{State: "down", Detail: "HTTP request failed; endpoint and error details withheld"}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == 200 {
		return check{State: "ok", Detail: "HTTP 200"}
	}
	return check{State: "down", Detail: fmt.Sprintf("HTTP %d", resp.StatusCode)}
}

func modelProbe(ctx context.Context, baseURL, apiKey string) check {
	result := httpProbe(ctx, strings.TrimRight(baseURL, "/")+"/models", map[string]string{"Authorization": "Bearer " + apiKey})
	if result.State == "ok" {
		result.Detail = "model listing reachable; generation not tested"
	} else if result.Detail == "HTTP 404" || result.Detail == "HTTP 405" {
		result.State = "unknown"
		result.Detail += "; listing unavailable, use check-model.sh"
	}
	return result
}

func status(ctx context.Context, root, base string, manager localrun.Manager, out io.Writer) (int, error) {
	results := []check{{Name: "Agent PID", State: "unknown"}}
	if process, err := manager.Running(); err == nil {
		results[0] = check{"Agent PID", "ok", fmt.Sprintf("verified pid=%d", process.PID)}
	} else if errors.Is(err, localrun.ErrNotRunning) {
		results[0] = check{"Agent PID", "down", "no running workspace Agent"}
	} else {
		results[0] = check{"Agent PID", "down", "PID identity mismatch; no signal sent"}
	}
	tasks := []struct {
		name string
		fn   func() check
	}{{"Agent readiness", func() check { return httpProbe(ctx, base+"/readyz", nil) }}}
	if os.Getenv("TRPC_AGENT_CONTROL_PLANE_BACKEND") == "postgres" {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"PostgreSQL", func() check {
			cfg, err := config.LoadControlPlaneConfigFromEnv()
			if err != nil {
				return check{State: "down", Detail: "invalid configuration"}
			}
			db, err := sql.Open("pgx", cfg.PostgresURL)
			if err != nil {
				return check{State: "down", Detail: "invalid connection settings"}
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			probe, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if db.PingContext(probe) != nil {
				return check{State: "down", Detail: "connection failed"}
			}
			return check{State: "ok", Detail: "database ping"}
		}})
	}
	redisRequired := false
	for _, key := range []string{"TRPC_AGENT_SESSION_BACKEND", "TRPC_AGENT_QUEUE_BACKEND", "TRPC_AGENT_COORDINATOR_BACKEND", "TRPC_AGENT_IDEMPOTENCY_BACKEND", "TRPC_AGENT_QUOTA_BACKEND"} {
		redisRequired = redisRequired || os.Getenv(key) == "redis"
	}
	if redisRequired {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"Redis", func() check {
			options, err := redis.ParseURL(os.Getenv("REDIS_URL"))
			if err != nil {
				return check{State: "down", Detail: "invalid configuration"}
			}
			options.MaxRetries = -1
			options.DialTimeout = 2 * time.Second
			client := redis.NewClient(options)
			defer client.Close()
			probe, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if client.Ping(probe).Err() != nil {
				return check{State: "down", Detail: "PING failed"}
			}
			return check{State: "ok", Detail: "PING succeeded"}
		}})
	}
	if os.Getenv("TRPC_AGENT_MODEL_PROVIDER") == "openai" {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"Model API", func() check {
			cfg, err := config.LoadModelConfigFromEnv()
			if err != nil {
				return check{State: "down", Detail: "invalid model configuration"}
			}
			baseURL := cfg.BaseURL
			if baseURL == "" {
				baseURL = "https://api.openai.com/v1"
			}
			return modelProbe(ctx, baseURL, cfg.APIKey)
		}})
	} else {
		results = append(results, check{"Model API", "skipped", "no external startup model configured"})
	}
	if key := os.Getenv("TRPC_AGENT_QDRANT_API_KEY"); key != "" {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"Local Qdrant", func() check {
			return httpProbe(ctx, "http://127.0.0.1:6333/collections", map[string]string{"api-key": key})
		}})
	}
	if os.Getenv("MINIO_ARTIFACT_CREDENTIALS") != "" {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"Local MinIO", func() check { return httpProbe(ctx, "http://127.0.0.1:9000/minio/health/ready", nil) }})
	}
	if public := os.Getenv("TRPC_AGENT_PUBLIC_BASE_URL"); public != "" {
		tasks = append(tasks, struct {
			name string
			fn   func() check
		}{"Public entry / Tunnel", func() check { return httpProbe(ctx, strings.TrimRight(public, "/")+"/healthz", nil) }})
	} else {
		results = append(results, check{"Public entry / Tunnel", "unknown", "set TRPC_AGENT_PUBLIC_BASE_URL to check the configured ingress"})
	}
	start := len(results)
	results = append(results, make([]check, len(tasks))...)
	var wg sync.WaitGroup
	for i, task := range tasks {
		wg.Add(1)
		go func(i int, task struct {
			name string
			fn   func() check
		}) {
			defer wg.Done()
			result := task.fn()
			result.Name = task.name
			results[start+i] = result
		}(i, task)
	}
	wg.Wait()
	dockerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(dockerCtx, "docker", "compose", "--profile", "observability", "ps", "--all", "--format", "json")
	cmd.Dir = root
	if raw, err := cmd.Output(); err == nil {
		for _, service := range parseComposeServices(raw) {
			state := "ok"
			if service.State != "running" || (service.Health != "" && service.Health != "healthy") {
				state = "unknown"
			}
			results = append(results, check{"Compose " + service.Service, state, service.State + " " + service.Health})
		}
	}
	code := 0
	for _, result := range results {
		fmt.Fprintf(out, "%-25s %-8s %s\n", result.Name, result.State, result.Detail)
		if result.State == "down" {
			code = 1
		}
	}
	fmt.Fprintln(out, "Read-only diagnostics: no model generation, IM send, migration or service restart. Local MinIO/Qdrant use Compose loopback ports; this is not a remote-backend audit.")
	return code, nil
}
