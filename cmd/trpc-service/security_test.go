package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The child has no .env, real model credentials, database, Redis or IM access.
func TestSecurityServiceProcess(t *testing.T) {
	if os.Getenv("TRPC_SECURITY_TEST_CHILD") != "yes" {
		return
	}
	flag.CommandLine = flag.NewFlagSet("service-test", flag.ExitOnError)
	os.Args = []string{"service-test", "-env-file", "", "-role", os.Getenv("TRPC_SECURITY_TEST_ROLE"), "-addr", os.Getenv("TRPC_SECURITY_TEST_ADDR")}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestRoleHTTPBoundaryIntegration(t *testing.T) {
	const apiToken = "http-integration-test-not-a-real-key-123456"
	const adminToken = "admin-integration-test-not-a-real-key-7890"
	for _, role := range []string{"gateway", "admin", "all"} {
		t.Run(role, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			_ = listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			t.Cleanup(cancel)
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSecurityServiceProcess$")
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if strings.HasPrefix(key, "TRPC_") || strings.HasPrefix(key, "OPENAI_") || strings.HasPrefix(key, "REDIS_") {
					continue
				}
				cmd.Env = append(cmd.Env, entry)
			}
			provider := "openai" // Non-model roles must boot even with missing model credentials.
			if role == "all" {
				provider = "mock"
			}
			cmd.Env = append(cmd.Env,
				"TRPC_SECURITY_TEST_CHILD=yes", "TRPC_SECURITY_TEST_ROLE="+role, "TRPC_SECURITY_TEST_ADDR="+addr,
				"TRPC_AGENT_MODEL_PROVIDER="+provider, "TRPC_AGENT_ADMIN_ENABLED=true", "TRPC_AGENT_ADMIN_TOKEN="+adminToken,
				"TRPC_AGENT_HTTP_API_ENABLED=true",
				`TRPC_AGENT_HTTP_API_PRINCIPALS_JSON=[{"name":"client","token":"`+apiToken+`","tenant_id":"tutorial-tenant","binding_keys":["tutorial-http"],"user_ids":["alice"]}]`,
			)
			var logs bytes.Buffer
			cmd.Stdout, cmd.Stderr = &logs, &logs
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { finished <- cmd.Wait() }()
			t.Cleanup(func() {
				_ = cmd.Process.Signal(syscall.SIGTERM)
				select {
				case err := <-finished:
					if err != nil {
						t.Errorf("test process stopped: %v; %s", err, logs.String())
					}
				case <-time.After(5 * time.Second):
					_ = cmd.Process.Kill()
					<-finished
					t.Error("test process did not shut down")
				}
			})
			client := &http.Client{Timeout: time.Second}
			ready := false
			for range 100 {
				res, err := client.Get("http://" + addr + "/readyz")
				if err == nil {
					_ = res.Body.Close()
					if res.StatusCode == 200 {
						ready = true
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !ready {
				t.Fatal("test process did not become ready")
			}
			for _, endpoint := range []string{"/chat", "/inbound", "/admin/tenants/get", "/callbacks/telegram/missing"} {
				for _, authenticated := range []bool{false, true} {
					body := map[string]string{"binding_key": "tutorial-http", "message_id": "scoped-request", "user_id": "alice", "session_id": "test-session", "message": "hello"}
					if strings.HasPrefix(endpoint, "/admin/") {
						body = map[string]string{"tenant_id": "tutorial-tenant"}
					}
					raw, _ := json.Marshal(body)
					req, _ := http.NewRequest(http.MethodPost, "http://"+addr+endpoint, bytes.NewReader(raw))
					if authenticated {
						token := apiToken
						if strings.HasPrefix(endpoint, "/admin/") {
							token = adminToken
						}
						req.Header.Set("Authorization", "Bearer "+token)
					}
					res, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, res.Body)
					_ = res.Body.Close()
					want := 401
					switch {
					case role == "admin" && !strings.HasPrefix(endpoint, "/admin/"):
						want = 404
					case role == "gateway" && (endpoint == "/chat" || strings.HasPrefix(endpoint, "/admin/")):
						want = 404
					case strings.HasPrefix(endpoint, "/callbacks/"):
						want = 400 // Routed through callback verification, never Bearer.
					case authenticated:
						want = 200
						if endpoint == "/inbound" {
							want = 202
						}
					}
					if res.StatusCode != want {
						t.Fatalf("%s auth=%t: %d, want %d", endpoint, authenticated, res.StatusCode, want)
					}
				}
			}
		})
	}
}
