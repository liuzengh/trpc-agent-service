package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEmbeddedDocsMCPStartsAndStopsWithAgent(t *testing.T) {
	freeAddr := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		return addr
	}
	addr, docsAddr := freeAddr(), freeAddr()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "architecture.md"), []byte("# Fixture\nRedis Session reference.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	credential, _ := json.Marshal(map[string]string{"url": "http://" + docsAddr + "/mcp", "bearer_token": "synthetic-mcp-token-0123456789abcdef"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSecurityServiceProcess$")
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		skip := false
		for _, prefix := range []string{"TRPC_", "OPENAI_", "REDIS_", "MCP_", "MEMORY_", "MINIO_", "WECOM_", "TELEGRAM_"} {
			skip = skip || strings.HasPrefix(key, prefix)
		}
		if !skip {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "TRPC_SECURITY_TEST_CHILD=yes", "TRPC_SECURITY_TEST_ROLE=all", "TRPC_SECURITY_TEST_ADDR="+addr, "TRPC_AGENT_MODEL_PROVIDER=mock", "TRPC_AGENT_DOCS_MCP_ENABLED=true", "TRPC_AGENT_DOCS_MCP_ADDR="+docsAddr, "TRPC_AGENT_DOCS_MCP_ROOT="+root, "MCP_DOCS_SERVER="+string(credential))
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-finished
		}
	}()
	client := &http.Client{Timeout: time.Second}
	for _, url := range []string{"http://" + addr + "/readyz", "http://" + docsAddr + "/healthz"} {
		ready := false
		for range 100 {
			res, err := client.Get(url)
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
			t.Fatal("owned test process failed readiness")
		}
	}
	res, err := client.Post("http://"+docsAddr+"/mcp", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal("MCP endpoint lacks authentication")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		waited = true
		if err != nil {
			t.Fatalf("shutdown=%v logs=%s", err, logs.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Agent did not shut down both listeners")
	}
	for _, address := range []string{addr, docsAddr} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = conn.Close()
			t.Fatal("listener survived Agent shutdown")
		}
	}
}
