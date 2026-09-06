//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestComposeRecoveryGate is the compose-level P2-02 drill: it builds the
// pinned recovery tool image, brings up the owner-scoped recovery profile
// (source + target + fresh empty Redis), seeds representative facts on the
// source, and drives backup -> verify -> migrate -> restore -> replay -> app
// readiness on the restored target through the real containers. Provider
// transport stays the closed runner responder, so every external request
// count is zero by construction.
func TestComposeRecoveryGate(t *testing.T) {
	if testing.Short() {
		t.Skip("compose recovery gate is a long drill")
	}
	root := repoRoot(t)
	runID := fmt.Sprintf("p202-recovery-%d", time.Now().UnixNano())
	dir := t.TempDir()
	secretsDir := filepath.Join(dir, "secrets")
	if err := os.Mkdir(secretsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	secretValue := "p202-compose-" + fmt.Sprint(time.Now().UnixNano())
	for _, name := range []string{"p202_pg_password", "p202_pg_runtime_password", "pg_password", "pg_runtime_password"} {
		if err := os.WriteFile(filepath.Join(secretsDir, name), []byte(secretValue), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	envFile := filepath.Join(dir, "env")
	env := fmt.Sprintf("P202_RUN_ID=%s\nP202_SECRET_DIR=%s\nP109_RUN_ID=p202-gate\nP109_SECRET_DIR=%s\nP109_APP_IMAGE=p109-deploy-app:local\nP202_APP_PORT=18082\n",
		runID, secretsDir, secretsDir)
	if err := os.WriteFile(envFile, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}

	compose := func(t *testing.T, timeout time.Duration, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		full := append([]string{"compose", "--project-directory", root, "--env-file", envFile}, args...)
		cmd := exec.CommandContext(ctx, "docker", full...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("docker compose %v failed: %v\n%s", args, err, tailOf(output, 2000))
		}
		return string(output)
	}

	// Build the app and tool images (tag+digest pinned bases).
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer buildCancel()
	for _, spec := range []struct{ dockerfile, tag string }{
		{"Dockerfile", "p109-deploy-app:local"},
		{"Dockerfile.recovery", "p202-recovery-tool:local"},
	} {
		cmd := exec.CommandContext(buildCtx, "docker", "build", "-f", filepath.Join(root, spec.dockerfile), "-t", spec.tag, root)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", spec.tag, err, tailOf(output, 2000))
		}
	}

	// Bring up the recovery profile dependencies.
	compose(t, 3*time.Minute, "--profile", "recovery", "up", "-d", "recovery-source", "recovery-target", "recovery-redis")
	t.Cleanup(func() {
		down := exec.Command("docker", "compose", "--project-directory", root, "--env-file", envFile,
			"--profile", "recovery", "down", "-v", "--timeout", "20")
		_, _ = down.CombinedOutput()
	})

	// Initialize the source schema (migrate-source provisions schema and the
	// runtime role) and seed representative facts as the offline operator.
	compose(t, 2*time.Minute, "run", "--rm", "recovery-migrate", "migrate-source")
	seedSQL := `
INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('p202-gate','p202-gate','active',1);
INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p202-gate','gate-app','gate-app');
INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p202-gate','lark','gate-binding','gate-ext');
INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user, state, last_event_seq)
VALUES ('p202-gate','gate-session','gate-app',1,'lark','gate-binding','gate-chat','gate-user','active',3);
INSERT INTO session_event (tenant_id, session_id, event_id, sequence, event_type, attempt, payload)
VALUES ('p202-gate','gate-session','gate-evt-1',1,'user.received',1,'{}'),
       ('p202-gate','gate-session','gate-evt-2',2,'agent.started',1,'{}'),
       ('p202-gate','gate-session','gate-evt-3',3,'assistant.completed',1,'{}');
`
	seed := exec.Command("docker", "compose", "--project-directory", root, "--env-file", envFile,
		"exec", "-T", "recovery-source", "psql", "-U", "trpc_app", "-d", "trpc_agent", "-v", "ON_ERROR_STOP=1")
	seed.Stdin = strings.NewReader(seedSQL)
	if output, err := seed.CombinedOutput(); err != nil {
		t.Fatalf("seed source: %v\n%s", err, tailOf(output, 1000))
	}

	// backup -> verify -> migrate target -> restore -> replay, all through
	// the recovery tool container.
	backupOut := compose(t, 4*time.Minute, "run", "--rm", "recovery-tool", "backup")
	var backupSummary struct {
		BackupDir string `json:"backup_dir"`
		RowsTotal int64  `json:"rows_total"`
	}
	if err := json.Unmarshal([]byte(jsonLine(t, backupOut)), &backupSummary); err != nil {
		t.Fatalf("backup summary: %v (%s)", err, tailOf([]byte(backupOut), 500))
	}
	if backupSummary.BackupDir == "" || backupSummary.RowsTotal == 0 {
		t.Fatalf("backup summary incomplete: %s", backupOut)
	}
	// The printed backup_dir is the container path /backups/<name>; the
	// follow-up runs share the same volume path.
	backupName := filepath.Base(backupSummary.BackupDir)
	compose(t, 3*time.Minute, "run", "--rm", "-e", "P202_BACKUP_DIR=/backups/"+backupName, "recovery-tool", "verify")
	compose(t, 2*time.Minute, "run", "--rm", "recovery-migrate", "migrate")
	compose(t, 4*time.Minute, "run", "--rm", "-e", "P202_BACKUP_DIR=/backups/"+backupName, "recovery-tool", "restore")
	replayOut := compose(t, 3*time.Minute, "run", "--rm", "recovery-tool", "replay")
	var replaySummary struct {
		Digest   string `json:"digest"`
		Sessions int64  `json:"sessions"`
		Events   int64  `json:"events"`
	}
	if err := json.Unmarshal([]byte(jsonLine(t, replayOut)), &replaySummary); err != nil {
		t.Fatalf("replay summary: %v (%s)", err, tailOf([]byte(replayOut), 500))
	}
	if len(replaySummary.Digest) != 64 || replaySummary.Sessions != 1 || replaySummary.Events != 3 {
		t.Fatalf("replay summary unexpected: %s", replayOut)
	}

	// Start the real service on the restored target and verify the unchanged
	// /livez and /healthz contracts over loopback.
	compose(t, 4*time.Minute, "--profile", "recovery", "up", "-d", "recovery-app")
	ready := false
	deadline := time.Now().Add(90 * time.Second)
	for !ready && time.Now().Before(deadline) {
		cmd := exec.Command("docker", "compose", "--project-directory", root, "--env-file", envFile,
			"exec", "-T", "recovery-app", "wget", "-q", "-O", "-", "http://127.0.0.1:8080/healthz")
		output, err := cmd.CombinedOutput()
		if err == nil && strings.Contains(string(output), "ok") {
			ready = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("recovery-app never became ready on the restored target")
	}
	live := exec.Command("docker", "compose", "--project-directory", root, "--env-file", envFile,
		"exec", "-T", "recovery-app", "wget", "-q", "-O", "-", "http://127.0.0.1:8080/livez")
	if output, err := live.CombinedOutput(); err != nil {
		t.Fatalf("livez on restored target: %v\n%s", err, tailOf(output, 500))
	}
}

// jsonLine extracts the tool's JSON summary line from compose run output
// (which also carries container lifecycle messages on stderr).
func jsonLine(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "{") {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("no JSON summary in compose output: %s", tailOf([]byte(output), 500))
	return ""
}

func tailOf(raw []byte, limit int) string {
	text := string(raw)
	if len(text) > limit {
		return text[len(text)-limit:]
	}
	return text
}
