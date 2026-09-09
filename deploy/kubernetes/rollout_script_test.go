package kubernetes_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRolloutScriptReplacesOnlyCompletedMigrationJobs(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if runtime.GOOS == "windows" {
		if git, gitErr := exec.LookPath("git"); gitErr == nil {
			candidate := filepath.Join(filepath.Dir(git), "..", "bin", "bash.exe")
			if _, statErr := os.Stat(candidate); statErr == nil {
				bash, err = candidate, nil
			}
		}
	}
	if err != nil {
		t.Skip("Bash is required for the rollout behavior test")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bundle := t.TempDir()
	for _, name := range []string{
		"migration.yaml", "profiles.yaml", "availability-policies.yaml", "worker.yaml",
		"summary.yaml", "pipeline.yaml", "admin.yaml", "gateway.yaml",
	} {
		if err := os.WriteFile(filepath.Join(bundle, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Exported shell functions intercept every kubectl call, including subprocesses.
	const harness = `
set -euo pipefail
kubectl() {
  case "$*" in
    *"get namespace"*) printf '%s\n' namespace/review ;;
    *"get job agent-migrate -o jsonpath"*)
      case "$*" in
        *".status.active"*) printf '%s' "$TEST_JOB_ACTIVE" ;;
        *".status.failed"*) printf '%s' "$TEST_JOB_FAILED" ;;
        *"Complete"*) printf '%s' "$TEST_JOB_COMPLETE" ;;
        *) return 1 ;;
      esac ;;
    *"get job agent-migrate"*) [[ "$TEST_JOB_PRESENT" == true ]] ;;
    *"delete job agent-migrate"*) printf '%s\n' MIGRATION_JOB_DELETED ;;
    *) return 0 ;;
  esac
}
export -f kubectl
export RELEASE_VERIFY_BIN="$(type -P true)"
bash scripts/k8s_apply.sh
`
	for _, test := range []struct {
		name, active, failed, complete, wantError string
		present, wantDelete                       bool
	}{
		{name: "absent"},
		{name: "no status", present: true, wantError: "agent-migrate is not complete"},
		{name: "pending", present: true, active: "0", failed: "0", wantError: "agent-migrate is not complete"},
		{name: "incomplete condition", present: true, complete: "False", wantError: "agent-migrate is not complete"},
		{name: "active", present: true, active: "1", wantError: "agent-migrate is active"},
		{name: "failed", present: true, failed: "1", wantError: "agent-migrate failed"},
		{name: "completed", present: true, complete: "True", wantDelete: true},
		{name: "completed with active pods", present: true, active: "1", complete: "True", wantError: "agent-migrate is active"},
		{name: "completed with failures", present: true, failed: "1", complete: "True", wantError: "agent-migrate failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			present := "false"
			if test.present {
				present = "true"
			}
			command := exec.Command(bash, "-c", harness)
			command.Dir = repo
			command.Env = append(os.Environ(),
				"PLATFORM_NAMESPACE=review-only",
				"RELEASE_MANIFEST_DIR="+filepath.ToSlash(bundle),
				"NETWORK_POLICY_FILE=deploy/kubernetes/network-policies.yaml",
				"RELEASE_SCHEMA_CLASS=compatible",
				"TEST_JOB_PRESENT="+present,
				"TEST_JOB_ACTIVE="+test.active,
				"TEST_JOB_FAILED="+test.failed,
				"TEST_JOB_COMPLETE="+test.complete,
			)
			output, err := command.CombinedOutput()
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("rollout failed: %v\n%s", err, output)
				}
			} else if err == nil || !strings.Contains(string(output), test.wantError) {
				t.Fatalf("expected failure %q, got %v\n%s", test.wantError, err, output)
			}
			if deleted := strings.Contains(string(output), "MIGRATION_JOB_DELETED"); deleted != test.wantDelete {
				t.Fatalf("migration Job deleted=%t, want %t\n%s", deleted, test.wantDelete, output)
			}
		})
	}
}

func TestRolloutScriptProtectsMigrationAndWaitsForReadiness(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "scripts", "k8s_apply.sh"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, required := range []string{
		"PLATFORM_NAMESPACE must explicitly name the dedicated application namespace",
		"RELEASE_MANIFEST_DIR must name a rendered, digest-pinned release bundle directory",
		"RELEASE_SCHEMA_CLASS must be bootstrap, compatible, or breaking",
		"GOTOOLCHAIN=local go run ./cmd/releaseverify",
		"--schema-class \"$release_schema_class\"",
		"--breaking-change-id \"${BREAKING_MIGRATION_CHANGE_ID:-}\"",
		"BREAKING_MIGRATION_DRAIN_EVIDENCE must name a non-empty drain evidence artifact",
		`"$release_manifest_dir/profiles.yaml"`,
		"runtime-data-plane-credentials",
		"agent-platform-access=$access",
		`kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/profiles.yaml"`,
		"Pods already exist; bootstrap releases must not cross an active runtime boundary",
		"has Pods without a Deployment; remove them before a breaking migration",
		"agent-migrate is active; wait for it to finish before another rollout",
		"agent-migrate failed; inspect and resolve it before another rollout",
		"kubectl -n \"$platform_namespace\" rollout status --timeout=\"$rollout_timeout\" \"deployment/$deployment\"",
		"kubectl -n \"$platform_namespace\" rollout status --timeout=\"$rollout_timeout\" deployment/agent-summary-worker",
		"kubectl -n \"$platform_namespace\" rollout status --timeout=\"$rollout_timeout\" deployment/agent-gateway",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("rollout script is missing %q", required)
		}
	}
	for _, workload := range []string{
		"agent-gateway",
		"agent-consumer",
		"agent-worker",
		"agent-summary-worker",
		"agent-delivery",
		"agent-admin",
	} {
		if !strings.Contains(contents, workload) {
			t.Errorf("migration write-silence workload set is missing %q", workload)
		}
	}
	if got := strings.Count(contents, "for workload in \"${application_workloads[@]}\"; do"); got != 2 {
		t.Fatalf("application workload set must guard bootstrap and breaking migrations, loop count=%d", got)
	}
	if !strings.Contains(contents, "require_workload_absent \"$workload\"") ||
		!strings.Contains(contents, "require_workload_stopped \"$workload\"") {
		t.Fatal("bootstrap and breaking migration classes must enforce the complete workload set")
	}
	if strings.Contains(contents, "platform_namespace=\"${platform_namespace:-default}\"") ||
		strings.Contains(contents, "delete job agent-migrate --ignore-not-found") {
		t.Fatal("rollout script still has an unsafe namespace or migration-job fallback")
	}
	if strings.Contains(contents, "apply -f deploy/kubernetes/") {
		t.Fatal("rollout script must apply only a verified release bundle, never source templates")
	}
	worker := strings.Index(contents, "apply -f \"$release_manifest_dir/worker.yaml\"")
	workerReady := strings.Index(contents, "rollout status --timeout=\"$rollout_timeout\" deployment/agent-worker")
	summaryWorker := strings.Index(contents, "apply -f \"$release_manifest_dir/summary.yaml\"")
	summaryReady := strings.Index(contents, "rollout status --timeout=\"$rollout_timeout\" deployment/agent-summary-worker")
	pipeline := strings.Index(contents, "apply -f \"$release_manifest_dir/pipeline.yaml\"")
	gateway := strings.Index(contents, "apply -f \"$release_manifest_dir/gateway.yaml\"")
	profiles := strings.Index(contents, "apply -f \"$release_manifest_dir/profiles.yaml\"")
	if profiles < 0 || worker < 0 || profiles >= worker {
		t.Fatal("runtime data-plane profiles must be applied before Worker")
	}
	if worker < 0 || gateway < 0 || worker >= gateway {
		t.Fatal("Gateway must be applied after the downstream Worker rollout starts")
	}
	if workerReady < 0 || pipeline < 0 || worker >= workerReady || workerReady >= pipeline {
		t.Fatal("Worker must become ready before Consumer and Delivery are submitted")
	}
	if summaryWorker < 0 || summaryReady < 0 || summaryWorker >= summaryReady || summaryReady >= pipeline {
		t.Fatal("Summary Worker must become ready before the asynchronous pipeline is submitted")
	}
}
