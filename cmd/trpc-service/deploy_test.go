package main

import (
	"os"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// TestK8sManifestsProvideKMSToken pins the KMS bootstrap contract: with
// resolver=kms the process refuses to start unless the bootstrap token file
// exists under SecretsDir, so a deployment that asks for kms must also mount a
// Secret volume at that dir carrying the token ref's file. The in-code
// fallback for the ref must agree with the deployed value: a default
// disagreeing with the mounted file name turns one dropped env var into a
// CrashLoop. Nothing in the toolchain checks YAML against Go, so this test
// guards the drift.
func TestK8sManifestsProvideKMSToken(t *testing.T) {
	cfg := readManifest(t, "../../deploy/k8s/config.yaml")
	if !strings.Contains(cfg, `TRPC_SECRET_RESOLVER: "kms"`) {
		t.Fatalf(`config.yaml must deploy the production resolver (TRPC_SECRET_RESOLVER: "kms"); if it moved off kms on purpose, update this test with it`)
	}
	dir := manifestEnv(t, cfg, "TRPC_SECRETS_DIR")
	ref := manifestEnv(t, cfg, "TRPC_KMS_TOKEN_REF")

	t.Setenv("TRPC_KMS_TOKEN_REF", "") // getenv treats empty as unset
	if got := config.Load().KMSTokenRef; got != ref {
		t.Errorf("config.yaml sets TRPC_KMS_TOKEN_REF=%q but config.Load() falls back to %q: the code default and the manifests disagree, so losing the env var silently points the resolver at a file that is never mounted", ref, got)
	}

	for _, role := range []string{"gateway", "worker", "admin"} {
		manifest := readManifest(t, "../../deploy/k8s/"+role+".yaml")
		if !strings.Contains(manifest, "mountPath: "+dir) {
			t.Errorf("%s.yaml: nothing mounted at TRPC_SECRETS_DIR %q — without the volume the bootstrap token never reaches the pod and it CrashLoops at startup", role, dir)
		}
		if !strings.Contains(manifest, "path: "+ref) {
			t.Errorf("%s.yaml: no Secret item files %q into TRPC_SECRETS_DIR — the resolver reads the token by that name", role, ref)
		}
	}
}

// TestK8sIngressIsTheOnlyPublicEntry pins the external topology: the gateway
// Service stays cluster-internal and declares ClusterIP explicitly on port 80,
// the Ingress is the single public entry, carries a TLS block, and its backend
// matches that Service.
//
// It also pins what must NOT be exposed: the mock channel's callback is an
// unauthenticated message injector, so a public path for it would hand out a
// way to forge inbound messages for any binding.
func TestK8sIngressIsTheOnlyPublicEntry(t *testing.T) {
	gw := stripComments(readManifest(t, "../../deploy/k8s/gateway.yaml"))
	ing := stripComments(readManifest(t, "../../deploy/k8s/ingress.yaml"))

	if !strings.Contains(gw, "type: ClusterIP") {
		t.Errorf("gateway.yaml: the gateway Service must declare `type: ClusterIP` so ingress.yaml stays the single public entry; a cluster with no ingress controller needs LoadBalancer plus TLS annotations and a deliberate deletion of ingress.yaml and this test")
	}
	if !strings.Contains(gw, "- {port: 80, targetPort: http}") {
		t.Errorf("gateway.yaml: expected the Service to publish port 80 -> http, which is what ingress.yaml routes to by number")
	}

	// channel_binding.webhook_path is auto-filled with
	// /callback/{channel}/{binding_id}, so this is the route that has to be
	// reachable from the internet for tenant callbacks to land at all.
	if !strings.Contains(ing, "path: /callback") {
		t.Errorf("ingress.yaml: no `/callback` path — web.BindingDispatcher's multi-tenant route is unreachable and every IM webhook 404s")
	}
	if !strings.Contains(ing, "name: trpc-gateway") {
		t.Errorf("ingress.yaml: backend does not point at the trpc-gateway Service")
	}
	if !strings.Contains(ing, "port: {number: 80}") {
		t.Errorf("ingress.yaml: backend port is not the Service's port 80")
	}
	if !strings.Contains(ing, "secretName:") {
		t.Errorf("ingress.yaml: no TLS block — the IM platforms only webhook to HTTPS served with a trusted certificate")
	}
	if strings.Contains(ing, "/mock/callback") {
		t.Errorf("ingress.yaml must not expose /mock/callback: the mock channel is an unauthenticated message injector (off unless TRPC_MOCK_CHANNEL=true), and a public path for it lets anyone forge inbound messages for any binding")
	}
}

// stripComments drops whole-line YAML comments, so a check for whether a path
// is exposed cannot be satisfied by a comment explaining that it is not.
func stripComments(manifest string) string {
	var keep []string
	for _, line := range strings.Split(manifest, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}

// readManifest reads one repo manifest; a missing file is a broken checkout,
// not a skip.
func readManifest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// manifestEnv reads one `KEY: "value"` line out of a manifest, failing the test
// when the key is absent or empty — a missing declaration is itself the drift
// this suite exists to catch.
func manifestEnv(t *testing.T, manifest, key string) string {
	t.Helper()
	for _, line := range strings.Split(manifest, "\n") {
		after, ok := strings.CutPrefix(strings.TrimSpace(line), key+":")
		if !ok {
			continue
		}
		v := strings.TrimSpace(after)
		if i := strings.Index(v, "#"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		v = strings.Trim(v, `"`)
		if v == "" {
			t.Fatalf("deploy/k8s/config.yaml declares %s empty", key)
		}
		return v
	}
	t.Fatalf("deploy/k8s/config.yaml does not declare %s", key)
	return ""
}
