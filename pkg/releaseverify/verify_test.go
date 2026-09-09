package releaseverify

import (
	"strings"
	"testing"
)

const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const runtimeDataPlaneConfig = `---
apiVersion: v1
kind: ConfigMap
metadata: {name: runtime-data-plane-profiles}
data:
  profiles.json: '[]'
  mcp-profiles.json: '[]'
`

const runtimeDataPlaneProfileEnv = `        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
`

const runtimeDataPlaneCredentialEnv = `        - name: DATA_PLANE_QDRANT_API_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: qdrant-api-key}}
        - name: DATA_PLANE_EMBEDDING_API_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: embedding-api-key}}
        - name: DATA_PLANE_S3_ACCESS_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: s3-access-key}}
        - name: DATA_PLANE_S3_SECRET_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: s3-secret-key}}
`

const mcpProfileEnv = `        - name: MCP_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: mcp-profiles.json
`

const mcpCredentialEnv = `        - name: TRPC_SECRET_MCP_SUPPORT_AUTH
          valueFrom:
            secretKeyRef:
              name: mcp-profile-credentials
              key: support-authorization
`

func TestValidateReleaseAcceptsDigestPinnedAttestedBundle(t *testing.T) {
	if err := ValidateRelease(validReleaseBundle(), []byte(validNetworkPolicy()), validReleaseContext()); err != nil {
		t.Fatalf("ValidateRelease: %v", err)
	}
}

func TestValidateReleaseRejectsMissingRuntimeDataPlaneProfiles(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), runtimeDataPlaneConfig, "", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "runtime-data-plane-profiles") {
		t.Fatalf("ValidateRelease error = %v, want missing runtime profile catalog rejection", err)
	}
}

func TestValidateReleaseRejectsInlineRuntimeDataPlaneCredential(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"profiles.json: '[]'",
		`profiles.json: '[{"id":"knowledge","backend":"qdrant","endpoint":"qdrant:6334","tls":true,"collection":"knowledge","dimension":3,"embeddingEndpoint":"https://embed.example/v1","embeddingModel":"embed","embeddingAPIKeyEnv":"DATA_PLANE_EMBEDDING_API_KEY","apiKey":"inline-secret"}]'`,
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "runtime data-plane profile") {
		t.Fatalf("ValidateRelease error = %v, want inline credential rejection", err)
	}
}

func TestValidateReleaseRejectsInvalidMCPProfileCatalog(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"mcp-profiles.json: '[]'",
		`mcp-profiles.json: '[{"id":"orders","transport":"streamable","serverUrl":"http://mcp.example.test","tools":["lookup"]}]'`,
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "MCP profile") {
		t.Fatalf("ValidateRelease error = %v, want invalid MCP profile rejection", err)
	}
}

func TestValidateReleaseRejectsMissingMCPWorkloadBinding(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), mcpProfileEnv, "", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "MCP_PROFILES") {
		t.Fatalf("ValidateRelease error = %v, want missing MCP profile binding rejection", err)
	}
}

func TestValidateReleaseRejectsMCPCredentialOutsideWorker(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		runtimeDataPlaneProfileEnv,
		runtimeDataPlaneProfileEnv+mcpCredentialEnv,
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "MCP credential") {
		t.Fatalf("ValidateRelease error = %v, want non-Worker MCP credential rejection", err)
	}
}

func TestValidateReleaseRejectsMissingReferencedMCPCredential(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"mcp-profiles.json: '[]'",
		`mcp-profiles.json: '[{"id":"orders","transport":"streamable","serverUrl":"https://mcp.example.test","tools":["lookup"],"headerRefs":{"Authorization":"env://TRPC_SECRET_MCP_ORDERS_AUTH"}}]'`,
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "TRPC_SECRET_MCP_ORDERS_AUTH") {
		t.Fatalf("ValidateRelease error = %v, want missing referenced MCP credential rejection", err)
	}
}

func TestValidateReleaseRejectsMissingRuntimeDataPlaneWorkloadBinding(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), runtimeDataPlaneProfileEnv, "", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "DATA_PLANE_PROFILES") {
		t.Fatalf("ValidateRelease error = %v, want missing workload profile binding rejection", err)
	}
}

func TestValidateReleaseRejectsMissingWorkerRuntimeDataPlaneCredential(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), runtimeDataPlaneCredentialEnv, "", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "DATA_PLANE_QDRANT_API_KEY") {
		t.Fatalf("ValidateRelease error = %v, want missing Worker data-plane credentials rejection", err)
	}
}

func TestValidateReleaseRejectsRuntimeDataPlaneCredentialOutsideWorker(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		runtimeDataPlaneProfileEnv,
		runtimeDataPlaneProfileEnv+runtimeDataPlaneCredentialEnv,
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "Worker-only") {
		t.Fatalf("ValidateRelease error = %v, want non-Worker credential rejection", err)
	}
}

func TestValidateReleaseRejectsMutableImage(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), "registry.example.invalid/agent-gateway@"+digest, "registry.example.invalid/agent-gateway:0.1.0", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "immutable sha256") {
		t.Fatalf("ValidateRelease error = %v, want immutable image rejection", err)
	}
}

func TestValidateReleaseRejectsUnattestedMesh(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), "value: \"true\"", "value: \"false\"", 1)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "WORKER_MESH_MTLS_ASSERTED") {
		t.Fatalf("ValidateRelease error = %v, want mesh assertion rejection", err)
	}
}

func TestValidateReleaseRejectsPublicEgress(t *testing.T) {
	policy := validNetworkPolicy() + ipBlockPolicy("0.0.0.0/0")
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "public CIDR") {
		t.Fatalf("ValidateRelease error = %v, want public egress rejection", err)
	}
}

func TestValidateReleaseRejectsSplitWorldEgress(t *testing.T) {
	policy := validNetworkPolicy() + ipBlockPolicy("0.0.0.0/1")
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "public CIDR") {
		t.Fatalf("ValidateRelease error = %v, want public egress rejection", err)
	}
}

func TestValidateReleaseRejectsNamespaceOnlyEgressGateway(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "\n      podSelector:\n        matchLabels:\n          app: agent-egress-gateway", "", 3)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "explicit Pod selector") {
		t.Fatalf("ValidateRelease error = %v, want namespace-only egress gateway rejection", err)
	}
}

func TestValidateReleaseRejectsPodOnlyEgressGateway(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "    - namespaceSelector:\n        matchLabels:\n          agent-platform-access: egress-gateway\n      podSelector:", "    - podSelector:", 3)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "egress gateway") {
		t.Fatalf("ValidateRelease error = %v, want pod-only egress gateway rejection", err)
	}
}

func TestValidateReleaseRejectsDefaultDenyThatDoesNotSelectEveryPod(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "  podSelector: {}\n  policyTypes: [Ingress, Egress]", "  podSelector:\n    matchLabels:\n      app: decoy\n  policyTypes: [Ingress, Egress]", 1)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "deny both ingress and egress") {
		t.Fatalf("ValidateRelease error = %v, want scoped default-deny rejection", err)
	}
}

func TestValidateReleaseRejectsDefaultDenyWithAllowRule(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "  policyTypes: [Ingress, Egress]", "  policyTypes: [Ingress, Egress]\n  ingress:\n  - {}", 1)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "deny both ingress and egress") {
		t.Fatalf("ValidateRelease error = %v, want permissive default-deny rejection", err)
	}
}

func TestValidateReleaseRejectsIncompleteDefaultDenyPolicyTypes(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "  policyTypes: [Ingress, Egress]", "  policyTypes: [Ingress]", 1)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "deny both ingress and egress") {
		t.Fatalf("ValidateRelease error = %v, want incomplete default-deny rejection", err)
	}
}

func TestValidateReleaseRejectsUnconstrainedEgressPeer(t *testing.T) {
	policy := validNetworkPolicy() + `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-egress-bypass
spec:
  podSelector:
    matchLabels: {app: agent-worker}
  egress:
  - to:
    - {}
`
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "explicit Pod selector") {
		t.Fatalf("ValidateRelease error = %v, want unconstrained peer rejection", err)
	}
}

func TestValidateReleaseRejectsEgressRuleWithoutDestination(t *testing.T) {
	policy := validNetworkPolicy() + `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-all-destinations
spec:
  podSelector:
    matchLabels: {app: agent-worker}
  egress:
  - ports:
    - {protocol: TCP, port: 443}
`
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "no constrained destination") {
		t.Fatalf("ValidateRelease error = %v, want destination-free rule rejection", err)
	}
}

func TestValidateReleaseRejectsBroadOrReservedPrivateEgress(t *testing.T) {
	for _, test := range []struct {
		cidr      string
		errorText string
	}{
		{cidr: "10.0.0.0/8", errorText: "exact private unicast host route"},
		{cidr: "169.254.169.254/32", errorText: "exact private unicast host route"},
		{cidr: "127.0.0.1/32", errorText: "exact private unicast host route"},
		{cidr: "fc00::/64", errorText: "exact private unicast host route"},
		{cidr: "::ffff:10.0.0.1/128", errorText: "public CIDR"},
	} {
		t.Run(test.cidr, func(t *testing.T) {
			policy := validNetworkPolicy() + `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: unmanaged-private-egress
spec:
  podSelector:
    matchLabels: {app: agent-worker}
  egress:
  - to:
    - ipBlock:
        cidr: ` + test.cidr + "\n"
			if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), test.errorText) {
				t.Fatalf("ValidateRelease error = %v, want unsafe private CIDR rejection", err)
			}
		})
	}
}

func TestValidateReleaseAcceptsExactPrivateBackendHost(t *testing.T) {
	policy := validNetworkPolicy() + `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: managed-postgres-host
spec:
  podSelector:
    matchLabels: {app: agent-worker}
  egress:
  - to:
    - ipBlock:
        cidr: 10.20.30.40/32
`
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err != nil {
		t.Fatalf("ValidateRelease exact private host: %v", err)
	}
}

func TestValidateReleaseBindsSchemaClassToMigrationArtifact(t *testing.T) {
	if err := ValidateRelease(
		validReleaseBundle(),
		[]byte(validNetworkPolicy()),
		ReleaseContext{SchemaClass: "breaking", BreakingChangeID: "CHANGE-1234"},
	); err == nil || !strings.Contains(err.Error(), "does not match rollout schema class") {
		t.Fatalf("ValidateRelease error = %v, want schema-class mismatch", err)
	}
}

func TestValidateReleaseBindsBreakingChangeIDToMigrationArtifact(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"agent.trpc.io/schema-class: compatible",
		"agent.trpc.io/schema-class: breaking\n    agent.trpc.io/breaking-change-id: CHANGE-1234",
		1,
	)
	context := ReleaseContext{SchemaClass: "breaking", BreakingChangeID: "CHANGE-1234"}
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), context); err != nil {
		t.Fatalf("ValidateRelease breaking bundle: %v", err)
	}
	context.BreakingChangeID = "CHANGE-OTHER"
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), context); err == nil || !strings.Contains(err.Error(), "approved change record") {
		t.Fatalf("ValidateRelease error = %v, want breaking change mismatch", err)
	}
}

func TestValidateReleaseRejectsUnexpectedBreakingChangeID(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"agent.trpc.io/schema-class: compatible",
		"agent.trpc.io/schema-class: compatible\n    agent.trpc.io/breaking-change-id: STALE-CHANGE",
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "must not carry") {
		t.Fatalf("ValidateRelease error = %v, want stale change ID rejection", err)
	}
}

func TestValidateReleaseRejectsUnapprovedBundleResources(t *testing.T) {
	for _, resource := range []string{
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: injected-config}\n",
		"apiVersion: apps/v1\nkind: DaemonSet\nmetadata: {name: injected-daemon}\n",
		"apiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata: {name: injected-rbac}\n",
	} {
		bundle := string(validReleaseBundle()[0]) + "\n---\n" + resource
		if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "unapproved resource") {
			t.Fatalf("ValidateRelease error = %v, want unapproved resource rejection", err)
		}
	}
}

func TestValidateReleaseRejectsMissingOrDuplicateApprovedResource(t *testing.T) {
	const service = `---
apiVersion: v1
kind: Service
metadata: {name: agent-admin}
spec:
  selector: {app: agent-admin}
  ports: [{port: 8081, targetPort: 8081}]
`
	bundle := string(validReleaseBundle()[0])
	missing := strings.Replace(bundle, service, "", 1)
	if err := ValidateRelease([][]byte{[]byte(missing)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "missing required resource") {
		t.Fatalf("ValidateRelease error = %v, want missing resource rejection", err)
	}
	duplicate := bundle + "\n" + service
	if err := ValidateRelease([][]byte{[]byte(duplicate)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "duplicate resource") {
		t.Fatalf("ValidateRelease error = %v, want duplicate resource rejection", err)
	}
}

func TestValidateReleaseRejectsNamespaceOverride(t *testing.T) {
	bundle := strings.Replace(
		string(validReleaseBundle()[0]),
		"name: agent-migrate\n  annotations:",
		"name: agent-migrate\n  namespace: another-namespace\n  annotations:",
		1,
	)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(validNetworkPolicy()), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "must not override") {
		t.Fatalf("ValidateRelease error = %v, want namespace override rejection", err)
	}
}

func validReleaseContext() ReleaseContext {
	return ReleaseContext{SchemaClass: "compatible"}
}

func ipBlockPolicy(cidr string) string {
	return `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: unmanaged-ip-egress
spec:
  podSelector:
    matchLabels: {app: agent-worker}
  egress:
  - to:
    - ipBlock:
        cidr: ` + cidr + "\n"
}

func validReleaseBundle() [][]byte {
	return [][]byte{[]byte(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-gateway
spec:
  template:
    metadata:
      labels: {app: agent-gateway}
    spec:
      containers:
      - name: gateway
        image: registry.example.invalid/agent-gateway@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-worker
spec:
  template:
    metadata:
      labels: {app: agent-worker}
    spec:
      containers:
      - name: worker
        image: registry.example.invalid/agent-worker@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
        - name: MCP_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: mcp-profiles.json
        - name: TRPC_SECRET_MCP_SUPPORT_AUTH
          valueFrom:
            secretKeyRef:
              name: mcp-profile-credentials
              key: support-authorization
        - name: DATA_PLANE_QDRANT_API_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: qdrant-api-key}}
        - name: DATA_PLANE_EMBEDDING_API_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: embedding-api-key}}
        - name: DATA_PLANE_S3_ACCESS_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: s3-access-key}}
        - name: DATA_PLANE_S3_SECRET_KEY
          valueFrom: {secretKeyRef: {name: runtime-data-plane-credentials, key: s3-secret-key}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-summary-worker
spec:
  template:
    metadata:
      labels: {app: agent-summary-worker}
    spec:
      containers:
      - name: summary-worker
        image: registry.example.invalid/agent-summary-worker@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-consumer
  annotations:
    agent.trpc.io/mesh-mtls-evidence: CHANGE-1234
spec:
  template:
    metadata:
      labels: {app: agent-consumer}
    spec:
      containers:
      - name: consumer
        image: registry.example.invalid/agent-consumer@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
        - name: WORKER_ENDPOINT
          value: http://agent-worker:9090
        - name: WORKER_TRANSPORT_MODE
          value: mesh
        - name: WORKER_MESH_MTLS_ASSERTED
          value: "true"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-delivery
spec:
  template:
    metadata:
      labels: {app: agent-delivery}
    spec:
      containers:
      - name: delivery
        image: registry.example.invalid/agent-delivery@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-admin
spec:
  template:
    metadata:
      labels: {app: agent-admin}
    spec:
      containers:
      - name: admin
        image: registry.example.invalid/agent-admin@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        env:
        - name: DATA_PLANE_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: profiles.json
        - name: MCP_PROFILES
          valueFrom:
            configMapKeyRef:
              name: runtime-data-plane-profiles
              key: mcp-profiles.json
---
apiVersion: batch/v1
kind: Job
metadata:
  name: agent-migrate
  annotations:
    agent.trpc.io/schema-class: compatible
spec:
  template:
    metadata:
      labels: {app: agent-migrate}
    spec:
      containers:
      - name: migrate
        image: registry.example.invalid/agent-migrate@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
---
apiVersion: v1
kind: Service
metadata: {name: agent-gateway}
spec:
  selector: {app: agent-gateway}
  ports: [{port: 80, targetPort: 8080}]
---
apiVersion: v1
kind: Service
metadata: {name: agent-worker}
spec:
  selector: {app: agent-worker}
  ports: [{port: 9090, targetPort: 9090}]
---
apiVersion: v1
kind: Service
metadata: {name: agent-admin}
spec:
  selector: {app: agent-admin}
  ports: [{port: 8081, targetPort: 8081}]
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: agent-gateway-hpa}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: agent-gateway}
  minReplicas: 3
  maxReplicas: 10
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: agent-worker-hpa}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: agent-worker}
  minReplicas: 5
  maxReplicas: 20
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: agent-summary-worker-hpa}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: agent-summary-worker}
  minReplicas: 2
  maxReplicas: 10
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-gateway}
spec: {minAvailable: 2, selector: {matchLabels: {app: agent-gateway}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-worker}
spec: {minAvailable: 3, selector: {matchLabels: {app: agent-worker}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-summary-worker}
spec: {minAvailable: 1, selector: {matchLabels: {app: agent-summary-worker}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-consumer}
spec: {minAvailable: 2, selector: {matchLabels: {app: agent-consumer}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-delivery}
spec: {minAvailable: 1, selector: {matchLabels: {app: agent-delivery}}}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: agent-admin}
spec: {minAvailable: 1, selector: {matchLabels: {app: agent-admin}}}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: runtime-data-plane-profiles}
data:
  profiles.json: '[]'
  mcp-profiles.json: '[]'
`)}
}

func validNetworkPolicy() string {
	return `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-boundaries
spec:
  podSelector:
    matchLabels:
      app: agent-worker
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          agent-platform-access: egress-gateway
      podSelector:
        matchLabels:
          app: agent-egress-gateway
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: summary-worker-boundaries
spec:
  podSelector:
    matchLabels:
      app: agent-summary-worker
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          agent-platform-access: egress-gateway
      podSelector:
        matchLabels:
          app: agent-egress-gateway
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: delivery-boundaries
spec:
  podSelector:
    matchLabels:
      app: agent-delivery
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          agent-platform-access: egress-gateway
      podSelector:
        matchLabels:
          app: agent-egress-gateway
`
}
