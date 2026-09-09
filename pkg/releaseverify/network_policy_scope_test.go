package releaseverify

import (
	"io"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateReleaseRejectsDefaultDenyWithMatchExpressions(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "  podSelector: {}\n  policyTypes: [Ingress, Egress]", `  podSelector:
    matchExpressions:
    - key: app
      operator: In
      values: [unrelated-workload]
  policyTypes: [Ingress, Egress]`, 1)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
		t.Fatal("default-deny selecting only unrelated Pods must not satisfy namespace-wide isolation")
	}
}

func TestValidateReleaseRejectsUnverifiedPolicyList(t *testing.T) {
	policy := validNetworkPolicy() + `
---
apiVersion: v1
kind: List
items:
- apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata: {name: allow-unrestricted-egress}
  spec:
    podSelector: {}
    policyTypes: [Egress]
    egress: [{}]
`
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
		t.Fatal("an unchecked resource list must not bypass egress validation")
	}
}

func TestValidateReleaseAcceptsNamespaceWideEmptySelectors(t *testing.T) {
	for _, selector := range []string{
		"{matchLabels: {}, matchExpressions: []}",
		"{matchLabels: null, matchExpressions: null}",
	} {
		t.Run(selector, func(t *testing.T) {
			policy := strings.Replace(validNetworkPolicy(), "podSelector: {}", "podSelector: "+selector, 1)
			if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err != nil {
				t.Fatalf("namespace-wide default-deny rejected: %v", err)
			}
		})
	}
}

func TestValidateReleaseAcceptsScopedPolicyExpressions(t *testing.T) {
	policy := validNetworkPolicy() + `
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: scoped-expression-policy}
spec:
  podSelector:
    matchExpressions:
    - key: app
      operator: In
      values: [agent-worker, agent-summary-worker]
  policyTypes: [Ingress]
`
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err != nil {
		t.Fatalf("scoped policy expressions rejected: %v", err)
	}
}

func TestValidateReleaseAcceptsPolicyLists(t *testing.T) {
	for _, kind := range []string{"List", "NetworkPolicyList"} {
		t.Run(kind, func(t *testing.T) {
			policy := networkPolicyList(t, kind, validNetworkPolicy())
			if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err != nil {
				t.Fatalf("valid NetworkPolicy list rejected: %v", err)
			}
		})
	}
}

func TestValidateReleaseChecksNestedPolicyLists(t *testing.T) {
	policy := networkPolicyList(t, "List", validNetworkPolicy()+ipBlockPolicy("0.0.0.0/0"))
	policy = networkPolicyList(t, "List", policy)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "public CIDR") {
		t.Fatalf("nested public egress error = %v, want public CIDR rejection", err)
	}
}

func TestValidateReleaseRejectsUnrecognizedOverlayResources(t *testing.T) {
	for _, resource := range []string{
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n",
		"apiVersion: unrecognized.example/v1\nkind: NetworkPolicy\nmetadata: {name: decoy}\n",
		"apiVersion: unrecognized.example/v1\nkind: List\nitems: []\n",
	} {
		policy := validNetworkPolicy() + "\n---\n" + resource
		if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
			t.Fatalf("unrecognized overlay resource accepted: %s", resource)
		}
	}
}

func TestValidateReleaseRejectsPolicyReplacementAndNamespaceOverride(t *testing.T) {
	for _, policy := range []string{
		validNetworkPolicy() + "\n---\n" + validNetworkPolicy(),
		strings.Replace(validNetworkPolicy(), "name: default-deny", "name: default-deny\n  namespace: unrelated", 1),
	} {
		if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
			t.Fatal("policy replacement or namespace override must fail before deployment")
		}
	}
}

func TestValidateReleaseBoundsPolicyListNesting(t *testing.T) {
	policy := validNetworkPolicy()
	for range 9 {
		policy = networkPolicyList(t, "List", policy)
	}
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "nesting limits") {
		t.Fatalf("nested list error = %v, want bounded traversal rejection", err)
	}
}

func TestValidateReleaseBoundsPolicyListResources(t *testing.T) {
	policy := networkPolicyList(t, "List", strings.Repeat(`
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: repeated-policy}
spec: {podSelector: {}, policyTypes: [Ingress]}
`, 1000))
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "resource or list nesting limits") {
		t.Fatalf("oversized list error = %v, want bounded traversal rejection", err)
	}
}

func networkPolicyList(t *testing.T, kind, data string) string {
	t.Helper()
	var items []map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(data))
	for {
		var item map[string]any
		if err := decoder.Decode(&item); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if item != nil {
			items = append(items, item)
		}
	}
	version := "v1"
	if kind == "NetworkPolicyList" {
		version = "networking.k8s.io/v1"
	}
	encoded, err := yaml.Marshal(map[string]any{"apiVersion": version, "kind": kind, "items": items})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
