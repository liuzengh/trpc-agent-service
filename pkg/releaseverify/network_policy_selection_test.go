package releaseverify

import (
	"strings"
	"testing"
)

func TestValidateReleaseRequiresEgressPolicyToSelectActualWorkerPods(t *testing.T) {
	for _, selector := range []string{
		"    matchLabels: {app: agent-worker}\n    matchExpressions: [{key: app, operator: NotIn, values: [agent-worker]}]",
		"    matchLabels: {app: agent-worker, tier: unmatched}",
		"    matchLabels: {app: agent-worker, absent-label: ''}",
	} {
		policy := replaceWorkerPolicySelector(selector)
		if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
			t.Fatalf("nonmatching Worker egress selector accepted: %s", selector)
		}
	}
}

func TestValidateReleaseEvaluatesStandardPolicyExpressions(t *testing.T) {
	for _, test := range []struct {
		name       string
		expression string
		selected   bool
	}{
		{"in matches", "{key: app, operator: In, values: [agent-worker]}", true},
		{"in missing", "{key: absent, operator: In, values: [agent-worker]}", false},
		{"not in matches", "{key: app, operator: NotIn, values: [unrelated]}", true},
		{"not in missing", "{key: absent, operator: NotIn, values: [unrelated]}", true},
		{"not in excludes", "{key: app, operator: NotIn, values: [agent-worker]}", false},
		{"exists matches", "{key: app, operator: Exists}", true},
		{"exists missing", "{key: absent, operator: Exists}", false},
		{"does not exist matches", "{key: absent, operator: DoesNotExist}", true},
		{"does not exist excludes", "{key: app, operator: DoesNotExist}", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := replaceWorkerPolicySelector("    matchExpressions: [" + test.expression + "]")
			err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext())
			if (err == nil) != test.selected {
				t.Fatalf("selector selected=%t, validation error=%v", test.selected, err)
			}
		})
	}
}

func TestValidateReleaseAllowsMatchingAdditionalPodLabels(t *testing.T) {
	bundle := strings.Replace(string(validReleaseBundle()[0]), "labels: {app: agent-worker}", "labels: {app: agent-worker, tier: runtime}", 1)
	policy := replaceWorkerPolicySelector(`    matchLabels: {app: agent-worker, tier: runtime}
    matchExpressions:
    - {key: tier, operator: In, values: [runtime]}
    - {key: disabled, operator: DoesNotExist}`)
	if err := ValidateRelease([][]byte{[]byte(bundle)}, []byte(policy), validReleaseContext()); err != nil {
		t.Fatalf("policy matching actual additional Pod labels rejected: %v", err)
	}
}

func TestValidateReleaseRejectsInvalidPolicyExpressions(t *testing.T) {
	for _, expression := range []string{
		"{key: app, operator: Equals, values: [agent-worker]}",
		"{key: app, operator: In, values: []}",
		"{key: app, operator: NotIn, values: []}",
		"{key: app, operator: Exists, values: [agent-worker]}",
		"{key: app, operator: DoesNotExist, values: [agent-worker]}",
		"{key: '', operator: Exists}",
		"{key: 'bad prefix/app', operator: Exists}",
		"{key: app, operator: In, values: ['bad value']}",
	} {
		policy := replaceWorkerPolicySelector("    matchLabels: {app: agent-worker}\n    matchExpressions: [" + expression + "]")
		if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "invalid Pod selector") {
			t.Fatalf("invalid selector expression accepted: expression=%s error=%v", expression, err)
		}
	}
}

func TestValidateReleaseRejectsInvalidEgressSelectorExpressions(t *testing.T) {
	policy := strings.Replace(validNetworkPolicy(), "          app: agent-egress-gateway", "          app: agent-egress-gateway\n        matchExpressions: [{key: app, operator: Exists, values: [invalid]}]", 1)
	if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil || !strings.Contains(err.Error(), "invalid egress selector") {
		t.Fatalf("invalid egress expression error=%v", err)
	}
}

func TestValidateReleaseRejectsContradictoryGatewayDestination(t *testing.T) {
	for _, test := range []struct {
		name, key, value string
	}{
		{"gateway Pod", "app", "agent-egress-gateway"},
		{"gateway namespace", "agent-platform-access", "egress-gateway"},
	} {
		t.Run(test.name, func(t *testing.T) {
			label := "          " + test.key + ": " + test.value
			policy := strings.Replace(validNetworkPolicy(), label, label+"\n        matchExpressions: [{key: "+test.key+", operator: NotIn, values: ["+test.value+"]}]", 1)
			if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err == nil {
				t.Fatal("impossible gateway destination accepted as controlled egress")
			}
		})
	}
}

func TestValidateReleasePreservesAdditionalGatewayConstraints(t *testing.T) {
	for _, expression := range []string{
		"{key: app, operator: In, values: [agent-egress-gateway]}",
		"{key: app, operator: Exists}",
		"{key: external-tier, operator: In, values: [gateway]}",
	} {
		policy := strings.Replace(validNetworkPolicy(), "          app: agent-egress-gateway", "          app: agent-egress-gateway\n        matchExpressions: ["+expression+"]", 1)
		if err := ValidateRelease(validReleaseBundle(), []byte(policy), validReleaseContext()); err != nil {
			t.Fatalf("compatible external gateway constraint rejected: %v", err)
		}
	}
}

func replaceWorkerPolicySelector(selector string) string {
	return strings.Replace(validNetworkPolicy(), "    matchLabels:\n      app: agent-worker\n", selector+"\n", 1)
}
