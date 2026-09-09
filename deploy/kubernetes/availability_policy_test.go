package kubernetes_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAvailabilityPoliciesCoverEveryLongRunningWorkload(t *testing.T) {
	data, err := os.ReadFile("availability-policies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"agent-admin":          false,
		"agent-consumer":       false,
		"agent-delivery":       false,
		"agent-gateway":        false,
		"agent-worker":         false,
		"agent-summary-worker": false,
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var policy struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
			Metadata   struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				MinAvailable int `yaml:"minAvailable"`
				Selector     struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"selector"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&policy); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if policy.APIVersion != "policy/v1" || policy.Kind != "PodDisruptionBudget" {
			t.Fatalf("unexpected availability resource %s %s", policy.APIVersion, policy.Kind)
		}
		if _, ok := want[policy.Metadata.Name]; !ok {
			t.Fatalf("unexpected PodDisruptionBudget %q", policy.Metadata.Name)
		}
		if want[policy.Metadata.Name] {
			t.Fatalf("duplicate PodDisruptionBudget %q", policy.Metadata.Name)
		}
		if policy.Spec.MinAvailable < 1 || policy.Spec.Selector.MatchLabels["app"] != policy.Metadata.Name {
			t.Fatalf("PodDisruptionBudget %q does not protect its workload", policy.Metadata.Name)
		}
		want[policy.Metadata.Name] = true
	}
	for workload, found := range want {
		if !found {
			t.Errorf("workload %q has no PodDisruptionBudget", workload)
		}
	}
}
