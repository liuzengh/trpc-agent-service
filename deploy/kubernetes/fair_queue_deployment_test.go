package kubernetes_test

import "testing"

func TestConsumerEnablesMeasuredFairQueueProfile(t *testing.T) {
	workloads := readWorkloads(t, "pipeline-deployments.yaml")
	for _, workload := range workloads {
		if workload.Kind != "Deployment" || workload.Metadata.Name != "agent-consumer" {
			continue
		}
		if len(workload.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("agent-consumer container count = %d, want 1", len(workload.Spec.Template.Spec.Containers))
		}
		env := make(map[string]string)
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			env[variable.Name] = variable.Value
		}
		if got := env["FAIR_QUEUE_ENABLED"]; got != "true" {
			t.Errorf("agent-consumer FAIR_QUEUE_ENABLED = %q, want true", got)
		}
		if got := env["CONCURRENCY"]; got != "4" {
			t.Errorf("agent-consumer CONCURRENCY = %q, want measured local-safe value 4", got)
		}
		return
	}
	t.Fatal("agent-consumer Deployment not found")
}
