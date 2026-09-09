package kubernetes_test

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestWorkerDeploymentUsesLeastPrivilegeRuntimeContracts(t *testing.T) {
	data, err := os.ReadFile("worker-deployment.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					AutomountServiceAccountToken *bool `yaml:"automountServiceAccountToken"`
					Containers                   []struct {
						Name string `yaml:"name"`
						Env  []struct {
							Name string `yaml:"name"`
						} `yaml:"env"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Spec.AutomountServiceAccountToken == nil ||
		*deployment.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatal("worker must disable the default Kubernetes service account token")
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("worker container count=%d", len(deployment.Spec.Template.Spec.Containers))
	}
	environment := make(map[string]bool)
	for _, variable := range deployment.Spec.Template.Spec.Containers[0].Env {
		environment[variable.Name] = true
	}
	for _, name := range []string{
		"STORAGE_BACKEND_PROFILES",
		"TENANT_POSTGRES_DSN",
		"TENANT_REDIS_URL",
		"EXECUTION_LEASE_TTL",
		"EXECUTION_HEARTBEAT_INTERVAL",
	} {
		if !environment[name] {
			t.Errorf("worker deployment is missing %s", name)
		}
	}
	if environment["EXECUTION_STALE_AFTER"] {
		t.Error("worker deployment still exposes the removed EXECUTION_STALE_AFTER setting")
	}
}

func TestConsumerDeploymentDeclaresAttestedMeshTransport(t *testing.T) {
	data, err := os.ReadFile("pipeline-deployments.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	found := false
	for {
		var value struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Env []struct {
								Name  string `yaml:"name"`
								Value string `yaml:"value"`
							} `yaml:"env"`
						} `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&value); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if value.Kind != "Deployment" || value.Metadata.Name != "agent-consumer" {
			continue
		}
		found = true
		if len(value.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("consumer container count=%d, want 1", len(value.Spec.Template.Spec.Containers))
		}
		environment := make(map[string]string)
		for _, variable := range value.Spec.Template.Spec.Containers[0].Env {
			environment[variable.Name] = variable.Value
		}
		if environment["WORKER_ENDPOINT"] != "http://agent-worker:9090" {
			t.Fatalf("consumer Worker endpoint=%q, want documented mesh app hop", environment["WORKER_ENDPOINT"])
		}
		if environment["WORKER_TRANSPORT_MODE"] != "mesh" {
			t.Fatalf("consumer transport mode=%q, want mesh", environment["WORKER_TRANSPORT_MODE"])
		}
		if environment["WORKER_MESH_MTLS_ASSERTED"] != "false" {
			t.Fatalf("consumer mesh assertion=%q, want fail-closed false baseline", environment["WORKER_MESH_MTLS_ASSERTED"])
		}
		executionTimeout, err := time.ParseDuration(environment["WORKER_EXECUTION_TIMEOUT"])
		if err != nil {
			t.Fatalf("consumer Worker execution timeout: %v", err)
		}
		processTimeout, err := time.ParseDuration(environment["PROCESS_TIMEOUT"])
		if err != nil {
			t.Fatalf("consumer process timeout: %v", err)
		}
		if executionTimeout != 90*time.Second || processTimeout < executionTimeout+35*time.Second {
			t.Fatalf("consumer timeout contract execution=%s process=%s", executionTimeout, processTimeout)
		}
	}
	if !found {
		t.Fatal("pipeline manifest has no agent-consumer deployment")
	}
}

func TestOnlyWorkerReceivesTenantStorageConnectionSecrets(t *testing.T) {
	files := []string{
		"gateway-deployment.yaml",
		"worker-deployment.yaml",
		"admin-deployment.yaml",
		"pipeline-deployments.yaml",
	}
	wantWorkerSecrets := map[string]bool{
		"TENANT_POSTGRES_DSN": false,
		"TENANT_REDIS_URL":    false,
	}
	for _, filename := range files {
		for _, workload := range readWorkloads(t, filename) {
			if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
				continue
			}
			for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
				if _, tracked := wantWorkerSecrets[variable.Name]; !tracked {
					continue
				}
				if workload.Metadata.Name != "agent-worker" {
					t.Errorf("Deployment %s must not receive %s", workload.Metadata.Name, variable.Name)
					continue
				}
				if variable.ValueFrom.SecretKeyRef.Name != "tenant-storage-credentials" ||
					variable.ValueFrom.SecretKeyRef.Key == "" {
					t.Errorf("Worker %s must come from tenant-storage-credentials", variable.Name)
				}
				wantWorkerSecrets[variable.Name] = true
			}
		}
	}
	for name, found := range wantWorkerSecrets {
		if !found {
			t.Errorf("Worker is missing %s", name)
		}
	}
}
