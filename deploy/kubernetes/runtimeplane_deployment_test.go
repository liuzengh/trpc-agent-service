package kubernetes_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type runtimePlaneWorkload struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Env []struct {
						Name      string `yaml:"name"`
						ValueFrom struct {
							ConfigMapKeyRef struct {
								Name string `yaml:"name"`
								Key  string `yaml:"key"`
							} `yaml:"configMapKeyRef"`
							SecretKeyRef struct {
								Name string `yaml:"name"`
								Key  string `yaml:"key"`
							} `yaml:"secretKeyRef"`
						} `yaml:"valueFrom"`
					} `yaml:"env"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestEveryTenantAwareDeploymentReceivesPublicRuntimeDataPlaneProfiles(t *testing.T) {
	want := map[string]bool{
		"agent-gateway": false, "agent-worker": false, "agent-summary-worker": false,
		"agent-consumer": false, "agent-delivery": false, "agent-admin": false,
	}
	for _, workload := range readRuntimePlaneWorkloads(t) {
		if workload.Kind != "Deployment" {
			continue
		}
		if _, expected := want[workload.Metadata.Name]; !expected {
			continue
		}
		if len(workload.Spec.Template.Spec.Containers) != 1 {
			t.Errorf("Deployment %s container count=%d", workload.Metadata.Name, len(workload.Spec.Template.Spec.Containers))
			continue
		}
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			if variable.Name != "DATA_PLANE_PROFILES" {
				continue
			}
			ref := variable.ValueFrom.ConfigMapKeyRef
			if ref.Name != "runtime-data-plane-profiles" || ref.Key != "profiles.json" {
				t.Errorf("Deployment %s has unsafe profile source %+v", workload.Metadata.Name, ref)
			}
			want[workload.Metadata.Name] = true
		}
	}
	for deployment, found := range want {
		if !found {
			t.Errorf("Deployment %s is missing DATA_PLANE_PROFILES", deployment)
		}
	}
}

func TestOnlyWorkerReceivesRuntimeDataPlaneCredentials(t *testing.T) {
	want := map[string]string{
		"DATA_PLANE_QDRANT_API_KEY":    "qdrant-api-key",
		"DATA_PLANE_EMBEDDING_API_KEY": "embedding-api-key",
		"DATA_PLANE_S3_ACCESS_KEY":     "s3-access-key",
		"DATA_PLANE_S3_SECRET_KEY":     "s3-secret-key",
	}
	found := make(map[string]bool, len(want))
	for _, workload := range readRuntimePlaneWorkloads(t) {
		if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
			continue
		}
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			expectedKey, tracked := want[variable.Name]
			if !tracked {
				continue
			}
			if workload.Metadata.Name != "agent-worker" {
				t.Errorf("Deployment %s receives Worker-only credential %s", workload.Metadata.Name, variable.Name)
				continue
			}
			ref := variable.ValueFrom.SecretKeyRef
			if ref.Name != "runtime-data-plane-credentials" || ref.Key != expectedKey {
				t.Errorf("Worker credential %s has wrong Secret reference %+v", variable.Name, ref)
			}
			found[variable.Name] = true
		}
	}
	for name := range want {
		if !found[name] {
			t.Errorf("Worker is missing credential %s", name)
		}
	}
}

func TestRuntimeDataPlaneConfigDefaultsToFailClosedEmptyCatalog(t *testing.T) {
	data, err := os.ReadFile("runtime-data-plane-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.Kind != "ConfigMap" || config.Metadata.Name != "runtime-data-plane-profiles" {
		t.Fatalf("unexpected runtime data-plane ConfigMap identity: %+v", config)
	}
	var profiles []map[string]interface{}
	if err := json.Unmarshal([]byte(config.Data["profiles.json"]), &profiles); err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 0 {
		t.Fatalf("baseline profile catalog must fail closed, found %d profiles", len(profiles))
	}
	var mcpProfiles []map[string]interface{}
	if err := json.Unmarshal([]byte(config.Data["mcp-profiles.json"]), &mcpProfiles); err != nil {
		t.Fatal(err)
	}
	if len(mcpProfiles) != 0 {
		t.Fatalf("baseline MCP catalog must fail closed, found %d profiles", len(mcpProfiles))
	}
}

func TestMCPProfilesReachOnlyAdmissionAndExecutionPlanes(t *testing.T) {
	want := map[string]bool{"agent-admin": false, "agent-worker": false}
	for _, workload := range readRuntimePlaneWorkloads(t) {
		if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
			continue
		}
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			if variable.Name != "MCP_PROFILES" {
				continue
			}
			if _, allowed := want[workload.Metadata.Name]; !allowed {
				t.Errorf("Deployment %s unexpectedly receives MCP_PROFILES", workload.Metadata.Name)
				continue
			}
			ref := variable.ValueFrom.ConfigMapKeyRef
			if ref.Name != "runtime-data-plane-profiles" || ref.Key != "mcp-profiles.json" {
				t.Errorf("Deployment %s has unsafe MCP profile source %+v", workload.Metadata.Name, ref)
			}
			want[workload.Metadata.Name] = true
		}
	}
	for deployment, found := range want {
		if !found {
			t.Errorf("Deployment %s is missing MCP_PROFILES", deployment)
		}
	}
}

func TestOnlyWorkerReceivesMCPProfileCredential(t *testing.T) {
	found := false
	for _, workload := range readRuntimePlaneWorkloads(t) {
		if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
			continue
		}
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			if variable.Name != "TRPC_SECRET_MCP_SUPPORT_AUTH" {
				continue
			}
			if workload.Metadata.Name != "agent-worker" {
				t.Errorf("Deployment %s receives Worker-only MCP credential", workload.Metadata.Name)
				continue
			}
			ref := variable.ValueFrom.SecretKeyRef
			if ref.Name != "mcp-profile-credentials" || ref.Key != "support-authorization" {
				t.Errorf("Worker MCP credential has wrong Secret reference %+v", ref)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("Worker is missing TRPC_SECRET_MCP_SUPPORT_AUTH")
	}
}

func TestProviderCredentialsReachOnlyRequiredProcesses(t *testing.T) {
	channelKeys := map[string]string{
		"TRPC_SECRET_WECOM_TOKEN":        "wecom-callback-token",
		"TRPC_SECRET_WECOM_CORP_SECRET":  "wecom-corp-secret",
		"TRPC_SECRET_WECOM_AES":          "wecom-encoding-aes-key",
		"TRPC_SECRET_TELEGRAM_BOT_TOKEN": "telegram-bot-token",
		"TRPC_SECRET_TELEGRAM_WEBHOOK":   "telegram-webhook-secret",
	}
	wantChannel := make(map[string]map[string]bool)
	for _, deployment := range []string{"agent-gateway", "agent-delivery"} {
		wantChannel[deployment] = make(map[string]bool, len(channelKeys))
	}
	wantModel := map[string]bool{"agent-worker": false, "agent-summary-worker": false}

	for _, workload := range readRuntimePlaneWorkloads(t) {
		if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
			continue
		}
		for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
			if expectedKey, tracked := channelKeys[variable.Name]; tracked {
				allowed, ok := wantChannel[workload.Metadata.Name]
				if !ok {
					t.Errorf("Deployment %s receives channel credential %s", workload.Metadata.Name, variable.Name)
					continue
				}
				ref := variable.ValueFrom.SecretKeyRef
				if ref.Name != "channel-credentials" || ref.Key != expectedKey {
					t.Errorf("Deployment %s channel credential %s has wrong Secret reference %+v", workload.Metadata.Name, variable.Name, ref)
				}
				allowed[variable.Name] = true
			}
			if variable.Name == "TRPC_SECRET_OPENAI_API_KEY" {
				if _, ok := wantModel[workload.Metadata.Name]; !ok {
					t.Errorf("Deployment %s receives model-provider credential", workload.Metadata.Name)
					continue
				}
				ref := variable.ValueFrom.SecretKeyRef
				if ref.Name != "model-provider-credentials" || ref.Key != "openai-api-key" {
					t.Errorf("Deployment %s model credential has wrong Secret reference %+v", workload.Metadata.Name, ref)
				}
				wantModel[workload.Metadata.Name] = true
			}
		}
	}
	for deployment, variables := range wantChannel {
		for variable := range channelKeys {
			if !variables[variable] {
				t.Errorf("Deployment %s is missing channel credential %s", deployment, variable)
			}
		}
	}
	for deployment, found := range wantModel {
		if !found {
			t.Errorf("Deployment %s is missing TRPC_SECRET_OPENAI_API_KEY", deployment)
		}
	}
}

func readRuntimePlaneWorkloads(t *testing.T) []runtimePlaneWorkload {
	t.Helper()
	files := []string{"gateway-deployment.yaml", "worker-deployment.yaml", "summary-worker-deployment.yaml", "admin-deployment.yaml", "pipeline-deployments.yaml"}
	var result []runtimePlaneWorkload
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var value runtimePlaneWorkload
			if err := decoder.Decode(&value); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}
			if value.Kind == "Deployment" || value.Kind == "Job" {
				result = append(result, value)
			}
		}
	}
	return result
}
