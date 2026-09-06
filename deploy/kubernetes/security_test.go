package kubernetes

import (
	"io"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type manifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	StringData map[string]string `yaml:"stringData"`
	Spec       struct {
		Template struct {
			Spec struct {
				Containers []struct {
					EnvFrom []struct {
						SecretRef *struct {
							Name string `yaml:"name"`
						} `yaml:"secretRef"`
					} `yaml:"envFrom"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func manifests(t *testing.T, fileName string) []manifest {
	t.Helper()
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	var result []manifest
	for {
		var value manifest
		if err := decoder.Decode(&value); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	return result
}

func TestManifestsUseRoleSpecificSecrets(t *testing.T) {
	expected := map[string]string{
		"agent-gateway": "trpc-agent-gateway-secrets", "agent-admin": "trpc-agent-admin-secrets",
		"agent-relay": "trpc-agent-relay-secrets", "agent-worker": "trpc-agent-worker-secrets",
		"agent-sender": "trpc-agent-sender-secrets", "agent-jobs": "trpc-agent-jobs-secrets",
		"trpc-agent-migrate": "trpc-agent-migration-secrets",
	}
	seen := map[string]bool{}
	for _, file := range []string{"platform.yaml", "migration-job.yaml"} {
		for _, doc := range manifests(t, file) {
			if doc.Kind != "Deployment" && doc.Kind != "Job" {
				continue
			}
			for _, container := range doc.Spec.Template.Spec.Containers {
				count := 0
				for _, source := range container.EnvFrom {
					if source.SecretRef == nil {
						continue
					}
					count++
					if source.SecretRef.Name != expected[doc.Metadata.Name] {
						t.Fatalf("unexpected secret for %s", doc.Metadata.Name)
					}
					seen[source.SecretRef.Name] = true
				}
				if count != 1 {
					t.Fatalf("secret count for %s = %d", doc.Metadata.Name, count)
				}
			}
		}
	}
	if len(seen) != 7 {
		t.Fatalf("role secret count=%d", len(seen))
	}
	for _, doc := range manifests(t, "secret.example.yaml") {
		if !seen[doc.Metadata.Name] {
			t.Fatalf("unused example %s", doc.Metadata.Name)
		}
		isModel := strings.Contains(doc.Metadata.Name, "worker") || strings.Contains(doc.Metadata.Name, "jobs")
		if _, present := doc.StringData["OPENAI_API_KEY"]; present != isModel {
			t.Fatal("model key on incorrect role")
		}
		if _, present := doc.StringData["TELEGRAM_BOT_TOKEN"]; present != strings.Contains(doc.Metadata.Name, "sender") {
			t.Fatal("Bot token on incorrect role")
		}
		if _, present := doc.StringData["TRPC_AGENT_ADMIN_PRINCIPALS_JSON"]; present != strings.Contains(doc.Metadata.Name, "admin") {
			t.Fatal("Admin token on incorrect role")
		}
	}
}
