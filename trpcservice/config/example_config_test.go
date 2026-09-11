package config

import (
	"os"
	"testing"
)

func TestPlatformExampleConfigurationLoads(t *testing.T) {
	for _, name := range []string{"../../configs/platform.example.json", "../../configs/platform.s3.example.json"} {
		file, err := os.Open(name)
		if err != nil {
			t.Fatalf("Open(%q) error = %v", name, err)
		}
		configuration, loadErr := Load(file)
		_ = file.Close()
		if loadErr != nil {
			t.Fatalf("Load(%q) error = %v", name, loadErr)
		}
		if len(configuration.ModelProviders) != 1 || configuration.ModelProviders[0].ID != "openai-primary" || configuration.ModelProviders[0].APIKeyRef != "env:MODEL_API_KEY" {
			t.Fatalf("%q model providers = %#v", name, configuration.ModelProviders)
		}
	}
}
