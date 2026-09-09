package kubernetes_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type autoscalingManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Metrics []struct {
			Type              string `yaml:"type"`
			ContainerResource struct {
				Name      string `yaml:"name"`
				Container string `yaml:"container"`
			} `yaml:"containerResource"`
		} `yaml:"metrics"`
	} `yaml:"spec"`
}

func TestHPAUsesBusinessContainerMetrics(t *testing.T) {
	tests := []struct {
		filename  string
		hpaName   string
		container string
	}{
		{"gateway-deployment.yaml", "agent-gateway-hpa", "gateway"},
		{"worker-deployment.yaml", "agent-worker-hpa", "worker"},
		{"summary-worker-deployment.yaml", "agent-summary-worker-hpa", "summary-worker"},
	}

	for _, test := range tests {
		t.Run(test.hpaName, func(t *testing.T) {
			file, err := os.Open(test.filename)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			decoder := yaml.NewDecoder(file)
			for {
				var value autoscalingManifest
				err := decoder.Decode(&value)
				if errors.Is(err, io.EOF) {
					t.Fatalf("HPA %s not found", test.hpaName)
				}
				if err != nil {
					t.Fatal(err)
				}
				if value.Kind != "HorizontalPodAutoscaler" || value.Metadata.Name != test.hpaName {
					continue
				}

				resources := make(map[string]bool)
				for _, metric := range value.Spec.Metrics {
					if metric.Type != "ContainerResource" || metric.ContainerResource.Container != test.container {
						t.Errorf("metric must target business container %q, got type=%q container=%q", test.container, metric.Type, metric.ContainerResource.Container)
						continue
					}
					resources[metric.ContainerResource.Name] = true
				}
				for _, resource := range []string{"cpu", "memory"} {
					if !resources[resource] {
						t.Errorf("missing %s ContainerResource metric for %s", resource, test.container)
					}
				}
				return
			}
		})
	}
}
