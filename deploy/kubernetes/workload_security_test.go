package kubernetes_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type workloadManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Replicas int `yaml:"replicas"`
		Template struct {
			Metadata struct {
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec struct {
				AutomountServiceAccountToken *bool `yaml:"automountServiceAccountToken"`
				TopologySpreadConstraints    []struct {
					MaxSkew           int    `yaml:"maxSkew"`
					MinDomains        int    `yaml:"minDomains"`
					TopologyKey       string `yaml:"topologyKey"`
					WhenUnsatisfiable string `yaml:"whenUnsatisfiable"`
					LabelSelector     struct {
						MatchLabels map[string]string `yaml:"matchLabels"`
					} `yaml:"labelSelector"`
				} `yaml:"topologySpreadConstraints"`
				Containers []struct {
					Env []struct {
						Name      string `yaml:"name"`
						Value     string `yaml:"value"`
						ValueFrom struct {
							SecretKeyRef struct {
								Name     string `yaml:"name"`
								Key      string `yaml:"key"`
								Optional *bool  `yaml:"optional"`
							} `yaml:"secretKeyRef"`
						} `yaml:"valueFrom"`
					} `yaml:"env"`
					VolumeMounts []struct {
						Name      string `yaml:"name"`
						MountPath string `yaml:"mountPath"`
						ReadOnly  *bool  `yaml:"readOnly"`
					} `yaml:"volumeMounts"`
				} `yaml:"containers"`
				Volumes []struct {
					Name   string `yaml:"name"`
					Secret struct {
						SecretName string `yaml:"secretName"`
						Items      []struct {
							Key  string `yaml:"key"`
							Path string `yaml:"path"`
						} `yaml:"items"`
					} `yaml:"secret"`
				} `yaml:"volumes"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestReplicatedDeploymentsRequireHardNodeSpread(t *testing.T) {
	files := []string{
		"gateway-deployment.yaml",
		"worker-deployment.yaml",
		"summary-worker-deployment.yaml",
		"admin-deployment.yaml",
		"pipeline-deployments.yaml",
	}
	for _, filename := range files {
		for _, workload := range readWorkloads(t, filename) {
			if workload.Kind != "Deployment" || workload.Spec.Replicas < 2 {
				continue
			}
			app := workload.Spec.Template.Metadata.Labels["app"]
			constraints := workload.Spec.Template.Spec.TopologySpreadConstraints
			if len(constraints) == 0 {
				t.Errorf("Deployment %s has %d replicas but no topology spread constraint", workload.Metadata.Name, workload.Spec.Replicas)
				continue
			}
			found := false
			for _, constraint := range constraints {
				if constraint.TopologyKey == "kubernetes.io/hostname" &&
					constraint.MaxSkew == 1 && constraint.MinDomains >= 2 &&
					constraint.WhenUnsatisfiable == "DoNotSchedule" &&
					constraint.LabelSelector.MatchLabels["app"] == app {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Deployment %s lacks a hard two-node spread constraint for app=%q", workload.Metadata.Name, app)
			}
		}
	}
}

func readWorkloads(t *testing.T, filename string) []workloadManifest {
	t.Helper()
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var workloads []workloadManifest
	for {
		var value workloadManifest
		if err := decoder.Decode(&value); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if value.Kind == "Deployment" || value.Kind == "Job" {
			workloads = append(workloads, value)
		}
	}
	return workloads
}

func TestAllWorkloadsDisableServiceAccountTokenMounts(t *testing.T) {
	files := []string{
		"gateway-deployment.yaml",
		"worker-deployment.yaml",
		"summary-worker-deployment.yaml",
		"admin-deployment.yaml",
		"pipeline-deployments.yaml",
		"migration-job.yaml",
	}
	for _, filename := range files {
		for _, workload := range readWorkloads(t, filename) {
			disabled := workload.Spec.Template.Spec.AutomountServiceAccountToken
			if disabled == nil || *disabled {
				t.Errorf("%s %s must set automountServiceAccountToken: false", workload.Kind, workload.Metadata.Name)
			}
		}
	}
}

func TestGatewayRequiresAuditIdentitySecret(t *testing.T) {
	workloads := readWorkloads(t, "gateway-deployment.yaml")
	if len(workloads) != 1 || len(workloads[0].Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("unexpected Gateway workload shape: %+v", workloads)
	}
	for _, variable := range workloads[0].Spec.Template.Spec.Containers[0].Env {
		if variable.Name != "AUDIT_IDENTITY_HMAC_KEY" {
			continue
		}
		ref := variable.ValueFrom.SecretKeyRef
		if ref.Name != "audit-identity" || ref.Key != "key" || (ref.Optional != nil && *ref.Optional) {
			t.Fatalf("Gateway audit identity secret is not required: %+v", ref)
		}
		return
	}
	t.Fatal("Gateway is missing AUDIT_IDENTITY_HMAC_KEY")
}

func TestAllHTTPDeploymentsRequireMetricsSecret(t *testing.T) {
	files := []string{
		"gateway-deployment.yaml",
		"worker-deployment.yaml",
		"summary-worker-deployment.yaml",
		"admin-deployment.yaml",
		"pipeline-deployments.yaml",
	}
	want := map[string]bool{
		"agent-gateway":        false,
		"agent-worker":         false,
		"agent-summary-worker": false,
		"agent-admin":          false,
		"agent-consumer":       false,
		"agent-delivery":       false,
	}
	for _, filename := range files {
		for _, workload := range readWorkloads(t, filename) {
			if workload.Kind != "Deployment" {
				continue
			}
			if _, expected := want[workload.Metadata.Name]; !expected {
				continue
			}
			if len(workload.Spec.Template.Spec.Containers) != 1 {
				t.Errorf("Deployment %s container count = %d, want 1", workload.Metadata.Name, len(workload.Spec.Template.Spec.Containers))
				continue
			}
			for _, variable := range workload.Spec.Template.Spec.Containers[0].Env {
				if variable.Name != "METRICS_AUTH_TOKEN" {
					continue
				}
				ref := variable.ValueFrom.SecretKeyRef
				if ref.Name != "metrics-auth" || ref.Key != "token" || (ref.Optional != nil && *ref.Optional) {
					t.Errorf("Deployment %s metrics secret is not required: %+v", workload.Metadata.Name, ref)
				}
				want[workload.Metadata.Name] = true
			}
		}
	}
	for deployment, found := range want {
		if !found {
			t.Errorf("Deployment %s is missing METRICS_AUTH_TOKEN", deployment)
		}
	}
}

func TestAllHTTPDeploymentsRequireOTLPTrustBundle(t *testing.T) {
	files := []string{
		"gateway-deployment.yaml",
		"worker-deployment.yaml",
		"summary-worker-deployment.yaml",
		"admin-deployment.yaml",
		"pipeline-deployments.yaml",
	}
	for _, filename := range files {
		for _, workload := range readWorkloads(t, filename) {
			if workload.Kind != "Deployment" || len(workload.Spec.Template.Spec.Containers) != 1 {
				continue
			}
			container := workload.Spec.Template.Spec.Containers[0]
			var certPath string
			insecure := ""
			for _, variable := range container.Env {
				if variable.Name == "OTEL_EXPORTER_OTLP_INSECURE" {
					insecure = variable.Value
				}
				if variable.Name == "OTEL_EXPORTER_OTLP_CERTIFICATE" {
					certPath = variable.Value
				}
			}
			if insecure != "false" {
				t.Errorf("Deployment %s must keep OTEL_EXPORTER_OTLP_INSECURE=false, got %q", workload.Metadata.Name, insecure)
			}
			if certPath != "/var/run/secrets/otel/ca.crt" {
				t.Errorf("Deployment %s is missing OTEL_EXPORTER_OTLP_CERTIFICATE", workload.Metadata.Name)
			}
			volumeFound := false
			for _, volume := range workload.Spec.Template.Spec.Volumes {
				if volume.Name != "otel-ca" || volume.Secret.SecretName != "otel-collector-tls" {
					continue
				}
				for _, item := range volume.Secret.Items {
					if item.Key == "ca.crt" && item.Path == "ca.crt" {
						volumeFound = true
					}
				}
			}
			if !volumeFound {
				t.Errorf("Deployment %s is missing otel-collector-tls ca.crt volume", workload.Metadata.Name)
			}
			mountFound := false
			for _, mount := range container.VolumeMounts {
				if mount.Name == "otel-ca" && mount.MountPath == "/var/run/secrets/otel" && mount.ReadOnly != nil && *mount.ReadOnly {
					mountFound = true
				}
			}
			if !mountFound {
				t.Errorf("Deployment %s must mount otel-ca read-only", workload.Metadata.Name)
			}
		}
	}
}
