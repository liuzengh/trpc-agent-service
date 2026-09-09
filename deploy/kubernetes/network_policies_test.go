package kubernetes_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type manifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		PolicyTypes []string      `yaml:"policyTypes"`
		Ingress     []interface{} `yaml:"ingress"`
		Egress      []interface{} `yaml:"egress"`
	} `yaml:"spec"`
}

type networkRule struct {
	Ports []struct {
		Port int `yaml:"port"`
	} `yaml:"ports"`
}

type meshManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ingress []networkRule `yaml:"ingress"`
		Egress  []networkRule `yaml:"egress"`
	} `yaml:"spec"`
}

func TestKubernetesYAMLParses(t *testing.T) {
	files, err := filepath.Glob("*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no Kubernetes manifests found")
	}
	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			file, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := yaml.NewDecoder(file)
			for document := 1; ; document++ {
				var value map[string]interface{}
				err := decoder.Decode(&value)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("document %d: %v", document, err)
				}
				if len(value) == 0 {
					continue
				}
				if value["apiVersion"] == nil || value["kind"] == nil || value["metadata"] == nil {
					t.Fatalf("document %d is missing apiVersion, kind or metadata", document)
				}
			}
		})
	}
}

func TestNetworkPolicyHasDefaultDenyAndEveryServiceBoundary(t *testing.T) {
	file, err := os.Open("network-policies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	policies := make(map[string]manifest)
	decoder := yaml.NewDecoder(file)
	for {
		var value manifest
		err := decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if value.Kind == "NetworkPolicy" {
			policies[value.Metadata.Name] = value
		}
	}

	defaultDeny, ok := policies["default-deny"]
	if !ok || strings.Join(defaultDeny.Spec.PolicyTypes, ",") != "Ingress,Egress" ||
		len(defaultDeny.Spec.Ingress) != 0 || len(defaultDeny.Spec.Egress) != 0 {
		t.Fatalf("default-deny must deny ingress and egress: %#v", defaultDeny.Spec)
	}
	expected := []string{
		"admin-boundaries", "consumer-boundaries", "delivery-boundaries",
		"gateway-boundaries", "migrate-boundaries", "summary-worker-boundaries", "worker-boundaries",
	}
	missing := make([]string, 0)
	for _, name := range expected {
		if _, ok := policies[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("missing service boundary policies: %v", missing)
	}
	for _, name := range expected {
		policy := policies[name]
		if name != "migrate-boundaries" && len(policy.Spec.Ingress) == 0 {
			t.Errorf("%s has no explicit ingress", name)
		}
		if len(policy.Spec.Egress) == 0 {
			t.Errorf("%s has no explicit egress", name)
		}
	}
}

func TestWorkerAndDeliveryUseControlledEgressGateway(t *testing.T) {
	data, err := os.ReadFile("network-policies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	if strings.Contains(contents, "cidr: 0.0.0.0/0") || strings.Contains(contents, "cidr: ::/0") {
		t.Fatal("baseline policy must not permit direct world egress")
	}
	if strings.Count(contents, "agent-platform-access: egress-gateway") != 3 ||
		strings.Count(contents, "app: agent-egress-gateway") != 3 {
		t.Fatal("Worker, Summary Worker and Delivery must each route provider traffic through the controlled egress gateway")
	}
}

func TestWorkerRuntimeDataPlaneEgressIsDestinationScoped(t *testing.T) {
	data, err := os.ReadFile("network-policies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	for {
		var value struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				PodSelector struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"podSelector"`
				Egress []struct {
					To []struct {
						NamespaceSelector struct {
							MatchLabels map[string]string `yaml:"matchLabels"`
						} `yaml:"namespaceSelector"`
						PodSelector struct {
							MatchLabels map[string]string `yaml:"matchLabels"`
						} `yaml:"podSelector"`
					} `yaml:"to"`
					Ports []struct {
						Port int `yaml:"port"`
					} `yaml:"ports"`
				} `yaml:"egress"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		if value.Kind != "NetworkPolicy" || value.Metadata.Name != "worker-runtime-data-plane-egress" {
			continue
		}
		found = true
		if value.Spec.PodSelector.MatchLabels["app"] != "agent-worker" {
			t.Error("runtime data-plane egress must select only Agent Workers")
		}
		want := map[string]map[int]bool{
			"qdrant":         {6334: false, 4143: false},
			"object-storage": {443: false, 4143: false},
		}
		for _, rule := range value.Spec.Egress {
			if len(rule.To) != 1 || rule.To[0].NamespaceSelector.MatchLabels["agent-platform-access"] != "data-plane" {
				t.Error("runtime data-plane rule is not restricted to the data-plane namespace label")
				continue
			}
			app := rule.To[0].PodSelector.MatchLabels["app"]
			ports, expected := want[app]
			if !expected {
				t.Errorf("unexpected runtime data-plane destination app=%q", app)
				continue
			}
			for _, port := range rule.Ports {
				if _, ok := ports[port.Port]; ok {
					ports[port.Port] = true
				}
			}
		}
		for app, ports := range want {
			for port, present := range ports {
				if !present {
					t.Errorf("runtime data-plane destination %s is missing port %d", app, port)
				}
			}
		}
	}
	if !found {
		t.Fatal("missing worker-runtime-data-plane-egress policy")
	}
}

func TestNetworkPolicyAllowsLinkerdTransparentProxyPaths(t *testing.T) {
	file, err := os.Open("network-policies.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	policies := make(map[string]meshManifest)
	decoder := yaml.NewDecoder(file)
	for {
		var value meshManifest
		err := decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if value.Kind == "NetworkPolicy" {
			policies[value.Metadata.Name] = value
		}
	}

	// Linkerd's inbound proxy redirects application traffic to 4143 before
	// NetworkPolicy evaluation. Every explicit ingress boundary must therefore
	// allow both its documented application port and the proxy port.
	for _, name := range []string{
		"gateway-boundaries", "consumer-boundaries", "worker-boundaries",
		"summary-worker-boundaries", "delivery-boundaries", "admin-boundaries", "postgres-ingress", "redis-ingress",
	} {
		policy, ok := policies[name]
		if !ok {
			t.Errorf("missing policy %s", name)
			continue
		}
		for index, rule := range policy.Spec.Ingress {
			if !hasNetworkPort(rule, 4143) {
				t.Errorf("%s ingress rule %d does not allow Linkerd proxy port 4143", name, index)
			}
		}
	}

	// The CNI also observes the rewritten destination on meshed egress. Keep
	// these grants separate and destination-specific instead of opening 4143
	// from every Pod to every Pod.
	for _, name := range []string{
		"meshed-postgres-egress", "meshed-redis-egress", "meshed-worker-egress",
	} {
		policy, ok := policies[name]
		if !ok {
			t.Errorf("missing policy %s", name)
			continue
		}
		if len(policy.Spec.Egress) != 1 || !hasNetworkPort(policy.Spec.Egress[0], 4143) {
			t.Errorf("%s must grant its selected destination on Linkerd proxy port 4143", name)
		}
	}

	controlPlane, ok := policies["allow-linkerd-control-plane"]
	if !ok || len(controlPlane.Spec.Egress) != 1 {
		t.Fatal("missing precise Linkerd control-plane egress policy")
	}
	for _, port := range []int{8080, 8086, 8090} {
		if !hasNetworkPort(controlPlane.Spec.Egress[0], port) {
			t.Errorf("Linkerd control-plane egress does not allow port %d", port)
		}
	}
}

func hasNetworkPort(rule networkRule, expected int) bool {
	for _, port := range rule.Ports {
		if port.Port == expected {
			return true
		}
	}
	return false
}
