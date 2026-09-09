// Package releaseverify validates the rendered Kubernetes inputs accepted by
// the production rollout script. It deliberately validates rendered release
// artifacts rather than the checked-in baseline templates, because a release
// must bind its exact OCI images and its cluster-specific transport policy.
package releaseverify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
)

var immutableImageReference = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)
var changeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const (
	schemaClassAnnotation    = "agent.trpc.io/schema-class"
	breakingChangeAnnotation = "agent.trpc.io/breaking-change-id"
)

// ReleaseContext binds operator rollout intent to the reviewed release
// artifact. It does not decide whether a migration is compatible; that remains
// a release-review responsibility.
type ReleaseContext struct {
	SchemaClass      string
	BreakingChangeID string
}

var requiredWorkloads = map[string]string{
	"agent-admin":          "Deployment",
	"agent-consumer":       "Deployment",
	"agent-delivery":       "Deployment",
	"agent-gateway":        "Deployment",
	"agent-migrate":        "Job",
	"agent-summary-worker": "Deployment",
	"agent-worker":         "Deployment",
}

type resourceKey struct {
	Kind string
	Name string
}

var requiredResources = map[resourceKey]string{
	{Kind: "ConfigMap", Name: "runtime-data-plane-profiles"}:            "v1",
	{Kind: "Deployment", Name: "agent-admin"}:                           "apps/v1",
	{Kind: "Deployment", Name: "agent-consumer"}:                        "apps/v1",
	{Kind: "Deployment", Name: "agent-delivery"}:                        "apps/v1",
	{Kind: "Deployment", Name: "agent-gateway"}:                         "apps/v1",
	{Kind: "Deployment", Name: "agent-summary-worker"}:                  "apps/v1",
	{Kind: "Deployment", Name: "agent-worker"}:                          "apps/v1",
	{Kind: "Job", Name: "agent-migrate"}:                                "batch/v1",
	{Kind: "Service", Name: "agent-admin"}:                              "v1",
	{Kind: "Service", Name: "agent-gateway"}:                            "v1",
	{Kind: "Service", Name: "agent-worker"}:                             "v1",
	{Kind: "HorizontalPodAutoscaler", Name: "agent-gateway-hpa"}:        "autoscaling/v2",
	{Kind: "HorizontalPodAutoscaler", Name: "agent-summary-worker-hpa"}: "autoscaling/v2",
	{Kind: "HorizontalPodAutoscaler", Name: "agent-worker-hpa"}:         "autoscaling/v2",
	{Kind: "PodDisruptionBudget", Name: "agent-admin"}:                  "policy/v1",
	{Kind: "PodDisruptionBudget", Name: "agent-consumer"}:               "policy/v1",
	{Kind: "PodDisruptionBudget", Name: "agent-delivery"}:               "policy/v1",
	{Kind: "PodDisruptionBudget", Name: "agent-gateway"}:                "policy/v1",
	{Kind: "PodDisruptionBudget", Name: "agent-summary-worker"}:         "policy/v1",
	{Kind: "PodDisruptionBudget", Name: "agent-worker"}:                 "policy/v1",
}

// ValidateRelease verifies the deployment contracts that cannot be expressed
// safely by a mutable image tag or a best-effort rollout script.
func ValidateRelease(manifests [][]byte, networkPolicy []byte, releaseContext ReleaseContext) error {
	workloads, err := loadWorkloads(manifests)
	if err != nil {
		return err
	}
	for name, kind := range requiredWorkloads {
		workload, ok := workloads[name]
		if !ok {
			return fmt.Errorf("release manifest is missing required %s %q", kind, name)
		}
		if workload.Kind != kind {
			return fmt.Errorf("release workload %q has kind %q, want %q", name, workload.Kind, kind)
		}
	}
	if err := validateConsumerTransport(workloads["agent-consumer"]); err != nil {
		return err
	}
	if err := validateRuntimeDataPlaneBindings(workloads); err != nil {
		return err
	}
	if err := validateMCPBindings(workloads); err != nil {
		return err
	}
	if err := validateMigrationContext(workloads["agent-migrate"], releaseContext); err != nil {
		return err
	}
	return validateNetworkPolicy(networkPolicy, workloads)
}

func validateMigrationContext(migration workload, releaseContext ReleaseContext) error {
	switch releaseContext.SchemaClass {
	case "bootstrap", "compatible", "breaking":
	default:
		return fmt.Errorf("release schema class %q is invalid", releaseContext.SchemaClass)
	}
	if got := migration.Metadata.Annotations[schemaClassAnnotation]; got != releaseContext.SchemaClass {
		return fmt.Errorf(
			"agent-migrate %s annotation %q does not match rollout schema class %q",
			schemaClassAnnotation,
			got,
			releaseContext.SchemaClass,
		)
	}
	artifactChangeID := migration.Metadata.Annotations[breakingChangeAnnotation]
	if releaseContext.SchemaClass != "breaking" {
		if artifactChangeID != "" || releaseContext.BreakingChangeID != "" {
			return fmt.Errorf("non-breaking release must not carry a breaking migration change ID")
		}
		return nil
	}
	if !changeIDPattern.MatchString(releaseContext.BreakingChangeID) {
		return fmt.Errorf("breaking release requires a bounded change record identifier")
	}
	if artifactChangeID != releaseContext.BreakingChangeID {
		return fmt.Errorf(
			"agent-migrate %s annotation does not match the approved change record",
			breakingChangeAnnotation,
		)
	}
	return nil
}

type workload struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
	Spec struct {
		Template struct {
			Metadata struct {
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec podSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type podSpec struct {
	Containers     []container `yaml:"containers"`
	InitContainers []container `yaml:"initContainers"`
}

type container struct {
	Name  string        `yaml:"name"`
	Image string        `yaml:"image"`
	Env   []environment `yaml:"env"`
}

type environment struct {
	Name      string `yaml:"name"`
	Value     string `yaml:"value"`
	ValueFrom struct {
		ConfigMapKeyRef resourceReference `yaml:"configMapKeyRef"`
		SecretKeyRef    resourceReference `yaml:"secretKeyRef"`
	} `yaml:"valueFrom"`
}

type resourceReference struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

func validateRuntimeDataPlaneBindings(workloads map[string]workload) error {
	profileConsumers := []struct {
		workload  string
		container string
	}{
		{workload: "agent-admin", container: "admin"},
		{workload: "agent-consumer", container: "consumer"},
		{workload: "agent-delivery", container: "delivery"},
		{workload: "agent-gateway", container: "gateway"},
		{workload: "agent-summary-worker", container: "summary-worker"},
		{workload: "agent-worker", container: "worker"},
	}
	credentialKeys := map[string]string{
		"DATA_PLANE_QDRANT_API_KEY":    "qdrant-api-key",
		"DATA_PLANE_EMBEDDING_API_KEY": "embedding-api-key",
		"DATA_PLANE_S3_ACCESS_KEY":     "s3-access-key",
		"DATA_PLANE_S3_SECRET_KEY":     "s3-secret-key",
	}
	credentialOrder := []string{
		"DATA_PLANE_QDRANT_API_KEY",
		"DATA_PLANE_EMBEDDING_API_KEY",
		"DATA_PLANE_S3_ACCESS_KEY",
		"DATA_PLANE_S3_SECRET_KEY",
	}
	workerCredentials := make(map[string]int, len(credentialKeys))
	for _, expected := range profileConsumers {
		workloadName, containerName := expected.workload, expected.container
		item := workloads[workloadName]
		profileBindings := 0
		for _, container := range item.Spec.Template.Spec.Containers {
			for _, variable := range container.Env {
				if variable.Name == "DATA_PLANE_PROFILES" {
					if container.Name != containerName || variable.Value != "" ||
						variable.ValueFrom.ConfigMapKeyRef.Name != "runtime-data-plane-profiles" ||
						variable.ValueFrom.ConfigMapKeyRef.Key != "profiles.json" {
						return fmt.Errorf("release workload %q has an invalid DATA_PLANE_PROFILES binding", workloadName)
					}
					profileBindings++
				}
				expectedKey, credential := credentialKeys[variable.Name]
				if !credential {
					continue
				}
				if workloadName != "agent-worker" || container.Name != "worker" {
					return fmt.Errorf("runtime data-plane credential %s is Worker-only", variable.Name)
				}
				if variable.Value != "" || variable.ValueFrom.SecretKeyRef.Name != "runtime-data-plane-credentials" ||
					variable.ValueFrom.SecretKeyRef.Key != expectedKey {
					return fmt.Errorf("Worker runtime data-plane credential %s has an invalid Secret binding", variable.Name)
				}
				workerCredentials[variable.Name]++
			}
		}
		if profileBindings != 1 {
			return fmt.Errorf("release workload %q must have exactly one DATA_PLANE_PROFILES binding", workloadName)
		}
	}
	for _, name := range credentialOrder {
		if workerCredentials[name] != 1 {
			return fmt.Errorf("release Worker must have exactly one %s Secret binding", name)
		}
	}
	return nil
}

func validateMCPBindings(workloads map[string]workload) error {
	profileConsumers := map[string]string{
		"agent-admin":  "admin",
		"agent-worker": "worker",
	}
	profileBindings := make(map[string]int, len(profileConsumers))
	credentialBindings := make(map[string]int)
	for workloadName, item := range workloads {
		if item.Kind != "Deployment" && item.Kind != "Job" {
			continue
		}
		containers := append([]container{}, item.Spec.Template.Spec.InitContainers...)
		containers = append(containers, item.Spec.Template.Spec.Containers...)
		for _, container := range containers {
			for _, variable := range container.Env {
				if variable.Name == "MCP_PROFILES" {
					expectedContainer, approved := profileConsumers[workloadName]
					if !approved || container.Name != expectedContainer || variable.Value != "" ||
						variable.ValueFrom.ConfigMapKeyRef.Name != "runtime-data-plane-profiles" ||
						variable.ValueFrom.ConfigMapKeyRef.Key != "mcp-profiles.json" {
						return fmt.Errorf("release workload %q has an invalid MCP_PROFILES binding", workloadName)
					}
					profileBindings[workloadName]++
				}
				if !strings.HasPrefix(variable.Name, "TRPC_SECRET_MCP_") {
					continue
				}
				if workloadName != "agent-worker" || container.Name != "worker" {
					return fmt.Errorf("MCP credential %s is Worker-only", variable.Name)
				}
				if variable.Value != "" || variable.ValueFrom.SecretKeyRef.Name != "mcp-profile-credentials" ||
					variable.ValueFrom.SecretKeyRef.Key == "" {
					return fmt.Errorf("Worker MCP credential %s has an invalid Secret binding", variable.Name)
				}
				credentialBindings[variable.Name]++
			}
		}
	}
	for workloadName := range profileConsumers {
		if profileBindings[workloadName] != 1 {
			return fmt.Errorf("release workload %q must have exactly one MCP_PROFILES binding", workloadName)
		}
	}

	config := workloads["runtime-data-plane-profiles"]
	var profiles []platformtool.MCPProfile
	if err := json.Unmarshal([]byte(config.Data["mcp-profiles.json"]), &profiles); err != nil {
		return fmt.Errorf("MCP profile catalog is invalid")
	}
	for _, profile := range profiles {
		for _, rawRef := range profile.HeaderRefs {
			const prefix = "env://TRPC_SECRET_MCP_"
			if !strings.HasPrefix(rawRef, prefix) {
				return fmt.Errorf("MCP profile credential references must use env://TRPC_SECRET_MCP_ names")
			}
			name := strings.TrimPrefix(rawRef, "env://")
			if credentialBindings[name] != 1 {
				return fmt.Errorf("release Worker must have exactly one %s Secret binding", name)
			}
		}
	}
	return nil
}

func loadWorkloads(manifests [][]byte) (map[string]workload, error) {
	if len(manifests) == 0 {
		return nil, fmt.Errorf("release manifest is required")
	}
	workloads := make(map[string]workload)
	resources := make(map[resourceKey]struct{}, len(requiredResources))
	for inputIndex, manifest := range manifests {
		if len(bytes.TrimSpace(manifest)) == 0 {
			return nil, fmt.Errorf("release manifest %d is empty", inputIndex+1)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(manifest))
		for document := 1; ; document++ {
			var item workload
			err := decoder.Decode(&item)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("decode release manifest %d document %d: %w", inputIndex+1, document, err)
			}
			if item.Kind == "" {
				continue
			}
			if item.Metadata.Name == "" {
				return nil, fmt.Errorf("release manifest %d document %d has unnamed %s", inputIndex+1, document, item.Kind)
			}
			key := resourceKey{Kind: item.Kind, Name: item.Metadata.Name}
			expectedAPIVersion, approved := requiredResources[key]
			if !approved {
				return nil, fmt.Errorf("release contains unapproved resource %s %q", item.Kind, item.Metadata.Name)
			}
			if item.APIVersion != expectedAPIVersion {
				return nil, fmt.Errorf(
					"release resource %s %q has apiVersion %q, want %q",
					item.Kind,
					item.Metadata.Name,
					item.APIVersion,
					expectedAPIVersion,
				)
			}
			if item.Metadata.Namespace != "" {
				return nil, fmt.Errorf("release resource %s %q must not override the rollout namespace", item.Kind, item.Metadata.Name)
			}
			if _, exists := resources[key]; exists {
				return nil, fmt.Errorf("release has duplicate resource %s %q", item.Kind, item.Metadata.Name)
			}
			resources[key] = struct{}{}
			if key == (resourceKey{Kind: "ConfigMap", Name: "runtime-data-plane-profiles"}) {
				profiles, ok := item.Data["profiles.json"]
				mcpProfiles, hasMCPProfiles := item.Data["mcp-profiles.json"]
				if !ok || !hasMCPProfiles || len(item.Data) != 2 {
					return nil, fmt.Errorf("runtime-data-plane-profiles must contain only profiles.json and mcp-profiles.json")
				}
				if _, err := runtimeplane.LoadProfileValidator(profiles); err != nil {
					return nil, fmt.Errorf("runtime data-plane profile catalog is invalid: %w", err)
				}
				resolver, err := platformtool.NewMCPAdmissionResolver(mcpProfiles)
				if err != nil {
					return nil, fmt.Errorf("MCP profile catalog is invalid: %w", err)
				}
				_ = resolver.Close()
				workloads[item.Metadata.Name] = item
			}
			if item.Kind != "Deployment" && item.Kind != "Job" {
				continue
			}
			if err := validatePodImages(item); err != nil {
				return nil, err
			}
			workloads[item.Metadata.Name] = item
		}
	}
	for key := range requiredResources {
		if _, ok := resources[key]; !ok {
			return nil, fmt.Errorf("release is missing required resource %s %q", key.Kind, key.Name)
		}
	}
	return workloads, nil
}

func validatePodImages(workload workload) error {
	containers := append([]container{}, workload.Spec.Template.Spec.InitContainers...)
	containers = append(containers, workload.Spec.Template.Spec.Containers...)
	if len(containers) == 0 {
		return fmt.Errorf("release workload %q has no containers", workload.Metadata.Name)
	}
	for _, container := range containers {
		if !immutableImageReference.MatchString(container.Image) {
			return fmt.Errorf("release workload %q container %q must use an immutable sha256 image digest", workload.Metadata.Name, container.Name)
		}
	}
	return nil
}

func validateConsumerTransport(consumer workload) error {
	env := make(map[string]string)
	for _, container := range consumer.Spec.Template.Spec.Containers {
		for _, variable := range container.Env {
			if variable.Value != "" {
				env[variable.Name] = variable.Value
			}
		}
	}
	mode := strings.ToLower(strings.TrimSpace(env["WORKER_TRANSPORT_MODE"]))
	endpoint := strings.TrimSpace(env["WORKER_ENDPOINT"])
	switch mode {
	case "production":
		if !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
			return fmt.Errorf("agent-consumer production transport requires an https Worker endpoint")
		}
	case "mesh":
		if env["WORKER_MESH_MTLS_ASSERTED"] != "true" {
			return fmt.Errorf("agent-consumer mesh transport requires WORKER_MESH_MTLS_ASSERTED=true in the release manifest")
		}
		evidence := consumer.Metadata.Annotations["agent.trpc.io/mesh-mtls-evidence"]
		if !validEvidence(evidence) {
			return fmt.Errorf("agent-consumer mesh transport requires a bounded mesh-mtls-evidence annotation")
		}
	default:
		return fmt.Errorf("agent-consumer transport mode %q is not production-safe", mode)
	}
	return nil
}

func validEvidence(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, "\r\n\x00")
}

type networkPolicy struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		PodSelector labelSelector    `yaml:"podSelector"`
		PolicyTypes []string         `yaml:"policyTypes"`
		Ingress     []map[string]any `yaml:"ingress"`
		Egress      []egressRule     `yaml:"egress"`
	} `yaml:"spec"`
}

type egressRule struct {
	To []networkPeer `yaml:"to"`
}

type networkPeer struct {
	IPBlock           *ipBlock       `yaml:"ipBlock"`
	NamespaceSelector *labelSelector `yaml:"namespaceSelector"`
	PodSelector       *labelSelector `yaml:"podSelector"`
}

type ipBlock struct {
	CIDR   string   `yaml:"cidr"`
	Except []string `yaml:"except"`
}

type labelSelector struct {
	MatchLabels      map[string]string          `yaml:"matchLabels"`
	MatchExpressions []labelSelectorRequirement `yaml:"matchExpressions"`
}

type labelSelectorRequirement struct {
	Key      string   `yaml:"key"`
	Operator string   `yaml:"operator"`
	Values   []string `yaml:"values"`
}

func loadNetworkPolicies(data []byte) ([]networkPolicy, error) {
	var policies []networkPolicy
	resources := 0
	var visit func(*yaml.Node, int) error
	visit = func(node *yaml.Node, depth int) error {
		if depth > 8 || resources >= 1000 {
			return fmt.Errorf("production NetworkPolicy exceeds resource or list nesting limits")
		}
		resources++
		var resource struct {
			APIVersion string      `yaml:"apiVersion"`
			Kind       string      `yaml:"kind"`
			Items      []yaml.Node `yaml:"items"`
		}
		if err := node.Decode(&resource); err != nil {
			return fmt.Errorf("decode production NetworkPolicy resource: %w", err)
		}
		switch resource.Kind {
		case "List", "NetworkPolicyList":
			wantVersion := "v1"
			if resource.Kind == "NetworkPolicyList" {
				wantVersion = "networking.k8s.io/v1"
			}
			if resource.APIVersion != wantVersion {
				return fmt.Errorf("production NetworkPolicy list %q has unsupported apiVersion %q", resource.Kind, resource.APIVersion)
			}
			for index := range resource.Items {
				if err := visit(&resource.Items[index], depth+1); err != nil {
					return err
				}
			}
		case "NetworkPolicy":
			if resource.APIVersion != "networking.k8s.io/v1" {
				return fmt.Errorf("production NetworkPolicy has unsupported apiVersion %q", resource.APIVersion)
			}
			var policy networkPolicy
			if err := node.Decode(&policy); err != nil {
				return fmt.Errorf("decode production NetworkPolicy: %w", err)
			}
			if policy.Metadata.Name == "" || policy.Metadata.Namespace != "" {
				return fmt.Errorf("production NetworkPolicy must be named and must not override the rollout namespace")
			}
			policies = append(policies, policy)
		default:
			return fmt.Errorf("production NetworkPolicy contains unsupported resource kind %q", resource.Kind)
		}
		return nil
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for document := 1; ; document++ {
		var node yaml.Node
		if err := decoder.Decode(&node); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode production NetworkPolicy document %d: %w", document, err)
		}
		if len(node.Content) == 0 || node.Content[0].Tag == "!!null" {
			continue
		}
		if err := visit(&node, 0); err != nil {
			return nil, err
		}
	}
	return policies, nil
}

func validateNetworkPolicy(data []byte, workloads map[string]workload) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("production NetworkPolicy is required")
	}
	policies, err := loadNetworkPolicies(data)
	if err != nil {
		return err
	}
	foundDefaultDeny := false
	foundControlledEgress := map[string]bool{}
	seen := make(map[string]bool)
	for _, policy := range policies {
		if seen[policy.Metadata.Name] {
			return fmt.Errorf("production NetworkPolicy has duplicate resource %q", policy.Metadata.Name)
		}
		seen[policy.Metadata.Name] = true
		if err := policy.Spec.PodSelector.validate(); err != nil {
			return fmt.Errorf("production NetworkPolicy %q has invalid Pod selector: %w", policy.Metadata.Name, err)
		}
		if policy.Metadata.Name == "default-deny" {
			if !isDefaultDeny(policy) {
				return fmt.Errorf("production NetworkPolicy %q must select every Pod and deny both ingress and egress", policy.Metadata.Name)
			}
			foundDefaultDeny = true
		}
		for _, rule := range policy.Spec.Egress {
			if len(rule.To) == 0 {
				return fmt.Errorf("production NetworkPolicy %q has an egress rule with no constrained destination", policy.Metadata.Name)
			}
			for _, peer := range rule.To {
				if peer.IPBlock != nil && isPublicCIDR(peer.IPBlock.CIDR) {
					return fmt.Errorf("production NetworkPolicy %q permits direct public CIDR %q", policy.Metadata.Name, peer.IPBlock.CIDR)
				}
				if err := validateEgressPeer(policy.Metadata.Name, peer); err != nil {
					return err
				}
				if isControlledEgressGateway(peer) {
					for _, app := range []string{"agent-worker", "agent-summary-worker", "agent-delivery"} {
						if policy.Spec.PodSelector.matches(workloads[app].Spec.Template.Metadata.Labels) {
							foundControlledEgress[app] = true
						}
					}
				}
			}
		}
	}
	if !foundDefaultDeny {
		return fmt.Errorf("production NetworkPolicy is missing default-deny")
	}
	for _, app := range []string{"agent-worker", "agent-summary-worker", "agent-delivery"} {
		if !foundControlledEgress[app] {
			return fmt.Errorf("production NetworkPolicy for %s must route public provider traffic through an egress gateway", app)
		}
	}
	return nil
}

func isDefaultDeny(policy networkPolicy) bool {
	if len(policy.Spec.PodSelector.MatchLabels) != 0 ||
		len(policy.Spec.PodSelector.MatchExpressions) != 0 ||
		len(policy.Spec.Ingress) != 0 || len(policy.Spec.Egress) != 0 {
		return false
	}
	types := make(map[string]bool, len(policy.Spec.PolicyTypes))
	for _, value := range policy.Spec.PolicyTypes {
		types[value] = true
	}
	return len(types) == 2 && types["Ingress"] && types["Egress"]
}

func isPublicCIDR(value string) bool {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(value))
	if err != nil {
		return true
	}
	prefix = prefix.Masked()
	for _, private := range nonPublicCIDRs {
		if private.Addr().Is4() != prefix.Addr().Is4() {
			continue
		}
		if private.Bits() <= prefix.Bits() && private.Contains(prefix.Addr()) {
			return false
		}
	}
	return true
}

func validateEgressPeer(policyName string, peer networkPeer) error {
	for _, selector := range []*labelSelector{peer.NamespaceSelector, peer.PodSelector} {
		if selector != nil {
			if err := selector.validate(); err != nil {
				return fmt.Errorf("production NetworkPolicy %q has invalid egress selector: %w", policyName, err)
			}
		}
	}
	if peer.IPBlock != nil {
		if peer.NamespaceSelector != nil || peer.PodSelector != nil {
			return fmt.Errorf("production NetworkPolicy %q mixes ipBlock and selectors in one egress peer", policyName)
		}
		prefix, err := netip.ParsePrefix(strings.TrimSpace(peer.IPBlock.CIDR))
		if err != nil {
			return fmt.Errorf("production NetworkPolicy %q has an invalid egress CIDR", policyName)
		}
		prefix = prefix.Masked()
		if len(peer.IPBlock.Except) != 0 || prefix.Bits() != prefix.Addr().BitLen() ||
			prefix.Addr().Is4In6() || !prefix.Addr().IsPrivate() {
			return fmt.Errorf(
				"production NetworkPolicy %q ipBlock must be an exact private unicast host route without exceptions",
				policyName,
			)
		}
		return nil
	}
	if peer.PodSelector == nil || len(peer.PodSelector.MatchLabels) == 0 {
		return fmt.Errorf("production NetworkPolicy %q has an egress peer without an explicit Pod selector", policyName)
	}
	if peer.NamespaceSelector != nil && len(peer.NamespaceSelector.MatchLabels) == 0 {
		return fmt.Errorf("production NetworkPolicy %q has an egress peer with an unconstrained namespace selector", policyName)
	}
	return nil
}

var nonPublicCIDRs = mustPrefixes(
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.0.0.0/24",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"224.0.0.0/4",
	"240.0.0.0/4",
	"::1/128",
	"fc00::/7",
	"fe80::/10",
	"ff00::/8",
)

func mustPrefixes(values ...string) []netip.Prefix {
	result := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			panic(err)
		}
		result = append(result, prefix)
	}
	return result
}

func isControlledEgressGateway(peer networkPeer) bool {
	return peer.NamespaceSelector != nil &&
		peer.NamespaceSelector.admitsRequiredLabel("agent-platform-access", "egress-gateway") &&
		peer.PodSelector != nil &&
		peer.PodSelector.admitsRequiredLabel("app", "agent-egress-gateway")
}
