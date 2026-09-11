package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/gowebpki/jcs"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var ErrInvalidManifestContent = errors.New("invalid runtime manifest content")

// CanonicalizeManifest returns an owned normalized value, its RFC 8785 JSON,
// and the SHA-256 identity of that exact content.
func CanonicalizeManifest(content ManifestContent) (ManifestContent, json.RawMessage, string, error) {
	normalized := normalizeManifestContent(content)
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return ManifestContent{}, nil, "", fmt.Errorf("%w: encode: %v", ErrInvalidManifestContent, err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return ManifestContent{}, nil, "", fmt.Errorf("%w: canonicalize: %v", ErrInvalidManifestContent, err)
	}
	sum := sha256.Sum256(canonical)
	return normalized, append(json.RawMessage(nil), canonical...),
		"sha256:" + hex.EncodeToString(sum[:]), nil
}

// DecodeManifestContent performs a closed typed decode. Semantic invariants and
// digest integrity are checked by ValidateManifestContent/VerifyManifestContent.
func DecodeManifestContent(raw json.RawMessage) (ManifestContent, error) {
	if len(raw) == 0 {
		return ManifestContent{}, ErrInvalidManifestContent
	}
	if err := validateDataCapabilityPresence(raw); err != nil {
		return ManifestContent{}, ErrInvalidManifestContent
	}
	var content ManifestContent
	if err := strictDecodeJSON(raw, &content); err != nil {
		return ManifestContent{}, fmt.Errorf("%w: %v", ErrInvalidManifestContent, err)
	}
	if err := validateManagedWire(raw); err != nil {
		return ManifestContent{}, err
	}
	return content, nil
}

// ValidateManifestContent accepts equivalent JSON representations (including
// PostgreSQL jsonb reserialization), but still requires a closed typed shape,
// valid semantic invariants, and the digest of the normalized RFC 8785 content.
func ValidateManifestContent(raw json.RawMessage, digest string) (ManifestContent, error) {
	content, err := DecodeManifestContent(raw)
	if err != nil {
		return ManifestContent{}, err
	}
	if err := validateManifestCredentialShape(content); err != nil {
		return ManifestContent{}, err
	}
	normalized, _, calculated, err := CanonicalizeManifest(content)
	if err != nil {
		return ManifestContent{}, err
	}
	if digest != calculated {
		return ManifestContent{}, ErrInvalidManifestContent
	}
	return normalized, nil
}

// VerifyManifestContent additionally requires the supplied wire bytes to be
// the canonical representation. It is used before immutable publication; read
// paths use ValidateManifestContent because jsonb does not preserve key order.
func VerifyManifestContent(raw json.RawMessage, digest string) (ManifestContent, error) {
	content, err := ValidateManifestContent(raw, digest)
	if err != nil {
		return ManifestContent{}, err
	}
	_, canonical, _, err := CanonicalizeManifest(content)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ManifestContent{}, ErrInvalidManifestContent
	}
	return content, nil
}

func validateManifestCredentialShape(content ManifestContent) error {
	if content.SchemaVersion != SchemaVersionV1 || content.CompilerVersion != CompilerVersionV1 ||
		content.RuntimeContractVersion != RuntimeContractVersionV1 || content.TenantID == "" ||
		content.PlatformContract.Version == "" || !validDigest(content.PlatformContract.Digest) ||
		content.Sources.Agent.AgentID == "" || content.Sources.Agent.VersionID == "" ||
		content.Sources.Agent.VersionNumber <= 0 || content.Sources.Agent.SchemaVersion != SchemaVersionV1 ||
		!validDigest(content.Sources.Agent.Digest) || content.Sources.Profile.ProfileID == "" ||
		content.Sources.Profile.RevisionID == "" || content.Sources.Profile.RevisionNumber <= 0 ||
		content.Sources.Profile.SchemaVersion != SchemaVersionV1 ||
		!validDigest(content.Sources.Profile.Digest) || content.AgentPlan.Root == "" ||
		content.Execution.Backend == "" || content.Execution.MaxRunSeconds <= 0 ||
		content.Execution.MaxToolCalls <= 0 || content.Execution.MaxOutputTokens <= 0 {
		return ErrInvalidManifestContent
	}
	if _, exists := content.AgentPlan.Nodes[content.AgentPlan.Root]; !exists {
		return ErrInvalidManifestContent
	}
	for _, resource := range content.Resources.Models {
		if !validCredentialUse(resource.Credential, CredentialPurposeAPIKey) {
			return ErrInvalidManifestContent
		}
	}
	for _, resource := range content.Resources.Tools {
		switch resource.Auth.Kind {
		case "none":
			if resource.Auth.Credential != nil {
				return ErrInvalidManifestContent
			}
		case "bearer":
			if resource.Auth.Credential == nil ||
				!validCredentialUse(*resource.Auth.Credential, CredentialPurposeBearerToken) {
				return ErrInvalidManifestContent
			}
		default:
			return ErrInvalidManifestContent
		}
	}
	for _, resource := range content.Resources.Knowledge {
		if resource.Credential != nil &&
			!validCredentialUse(*resource.Credential, CredentialPurposeQdrantAPIKey) {
			return ErrInvalidManifestContent
		}
		if !validCredentialUse(resource.Embedding.Credential, CredentialPurposeEmbeddingAPIKey) {
			return ErrInvalidManifestContent
		}
	}
	for _, resource := range content.Resources.Storage {
		if resource.Credentials != nil && (resource.Kind != profiledomain.StorageKindManagedArtifact || resource.Backend == nil || !validArtifactCredentials(resource.Credentials, *resource.Backend)) {
			return ErrInvalidManifestContent
		}
		if resource.Kind.Managed() {
			if resource.Backend != nil && managedPasswordBackend(resource.Kind, *resource.Backend) && ((resource.Kind == "managed_memory" && resource.Backend.Kind == "postgresql") || resource.Credential != (CredentialUse{})) {
				if !validCredentialUse(resource.Credential, CredentialPurposeDSNPassword) {
					return ErrInvalidManifestContent
				}
			} else if resource.Credential != (CredentialUse{}) {
				return ErrInvalidManifestContent
			}
			continue
		}
		if !validCredentialUse(resource.Credential, CredentialPurposeDSN) {
			return ErrInvalidManifestContent
		}
	}
	return validateManifestSemantics(content)
}

func validCredentialUse(use CredentialUse, purpose string) bool {
	if len(use.CredentialID) != 36 || !bytes.HasPrefix([]byte(use.CredentialID), []byte("crd_")) ||
		use.Purpose != purpose || !validDigest(use.AudienceDigest) {
		return false
	}
	for _, digit := range use.CredentialID[4:] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != 71 || !bytes.HasPrefix([]byte(value), []byte("sha256:")) {
		return false
	}
	for _, digit := range value[7:] {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

func strictDecodeJSON(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return errors.New("JSON must contain exactly one value")
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				nameToken, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := nameToken.(string)
				if !ok {
					return errors.New("JSON object name is not a string")
				}
				if _, duplicate := seen[name]; duplicate {
					return errors.New("JSON object contains a duplicate key")
				}
				seen[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("unterminated JSON object")
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("unterminated JSON array")
			}
		default:
			return errors.New("unexpected JSON delimiter")
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return errors.New("JSON must contain exactly one value")
	}
	return nil
}

func normalizeManifestContent(content ManifestContent) ManifestContent {
	normalized := content
	if content.Runtime != nil {
		runtime := *content.Runtime
		if runtime.Summary != nil {
			summary := *runtime.Summary
			runtime.Summary = &summary
		}
		normalized.Runtime = &runtime
	}
	normalized.AgentPlan = AgentPlan{
		Root:  content.AgentPlan.Root,
		Nodes: make(map[string]ManifestNode, len(content.AgentPlan.Nodes)),
	}
	for id, node := range content.AgentPlan.Nodes {
		if node.Memory != nil {
			memory := *node.Memory
			memory.Tools = sortedUnique(memory.Tools)
			if memory.PreloadLimit != nil {
				limit := *memory.PreloadLimit
				memory.PreloadLimit = &limit
			}
			node.Memory = &memory
		}
		if node.Workspace != nil {
			w := *node.Workspace
			w.Tools = sortedUnique(w.Tools)
			node.Workspace = &w
		}
		if node.Artifact != nil {
			artifact := *node.Artifact
			node.Artifact = &artifact
		}
		if node.AddSessionSummary != nil {
			enabled := *node.AddSessionSummary
			node.AddSessionSummary = &enabled
		}
		node.ToolResources = sortedUnique(node.ToolResources)
		node.KnowledgeResources = sortedUnique(node.KnowledgeResources)
		node.CallableEntries = sortedUnique(node.CallableEntries)
		node.Children = append([]string{}, node.Children...)
		if node.Generation != nil {
			generation := *node.Generation
			if generation.Temperature != nil {
				temperature := *generation.Temperature
				generation.Temperature = &temperature
			}
			if generation.MaxOutputTokens != nil {
				tokens := *generation.MaxOutputTokens
				generation.MaxOutputTokens = &tokens
			}
			node.Generation = &generation
		}
		normalized.AgentPlan.Nodes[id] = node
	}
	normalized.Resources = ManifestResources{
		Executors: make(map[string]ManifestExecutorResource, len(content.Resources.Executors)),
		Models:    make(map[string]ManifestModelResource, len(content.Resources.Models)),
		Tools:     make(map[string]ManifestToolResource, len(content.Resources.Tools)),
		Knowledge: make(map[string]ManifestKnowledgeResource, len(content.Resources.Knowledge)),
		Storage:   make(map[string]ManifestStorageResource, len(content.Resources.Storage)),
	}
	for name, resource := range content.Resources.Executors {
		normalized.Resources.Executors[name] = resource
	}
	for name, resource := range content.Resources.Models {
		resource.Capabilities = sortedUnique(resource.Capabilities)
		normalized.Resources.Models[name] = resource
	}
	for name, resource := range content.Resources.Tools {
		if resource.Auth.Credential != nil {
			credential := *resource.Auth.Credential
			resource.Auth.Credential = &credential
		}
		normalized.Resources.Tools[name] = resource
	}
	for name, resource := range content.Resources.Knowledge {
		if resource.Backend != nil {
			b := resource.Backend.Clone()
			resource.Backend = &b
		}
		if resource.Credential != nil {
			credential := *resource.Credential
			resource.Credential = &credential
		}
		normalized.Resources.Knowledge[name] = resource
	}
	for name, resource := range content.Resources.Storage {
		if resource.Credentials != nil {
			c := *resource.Credentials
			resource.Credentials = &c
		}
		if resource.Backend != nil {
			b := resource.Backend.Clone()
			resource.Backend = &b
		}
		normalized.Resources.Storage[name] = resource
	}
	normalized.ResolvedRequirements = ResolvedRequirements{
		Executors: cloneMap(content.ResolvedRequirements.Executors),
		Models:    cloneMap(content.ResolvedRequirements.Models),
		Tools:     cloneMap(content.ResolvedRequirements.Tools),
		Knowledge: cloneMap(content.ResolvedRequirements.Knowledge),
	}
	normalized.StorageRoles = cloneMap(content.StorageRoles)
	normalized.Execution = content.Execution
	normalized.Execution.AllowedEndpointHosts = sortedUnique(content.Execution.AllowedEndpointHosts)
	return normalized
}
