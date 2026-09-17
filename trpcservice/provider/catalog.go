// Package provider defines the trusted Provider Catalog and immutable,
// secret-free Model/Backend Profile snapshots.
package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Kind string

const (
	KindModel   Kind = "model"
	KindBackend Kind = "backend"
)

type OptionType string

const (
	OptionString  OptionType = "string"
	OptionInteger OptionType = "integer"
	OptionBoolean OptionType = "boolean"
)

type OptionRule struct {
	Type     OptionType
	Required bool
	Default  string
	Enum     []string
	Min      int64
	Max      int64
}

type CapabilitySet map[string]bool

type Schema struct {
	Kind              Kind
	Name              string
	SchemaVersion     uint16
	AllowedModels     []string
	EndpointSchemes   []string
	EndpointHosts     []string
	OptionRules       map[string]OptionRule
	SecretRequirement string
	Capabilities      CapabilitySet
}

type Catalog struct {
	mu      sync.RWMutex
	schemas map[schemaKey]Schema
}

type schemaKey struct {
	Kind    Kind
	Name    string
	Version uint16
}

// DeepSeekModelSchema is the production schema for DeepSeek's official
// OpenAI-compatible endpoint. Provider additions remain code-reviewed catalog
// entries rather than runtime configuration.
func DeepSeekModelSchema() Schema {
	return Schema{
		Kind: KindModel, Name: "deepseek", SchemaVersion: 1,
		// The local service deliberately has one capability-complete default:
		// image-capable chat. Text requests are valid inputs to this model too.
		AllowedModels:   []string{"deepseek-v4-flash-vision-exp"},
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.deepseek.com"},
		OptionRules: map[string]OptionRule{
			"timeout_ms":             {Type: OptionInteger, Default: "60000", Min: 100, Max: 600000},
			"timeout_retry_attempts": {Type: OptionInteger, Default: "1", Min: 1, Max: 3},
			"channel_buffer_size":    {Type: OptionInteger, Default: "256", Min: 1, Max: 4096},
		},
		SecretRequirement: "required",
	}
}

// FakeModelSchema is an intentionally local-only deterministic model surface
// for smoke tests and demos. It carries no endpoint and rejects SecretRef, so
// selecting it cannot cause an outbound credential or network dependency.
func FakeModelSchema() Schema {
	return Schema{
		Kind: KindModel, Name: "fake", SchemaVersion: 1,
		AllowedModels:     []string{"fake-deterministic-v1"},
		SecretRequirement: "forbidden",
		OptionRules: map[string]OptionRule{
			"response":      {Type: OptionString, Default: "fake response"},
			"stream_deltas": {Type: OptionString},
		},
	}
}

// FakeEmbeddingSchema is a credential-free, deterministic embedding surface
// for local Knowledge fixtures. It is deliberately a distinct provider from
// the fake chat model: an embedding profile can never be selected as an Agent
// model, and its fixed vector size remains part of the immutable profile.
func FakeEmbeddingSchema() Schema {
	return Schema{
		Kind: KindModel, Name: "fake-embedding", SchemaVersion: 1,
		AllowedModels:     []string{"fake-embedding-v1"},
		SecretRequirement: "forbidden",
		OptionRules: map[string]OptionRule{
			"dimensions": {Type: OptionInteger, Required: true, Min: 1, Max: 65536},
		},
	}
}

// OpenAIEmbeddingSchema is the reviewed profile shape used by the framework
// Knowledge embedder. It is deliberately separate from chat-model schemas:
// an embedding profile cannot be selected as an Agent model and its immutable
// dimensions are checked against the bound vector collection at runtime.
func OpenAIEmbeddingSchema() Schema {
	return Schema{
		Kind: KindModel, Name: "openai-embedding", SchemaVersion: 1,
		AllowedModels:   []string{"text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"},
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"api.openai.com"},
		OptionRules: map[string]OptionRule{
			"dimensions": {Type: OptionInteger, Required: true, Min: 1, Max: 65536},
		},
		SecretRequirement: "required",
	}
}

// QdrantVectorSchema pins the configuration surface accepted for a Qdrant
// Knowledge backend. Credentials deliberately remain a versioned SecretRef;
// neither API keys nor connection strings may enter a backend profile.
func QdrantVectorSchema() Schema {
	return Schema{
		Kind: KindBackend, Name: "qdrant", SchemaVersion: 1,
		OptionRules: map[string]OptionRule{
			"collection": {Type: OptionString, Required: true},
			"endpoint":   {Type: OptionString, Required: true},
			// sdk uses the official Qdrant VectorStore over gRPC. native keeps
			// the compatibility reader for pre-SDK collections.
			"runtime_engine":     {Type: OptionString, Default: "native", Enum: []string{"native", "sdk"}},
			"grpc_port":          {Type: OptionInteger, Default: "6334", Min: 1, Max: 65535},
			"snapshot_watermark": {Type: OptionString, Required: true},
			"vector_generation":  {Type: OptionString, Required: true},
			"timeout_ms":         {Type: OptionInteger, Default: "20000", Min: 100, Max: 600000},
			"vector_size":        {Type: OptionInteger, Required: true, Min: 1, Max: 65536},
		},
		SecretRequirement: "required",
		Capabilities: CapabilitySet{
			"idempotent_upsert":    true,
			"migration_dual_write": true,
			"tenant_filter":        true,
		},
	}
}

// LocalQdrantVectorSchema is deliberately narrower than the production
// Qdrant schema: it admits only the Compose-network endpoint used by the
// credential-free WebUI fixture. Keeping it a separate provider makes an HTTP
// endpoint impossible to publish accidentally in a production qdrant profile.
func LocalQdrantVectorSchema() Schema {
	schema := QdrantVectorSchema()
	schema.Name = "qdrant-local"
	return schema
}

// PostgresBackendSchema describes the original shared PostgreSQL persistence
// plane. It remains immutable for already published v1 Profiles, which always
// select the deployment's default data plane.
func PostgresBackendSchema() Schema {
	return Schema{
		Kind: KindBackend, Name: "postgres", SchemaVersion: 1,
		SecretRequirement: "forbidden",
		Capabilities: CapabilitySet{
			"atomic_turn_commit": true,
			"strong_ryw":         true,
			"summary_cas":        true,
		},
	}
}

// PostgresBackendSchemaV2 adds an immutable deployment connection selector.
// It is intentionally a new schema version: changing v1 normalization would
// invalidate the content digests of already published Backend Profiles.
func PostgresBackendSchemaV2() Schema {
	return Schema{
		Kind: KindBackend, Name: "postgres", SchemaVersion: 2,
		// connection_id is an opaque deployment selector, never a DSN or a
		// credential. The process maps it to a protected connection at startup.
		// Keeping it in the immutable profile lets a published ConfigSnapshot
		// select a data plane without putting connection material in tenant data.
		OptionRules: map[string]OptionRule{
			"connection_id": {Type: OptionString, Required: true},
		},
		SecretRequirement: "forbidden",
		Capabilities: CapabilitySet{
			"atomic_turn_commit": true,
			"strong_ryw":         true,
			"summary_cas":        true,
		},
	}
}

// RedisMemoryBackendSchema is deliberately scoped to long-term Memory. Redis
// is not admitted as a Session backend because it cannot satisfy this
// service's fenced CommitTurn transaction contract. connection_id selects a
// deployment-owned Redis URL; neither URLs nor credentials are tenant data.
func RedisMemoryBackendSchema() Schema {
	return Schema{
		Kind: KindBackend, Name: "redis-memory", SchemaVersion: 1,
		OptionRules: map[string]OptionRule{
			"connection_id": {Type: OptionString, Required: true},
		},
		SecretRequirement: "forbidden",
		Capabilities: CapabilitySet{
			"strong_ryw": true,
		},
	}
}

// InMemoryBackendSchema is intentionally development-only.  It is useful for
// isolated demos and tests, but has no cross-process visibility and therefore
// must never be admitted by a horizontally scaled worker deployment.
func InMemoryBackendSchema() Schema {
	return Schema{
		Kind: KindBackend, Name: "inmemory-memory", SchemaVersion: 1,
		SecretRequirement: "forbidden",
		Capabilities: CapabilitySet{
			"strong_ryw":       true,
			"single_node_only": true,
		},
	}
}

// Mem0MemoryBackendSchema selects a deployment-owned Mem0 endpoint.  A cloud
// profile has an optional SecretRef at schema level because self-hosted OSS
// has no API key; Resolver applies the stricter mode-specific rule.
func Mem0MemoryBackendSchema() Schema {
	return Schema{
		Kind: KindBackend, Name: "mem0-memory", SchemaVersion: 1,
		OptionRules: map[string]OptionRule{
			"connection_id": {Type: OptionString, Required: true},
		},
		SecretRequirement: "optional",
		Capabilities: CapabilitySet{
			"eventual_visibility": true,
			"external_ingest":     true,
			"read_only_tools":     true,
		},
	}
}

func NewCatalog(schemas ...Schema) (*Catalog, error) {
	catalog := &Catalog{schemas: make(map[schemaKey]Schema, len(schemas))}
	for _, schema := range schemas {
		if err := catalog.Register(schema); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *Catalog) Register(schema Schema) error {
	if c == nil || (schema.Kind != KindModel && schema.Kind != KindBackend) || schema.Name == "" ||
		schema.SchemaVersion < 1 || !validSecretRequirement(schema.SecretRequirement) {
		return runtime.ErrInvariantViolation
	}
	for name, rule := range schema.OptionRules {
		if name == "" || sensitiveOption(name) || !validOptionRule(rule) {
			return runtime.ErrInvariantViolation
		}
	}
	key := schemaKey{schema.Kind, schema.Name, schema.SchemaVersion}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.schemas[key]; exists {
		return runtime.ErrVersionConflict
	}
	c.schemas[key] = cloneSchema(schema)
	return nil
}

func (c *Catalog) Resolve(kind Kind, name string, version uint16) (Schema, error) {
	if c == nil {
		return Schema{}, runtime.ErrCapabilityUnsupported
	}
	c.mu.RLock()
	schema, ok := c.schemas[schemaKey{kind, name, version}]
	c.mu.RUnlock()
	if !ok {
		return Schema{}, runtime.ErrNotFound
	}
	return cloneSchema(schema), nil
}

type ModelProfileSnapshot struct {
	TenantID      string
	ProfileID     string
	ProfileKey    string
	DisplayName   string
	Status        string
	SchemaVersion uint16
	Provider      string
	Model         string
	Endpoint      string
	Options       map[string]string
	SecretRef     secrets.SecretRef
	Generation    map[string]any
	ContentDigest string
	Version       int64
}

type BackendProfileSnapshot struct {
	TenantID      string
	ProfileID     string
	ProfileKey    string
	DisplayName   string
	Status        string
	SchemaVersion uint16
	Provider      string
	Configuration map[string]string
	CredentialRef secrets.SecretRef
	Capabilities  CapabilitySet
	ContentDigest string
	Version       int64
}

func (c *Catalog) NormalizeModel(input ModelProfileSnapshot) (ModelProfileSnapshot, error) {
	schema, err := c.Resolve(KindModel, input.Provider, input.SchemaVersion)
	if err != nil {
		return ModelProfileSnapshot{}, err
	}
	if err := validateIdentity(input.TenantID, input.ProfileID, input.ProfileKey, input.Status, input.Version); err != nil {
		return ModelProfileSnapshot{}, err
	}
	if !contains(schema.AllowedModels, input.Model) {
		return ModelProfileSnapshot{}, runtime.ErrCapabilityUnsupported
	}
	if err := validateEndpoint(input.Endpoint, schema); err != nil {
		return ModelProfileSnapshot{}, err
	}
	options, err := normalizeOptions(input.Options, schema.OptionRules)
	if err != nil {
		return ModelProfileSnapshot{}, err
	}
	if schema.Name == "fake" && schema.SchemaVersion == 1 {
		if err := validateFakeModelOptions(input.Model, input.Endpoint, options); err != nil {
			return ModelProfileSnapshot{}, err
		}
	}
	if schema.Name == "fake-embedding" && schema.SchemaVersion == 1 &&
		(input.Model != "fake-embedding-v1" || input.Endpoint != "") {
		return ModelProfileSnapshot{}, runtime.ErrCapabilityUnsupported
	}
	if err := validateSecret(input.SecretRef, schema.SecretRequirement); err != nil {
		return ModelProfileSnapshot{}, err
	}
	input.Options = options
	input.Generation = cloneAnyMap(input.Generation)
	input.ContentDigest, err = digest(struct {
		SchemaVersion uint16
		Provider      string
		Model         string
		Endpoint      string
		Options       map[string]string
		SecretRef     secrets.SecretRef
		Generation    map[string]any
	}{input.SchemaVersion, input.Provider, input.Model, input.Endpoint, input.Options, input.SecretRef, input.Generation})
	return input, err
}

func validateFakeModelOptions(model, endpoint string, options map[string]string) error {
	if model != "fake-deterministic-v1" || endpoint != "" || !utf8.ValidString(options["response"]) || len(options["response"]) > 65536 {
		return runtime.ErrCapabilityUnsupported
	}
	script, exists := options["stream_deltas"]
	if !exists {
		return nil
	}
	var deltas []string
	if err := json.Unmarshal([]byte(script), &deltas); err != nil || len(deltas) == 0 || len(deltas) > 128 {
		return runtime.ErrCapabilityUnsupported
	}
	var total int
	for _, delta := range deltas {
		if delta == "" || !utf8.ValidString(delta) {
			return runtime.ErrCapabilityUnsupported
		}
		total += len(delta)
		if total > 65536 {
			return runtime.ErrCapabilityUnsupported
		}
	}
	return nil
}

func (c *Catalog) NormalizeBackend(input BackendProfileSnapshot) (BackendProfileSnapshot, error) {
	schema, err := c.Resolve(KindBackend, input.Provider, input.SchemaVersion)
	if err != nil {
		return BackendProfileSnapshot{}, err
	}
	if err := validateIdentity(input.TenantID, input.ProfileID, input.ProfileKey, input.Status, input.Version); err != nil {
		return BackendProfileSnapshot{}, err
	}
	configuration, err := normalizeOptions(input.Configuration, schema.OptionRules)
	if err != nil {
		return BackendProfileSnapshot{}, err
	}
	if schema.Name == "qdrant" && schema.SchemaVersion == 1 {
		if err := validateQdrantConfiguration(configuration); err != nil {
			return BackendProfileSnapshot{}, err
		}
	}
	if schema.Name == "qdrant-local" && schema.SchemaVersion == 1 {
		if err := validateLocalQdrantConfiguration(configuration); err != nil {
			return BackendProfileSnapshot{}, err
		}
	}
	if schema.Name == "postgres" && schema.SchemaVersion == 2 {
		if err := validatePostgresConfiguration(configuration); err != nil {
			return BackendProfileSnapshot{}, err
		}
	}
	if schema.Name == "redis-memory" && schema.SchemaVersion == 1 {
		if err := validateRedisConfiguration(configuration); err != nil {
			return BackendProfileSnapshot{}, err
		}
	}
	if schema.Name == "mem0-memory" && schema.SchemaVersion == 1 {
		if err := validateRedisConfiguration(configuration); err != nil {
			return BackendProfileSnapshot{}, err
		}
	}
	if err := validateSecret(input.CredentialRef, schema.SecretRequirement); err != nil {
		return BackendProfileSnapshot{}, err
	}
	for capability, enabled := range input.Capabilities {
		if enabled && !schema.Capabilities[capability] {
			return BackendProfileSnapshot{}, runtime.ErrCapabilityUnsupported
		}
	}
	input.Configuration = configuration
	input.Capabilities = cloneCapabilities(input.Capabilities)
	input.ContentDigest, err = digest(struct {
		SchemaVersion uint16
		Provider      string
		Configuration map[string]string
		CredentialRef secrets.SecretRef
		Capabilities  CapabilitySet
	}{input.SchemaVersion, input.Provider, input.Configuration, input.CredentialRef, input.Capabilities})
	return input, err
}

func validateQdrantConfiguration(configuration map[string]string) error {
	endpoint, err := url.Parse(configuration["endpoint"])
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return runtime.ErrCapabilityUnsupported
	}
	collection := configuration["collection"]
	if collection == "" || len(collection) > 255 {
		return runtime.ErrInvariantViolation
	}
	for _, value := range collection {
		if !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-') {
			return runtime.ErrCapabilityUnsupported
		}
	}
	return nil
}

func validateLocalQdrantConfiguration(configuration map[string]string) error {
	if err := validateQdrantConfiguration(map[string]string{
		"endpoint": "https://qdrant.local.invalid", "collection": configuration["collection"],
	}); err != nil {
		return err
	}
	endpoint, err := url.Parse(configuration["endpoint"])
	if err != nil || endpoint.Scheme != "http" || endpoint.Host != "qdrant:6333" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "" {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func validatePostgresConfiguration(configuration map[string]string) error {
	return validateConnectionID(configuration["connection_id"])
}

func validateRedisConfiguration(configuration map[string]string) error {
	return validateConnectionID(configuration["connection_id"])
}

func validateConnectionID(connectionID string) error {
	if len(connectionID) == 0 || len(connectionID) > 128 {
		return runtime.ErrInvariantViolation
	}
	for _, value := range connectionID {
		if !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '-') {
			return runtime.ErrCapabilityUnsupported
		}
	}
	return nil
}

func validateIdentity(tenantID, profileID, profileKey, status string, version int64) error {
	if tenantID == "" || profileID == "" || profileKey == "" || version < 1 ||
		(status != "active" && status != "suspended" && status != "disabled") {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func validateEndpoint(raw string, schema Schema) error {
	if raw == "" {
		return nil
	}
	if strings.IndexFunc(raw, unicode.IsControl) >= 0 {
		return runtime.ErrCapabilityUnsupported
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return runtime.ErrCapabilityUnsupported
	}
	if !contains(schema.EndpointSchemes, strings.ToLower(parsed.Scheme)) || !containsFold(schema.EndpointHosts, parsed.Hostname()) {
		return runtime.ErrCapabilityUnsupported
	}
	if address := net.ParseIP(parsed.Hostname()); address != nil && (address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast()) {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func normalizeOptions(input map[string]string, rules map[string]OptionRule) (map[string]string, error) {
	output := make(map[string]string, len(rules))
	for name := range input {
		if sensitiveOption(name) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if _, ok := rules[name]; !ok {
			return nil, runtime.ErrCapabilityUnsupported
		}
	}
	for name, rule := range rules {
		value, exists := input[name]
		if !exists || strings.TrimSpace(value) == "" {
			value = rule.Default
		}
		if value == "" {
			if rule.Required {
				return nil, runtime.ErrInvariantViolation
			}
			continue
		}
		normalized, err := normalizeOption(value, rule)
		if err != nil {
			return nil, err
		}
		output[name] = normalized
	}
	return output, nil
}

func normalizeOption(value string, rule OptionRule) (string, error) {
	value = strings.TrimSpace(value)
	switch rule.Type {
	case OptionString:
		if len(rule.Enum) != 0 && !contains(rule.Enum, value) {
			return "", runtime.ErrCapabilityUnsupported
		}
		return value, nil
	case OptionInteger:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || (rule.Min != 0 && parsed < rule.Min) || (rule.Max != 0 && parsed > rule.Max) {
			return "", runtime.ErrCapabilityUnsupported
		}
		return strconv.FormatInt(parsed, 10), nil
	case OptionBoolean:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return "", runtime.ErrCapabilityUnsupported
		}
		return strconv.FormatBool(parsed), nil
	default:
		return "", runtime.ErrInvariantViolation
	}
}

func sensitiveOption(name string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "").Replace(strings.ToLower(name))
	for _, forbidden := range []string{"apikey", "password", "token", "authorization", "dsn", "credential", "secret"} {
		if strings.Contains(normalized, forbidden) {
			return true
		}
	}
	return false
}

func validateSecret(ref secrets.SecretRef, requirement string) error {
	present := ref.Ref != "" || ref.Version != 0
	if present && (ref.Ref == "" || ref.Version < 1) {
		return runtime.ErrInvariantViolation
	}
	if requirement == "required" && !present || requirement == "forbidden" && present {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func validSecretRequirement(value string) bool {
	return value == "required" || value == "optional" || value == "forbidden"
}

func validOptionRule(rule OptionRule) bool {
	return rule.Type == OptionString || rule.Type == OptionInteger || rule.Type == OptionBoolean
}

func digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("profile digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneSchema(input Schema) Schema {
	input.AllowedModels = append([]string(nil), input.AllowedModels...)
	input.EndpointSchemes = append([]string(nil), input.EndpointSchemes...)
	input.EndpointHosts = append([]string(nil), input.EndpointHosts...)
	input.OptionRules = cloneRules(input.OptionRules)
	input.Capabilities = cloneCapabilities(input.Capabilities)
	sort.Strings(input.AllowedModels)
	sort.Strings(input.EndpointSchemes)
	sort.Strings(input.EndpointHosts)
	return input
}

func cloneRules(input map[string]OptionRule) map[string]OptionRule {
	output := make(map[string]OptionRule, len(input))
	for key, rule := range input {
		rule.Enum = append([]string(nil), rule.Enum...)
		sort.Strings(rule.Enum)
		output[key] = rule
	}
	return output
}

func cloneCapabilities(input CapabilitySet) CapabilitySet {
	output := make(CapabilitySet, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	data, _ := json.Marshal(input)
	var output map[string]any
	_ = json.Unmarshal(data, &output)
	return output
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
