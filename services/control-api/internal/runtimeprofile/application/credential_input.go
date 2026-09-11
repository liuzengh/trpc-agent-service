package application

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// ProfileWrite is a write-only command document, never a persistent Spec.
type ProfileWrite struct {
	ExpectedDraftRevision     int64             `json:"expected_draft_revision"`
	CredentialProtocolVersion string            `json:"credential_protocol_version"`
	Config                    ProfileConfig     `json:"config"`
	Credentials               CredentialActions `json:"credentials"`
}
type CredentialActions map[string]map[string]map[string]CredentialAction
type CredentialAction struct {
	Action                     string  `json:"action"`
	ExpectedCredentialRevision int64   `json:"expected_credential_revision,omitempty"`
	Value                      *string `json:"value,omitempty"`
}
type ProfileConfig struct {
	Executors map[string]domain.ExecutorResource `json:"executors,omitempty"`
	Models    map[string]ModelConfig             `json:"models"`
	Tools     map[string]ToolConfig              `json:"tools"`
	Knowledge map[string]KnowledgeConfig         `json:"knowledge"`
	Storage   map[string]StorageConfig           `json:"storage"`
}
type ModelConfig struct {
	Kind         domain.ModelKind `json:"kind"`
	Model        string           `json:"model"`
	BaseURL      string           `json:"base_url"`
	Capabilities []string         `json:"capabilities"`
}
type ToolConfig struct {
	Kind        domain.ToolKind `json:"kind"`
	ServerURL   string          `json:"server_url"`
	ToolsetName string          `json:"toolset_name"`
	ToolName    string          `json:"tool_name"`
	Auth        ToolAuthConfig  `json:"auth"`
	Capability  string          `json:"capability"`
}
type ToolAuthConfig struct {
	Kind domain.AuthKind `json:"kind"`
}
type KnowledgeConfig struct {
	BackendID       string               `json:"backend_id,omitempty"`
	BackendRevision uint64               `json:"backend_revision,omitempty"`
	Kind            domain.KnowledgeKind `json:"kind"`
	Host            string               `json:"host"`
	Port            int64                `json:"port"`
	TLS             bool                 `json:"tls"`
	Collection      string               `json:"collection"`
	Embedding       EmbeddingConfig      `json:"embedding"`
}
type EmbeddingConfig struct {
	Model      string `json:"model"`
	BaseURL    string `json:"base_url"`
	Dimensions int64  `json:"dimensions"`
}
type StorageConfig struct {
	BackendID       string                     `json:"backend_id,omitempty"`
	BackendRevision uint64                     `json:"backend_revision,omitempty"`
	Kind            domain.StorageKind         `json:"kind"`
	Destination     *domain.StorageDestination `json:"destination,omitempty"`
}

var resourceName = regexp.MustCompile("^[a-z][a-z0-9_-]{0,63}$")

// DecodeProfileWrite rejects unknown/duplicate fields before persistence or
// logging. Its errors contain no input excerpts.
func DecodeProfileWrite(data []byte) (ProfileWrite, error) {
	var input ProfileWrite
	if err := decodeCredentialJSON(data, &input); err != nil {
		return ProfileWrite{}, err
	}
	if !managedWireFields(data) {
		return ProfileWrite{}, domain.ErrCredentialInput
	}
	if err := input.validate(); err != nil {
		return ProfileWrite{}, err
	}
	return input, nil
}
func decodeCredentialJSON(data []byte, dst any) error {
	if len(data) == 0 || len(data) > domain.MaxDocumentBytes || !utf8.Valid(data) {
		return domain.ErrCredentialInput
	}
	if _, err := jcs.Transform(data); err != nil {
		return domain.ErrCredentialInput
	}
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil || containsNull(tree) || !exactJSONFields(tree, reflect.TypeOf(dst).Elem()) {
		return domain.ErrCredentialInput
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return domain.ErrCredentialInput
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return domain.ErrCredentialInput
	}
	return nil
}

// encoding/json matches tags case-insensitively; platform DTOs do not.
func exactJSONFields(value any, typ reflect.Type) bool {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		obj, ok := value.(map[string]any)
		if !ok {
			return false
		}
		fields := make(map[string]reflect.Type)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")[0]
			if tag != "" && tag != "-" {
				fields[tag] = field.Type
			}
		}
		for key, child := range obj {
			field, ok := fields[key]
			if !ok || !exactJSONFields(child, field) {
				return false
			}
		}
	case reflect.Map:
		obj, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, child := range obj {
			if !exactJSONFields(child, typ.Elem()) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		values, ok := value.([]any)
		if !ok {
			return false
		}
		for _, child := range values {
			if !exactJSONFields(child, typ.Elem()) {
				return false
			}
		}
	}
	return true
}

func containsNull(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case map[string]any:
		for _, child := range v {
			if containsNull(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if containsNull(child) {
				return true
			}
		}
	}
	return false
}

func (w ProfileWrite) validate() error {
	if w.ExpectedDraftRevision <= 0 || w.CredentialProtocolVersion != domain.CredentialProtocolVersionV1 {
		return domain.ErrCredentialInput
	}
	if w.Config.Models == nil || w.Config.Tools == nil || w.Config.Knowledge == nil || w.Config.Storage == nil {
		return domain.ErrCredentialInput
	}
	if len(w.Config.Models) > domain.MaxModelResources || len(w.Config.Tools) > domain.MaxToolResources ||
		len(w.Config.Executors) > 16 || len(w.Config.Knowledge) > domain.MaxKnowledgeResources || len(w.Config.Storage) > domain.MaxStorageResources {
		return domain.ErrCredentialInput
	}
	for _, keys := range [][]string{mapKeys(w.Config.Models), mapKeys(w.Config.Tools), mapKeys(w.Config.Knowledge), mapKeys(w.Config.Storage), mapKeys(w.Config.Executors)} {
		for _, key := range keys {
			if !resourceName.MatchString(key) {
				return domain.ErrCredentialInput
			}
		}
	}
	for _, executor := range w.Config.Executors {
		if executor.Kind != "sdk_sandbox" {
			return domain.ErrCredentialInput
		}
	}
	// Incomplete Drafts are allowed, but credential-bearing URL components must
	// never enter the ordinary configuration store or its public read projection.
	for _, model := range w.Config.Models {
		if !nonSecretEndpoint(model.BaseURL) {
			return domain.ErrCredentialInput
		}
	}
	for _, tool := range w.Config.Tools {
		if !nonSecretEndpoint(tool.ServerURL) {
			return domain.ErrCredentialInput
		}
	}
	for _, knowledge := range w.Config.Knowledge {
		if knowledge.Kind == domain.KnowledgeKindManaged {
			if knowledge.BackendID == "" || knowledge.BackendRevision == 0 || knowledge.Host != "" || knowledge.Port != 0 || knowledge.TLS || knowledge.Collection != "" {
				return domain.ErrCredentialInput
			}
		} else if knowledge.BackendID != "" || knowledge.BackendRevision != 0 {
			return domain.ErrCredentialInput
		}
		if strings.ContainsAny(knowledge.Host, "@/?#") || !nonSecretEndpoint(knowledge.Embedding.BaseURL) {
			return domain.ErrCredentialInput
		}
	}
	for name, storage := range w.Config.Storage {
		if storage.Kind.Managed() {
			if name != storage.Kind.Role() || storage.BackendID == "" || storage.BackendRevision == 0 || storage.Destination != nil {
				return domain.ErrCredentialInput
			}
		} else if storage.BackendID != "" || storage.BackendRevision != 0 {
			return domain.ErrCredentialInput
		}
		if storage.Destination != nil && strings.ContainsAny(storage.Destination.Host, "@/?#") {
			return domain.ErrCredentialInput
		}
	}
	for category, resources := range w.Credentials {
		if category != "models" && category != "tools" && category != "knowledge" && category != "storage" {
			return domain.ErrCredentialInput
		}
		for name, purposes := range resources {
			if !resourceName.MatchString(name) {
				return domain.ErrCredentialInput
			}
			for purpose, action := range purposes {
				if !w.Config.acceptsPurpose(category, name, purpose, action.Action == "clear") {
					return domain.ErrCredentialInput
				}
				if err := action.validate(false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func nonSecretEndpoint(raw string) bool {
	if raw == "" {
		return true
	}
	parsed, err := url.Parse(raw)
	return err == nil && parsed.User == nil && parsed.RawQuery == "" && !parsed.ForceQuery &&
		parsed.Fragment == "" && !strings.Contains(raw, "#")
}

func (a CredentialAction) validate(live bool) error {
	switch a.Action {
	case "keep", "clear":
		if a.Value != nil || a.ExpectedCredentialRevision < 0 || (!live && a.ExpectedCredentialRevision != 0) {
			return domain.ErrCredentialInput
		}
		if live && a.Action == "clear" && a.ExpectedCredentialRevision <= 0 {
			return domain.ErrCredentialInput
		}
	case "replace":
		if a.Value == nil || !validCredentialValue(*a.Value) {
			return domain.ErrCredentialInput
		}
		if live && a.ExpectedCredentialRevision <= 0 || !live && a.ExpectedCredentialRevision != 0 {
			return domain.ErrCredentialInput
		}
	default:
		return domain.ErrCredentialInput
	}
	return nil
}
func validCredentialValue(v string) bool {
	if len(v) == 0 || len(v) > domain.MaxCredentialValueBytes || strings.TrimSpace(v) == "" || !utf8.ValidString(v) {
		return false
	}
	if strings.ContainsAny(v, "\x00\r\n") {
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(v))
	if strings.Trim(lower, "*•● ") == "" || strings.HasPrefix(lower, "<") || lower == "[redacted]" || lower == "unchanged" || lower == "redacted" {
		return false
	}
	return true
}
func mapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (c ProfileConfig) acceptsPurpose(category, name, purpose string, clearing bool) bool {
	switch category {
	case "models":
		_, ok := c.Models[name]
		return ok && purpose == "api_key"
	case "tools":
		r, ok := c.Tools[name]
		return ok && purpose == "bearer_token" && (r.Auth.Kind == domain.AuthKindBearer || clearing)
	case "knowledge":
		_, ok := c.Knowledge[name]
		return ok && (purpose == "embedding_api_key" || purpose == "qdrant_api_key")
	case "storage":
		r, ok := c.Storage[name]
		return ok && ((r.Kind == domain.StorageKindManagedArtifact && (purpose == "access_key_id" || purpose == "secret_access_key")) || (!r.Kind.Managed() && purpose == "dsn") || ((r.Kind == domain.StorageKindManagedMemory || r.Kind == domain.StorageKindManagedSession) && purpose == "dsn_password"))
	}
	return false
}

// ParseStorageCredential accepts only an explicit PostgreSQL URI and separates
// its password from the fixed destination. Arbitrary driver options, keyword
// DSNs, implicit SSL policy, multi-host routing and sockets are not accepted.
func ParseStorageCredential(value string) (domain.StorageDestination, []byte, error) {
	fail := func() (domain.StorageDestination, []byte, error) {
		return domain.StorageDestination{}, nil, domain.ErrCredentialInput
	}
	if !validCredentialValue(value) || strings.Contains(value, "#") {
		return fail()
	}
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.Opaque != "" ||
		u.Fragment != "" || u.Hostname() == "" || strings.ContainsAny(u.Hostname(), ",/\\ \t") {
		return fail()
	}
	password, ok := u.User.Password()
	username := u.User.Username()
	if !ok || !validCredentialValue(password) || username == "" || len(username) > 128 || strings.ContainsAny(username, "\x00\r\n") {
		return fail()
	}
	host := strings.ToLower(u.Hostname())
	if len(host) > 253 {
		return fail()
	}
	port := int64(5432)
	if u.Port() != "" {
		port, err = strconv.ParseInt(u.Port(), 10, 64)
		if err != nil || port < 1 || port > 65535 {
			return fail()
		}
	}
	if strings.HasPrefix(u.Host, "[") && net.ParseIP(host) == nil {
		return fail()
	}
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" || len(database) > 128 || strings.ContainsAny(database, "/\x00\r\n") {
		return fail()
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 1 || len(query["sslmode"]) != 1 {
		return fail()
	}
	mode := query.Get("sslmode")
	if mode != "disable" && mode != "require" && mode != "verify-full" {
		return fail()
	}
	return domain.StorageDestination{Host: host, Port: port, Database: database, Username: username, SSLMode: mode}, []byte(password), nil
}
