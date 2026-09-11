package log

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

type Entry struct {
	AuditID        string              `json:"audit_id,omitempty"`
	Timestamp      time.Time           `json:"timestamp"`
	TenantID       string              `json:"tenant_id"`
	Channel        string              `json:"channel"`
	BindingID      string              `json:"binding_id,omitempty"`
	UserID         string              `json:"user_id"`
	SessionID      string              `json:"session_id"`
	AgentName      string              `json:"agent_name"`
	ToolName       string              `json:"tool_name,omitempty"`
	Decision       string              `json:"decision"`
	Reason         string              `json:"reason,omitempty"`
	LatencyMS      int64               `json:"latency_ms"`
	ErrorType      string              `json:"error_type,omitempty"`
	CostUSD        float64             `json:"cost_usd"`
	TraceID        string              `json:"trace_id,omitempty"`
	RequestID      string              `json:"request_id"`
	ConfigRevision string              `json:"config_revision"`
	ContentHash    string              `json:"content_hash,omitempty"`
	ToolArgsHash   string              `json:"tool_args_hash,omitempty"`
	OperationKey   string              `json:"operation_key,omitempty"`
	OperationPhase string              `json:"operation_phase,omitempty"`
	OperationState string              `json:"operation_state,omitempty"`
	PolicySnapshot *config.AuditPolicy `json:"-"`
	SecretEnvNames []string            `json:"-"`
}

type Sink interface {
	Write(Entry) error
}

// ContextSink is an optional extension used by request paths that need the
// audit storage span to remain attached to the caller's trace. Sink keeps its
// historical context-free method so local/demo and embedding implementations
// do not need a breaking API change.
type ContextSink interface {
	WriteContext(context.Context, Entry) error
}

type TenantPolicyProvider interface {
	Tenant(string) (config.TenantConfig, error)
}

// Router applies each tenant's audit sink and redaction policy. Files are
// opened lazily and retained, avoiding per-request file descriptors.
type Router struct {
	mu          sync.Mutex
	provider    TenantPolicyProvider
	stdout      io.Writer
	secrets     []string
	files       map[string]*os.File
	fileSinks   map[string]*JSONLines
	stdoutSinks map[string]*JSONLines
	sqlSink     *PostgresSink
}

func NewRouter(provider TenantPolicyProvider, stdout io.Writer, secretValues []string) *Router {
	return NewRouterWithSQL(provider, stdout, secretValues, nil)
}

// NewRouterWithSQL adds the durable SQL sink while preserving the existing
// stdout/file routing for tenants that have not opted into it.
func NewRouterWithSQL(provider TenantPolicyProvider, stdout io.Writer, secretValues []string, sqlSink *PostgresSink) *Router {
	if stdout == nil {
		stdout = os.Stdout
	}
	return &Router{
		provider: provider, stdout: stdout, secrets: append([]string(nil), secretValues...),
		files: make(map[string]*os.File), fileSinks: make(map[string]*JSONLines), stdoutSinks: make(map[string]*JSONLines),
		sqlSink: sqlSink,
	}
}

func (r *Router) Write(entry Entry) error {
	return r.WriteContext(context.Background(), entry)
}

func (r *Router) WriteContext(ctx context.Context, entry Entry) error {
	if r == nil {
		return nil
	}
	if entry.PolicySnapshot == nil && r.provider == nil {
		return nil
	}
	var policy config.AuditPolicy
	revision := entry.ConfigRevision
	if entry.PolicySnapshot != nil {
		policy = *entry.PolicySnapshot
	} else {
		tenant, err := r.provider.Tenant(entry.TenantID)
		if err != nil {
			return err
		}
		policy = tenant.Audit
		if revision == "" {
			revision = tenant.Version
		}
	}
	if !policy.Enabled {
		return nil
	}
	secretValues := append([]string(nil), r.secrets...)
	for _, envName := range entry.SecretEnvNames {
		if value, err := config.Secret(envName); err == nil && value != "" {
			secretValues = append(secretValues, value)
		}
	}
	redactor := NewRedactor(policy.RedactPatterns, secretValues)
	entry.AuditID = StableAuditID(entry)
	sinkKey := entry.TenantID + "\x1f" + revision
	switch policy.Sink {
	case "", "stdout":
		r.mu.Lock()
		sink := r.stdoutSinks[sinkKey]
		if sink == nil {
			sink = NewJSONLines(r.stdout, redactor)
			r.stdoutSinks[sinkKey] = sink
		}
		r.mu.Unlock()
		return sink.WriteWithRedactor(entry, redactor)
	case "sql":
		if r.sqlSink == nil {
			return errors.New("audit sql sink is not configured")
		}
		entry.Reason = redactor.Clean(entry.Reason)
		entry.ErrorType = redactor.Clean(entry.ErrorType)
		return r.sqlSink.WriteContext(ctx, entry)
	case "file":
		if policy.Path == "" {
			return errors.New("audit file sink requires a path")
		}
		r.mu.Lock()
		sink := r.fileSinks[sinkKey]
		if sink == nil {
			file, openErr := os.OpenFile(policy.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if openErr != nil {
				r.mu.Unlock()
				return fmt.Errorf("open tenant audit file: %w", openErr)
			}
			r.files[sinkKey] = file
			sink = NewJSONLines(file, redactor)
			r.fileSinks[sinkKey] = sink
		}
		r.mu.Unlock()
		return sink.WriteWithRedactor(entry, redactor)
	case "disabled":
		return nil
	default:
		return fmt.Errorf("unsupported audit sink %q", policy.Sink)
	}
}

func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var result error
	for id, file := range r.files {
		if err := file.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close audit file for %s: %w", id, err))
		}
	}
	r.files = nil
	r.fileSinks = nil
	if r.sqlSink != nil {
		result = errors.Join(result, r.sqlSink.Close())
	}
	return result
}

type JSONLines struct {
	mu       sync.Mutex
	w        io.Writer
	redactor *Redactor
}

func NewJSONLines(w io.Writer, redactor *Redactor) *JSONLines {
	if w == nil {
		w = os.Stdout
	}
	if redactor == nil {
		redactor = NewRedactor(nil, nil)
	}
	return &JSONLines{w: w, redactor: redactor}
}

func (s *JSONLines) Write(entry Entry) error {
	return s.WriteWithRedactor(entry, s.redactor)
}

// WriteWithRedactor preserves the sink's write serialization while applying
// the current request/revision secret set. This matters when a credential is
// rotated without changing the tenant revision, and for console-provisioned
// model keys that live in the in-process secret registry.
func (s *JSONLines) WriteWithRedactor(entry Entry, redactor *Redactor) error {
	if redactor == nil {
		redactor = NewRedactor(nil, nil)
	}
	entry.Reason = redactor.Clean(entry.Reason)
	entry.ErrorType = redactor.Clean(entry.ErrorType)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := json.NewEncoder(s.w).Encode(entry); err != nil {
		return fmt.Errorf("write audit record: %w", err)
	}
	return nil
}

type Redactor struct {
	patterns []*regexp.Regexp
	secrets  []string
}

func NewRedactor(extraPatterns, secretValues []string) *Redactor {
	patterns := []string{
		`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`,
		`(?:\+?86[- ]?)?1[3-9][0-9]{9}`,
		`(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,;]+`,
		`(?i)#approve:[a-f0-9-]{16,64}`,
	}
	patterns = append(patterns, extraPatterns...)
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		if re, err := regexp.Compile(pattern); err == nil {
			compiled = append(compiled, re)
		}
	}
	values := make([]string, 0, len(secretValues))
	for _, value := range secretValues {
		if len(value) >= 4 {
			values = append(values, value)
		}
	}
	return &Redactor{patterns: compiled, secrets: values}
}

func (r *Redactor) Clean(value string) string {
	for _, secret := range r.secrets {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	for _, pattern := range r.patterns {
		value = pattern.ReplaceAllString(value, "[REDACTED]")
	}
	return value
}

func ContentHash(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
}

// StableAuditID returns an identifier which is independent of delivery
// attempts. Replay code should reuse Entry.AuditID when it has one; for older
// callers this deterministic fallback keeps the same logical operation
// idempotent without including the wall-clock timestamp.
func StableAuditID(entry Entry) string {
	if entry.AuditID != "" {
		return entry.AuditID
	}
	seed := strings.Join([]string{
		entry.TenantID, entry.RequestID, entry.OperationKey, entry.OperationPhase,
		entry.SessionID, entry.UserID, entry.ToolName, entry.Decision,
		entry.ContentHash, entry.ToolArgsHash,
	}, "\x1f")
	digest := sha256.Sum256([]byte(seed))
	return "audit-" + hex.EncodeToString(digest[:16])
}

// StableOperationAuditID is used when a request can be replayed with a new
// process/request UUID. The operation key is the durable identity; phase and
// decision keep independent audit facts distinct.
func StableOperationAuditID(tenantID, operationKey, phase, decision string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{tenantID, operationKey, phase, decision}, "\x1f")))
	return "audit-" + hex.EncodeToString(digest[:16])
}
