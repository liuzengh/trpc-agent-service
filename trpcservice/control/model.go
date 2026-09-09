// Package control defines the Redis-backed governance and node control plane.
package control

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	MaxRedactionPatterns = 32
	MaxRedactionBytes    = 256
	MaxQueryLimit        = 100
)

var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

var (
	ErrUnavailable              = errors.New("control plane unavailable")
	ErrPolicyMissing            = errors.New("tenant policy is missing")
	ErrPlacementMissing         = errors.New("tenant placement is missing")
	ErrRevisionConflict         = errors.New("control plane revision conflict")
	ErrNodeConflict             = errors.New("node lease conflict")
	ErrNodeNotFound             = errors.New("node not found")
	ErrAssignmentNotFound       = errors.New("node assignment not found")
	ErrAssignmentConflict       = errors.New("node assignment digest conflict")
	ErrAssignmentNotOverridable = errors.New("node assignment is not overridable")
	ErrConfirmationNotFound     = errors.New("tool confirmation not found or expired")
	ErrConfirmationMismatch     = errors.New("tool confirmation does not match")
	ErrAdminAPIDisabled         = errors.New("admin API is disabled")
	ErrAdminUnauthorized        = errors.New("admin API authorization failed")
)

type TenantPolicy struct {
	TenantID             string    `json:"tenant_id"`
	Revision             int64     `json:"revision"`
	ActorAllowlistHashes []string  `json:"actor_allowlist_hashes"`
	ToolAllowlist        []string  `json:"tool_allowlist"`
	DangerousTools       []string  `json:"dangerous_tools"`
	RedactionPatterns    []string  `json:"redaction_patterns"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func DefaultTenantPolicy(tenantID string, nonDangerousTools []string, now time.Time) TenantPolicy {
	tools := normalizedNames(nonDangerousTools)
	filtered := tools[:0]
	for _, name := range tools {
		if name != "phase6.dangerous_echo" {
			filtered = append(filtered, name)
		}
	}
	return TenantPolicy{
		TenantID: tenantID, Revision: 1,
		ActorAllowlistHashes: []string{}, ToolAllowlist: filtered,
		DangerousTools: []string{}, RedactionPatterns: []string{},
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
}

func (p TenantPolicy) Validate() error {
	if err := tenant.ValidateID("policy tenant id", p.TenantID); err != nil {
		return err
	}
	if p.Revision < 1 || p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() || p.UpdatedAt.Before(p.CreatedAt) {
		return errors.New("tenant policy revision or timestamps are invalid")
	}
	for _, digest := range p.ActorAllowlistHashes {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 32 {
			return errors.New("tenant policy actor allowlist contains an invalid HMAC")
		}
	}
	for _, collection := range [][]string{p.ToolAllowlist, p.DangerousTools} {
		for _, name := range collection {
			if !toolNamePattern.MatchString(name) {
				return errors.New("tenant policy contains an invalid tool name")
			}
		}
	}
	if len(p.RedactionPatterns) > MaxRedactionPatterns {
		return fmt.Errorf("tenant policy has more than %d redaction patterns", MaxRedactionPatterns)
	}
	for _, pattern := range p.RedactionPatterns {
		if pattern == "" || len(pattern) > MaxRedactionBytes {
			return fmt.Errorf("tenant redaction patterns must be between 1 and %d bytes", MaxRedactionBytes)
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return errors.New("tenant policy contains an invalid RE2 redaction pattern")
		}
	}
	return nil
}

type PlacementMode string

const (
	PlacementShared    PlacementMode = "shared"
	PlacementDedicated PlacementMode = "dedicated"
)

type TenantPlacement struct {
	TenantID  string        `json:"tenant_id"`
	Revision  int64         `json:"revision"`
	Mode      PlacementMode `json:"mode"`
	NodeID    string        `json:"node_id,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

func DefaultTenantPlacement(tenantID string, now time.Time) TenantPlacement {
	return TenantPlacement{TenantID: tenantID, Revision: 1, Mode: PlacementShared, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
}

func (p TenantPlacement) Validate() error {
	if err := tenant.ValidateID("placement tenant id", p.TenantID); err != nil {
		return err
	}
	if p.Revision < 1 || p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() || p.UpdatedAt.Before(p.CreatedAt) {
		return errors.New("tenant placement revision or timestamps are invalid")
	}
	switch p.Mode {
	case PlacementShared:
		if p.NodeID != "" {
			return errors.New("shared tenant placement must not specify node_id")
		}
	case PlacementDedicated:
		if err := tenant.ValidateID("dedicated placement node id", p.NodeID); err != nil {
			return err
		}
	default:
		return errors.New("tenant placement mode is invalid")
	}
	return nil
}

type NodeState string

const (
	NodeReady    NodeState = "ready"
	NodeDraining NodeState = "draining"
	NodeOffline  NodeState = "offline"
)

type NodeRecord struct {
	NodeID               string            `json:"node_id"`
	Role                 string            `json:"role"`
	State                NodeState         `json:"state"`
	Capacity             int               `json:"capacity"`
	Inflight             int               `json:"inflight"`
	BuildVersion         string            `json:"build_version"`
	ProtocolCapabilities []string          `json:"protocol_capabilities"`
	BootID               string            `json:"boot_id"`
	LeaseUntil           time.Time         `json:"lease_until"`
	StartedAt            time.Time         `json:"started_at"`
	LastHeartbeat        time.Time         `json:"last_heartbeat"`
	Labels               map[string]string `json:"labels,omitempty"`
}

func (n NodeRecord) Validate() error {
	if err := tenant.ValidateID("node id", n.NodeID); err != nil {
		return err
	}
	if err := tenant.ValidateID("node role", n.Role); err != nil {
		return err
	}
	if err := tenant.ValidateID("node boot id", n.BootID); err != nil {
		return err
	}
	if n.State != NodeReady && n.State != NodeDraining && n.State != NodeOffline {
		return errors.New("node state is invalid")
	}
	if n.Capacity < 1 || n.Inflight < 0 || n.Inflight > n.Capacity {
		return errors.New("node capacity or inflight is invalid")
	}
	if strings.TrimSpace(n.BuildVersion) == "" || n.StartedAt.IsZero() || n.LastHeartbeat.IsZero() || n.LeaseUntil.IsZero() {
		return errors.New("node build or timestamps are invalid")
	}
	for _, capability := range n.ProtocolCapabilities {
		if !toolNamePattern.MatchString(capability) {
			return errors.New("node protocol capability is invalid")
		}
	}
	for key, value := range n.Labels {
		if !toolNamePattern.MatchString(key) || len(value) > tenant.MaxIDBytes {
			return errors.New("node label is invalid")
		}
	}
	return nil
}

type AssignmentState string

const (
	AssignmentPlanned   AssignmentState = "planned"
	AssignmentNodeWait  AssignmentState = "node_wait"
	AssignmentAdmitted  AssignmentState = "admitted"
	AssignmentRunning   AssignmentState = "running"
	AssignmentBlocked   AssignmentState = "blocked"
	AssignmentCompleted AssignmentState = "completed"
)

type NodeAssignment struct {
	InboxID       string          `json:"inbox_id"`
	TenantID      string          `json:"tenant_id"`
	AgentAppID    string          `json:"agent_app_id"`
	PayloadDigest string          `json:"payload_digest"`
	NodeID        string          `json:"node_id"`
	Mode          PlacementMode   `json:"mode"`
	State         AssignmentState `json:"state"`
	Revision      int64           `json:"revision"`
	BlockedReason string          `json:"blocked_reason,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

func (a NodeAssignment) Validate() error {
	for field, value := range map[string]string{"assignment inbox id": a.InboxID, "assignment tenant id": a.TenantID, "assignment agent app id": a.AgentAppID, "assignment node id": a.NodeID} {
		if err := tenant.ValidateID(field, value); err != nil {
			return err
		}
	}
	if len(a.PayloadDigest) != 64 {
		return errors.New("assignment payload digest is invalid")
	}
	if _, err := hex.DecodeString(a.PayloadDigest); err != nil {
		return errors.New("assignment payload digest is invalid")
	}
	if a.Mode != PlacementShared && a.Mode != PlacementDedicated {
		return errors.New("assignment mode is invalid")
	}
	if a.State != AssignmentPlanned && a.State != AssignmentNodeWait && a.State != AssignmentAdmitted && a.State != AssignmentRunning && a.State != AssignmentBlocked && a.State != AssignmentCompleted {
		return errors.New("assignment state is invalid")
	}
	if a.Revision < 1 || a.CreatedAt.IsZero() || a.UpdatedAt.IsZero() || a.UpdatedAt.Before(a.CreatedAt) {
		return errors.New("assignment revision or timestamps are invalid")
	}
	if a.State == AssignmentBlocked && a.BlockedReason == "" {
		return errors.New("blocked assignment reason is missing")
	}
	return nil
}

type AuditRecord struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	Channel         string    `json:"channel,omitempty"`
	ActorUserIDHash string    `json:"actor_user_id_hash,omitempty"`
	SessionIDHash   string    `json:"session_id_hash,omitempty"`
	AgentAppID      string    `json:"agent_app_id,omitempty"`
	ToolName        string    `json:"tool_name,omitempty"`
	EventType       string    `json:"event_type"`
	Decision        string    `json:"decision,omitempty"`
	LatencyMicros   int64     `json:"latency_micros,omitempty"`
	ErrorType       string    `json:"error_type,omitempty"`
	CostMicros      *int64    `json:"cost_micros,omitempty"`
	TraceID         string    `json:"trace_id,omitempty"`
	RequestID       string    `json:"request_id,omitempty"`
	TaskID          string    `json:"task_id,omitempty"`
	Sequence        int       `json:"sequence"`
	OccurredAt      time.Time `json:"occurred_at"`
}

func (r AuditRecord) Validate() error {
	if err := tenant.ValidateID("audit id", r.ID); err != nil {
		return err
	}
	if err := tenant.ValidateID("audit tenant id", r.TenantID); err != nil {
		return err
	}
	if r.EventType == "" || r.Sequence < 0 || r.OccurredAt.IsZero() || r.LatencyMicros < 0 {
		return errors.New("audit record metadata is invalid")
	}
	if r.ErrorType != "" {
		switch r.ErrorType {
		case "actor_forbidden", "tenant_policy_missing", "tenant_policy_unavailable", "confirmation_denied", "unknown":
		default:
			return errors.New("audit error type is invalid")
		}
	}
	if r.ToolName != "" && !toolNamePattern.MatchString(r.ToolName) {
		return errors.New("audit tool name is invalid")
	}
	return nil
}

type MetricEvent struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	AgentAppID string            `json:"agent_app_id,omitempty"`
	Name       string            `json:"name"`
	Value      float64           `json:"value"`
	Count      int64             `json:"count"`
	Labels     map[string]string `json:"labels,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
}

func (e MetricEvent) Validate() error {
	if err := tenant.ValidateID("metric id", e.ID); err != nil {
		return err
	}
	if err := tenant.ValidateID("metric tenant id", e.TenantID); err != nil {
		return err
	}
	if !toolNamePattern.MatchString(e.Name) || e.Count < 1 || e.OccurredAt.IsZero() {
		return errors.New("metric event metadata is invalid")
	}
	for key, value := range e.Labels {
		if !allowedMetricLabel(key) || len(value) > tenant.MaxIDBytes {
			return errors.New("metric event contains a forbidden or invalid label")
		}
	}
	return nil
}

func allowedMetricLabel(name string) bool {
	switch name {
	case "tenant_id", "agent_app_id", "channel", "backend_kind", "node_id", "tool_name", "error_type":
		return true
	case "status", "operation", "token_type", "reason":
		return true
	default:
		return false
	}
}

type AuditQuery struct {
	TenantID  string
	From      time.Time
	To        time.Time
	Decision  string
	ToolName  string
	ErrorType string
	Limit     int
	Cursor    string
}

type MetricQuery struct {
	TenantID   string
	AgentAppID string
	From       time.Time
	To         time.Time
	Limit      int
	Cursor     string
}

type Confirmation struct {
	TenantID        string    `json:"tenant_id"`
	ActorUserIDHash string    `json:"actor_user_id_hash"`
	SessionID       string    `json:"session_id"`
	ToolName        string    `json:"tool_name"`
	ArgsDigest      string    `json:"args_digest"`
	Nonce           string    `json:"nonce"`
	State           string    `json:"state"`
	ExpiresAt       time.Time `json:"expires_at"`
}

func (c Confirmation) Validate() error {
	if err := tenant.ValidateID("confirmation tenant id", c.TenantID); err != nil {
		return err
	}
	if c.ActorUserIDHash == "" || c.SessionID == "" || !toolNamePattern.MatchString(c.ToolName) || c.ArgsDigest == "" || c.Nonce == "" || c.ExpiresAt.IsZero() {
		return errors.New("tool confirmation is invalid")
	}
	if c.State != "requested" && c.State != "approved" {
		return errors.New("tool confirmation state is invalid")
	}
	return nil
}

func normalizedNames(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
