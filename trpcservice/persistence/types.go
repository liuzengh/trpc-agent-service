// Package persistence defines the backend-neutral SQL turn persistence
// contract shared by Runtime, Worker, Redis coordination, and SQL committers.
package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	EnvelopeSchemaVersion    = 1
	FingerprintSchemaVersion = 1
)

var (
	ErrInvalidEnvelope         = errors.New("invalid persistence envelope")
	ErrFingerprintConflict     = errors.New("backend fingerprint conflict")
	ErrBackendUnavailable      = errors.New("SQL backend unavailable")
	ErrSchemaIncompatible      = errors.New("SQL schema incompatible")
	ErrCommitDigestConflict    = errors.New("SQL commit digest conflict")
	ErrSessionSequenceConflict = errors.New("SQL session sequence conflict")
	ErrPostgresSummaryDisabled = errors.New("PostgreSQL summary is disabled")
)

type BackendFingerprint struct {
	SchemaVersion    int                `json:"schema_version"`
	Kind             tenant.StorageKind `json:"kind"`
	StorageProfileID string             `json:"storage_profile_id"`
	DatabaseIdentity string             `json:"database_identity,omitempty"`
	Namespace        string             `json:"namespace"`
}

func (f BackendFingerprint) Validate() error {
	if f.SchemaVersion != FingerprintSchemaVersion || f.Kind == "" || f.StorageProfileID == "" || f.Namespace == "" {
		return errors.New("invalid backend fingerprint")
	}
	if (f.Kind == tenant.StorageKindPostgres || f.Kind == tenant.StorageKindMySQL) && f.DatabaseIdentity == "" {
		return errors.New("invalid SQL backend fingerprint")
	}
	return nil
}

func (f BackendFingerprint) Digest() (string, error) {
	if err := f.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type Route struct {
	TenantID    string             `json:"tenant_id"`
	AgentAppID  string             `json:"agent_app_id"`
	Fingerprint BackendFingerprint `json:"backend_fingerprint"`
}

func (r Route) Validate() error {
	if r.TenantID == "" || r.AgentAppID == "" {
		return errors.New("invalid persistence route")
	}
	return r.Fingerprint.Validate()
}

func (r Route) IsSQL() bool {
	return r.Fingerprint.Kind == tenant.StorageKindPostgres || r.Fingerprint.Kind == tenant.StorageKindMySQL
}

type Envelope struct {
	SchemaVersion      int                     `json:"schema_version"`
	TaskID             string                  `json:"task_id"`
	PayloadDigest      string                  `json:"payload_digest"`
	TenantID           string                  `json:"tenant_id"`
	AgentAppID         string                  `json:"agent_app_id"`
	StorageProfileID   string                  `json:"storage_profile_id"`
	BackendKind        tenant.StorageKind      `json:"backend_kind"`
	BackendFingerprint BackendFingerprint      `json:"backend_fingerprint"`
	SessionCoord       string                  `json:"session_coord"`
	SessionSeq         int64                   `json:"session_seq"`
	TurnCommit         sessionfence.TurnCommit `json:"turn_commit"`
	Reply              message.OutboundMessage `json:"reply"`
	PreparedAt         time.Time               `json:"prepared_at"`
	EnvelopeDigest     string                  `json:"envelope_digest"`
	PersistAttempt     int                     `json:"persist_attempt"`
	TraceParent        string                  `json:"trace_parent,omitempty"`
	DigestVersion      int                     `json:"digest_version,omitempty"`
}

func NewEnvelope(task message.ExecutionTask, route Route, commit sessionfence.TurnCommit, reply message.OutboundMessage, preparedAt time.Time) (Envelope, error) {
	e := Envelope{
		SchemaVersion: EnvelopeSchemaVersion, TaskID: task.TaskID, PayloadDigest: task.PayloadDigest,
		TenantID: task.TenantID, AgentAppID: task.AgentAppID,
		StorageProfileID: route.Fingerprint.StorageProfileID, BackendKind: route.Fingerprint.Kind,
		BackendFingerprint: route.Fingerprint, SessionCoord: commit.SessionCoord, SessionSeq: commit.SessionSeq,
		TurnCommit: commit, Reply: reply, PreparedAt: preparedAt.UTC(), PersistAttempt: 1,
		TraceParent: task.TraceParent, DigestVersion: task.DigestVersion,
	}
	digest, err := e.CalculateDigest()
	if err != nil {
		return Envelope{}, err
	}
	e.EnvelopeDigest = digest
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

func (e Envelope) CalculateDigest() (string, error) {
	copyEnvelope := e
	copyEnvelope.EnvelopeDigest = ""
	// Retry bookkeeping changes independently from the immutable staged turn.
	// SQL receipts therefore bind the payload digest, not the attempt counter.
	copyEnvelope.PersistAttempt = 0
	raw, err := json.Marshal(copyEnvelope)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (e Envelope) Validate() error {
	if e.SchemaVersion != EnvelopeSchemaVersion || e.TaskID == "" || e.PayloadDigest == "" || e.TenantID == "" || e.AgentAppID == "" ||
		e.StorageProfileID == "" || !e.BackendKind.IsSQL() || e.SessionCoord == "" || e.SessionSeq < 1 || e.PersistAttempt < 1 || e.PreparedAt.IsZero() {
		return ErrInvalidEnvelope
	}
	if err := e.BackendFingerprint.Validate(); err != nil || e.BackendFingerprint.Kind != e.BackendKind || e.BackendFingerprint.StorageProfileID != e.StorageProfileID {
		return ErrInvalidEnvelope
	}
	if e.TurnCommit.SessionCoord != e.SessionCoord || e.TurnCommit.SessionSeq != e.SessionSeq || e.TurnCommit.AppName == "" || e.TurnCommit.UserID == "" || e.TurnCommit.SessionID == "" {
		return ErrInvalidEnvelope
	}
	if e.TurnCommit.TraceParent != e.TraceParent || e.TurnCommit.DigestVersion != e.DigestVersion {
		return ErrInvalidEnvelope
	}
	digest, err := e.CalculateDigest()
	if err != nil || !message.ConstantTimeDigestEqual(digest, e.EnvelopeDigest) {
		return ErrInvalidEnvelope
	}
	return nil
}

type Committer interface {
	Ready(context.Context) error
	Commit(context.Context, Envelope) error
}
