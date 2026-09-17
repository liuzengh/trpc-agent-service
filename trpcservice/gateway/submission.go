package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// RunSubmission contains only values that have already been authorized and
// canonicalized at the protocol boundary. Tenant, user and session identities
// must never be copied directly from an untrusted request body.
type RunSubmission struct {
	Tenant         tenant.Context
	UserID         string
	SessionID      string
	IdempotencyKey string
	Protocol       string
	Text           string
	TraceParent    string
}

type normalizedRunInput struct {
	SchemaVersion int    `json:"schema_version"`
	Text          string `json:"text"`
}

// RunSubmitter persists the stable Inbox identity and normalized payload
// before asking the shared dispatcher to prepare execution.
type RunSubmitter struct {
	Inbox             messaging.InboxClaimer
	Payloads          messaging.PayloadStore
	Dispatcher        Dispatcher
	PayloadKeyVersion int64
	Telemetry         telemetry.Provider
}

func (s RunSubmitter) Submit(ctx context.Context, in RunSubmission) (result ExecutionHandle, resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, s.Telemetry, in.TraceParent, telemetry.OperationGatewaySubmit,
		telemetry.ComponentAttribute(telemetry.ComponentGateway))
	defer func() { finish(resultErr) }()
	if s.Inbox == nil || s.Payloads == nil || s.Dispatcher == nil || s.PayloadKeyVersion < 1 {
		return ExecutionHandle{}, runtime.ErrCapabilityUnsupported
	}
	if err := in.Tenant.Validate(); err != nil {
		return ExecutionHandle{}, err
	}
	if strings.TrimSpace(in.UserID) == "" || strings.TrimSpace(in.SessionID) == "" ||
		strings.TrimSpace(in.IdempotencyKey) == "" || strings.TrimSpace(in.Protocol) == "" ||
		strings.TrimSpace(in.Text) == "" {
		return ExecutionHandle{}, runtime.ErrInvalidEnvelope
	}
	payload, err := json.Marshal(normalizedRunInput{SchemaVersion: 1, Text: in.Text})
	if err != nil {
		return ExecutionHandle{}, err
	}
	digest := sha256.Sum256(payload)
	digestHex := hex.EncodeToString(digest[:])
	keyVersion := s.PayloadKeyVersion
	key := messaging.InboxKey{TenantID: in.Tenant.TenantID, Channel: in.Tenant.Channel,
		ExternalAccountID: in.Tenant.AgentAppID, ExternalMessageID: in.IdempotencyKey}
	claimed, err := s.Inbox.ClaimInbox(ctx, messaging.ClaimInboxRequest{InboxKey: key,
		AgentAppID: in.Tenant.AgentAppID, SessionID: in.SessionID, ExternalUserID: in.UserID,
		PayloadDigest: digestHex, KeyVersion: keyVersion, InitialState: messaging.InboxDispatchPending})
	if err != nil {
		return ExecutionHandle{}, err
	}
	if err := s.Payloads.PutPayload(ctx, messaging.PayloadRecord{TenantID: in.Tenant.TenantID,
		RequestID: claimed.RequestID, PayloadRef: claimed.PayloadRef, ContentDigest: digestHex,
		Content: payload, KeyVersion: keyVersion}); err != nil {
		return ExecutionHandle{}, err
	}
	return s.Dispatcher.Dispatch(ctx, DispatchRequest{Tenant: in.Tenant, RequestID: claimed.RequestID,
		SessionID: in.SessionID, UserID: in.UserID, PayloadRef: claimed.PayloadRef,
		TraceParent: telemetry.EffectiveTraceParent(ctx, in.TraceParent)})
}
