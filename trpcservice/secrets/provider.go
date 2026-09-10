// Package secrets defines least-privilege secret resolution.
package secrets

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type SecretRef struct {
	Ref     string `json:"ref"`
	Version int64  `json:"version"`
}
type SecretValue struct {
	Bytes   []byte
	Version int64
}

type Purpose string

const (
	PurposeChannelVerify  Purpose = "channel_verify"
	PurposeChannelSend    Purpose = "channel_send"
	PurposeTenantIdentity Purpose = "tenant_identity"
	PurposeTenantSession  Purpose = "tenant_session"
	PurposeModelCall      Purpose = "model_call"
	PurposeToolCall       Purpose = "tool_call"
	PurposeBackendConnect Purpose = "backend_connect"
	PurposePayloadEncrypt Purpose = "payload_encrypt"
	PurposeGatewayAuth    Purpose = "gateway_auth"
	PurposeAdminAuth      Purpose = "admin_auth"
	PurposeAuditQueryAuth Purpose = "audit_query_auth"
)

type Scope struct {
	TenantID        string
	Subject         string
	Purpose         Purpose
	ResourceID      string
	ResourceVersion int64
}

type Provider interface {
	Resolve(context.Context, Scope, SecretRef) (SecretValue, error)
}
type SecretProvider = Provider

func ValidateRequest(scope Scope, ref SecretRef) error {
	if !validText(scope.TenantID) || !validText(scope.Subject) || !validText(scope.ResourceID) ||
		scope.ResourceVersion < 1 || !validText(ref.Ref) || ref.Version < 1 || !validPurpose(scope.Purpose) {
		return runtime.ErrTenantScope
	}
	return nil
}

func validPurpose(value Purpose) bool {
	switch value {
	case PurposeChannelVerify, PurposeChannelSend, PurposeTenantIdentity, PurposeTenantSession,
		PurposeModelCall, PurposeToolCall, PurposeBackendConnect, PurposePayloadEncrypt, PurposeGatewayAuth,
		PurposeAdminAuth, PurposeAuditQueryAuth:
		return true
	default:
		return false
	}
}

func validText(value string) bool {
	return strings.TrimSpace(value) == value && value != "" && !strings.ContainsRune(value, 0)
}
