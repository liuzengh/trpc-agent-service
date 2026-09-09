// Package feishu implements the Feishu (Lark) channel adapter and sender:
// event-subscription callbacks (URL verification, Verification Token and
// Encrypt Key checks) inbound, and tenant_access_token based replies through
// the durable Outbox. The adapter never calls the model directly; verified
// messages enter the shared gateway.InboundMessage -> Inbox -> Runner chain.
package feishu

import (
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrUnsupportedMessage marks authenticated Feishu events that are not
	// valid Agent inputs and must be acknowledged without retry.
	ErrUnsupportedMessage = errors.New("feishu: unsupported message")
	// ErrBindingMismatch marks a callback whose app_id does not match the
	// server-owned binding, which indicates cross-tenant or replay forgery.
	ErrBindingMismatch = errors.New("feishu: callback binding mismatch")
	// ErrVerification marks a Verification Token mismatch.
	ErrVerification = errors.New("feishu: verification token mismatch")
)

// Binding is the immutable server-owned scope for one Feishu application,
// resolved from the control plane at callback time. FeishuAppID is the
// application id (cli_...) stored as provider_account_id; VerificationToken
// and EncryptKey are resolved from SecretRefs and must never be logged.
type Binding struct {
	TenantID, AppID, BindingID string
	FeishuAppID                string
	VerificationToken          string
	EncryptKey                 string
	AppSecret                  string
	ConfigVersion              tenant.ConfigVersion
}
