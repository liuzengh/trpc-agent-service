package modelclient

import (
	"context"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/generation"
)

type PreviousCredentialReader interface {
	PreviousModelCredential(context.Context, string, string, int64) (secrets.SecretRef, int64, error)
}

// CredentialInvalidator applies an already-authorized profile invalidation at
// one runtime node. The durable Outbox/control relay remains the delivery
// mechanism; this component never treats a broadcast as authority.
type CredentialInvalidator struct {
	Pool    *generation.Pool
	Bundles profile.TenantBundleRetirer
	Subject string
}

func (i CredentialInvalidator) InvalidateModel(tenantID, profileID string, profileVersion int64, ref secrets.SecretRef) error {
	if i.Pool == nil || i.Bundles == nil || tenantID == "" || profileID == "" || profileVersion < 1 || i.Subject == "" {
		return runtime.ErrInvariantViolation
	}
	scope := secrets.Scope{TenantID: tenantID, Subject: i.Subject, Purpose: secrets.PurposeModelCall, ResourceID: profileID, ResourceVersion: profileVersion}
	if err := i.Pool.Invalidate(scope, ref); err != nil {
		return err
	}
	i.Bundles.RetireTenant(tenantID)
	return nil
}

// ConsumeProfileInvalidation resolves the previous immutable Profile revision
// from authority, then retires that exact generation. The provider-profile URI
// is an opaque durable reference, never a SecretRef or transport credential.
func (i CredentialInvalidator) ConsumeProfileInvalidation(ctx context.Context, reader PreviousCredentialReader, tenantID, aggregateID string, version uint64, payloadRef string) error {
	if reader == nil || version < 1 {
		return runtime.ErrInvariantViolation
	}
	parsedTenant, kind, profileID, profileVersion, err := parseProfileInvalidation(payloadRef)
	if err != nil || parsedTenant != tenantID || kind != "model" || profileID != aggregateID || profileVersion != int64(version) {
		return runtime.ErrVersionMismatch
	}
	oldRef, oldProfileVersion, err := reader.PreviousModelCredential(ctx, tenantID, profileID, profileVersion)
	if err != nil {
		return err
	}
	return i.InvalidateModel(tenantID, profileID, oldProfileVersion, oldRef)
}

func parseProfileInvalidation(raw string) (tenantID, kind, profileID string, version int64, err error) {
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil || parsed.Scheme != "provider-profile" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", "", 0, runtime.ErrInvariantViolation
	}
	parts := strings.Split(strings.TrimPrefix(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", 0, runtime.ErrInvariantViolation
	}
	version, parseErr = strconv.ParseInt(parts[2], 10, 64)
	if parseErr != nil || version < 1 {
		return "", "", "", 0, runtime.ErrInvariantViolation
	}
	return parsed.Host, parts[0], parts[1], version, nil
}
