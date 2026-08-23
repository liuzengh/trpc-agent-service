package storage

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/session"
)

func dedupKeyString(k DedupKey) string {
	return strings.Join([]string{k.TenantID, k.Channel, k.BindingID, k.ExternalMessageID}, "\x00")
}

func ClaimEpochResource(k DedupKey) string {
	return "claim:" + strconv.Itoa(len(k.Channel)) + ":" + k.Channel + ":" + strconv.Itoa(len(k.BindingID)) + ":" + k.BindingID
}

func ValidateDedupKey(k DedupKey) error {
	for name, value := range map[string]string{"tenant_id": k.TenantID, "channel": k.Channel, "binding_id": k.BindingID, "external_message_id": k.ExternalMessageID} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "|\\/\x00") {
			return fmt.Errorf("%w: invalid %s", ErrInvalidArgument, name)
		}
	}
	return nil
}

func validateDedupKey(k DedupKey) error { return ValidateDedupKey(k) }

const maxOutboxPayloadBytes = 1 << 20

func outboxKey(tenantID, id string) string { return tenantID + "\x00" + id }

func fakeURL(aTenant, id string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", fmt.Errorf("%w: presigned URL ttl must be positive", ErrInvalidArgument)
	}
	return "https://object.invalid/tenants/" + aTenant + "/artifacts/" + id, nil
}

func ensureTenantContext(tcTenant string, validate func() error) error {
	if err := validate(); err != nil {
		return fmt.Errorf("%w: tenant context: %v", ErrInvalidArgument, err)
	}
	if tcTenant == "" {
		return ErrTenantMismatch
	}
	return nil
}

func sameTenant(tcTenant, resourceTenant string) error {
	if tcTenant == "" || resourceTenant == "" || tcTenant != resourceTenant {
		return ErrTenantMismatch
	}
	return nil
}

func validateSessionID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("%w: session id is required", ErrInvalidArgument)
	}
	return nil
}

func validateEventForSession(s session.Session, e session.SessionEvent) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.TenantID != s.TenantID || e.SessionID != s.ID {
		return ErrTenantMismatch
	}
	if e.Seq != s.LastEventSeq+1 {
		return ErrConflict
	}
	return nil
}
