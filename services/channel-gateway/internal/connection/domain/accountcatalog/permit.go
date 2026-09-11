package accountcatalog

import (
	"context"
	"time"
)

// UseBinding is process-internal evidence, never an HTTP request/response DTO.
// Only the trusted directory lifecycle issues permits after its own refresh.
type UseBinding struct {
	ScopeID, SourceEpoch, InstanceID, InstanceEpoch, TenantID, Provider, AccountID, Kind string
	ConnectionRevision, QualificationGeneration, ClientGeneration                        int64
}
type Permit struct {
	binding UseBinding
	ctx     context.Context
	cancel  context.CancelFunc
	until   time.Time
}

func NewPermit(parent context.Context, b UseBinding, until time.Time) (*Permit, error) {
	if parent == nil || parent.Err() != nil || !until.After(time.Now()) || !ValidID(b.ScopeID) || !ValidEpoch(b.SourceEpoch) || !ValidID(b.InstanceID) || !ValidEpoch(b.InstanceEpoch) || !ValidID(b.TenantID) || !ValidID(b.AccountID) || !ValidRevision(b.ConnectionRevision) || b.QualificationGeneration < 1 || b.ClientGeneration < 1 {
		return nil, ErrUnauthorized
	}
	if !ValidUseKind(b.Provider, b.Kind) {
		return nil, ErrUnauthorized
	}
	ctx, cancel := context.WithDeadline(parent, until)
	return &Permit{b, ctx, cancel, until}, nil
}
func ValidUseKind(provider, kind string) bool {
	if provider == "wecom" {
		return kind == "wecom_connection" || kind == "wecom_ingress" || kind == "wecom_delivery"
	}
	return provider == "telegram" && (kind == "telegram_receiver" || kind == "telegram_webhook" || kind == "telegram_delivery" || kind == "telegram_registration")
}
func (p *Permit) Check() error {
	if p == nil || p.ctx == nil || p.ctx.Err() != nil || !time.Now().Before(p.until) {
		return ErrUnauthorized
	}
	return nil
}
func (p *Permit) Binding() UseBinding {
	if p == nil {
		return UseBinding{}
	}
	return p.binding
}
func (p *Permit) Context() context.Context {
	if p == nil {
		return nil
	}
	return p.ctx
}
func (p *Permit) Revoke() {
	if p != nil {
		p.cancel()
	}
}
