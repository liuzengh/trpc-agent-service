// Package accountuse issues process-local, exact-version use contexts and
// resolves credentials through consumer-owned ports. It owns no HTTP or SQL.
package accountuse

import (
	"context"
	"time"

	refresh "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/catalogrefresh"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Directory interface {
	Lookup(context.Context, string) (refresh.View, error)
}
type Issuer interface {
	Issue(context.Context, c.Poll, c.Qualification, c.Account, string, int64) (*c.Permit, error)
}
type Value struct {
	Purpose, ID, Value string
	Version            int64
}
type Credentials interface {
	ResolveValues(context.Context, c.Account, c.ResolveRequest) ([]Value, error)
}
type Owner interface {
	Check(context.Context, d.OwnerGrant) error
}
type Service struct {
	Directory   Directory
	Issuer      Issuer
	Credentials Credentials
	Owner       Owner
}

func (s *Service) Open(ctx context.Context, id, kind string, revision, generation int64) (*c.Permit, c.Account, error) {
	if ctx == nil || s.Directory == nil || s.Issuer == nil {
		return nil, c.Account{}, c.ErrInvalid
	}
	v, e := s.Directory.Lookup(ctx, id)
	if e != nil {
		return nil, c.Account{}, e
	}
	if revision != v.Account.ConnectionRevision {
		return nil, c.Account{}, c.ErrVersion
	}
	p, e := s.Issuer.Issue(v.Context, v.Poll, v.Qualification, v.Account, kind, generation)
	return p, v.Account, e
}

// Resolve checks the exact version and live owner before AND after remote I/O.
// Registration's local fence is checked by its operation owner around this call.
func (s *Service) Resolve(ctx context.Context, p *c.Permit, a c.Account, owner *d.OwnerGrant, registrationEpoch *int64) ([]Value, error) {
	if ctx == nil || p.Check() != nil || s.Credentials == nil {
		return nil, c.ErrUnauthorized
	}
	b := p.Binding()
	if b.AccountID != a.ID || b.TenantID != a.TenantID || b.Provider != a.Provider || b.ConnectionRevision != a.ConnectionRevision {
		return nil, c.ErrUnauthorized
	}
	r := c.ResolveRequest{SchemaVersion: 1, ScopeID: b.ScopeID, SourceEpoch: b.SourceEpoch, ConnectionRevision: b.ConnectionRevision, Consumer: c.Consumer{Kind: b.Kind, InstanceID: b.InstanceID, RegistrationEpoch: registrationEpoch}}
	purposes := map[string][]string{"telegram_receiver": {"telegram.bot_token"}, "wecom_connection": {"wecom.bot_secret"}, "telegram_webhook": {"telegram.webhook_secret"}, "telegram_delivery": {"telegram.bot_token"}, "telegram_registration": {"telegram.bot_token", "telegram.webhook_secret"}}[b.Kind]
	if owner != nil {
		e := owner.Epoch
		r.Consumer.OwnerEpoch = &e
	}
	for _, purpose := range purposes {
		for _, v := range a.Credentials {
			if v.Purpose == purpose {
				r.Uses = append(r.Uses, c.Use{Purpose: v.Purpose, ID: v.ID, Version: v.Version})
			}
		}
	}
	if r.Validate(a) != nil {
		return nil, c.ErrUnauthorized
	}
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stop := context.AfterFunc(p.Context(), cancel)
	defer stop()
	check := func() error {
		if op.Err() != nil || p.Check() != nil {
			return c.ErrExpired
		}
		v, e := s.Directory.Lookup(op, a.ID)
		if e != nil || v.Account.ConnectionRevision != a.ConnectionRevision || v.Context.Err() != nil || v.Qualification.Generation != b.QualificationGeneration {
			return c.ErrUnauthorized
		}
		if b.Kind == "wecom_connection" {
			if owner == nil || owner.AccountID != a.ID || owner.InstanceID != b.InstanceID || owner.Revision != a.ConnectionRevision || s.Owner == nil {
				return c.ErrUnauthorized
			}
			ownerCtx, ownerCancel := context.WithTimeout(op, 2*time.Second)
			e = s.Owner.Check(ownerCtx, *owner)
			ownerCancel()
			if e != nil {
				return c.ErrUnauthorized
			}
		}
		return nil
	}
	if e := check(); e != nil {
		return nil, e
	}
	values, e := s.Credentials.ResolveValues(op, a, r)
	if e != nil {
		return nil, e
	}
	if e = check(); e != nil {
		return nil, e
	}
	return values, nil
}
