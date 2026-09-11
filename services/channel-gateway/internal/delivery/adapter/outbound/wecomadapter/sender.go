// Package wecomadapter translates Delivery's immutable plan to a reserved local
// Connection capability. It has no SDK, database, or credential dependency.
package wecomadapter

import (
	"context"
	"sync"
	"sync/atomic"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type Session interface {
	SendFinal(context.Context, string, string) d.Result
	Release()
}
type SessionSource interface {
	ReserveOriginal(context.Context, d.Target) (Session, error)
}
type Provider struct{ source SessionSource }

func NewProvider(source SessionSource) (*Provider, error) {
	if source == nil {
		return nil, d.ErrUnavailable
	}
	return &Provider{source: source}, nil
}
func (p *Provider) Reserve(ctx context.Context, r app.SendRequest) (app.ReservedSender, error) {
	digest, err := d.RequestDigest(r.Claim)
	if err != nil {
		return nil, err
	}
	if ctx == nil || r.Claim.Target.Provider != "wecom" || r.RequestDigest != digest || r.RequestID != r.Claim.Target.CallbackRequestID {
		return nil, d.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	session, err := p.source.ReserveOriginal(ctx, r.Claim.Target)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, d.ErrUnavailable
	}
	if r.Claim.Target.Origin != nil {
		origin := *r.Claim.Target.Origin
		r.Claim.Target.Origin = &origin
	}
	if r.Claim.Owner != nil {
		owner := *r.Claim.Owner
		r.Claim.Owner = &owner
	}
	return &reservation{session: session, request: r}, nil
}

type reservation struct {
	session        Session
	request        app.SendRequest
	used, released atomic.Bool
	release        sync.Once
}

func (r *reservation) Release() { r.release.Do(func() { r.released.Store(true); r.session.Release() }) }
func (r *reservation) SendFinal(ctx context.Context, a d.Attempt) d.Result {
	no := d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorPermanent}
	if r.released.Load() || !r.used.CompareAndSwap(false, true) || ctx == nil {
		return no
	}
	if ctx.Err() != nil {
		no.ErrorClass = d.ErrorDeadline
		return no
	}
	c := r.request.Claim
	if a.ID == "" || a.Number < 1 || a.EvidenceToken == "" || a.ClaimToken != c.Token || a.RequestID != r.request.RequestID || a.RequestDigest != r.request.RequestDigest || a.PartID != c.Part.ID || a.IntentID != c.Intent.ID {
		return no
	}
	digest, err := d.RequestDigest(d.Claim{Part: d.Part{ID: a.PartID, IntentID: a.IntentID, Index: c.Part.Index, Text: a.Text}, Intent: a.Intent, Target: a.Target, InstanceID: a.InstanceID, Owner: a.Owner})
	if err != nil || digest != a.RequestDigest {
		return no
	}
	result := r.session.SendFinal(ctx, a.PartID, a.Text)
	if result.Validate() != nil {
		return d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}
	}
	return result
}
