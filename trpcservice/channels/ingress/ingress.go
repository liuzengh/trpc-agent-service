// Package ingress implements the pre-tenant, single-use verification boundary
// shared by every Channel provider.
package ingress

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

const PurposeChannelVerify = "channel_verify"

type CandidateState string

const (
	CandidateIssued           CandidateState = "issued"
	CandidateVerifierAcquired CandidateState = "verifier_acquired"
	CandidateVerified         CandidateState = "verified"
	CandidatePromoted         CandidateState = "promoted"
	CandidateBurned           CandidateState = "burned"
)

// BindingRoute is trusted control-plane input. Public resolution returns only
// its opaque route and never exposes the tenant-owned fields below.
type BindingRoute struct {
	OpaqueBindingID, Channel, RouteKeyDigest string
	TenantID, AgentAppID, ChannelBindingID   string
	ExternalAccountID                        string
	TenantVersion, BindingVersion            int64
	SecretRef                                secrets.SecretRef
	IdentitySecretRef, SessionSecretRef      secrets.SecretRef
	Enabled                                  bool
}

type CandidateRecord struct {
	TokenDigest, ReceiptDigest, ProtocolIdentityDigest string
	OpaqueBindingID, Channel, RouteKeyDigest, Purpose  string
	BindingVersion, Version                            int64
	State                                              CandidateState
	IssuedAt, ExpiresAt, VerifiedAt                    time.Time
}

type Store interface {
	PutBindingRoute(context.Context, BindingRoute) error
	ResolveBindingRoute(context.Context, string, string) (BindingRoute, error)
	IssueCandidate(context.Context, CandidateRecord) error
	AcquireCandidate(context.Context, string, int64, time.Time) (CandidateRecord, BindingRoute, error)
	MarkCandidateVerified(context.Context, string, int64, string, string, time.Time) (CandidateRecord, error)
	PromoteCandidate(context.Context, string, int64, string, string, time.Time, time.Time) (CandidateRecord, BindingRoute, error)
	BurnCandidate(context.Context, string, int64) error
	BurnExpiredCandidates(context.Context, time.Time, int) (int, error)
}

type Resolver struct {
	Store       Store
	Secrets     secrets.Provider
	TTL         time.Duration
	Now         func() time.Time
	RandomToken func() (string, error)
}

func (r Resolver) ResolveCandidate(ctx context.Context, hint channel.PublicRouteHint) (channel.CandidateBindingContext, error) {
	if r.Store == nil || hint.Channel == "" || hint.RouteKeyDigest == "" || hint.IngressAttemptID == "" {
		return channel.CandidateBindingContext{}, runtime.ErrInvariantViolation
	}
	route, err := r.Store.ResolveBindingRoute(ctx, hint.Channel, hint.RouteKeyDigest)
	if err != nil {
		return channel.CandidateBindingContext{}, err
	}
	if !route.Enabled {
		return channel.CandidateBindingContext{}, runtime.ErrVersionMismatch
	}
	token, err := r.token()
	if err != nil {
		return channel.CandidateBindingContext{}, err
	}
	now := r.now()
	if token == "" || now.IsZero() {
		return channel.CandidateBindingContext{}, runtime.ErrInvariantViolation
	}
	ttl := r.TTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	candidate := channel.CandidateBindingContext{
		Channel: route.Channel, RouteKeyDigest: route.RouteKeyDigest, CandidateToken: token,
		BindingVersion: route.BindingVersion, Purpose: PurposeChannelVerify, IssuedAt: now, ExpiresAt: now.Add(ttl),
	}
	record := CandidateRecord{
		TokenDigest: digest(token), OpaqueBindingID: route.OpaqueBindingID, Channel: route.Channel,
		RouteKeyDigest: route.RouteKeyDigest, Purpose: candidate.Purpose, BindingVersion: route.BindingVersion,
		State: CandidateIssued, IssuedAt: candidate.IssuedAt, ExpiresAt: candidate.ExpiresAt,
	}
	if err := r.Store.IssueCandidate(ctx, record); err != nil {
		return channel.CandidateBindingContext{}, err
	}
	return candidate, nil
}

func (r Resolver) AcquireVerifier(ctx context.Context, candidate channel.CandidateBindingContext) (channel.ScopedVerifierHandle, error) {
	if r.Store == nil || r.Secrets == nil || !validCandidate(candidate) {
		return nil, runtime.ErrInvariantViolation
	}
	now := r.now()
	record, route, err := r.Store.AcquireCandidate(ctx, digest(candidate.CandidateToken), candidate.BindingVersion, now)
	if err != nil {
		return nil, err
	}
	if record.Channel != candidate.Channel || record.RouteKeyDigest != candidate.RouteKeyDigest || record.Purpose != candidate.Purpose ||
		record.BindingVersion != candidate.BindingVersion || !sameInstant(record.IssuedAt, candidate.IssuedAt) || !sameInstant(record.ExpiresAt, candidate.ExpiresAt) {
		_ = r.Store.BurnCandidate(context.Background(), record.TokenDigest, record.Version)
		return nil, runtime.ErrVersionMismatch
	}
	secret, err := r.Secrets.Resolve(ctx, secrets.Scope{
		TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion,
	}, route.SecretRef)
	if err != nil {
		_ = r.Store.BurnCandidate(context.Background(), record.TokenDigest, record.Version)
		return nil, err
	}
	if secret.Version != route.SecretRef.Version || len(secret.Bytes) == 0 {
		_ = r.Store.BurnCandidate(context.Background(), record.TokenDigest, record.Version)
		return nil, runtime.ErrVersionMismatch
	}
	return &verifierHandle{store: r.Store, candidate: candidate, record: record, secret: append([]byte(nil), secret.Bytes...), now: r.now, token: r.token}, nil
}

func (r Resolver) PromoteVerified(ctx context.Context, candidate channel.CandidateBindingContext, receipt channel.VerificationReceipt) (channel.VerifiedBinding, error) {
	if r.Store == nil || !validCandidate(candidate) || receipt.CandidateToken != candidate.CandidateToken ||
		receipt.Purpose != candidate.Purpose || receipt.ProtocolIdentityDigest == "" || receipt.ReceiptToken == "" || receipt.VerifiedAt.IsZero() {
		return channel.VerifiedBinding{}, runtime.ErrVersionMismatch
	}
	_, route, err := r.Store.PromoteCandidate(ctx, digest(candidate.CandidateToken), candidate.BindingVersion, digest(receipt.ReceiptToken),
		receipt.ProtocolIdentityDigest, receipt.VerifiedAt, r.now())
	if err != nil {
		return channel.VerifiedBinding{}, err
	}
	if !route.Enabled || route.BindingVersion != candidate.BindingVersion {
		return channel.VerifiedBinding{}, runtime.ErrVersionMismatch
	}
	return channel.VerifiedBinding{
		TenantID: route.TenantID, AgentAppID: route.AgentAppID, ChannelBindingID: route.ChannelBindingID,
		Channel: route.Channel, ExternalAccountID: route.ExternalAccountID,
		TenantVersion: route.TenantVersion, BindingVersion: route.BindingVersion,
		IdentitySecretRef: route.IdentitySecretRef, SessionSecretRef: route.SessionSecretRef,
	}, nil
}

func (r Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r Resolver) token() (string, error) {
	if r.RandomToken != nil {
		return r.RandomToken()
	}
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

type verifierHandle struct {
	mu        sync.Mutex
	store     Store
	candidate channel.CandidateBindingContext
	record    CandidateRecord
	secret    []byte
	now       func() time.Time
	token     func() (string, error)
	closed    bool
}

func (h *verifierHandle) Verify(ctx context.Context, request channel.CallbackRequest, verify channel.ProtocolVerifier) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || verify == nil || len(h.secret) == 0 {
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, runtime.ErrVersionConflict
	}
	material := append([]byte(nil), h.secret...)
	payload, err := verify(ctx, request, material)
	zero(material)
	zero(h.secret)
	h.secret = nil
	h.closed = true
	if err != nil || payload.ProtocolIdentityDigest == "" {
		_ = h.store.BurnCandidate(context.Background(), h.record.TokenDigest, h.record.Version)
		if err == nil {
			err = runtime.ErrVersionMismatch
		}
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	receiptToken, err := h.token()
	if err != nil {
		_ = h.store.BurnCandidate(context.Background(), h.record.TokenDigest, h.record.Version)
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	verifiedAt := h.now().UTC()
	record, err := h.store.MarkCandidateVerified(ctx, h.record.TokenDigest, h.record.Version, digest(receiptToken), payload.ProtocolIdentityDigest, verifiedAt)
	if err != nil {
		_ = h.store.BurnCandidate(context.Background(), h.record.TokenDigest, h.record.Version)
		return channel.VerifiedCallback{}, channel.VerificationReceipt{}, err
	}
	h.record = record
	callback := channel.VerifiedCallback{
		Body: append([]byte(nil), payload.Body...), Headers: cloneMap(payload.Headers), ReceivedAt: request.ReceivedAt,
		ProtocolIdentityDigest: payload.ProtocolIdentityDigest,
	}
	receipt := channel.VerificationReceipt{
		CandidateToken: h.candidate.CandidateToken, ReceiptToken: receiptToken, Purpose: h.candidate.Purpose,
		ProtocolIdentityDigest: payload.ProtocolIdentityDigest, VerifiedAt: verifiedAt,
	}
	return callback, receipt, nil
}

func (h *verifierHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	zero(h.secret)
	h.secret = nil
	return h.store.BurnCandidate(context.Background(), h.record.TokenDigest, h.record.Version)
}

func validCandidate(candidate channel.CandidateBindingContext) bool {
	return candidate.Channel != "" && candidate.RouteKeyDigest != "" && candidate.CandidateToken != "" &&
		candidate.BindingVersion > 0 && candidate.Purpose == PurposeChannelVerify && !candidate.IssuedAt.IsZero() && candidate.ExpiresAt.After(candidate.IssuedAt)
}

func sameInstant(left, right time.Time) bool {
	return left.UTC().UnixMicro() == right.UTC().UnixMicro()
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

var _ channel.IngressBindingResolver = Resolver{}
