package application

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type AcceptOptions struct {
	Timeout       time.Duration
	MaxConcurrent int
}
type Acceptor struct {
	ledger         Ledger
	admissions     AdmissionReplyReader
	verifier       CommittedFinalVerifier
	timeout        time.Duration
	slots, lookups chan struct{}
	stopping       atomic.Bool
}

func NewAcceptor(ledger Ledger, admissions AdmissionReplyReader, verifier CommittedFinalVerifier, options AcceptOptions) (*Acceptor, error) {
	if ledger == nil || admissions == nil || verifier == nil {
		return nil, domain.ErrUnavailable
	}
	if options.Timeout == 0 {
		options.Timeout = 5 * time.Second
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = 64
	}
	if options.Timeout < time.Millisecond || options.Timeout > time.Minute || options.MaxConcurrent < 1 || options.MaxConcurrent > 1024 {
		return nil, domain.ErrInvalid
	}
	return &Acceptor{ledger: ledger, admissions: admissions, verifier: verifier, timeout: options.Timeout, slots: make(chan struct{}, options.MaxConcurrent), lookups: make(chan struct{}, options.MaxConcurrent)}, nil
}
func (s *Acceptor) Stop() { s.stopping.Store(true) }
func (s *Acceptor) find(ctx context.Context, i domain.Intent, digest string) (domain.Receipt, bool, error) {
	select {
	case s.lookups <- struct{}{}:
		defer func() { <-s.lookups }()
	default:
		return domain.Receipt{}, false, domain.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return domain.Receipt{}, false, err
	}
	receipt, old, found, err := s.ledger.Find(ctx, i.ID)
	if err != nil {
		return domain.Receipt{}, false, err
	}
	if found && old != digest {
		return domain.Receipt{}, false, domain.ErrConflict
	}
	return receipt, found, nil
}
func (s *Acceptor) finalReceipt(ctx context.Context, i domain.Intent, digest string, original error) (domain.Receipt, error) {
	receipt, found, err := s.find(ctx, i, digest)
	if err != nil {
		return domain.Receipt{}, err
	}
	if found {
		return receipt, nil
	}
	return domain.Receipt{}, original
}

// AcceptReplyIntent only commits a verified immutable Final. A durable duplicate
// is independent of today's routing/account state and the verifier's availability.
func (s *Acceptor) AcceptReplyIntent(parent context.Context, i domain.Intent) (domain.Receipt, error) {
	if parent == nil {
		return domain.Receipt{}, domain.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, s.timeout)
	defer cancel()
	digest, err := domain.IntentDigest(i)
	if err != nil {
		return domain.Receipt{}, err
	}
	receipt, found, err := s.find(ctx, i, digest)
	if err != nil || found {
		return receipt, err
	}
	if s.stopping.Load() {
		return s.finalReceipt(ctx, i, digest, domain.ErrUnavailable)
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return s.finalReceipt(ctx, i, digest, domain.ErrUnavailable)
	}
	target, err := s.admissions.ReadReplyTarget(ctx, i.AdmissionID, i.RunID)
	if err != nil {
		return s.finalReceipt(ctx, i, digest, err)
	}
	if err = target.Validate(); err != nil {
		return s.finalReceipt(ctx, i, digest, err)
	}
	proof, err := s.verifier.VerifyCommittedFinal(ctx, i, digest)
	if err != nil {
		return s.finalReceipt(ctx, i, digest, err)
	}
	expected := FinalAuthorization{IntentID: i.ID, Digest: digest, AdmissionID: i.AdmissionID, RunID: i.RunID, AttemptID: i.AttemptID, CompletionID: i.CompletionID, ExecutionGeneration: i.ExecutionGeneration, Sequence: i.Sequence, TenantID: target.TenantID, ManifestDigest: target.ManifestDigest}
	if proof != expected {
		return s.finalReceipt(ctx, i, digest, domain.ErrUnauthorized)
	}
	parts, err := domain.Plan(target, i)
	if err != nil {
		return s.finalReceipt(ctx, i, digest, err)
	}
	if s.stopping.Load() {
		return s.finalReceipt(ctx, i, digest, domain.ErrUnavailable)
	}
	if err = ctx.Err(); err != nil {
		return domain.Receipt{}, err
	}
	receipt, err = s.ledger.Accept(ctx, domain.Prepared{Intent: i, Digest: digest, Target: target, Parts: parts})
	if errors.Is(err, domain.ErrExpired) {
		return s.finalReceipt(ctx, i, digest, err)
	}
	return receipt, err
}
