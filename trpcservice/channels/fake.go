package channels

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// DefaultFakeMaxClockSkew is the timestamp window used by the offline fake.
// Production adapters must apply their provider-specific replay policy.
const DefaultFakeMaxClockSkew = 5 * time.Minute

// DefaultFakeMaxHandles bounds outstanding verifier capabilities in the
// offline fake. Production adapters must apply their own capacity policy.
const DefaultFakeMaxHandles = 4096

// FakeResolverOptions configures the deterministic clock and timestamp window
// used by FakeCandidateResolver.
type FakeResolverOptions struct {
	Clock        func() time.Time
	MaxClockSkew time.Duration
	MaxHandles   int
}

type fakeVerifierState struct {
	binding   Binding
	secret    string
	purpose   VerificationPurpose
	expiresAt time.Time
}

// FakeCandidateResolver is an offline candidate-scoped resolver and verifier.
// It deliberately models the security boundary without implementing a real
// WeCom or Telegram protocol. Secrets remain private to this test double.
// It lives in the channels package so only a completed Verify call can mint
// the proof-bearing VerifiedBinding value.
type FakeCandidateResolver struct {
	repo         CandidateConsumer
	secrets      map[SecretScope]string
	clock        func() time.Time
	maxClockSkew time.Duration
	maxHandles   int

	mu         sync.Mutex
	handles    map[string]fakeVerifierState
	usedNonces map[string]time.Time
}

// NewFakeCandidateResolver creates an offline resolver backed by a candidate
// consumer. The input secret map is copied and never returned by this package.
func NewFakeCandidateResolver(repo CandidateConsumer, secrets map[SecretScope]string, options ...FakeResolverOptions) *FakeCandidateResolver {
	configuration := FakeResolverOptions{
		Clock:        func() time.Time { return time.Now().UTC() },
		MaxClockSkew: DefaultFakeMaxClockSkew, MaxHandles: DefaultFakeMaxHandles,
	}
	if len(options) > 0 {
		if options[0].Clock != nil {
			configuration.Clock = options[0].Clock
		}
		if options[0].MaxClockSkew > 0 {
			configuration.MaxClockSkew = options[0].MaxClockSkew
		}
		if options[0].MaxHandles > 0 {
			configuration.MaxHandles = options[0].MaxHandles
		}
	}
	secretCopy := make(map[SecretScope]string, len(secrets))
	for scope, secret := range secrets {
		secretCopy[scope] = secret
	}
	return &FakeCandidateResolver{
		repo: repo, secrets: secretCopy, clock: configuration.Clock,
		maxClockSkew: configuration.MaxClockSkew, maxHandles: configuration.MaxHandles,
		handles: make(map[string]fakeVerifierState), usedNonces: make(map[string]time.Time),
	}
}

var _ CandidateResolver = (*FakeCandidateResolver)(nil)
var _ CandidateVerifier = (*FakeCandidateResolver)(nil)

// ResolveCandidate consumes a public candidate and mints one purpose-bound
// verifier handle. Tenant identity is obtained only from the consumed
// Binding; request fields never select it.
func (r *FakeCandidateResolver) ResolveCandidate(ctx context.Context, request CandidateSecretRequest) (ScopedVerifierHandle, error) {
	if err := checkFakeContext(ctx); err != nil {
		return ScopedVerifierHandle{}, err
	}
	if r == nil || r.repo == nil || request.Purpose != PurposeWebhookVerification || request.Candidate.Purpose != request.Purpose {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	now := r.nowUTC()
	if err := request.Candidate.Validate(now); err != nil {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	binding, err := r.repo.ConsumeCandidate(ctx, request.Candidate)
	if err != nil {
		if IsContextCancellation(err) {
			return ScopedVerifierHandle{}, err
		}
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	if err := checkFakeContext(ctx); err != nil {
		return ScopedVerifierHandle{}, err
	}
	scope := SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef}
	if err := scope.Validate(); err != nil {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	secret, ok := r.secrets[scope]
	if !ok || secret == "" {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	token, err := newFakeHandleToken()
	if err != nil {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	handle, err := NewScopedVerifierHandle(token, request.Purpose, request.Candidate.ExpiresAt)
	if err != nil {
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	r.mu.Lock()
	r.pruneHandlesLocked(now)
	if len(r.handles) >= r.maxHandles {
		r.mu.Unlock()
		return ScopedVerifierHandle{}, ErrVerificationFailed
	}
	r.handles[token] = fakeVerifierState{binding: binding.Clone(), secret: secret, purpose: request.Purpose, expiresAt: request.Candidate.ExpiresAt}
	r.mu.Unlock()
	return handle, nil
}

// Verify authenticates a fake HMAC proof, consumes the handle, rechecks the
// current active Binding version, and returns only trusted routing identity.
func (r *FakeCandidateResolver) Verify(ctx context.Context, handle ScopedVerifierHandle, request VerificationRequest) (VerifiedBinding, error) {
	if err := checkFakeContext(ctx); err != nil {
		return VerifiedBinding{}, err
	}
	if r == nil || r.repo == nil || handle.Token() == "" {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	now := r.nowUTC()
	state, exists := r.consumeHandle(handle.Token(), now)
	if !validHandle(state, exists, handle, request, now) {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	if !validFakeVerificationRequest(request, now, r.maxClockSkew) {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	current, err := r.repo.Get(ctx, state.binding.TenantID, state.binding.BindingID)
	if err != nil {
		if IsContextCancellation(err) {
			return VerifiedBinding{}, err
		}
		return VerifiedBinding{}, ErrVerificationFailed
	}
	if !matchesVerifiedBinding(*current, state.binding) {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	expected := SignFakeRequest(state.secret, request)
	if !hmac.Equal([]byte(expected), []byte(request.Signature)) {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	if !r.recordNonce(state.binding, request.Nonce, request.Timestamp, now) {
		return VerifiedBinding{}, ErrVerificationFailed
	}
	return newVerifiedBinding(*current)
}

func (r *FakeCandidateResolver) consumeHandle(token string, now time.Time) (fakeVerifierState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneHandlesLocked(now)
	state, exists := r.handles[token]
	delete(r.handles, token)
	return state, exists
}

func validHandle(state fakeVerifierState, exists bool, handle ScopedVerifierHandle, request VerificationRequest, now time.Time) bool {
	return exists && state.purpose == handle.Purpose && state.expiresAt.Equal(handle.ExpiresAt) && handle.Purpose == request.Purpose && now.Before(handle.ExpiresAt)
}

func matchesVerifiedBinding(current, original Binding) bool {
	return current.Status == StatusActive && current.Version == original.Version && current.ConfigDigest == original.ConfigDigest && current.Channel == original.Channel && current.ProviderAccountID == original.ProviderAccountID
}

func (r *FakeCandidateResolver) recordNonce(binding Binding, nonce string, timestamp, now time.Time) bool {
	nonceKey := binding.TenantID + "\x00" + binding.BindingID + "\x00" + nonce
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, expiry := range r.usedNonces {
		if now.After(expiry) {
			delete(r.usedNonces, key)
		}
	}
	if _, replayed := r.usedNonces[nonceKey]; replayed {
		return false
	}
	nonceExpiry := timestamp.Add(r.maxClockSkew)
	minimumExpiry := now.Add(r.maxClockSkew)
	if minimumExpiry.After(nonceExpiry) {
		nonceExpiry = minimumExpiry
	}
	r.usedNonces[nonceKey] = nonceExpiry
	return true
}

// SignFakeRequest creates the deterministic HMAC proof accepted by the fake.
// RouteHints are intentionally excluded: they are untrusted and changing them
// must never change the verified Binding selected by the resolver.
func SignFakeRequest(secret string, request VerificationRequest) string {
	mac := hmac.New(sha256.New, []byte(secret))
	writeFakePart(mac, string(request.Purpose))
	writeFakePart(mac, request.Timestamp.UTC().Format(time.RFC3339Nano))
	writeFakePart(mac, request.Nonce)
	writeFakePart(mac, request.MessageDigest)
	writeFakePart(mac, request.ReceiveID)
	return hex.EncodeToString(mac.Sum(nil))
}

func validFakeVerificationRequest(request VerificationRequest, now time.Time, maxClockSkew time.Duration) bool {
	if request.Purpose != PurposeWebhookVerification || request.Timestamp.IsZero() || request.Timestamp.Location() != time.UTC || maxClockSkew <= 0 {
		return false
	}
	if request.Timestamp.Before(now.Add(-maxClockSkew)) || request.Timestamp.After(now.Add(maxClockSkew)) {
		return false
	}
	if !validFakeText(request.Nonce, 256) || !validLowerHexDigest(request.MessageDigest) || !validFakeText(request.Signature, 128) {
		return false
	}
	return request.ReceiveID == "" || validFakeText(request.ReceiveID, 256)
}

func validFakeText(value string, maxLength int) bool {
	return value != "" && len([]rune(value)) <= maxLength && !strings.ContainsFunc(value, unicode.IsControl)
}

func validLowerHexDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func writeFakePart(builder interface{ Write([]byte) (int, error) }, value string) {
	_, _ = builder.Write([]byte(strconv.Itoa(len([]byte(value)))))
	_, _ = builder.Write([]byte{':'})
	_, _ = builder.Write([]byte(value))
}

func newFakeHandleToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func (r *FakeCandidateResolver) pruneHandlesLocked(now time.Time) {
	for token, state := range r.handles {
		if !now.Before(state.expiresAt) {
			delete(r.handles, token)
		}
	}
}

func (r *FakeCandidateResolver) nowUTC() time.Time {
	now := r.clock()
	if now.IsZero() {
		now = time.Now()
	}
	return now.UTC()
}

// NowUTC exposes the fake clock boundary for package-level offline tests.
func (r *FakeCandidateResolver) NowUTC() time.Time { return r.nowUTC() }

// HandleCount reports the number of outstanding one-time verifier handles.
func (r *FakeCandidateResolver) HandleCount() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handles)
}

func checkFakeContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
