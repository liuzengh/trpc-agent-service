// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrApprovalStoreUnavailable = errors.New("tool approval store is unavailable")
	ErrApprovalInvalid          = errors.New("tool approval is invalid or expired")
	ErrApprovalAlreadyUsed      = errors.New("tool approval was already consumed")
	ErrApprovalNotFound         = errors.New("tool approval challenge was not found")
	ErrApprovalNotGranted       = errors.New("tool approval has not been granted")
	ErrApprovalAmbiguous        = errors.New("tool approval recovery state is ambiguous")
	// ErrApprovalResumeInvalid means a retry's previously inspected challenge
	// can no longer authorize execution. Callers must stop the Runner and
	// reconcile instead of creating a replacement challenge implicitly.
	ErrApprovalResumeInvalid = errors.New("tool approval resume is no longer valid")
)

const (
	approvalTokenBytes   = 32
	approvalIDBytes      = 16
	defaultApprovalTTL   = 5 * time.Minute
	maxApprovalTTL       = 24 * time.Hour
	maxApprovalArgsBytes = 1 << 20
	// Keep canonicalization bounded independently of the webhook parser. Tool
	// arguments can be produced directly by a model and therefore may bypass
	// Gateway's payload-depth guard; an unbounded recursive decoder would allow a
	// deeply nested 1 MiB JSON value to exhaust the Worker stack.
	maxApprovalJSONDepth = 64
)

// ApprovalRequest is the exact authorization scope for one dangerous tool
// invocation. ArgsHash binds approval to the canonical argument payload;
// InvocationID binds it to the durable Inbox/execution idempotency key.
type ApprovalRequest struct {
	TenantID       string
	UserID         string
	SessionOwnerID string
	SessionID      string
	ToolName       string
	ArgsHash       string
	InvocationID   string
}

// ApprovalInvocationScope identifies one durable inbound invocation without
// exposing tool arguments or approval capability material. It is used only to
// discover whether a retry must resume an already-persisted pending tool call.
type ApprovalInvocationScope struct {
	TenantID       string
	UserID         string
	SessionOwnerID string
	SessionID      string
	InvocationID   string
}

// ApprovalChallenge is safe to return to a caller. It contains no secret
// token; only the approver can exchange ChallengeID for an opaque grant.
type ApprovalChallenge struct {
	ChallengeID string
	Request     ApprovalRequest
	ExpiresAt   time.Time
}

// ApprovalGrant is returned by an ApprovalStore to its trusted caller. The
// token is opaque and is stored only as a hash by the store. The HTTP control
// plane does not expose it; durable queue retries consume granted state by
// invocation scope instead.
type ApprovalGrant struct {
	ChallengeID string
	Token       string
	ExpiresAt   time.Time
}

// ApprovalStore is the durable approval capability seam. Production
// deployments must back this with tenant-scoped durable storage and an atomic
// consume operation; MemoryApprovalStore exists only for tests and local
// single-process demonstrations.
type ApprovalStore interface {
	CreateChallenge(context.Context, ApprovalRequest, time.Duration) (ApprovalChallenge, error)
	Grant(context.Context, string, string) (ApprovalGrant, error)
	Consume(context.Context, ApprovalRequest, string) error
}

// ApprovalInspector is an optional read capability used by the control plane
// to display a challenge before granting it. It deliberately does not expose
// the token hash or any raw tool arguments.
type ApprovalInspector interface {
	GetChallenge(context.Context, string) (ApprovalChallenge, error)
}

// ApprovalLister exposes pending scopes to an authorized operator. It never
// returns raw arguments or token material; the list exists so asynchronous IM
// consumers can discover a challenge after the Worker returned HTTP 428.
type ApprovalLister interface {
	ListChallenges(context.Context, string, int) ([]ApprovalChallenge, error)
}

// ApprovalGrantConsumer is the queue-safe approval path. A durable retry can
// consume an operator grant by its exact invocation scope without transporting
// the raw token through Inbox, Session, or model input.
type ApprovalGrantConsumer interface {
	ConsumeGranted(context.Context, ApprovalRequest) error
}

// ApprovalGrantConsumerForChallenge is the stronger queue-resume capability.
// It binds consumption to both the exact invocation scope and the challenge
// ID observed during admission. Without the challenge ID, a worker that loses
// a grant race could accidentally consume a replacement challenge for the
// same invocation.
type ApprovalGrantConsumerForChallenge interface {
	ConsumeGrantedForChallenge(context.Context, ApprovalRequest, string) error
}

// ApprovalResumeInspector is an optional read capability for the Worker
// retry path. A returned challenge is still unconsumed; the governance plugin
// remains the only component allowed to consume its one-time grant.
type ApprovalResumeInspector interface {
	FindActiveApproval(context.Context, ApprovalInvocationScope) (ApprovalChallenge, error)
}

// ApprovalGrantInspector reports whether one active challenge has been
// explicitly granted, without exposing the grant token. It is intentionally
// separate from ApprovalResumeInspector so existing approval stores can adopt
// the pre-execution admission check without changing the resume lookup API.
// Production workers require both capabilities when confirmation is enabled.
type ApprovalGrantInspector interface {
	IsApprovalGranted(context.Context, string) (bool, error)
}

// ApprovalResumeState is the result of one atomic inspection of a durable
// approval row. Challenge metadata and grant state must come from the same
// store operation; reading them through two independent calls can admit a
// retry after another worker has consumed the one-time grant.
type ApprovalResumeState struct {
	Challenge ApprovalChallenge
	Granted   bool
}

// ApprovalResumeStateInspector is required by production retry admission.
// Implementations must evaluate the active/expired/consumed predicate and the
// grant flag from one consistency boundary. Stores should return
// ErrApprovalNotFound when no active challenge exists and ErrApprovalAmbiguous
// when more than one active challenge matches the invocation scope.
type ApprovalResumeStateInspector interface {
	InspectApprovalResume(context.Context, ApprovalInvocationScope) (ApprovalResumeState, error)
}

type approvalRecord struct {
	challenge ApprovalChallenge
	tokenHash [sha256.Size]byte
	granted   bool
	consumed  bool
}

// MemoryApprovalStore is deterministic and concurrency-safe, but not
// restart-safe or multi-node. It is intentionally named so callers cannot
// mistake it for a production durable adapter.
type MemoryApprovalStore struct {
	mu      sync.Mutex
	clock   func() time.Time
	records map[string]*approvalRecord
}

func NewMemoryApprovalStore() *MemoryApprovalStore {
	return &MemoryApprovalStore{clock: time.Now, records: make(map[string]*approvalRecord)}
}

func (s *MemoryApprovalStore) CreateChallenge(_ context.Context, request ApprovalRequest, ttl time.Duration) (ApprovalChallenge, error) {
	if err := validateApprovalRequest(request); err != nil {
		return ApprovalChallenge{}, err
	}
	if ttl <= 0 {
		ttl = defaultApprovalTTL
	}
	if ttl < time.Second {
		ttl = time.Second
	}
	if ttl > maxApprovalTTL {
		return ApprovalChallenge{}, fmt.Errorf("%w: approval lifetime exceeds the maximum", ErrApprovalInvalid)
	}
	id, err := randomToken(approvalIDBytes)
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("create approval challenge: %w", err)
	}
	now := s.now()
	challenge := ApprovalChallenge{ChallengeID: id, Request: request, ExpiresAt: now.Add(ttl)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked(now)
	if s.records == nil {
		s.records = make(map[string]*approvalRecord)
	}
	// A retry of the same durable invocation must not create an unbounded
	// collection of indistinguishable challenges. Reuse the active challenge
	// until it is consumed or expires; this also makes Consume deterministic.
	for _, record := range s.records {
		if sameApprovalRequest(record.challenge.Request, request) &&
			now.Before(record.challenge.ExpiresAt) && !record.consumed {
			return record.challenge, nil
		}
	}
	s.records[id] = &approvalRecord{challenge: challenge}
	return challenge, nil
}

func (s *MemoryApprovalStore) Grant(_ context.Context, challengeID, approver string) (ApprovalGrant, error) {
	if !validApprovalID(challengeID) || !validApprovalPrincipal(approver) {
		return ApprovalGrant{}, ErrApprovalInvalid
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[challengeID]
	if !ok {
		return ApprovalGrant{}, ErrApprovalNotFound
	}
	if !now.Before(record.challenge.ExpiresAt) || record.consumed {
		delete(s.records, challengeID)
		return ApprovalGrant{}, ErrApprovalInvalid
	}
	if record.granted {
		return ApprovalGrant{}, ErrApprovalAlreadyUsed
	}
	token, err := randomToken(approvalTokenBytes)
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("grant approval: %w", err)
	}
	record.tokenHash = sha256.Sum256([]byte(token))
	record.granted = true
	return ApprovalGrant{ChallengeID: challengeID, Token: token, ExpiresAt: record.challenge.ExpiresAt}, nil
}

func (s *MemoryApprovalStore) Consume(_ context.Context, request ApprovalRequest, token string) error {
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	if !validApprovalToken(token) {
		return ErrApprovalInvalid
	}
	now := s.now()
	hash := sha256.Sum256([]byte(token))
	s.mu.Lock()
	defer s.mu.Unlock()
	matchedRequest := false
	for id, record := range s.records {
		if !sameApprovalRequest(record.challenge.Request, request) {
			continue
		}
		matchedRequest = true
		if !record.granted || record.consumed || !now.Before(record.challenge.ExpiresAt) {
			if !now.Before(record.challenge.ExpiresAt) {
				delete(s.records, id)
			}
			continue
		}
		if subtle.ConstantTimeCompare(record.tokenHash[:], hash[:]) != 1 {
			continue
		}
		// Mark consumed while holding the same mutex used by Grant/Consume.
		record.consumed = true
		delete(s.records, id)
		return nil
	}
	if matchedRequest {
		return ErrApprovalInvalid
	}
	return ErrApprovalNotFound
}

// ConsumeGranted atomically consumes the grant for an exact request. It is
// used by durable queue retries after the Admin API has granted a challenge.
func (s *MemoryApprovalStore) ConsumeGranted(_ context.Context, request ApprovalRequest) error {
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := false
	for id, record := range s.records {
		if !sameApprovalRequest(record.challenge.Request, request) {
			continue
		}
		matched = true
		if !now.Before(record.challenge.ExpiresAt) || record.consumed {
			delete(s.records, id)
			continue
		}
		if !record.granted {
			return ErrApprovalNotGranted
		}
		record.consumed = true
		delete(s.records, id)
		return nil
	}
	if matched {
		return ErrApprovalNotGranted
	}
	return ErrApprovalNotFound
}

// ConsumeGrantedForChallenge atomically consumes a grant while binding it to
// the challenge ID captured by retry admission. A request-scope match alone is
// insufficient because the original challenge may have been consumed and a
// replacement challenge created before this worker reaches the tool hook.
func (s *MemoryApprovalStore) ConsumeGrantedForChallenge(_ context.Context, request ApprovalRequest, challengeID string) error {
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	if !validApprovalID(challengeID) {
		return ErrApprovalInvalid
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[challengeID]
	if !ok {
		return ErrApprovalNotFound
	}
	if !sameApprovalRequest(record.challenge.Request, request) {
		return ErrApprovalNotFound
	}
	if record.consumed {
		delete(s.records, challengeID)
		return ErrApprovalNotFound
	}
	if !now.Before(record.challenge.ExpiresAt) {
		delete(s.records, challengeID)
		return ErrApprovalInvalid
	}
	if !record.granted {
		return ErrApprovalNotGranted
	}
	record.consumed = true
	delete(s.records, challengeID)
	return nil
}

// FindActiveApproval returns the sole unconsumed, non-expired challenge for
// one durable inbound invocation. More than one active challenge is an
// ambiguous recovery state, so callers must fail closed instead of guessing
// which pending tool call to resume.
func (s *MemoryApprovalStore) FindActiveApproval(_ context.Context, scope ApprovalInvocationScope) (ApprovalChallenge, error) {
	if err := validateApprovalInvocationScope(scope); err != nil {
		return ApprovalChallenge{}, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var found *ApprovalChallenge
	for id, record := range s.records {
		if record.consumed || !now.Before(record.challenge.ExpiresAt) {
			delete(s.records, id)
			continue
		}
		if !sameApprovalInvocationScope(record.challenge.Request, scope) {
			continue
		}
		if found != nil {
			return ApprovalChallenge{}, ErrApprovalAmbiguous
		}
		challenge := record.challenge
		found = &challenge
	}
	if found == nil {
		return ApprovalChallenge{}, ErrApprovalNotFound
	}
	return *found, nil
}

// IsApprovalGranted reports the current grant state for a challenge. An
// ungranted, active challenge returns (false, nil); expired, consumed or
// unknown challenges return an inspectable error so callers fail closed.
func (s *MemoryApprovalStore) IsApprovalGranted(_ context.Context, challengeID string) (bool, error) {
	if !validApprovalID(challengeID) {
		return false, ErrApprovalInvalid
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[challengeID]
	if !ok {
		return false, ErrApprovalNotFound
	}
	if record.consumed || !now.Before(record.challenge.ExpiresAt) {
		delete(s.records, challengeID)
		return false, ErrApprovalInvalid
	}
	return record.granted, nil
}

// InspectApprovalResume implements ApprovalResumeStateInspector. Holding the
// store mutex across challenge lookup and grant-state capture gives local
// tests the same atomic read contract required of durable adapters.
func (s *MemoryApprovalStore) InspectApprovalResume(_ context.Context, scope ApprovalInvocationScope) (ApprovalResumeState, error) {
	if s == nil {
		return ApprovalResumeState{}, ErrApprovalStoreUnavailable
	}
	if err := validateApprovalInvocationScope(scope); err != nil {
		return ApprovalResumeState{}, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var found *approvalRecord
	for id, record := range s.records {
		if record.consumed || !now.Before(record.challenge.ExpiresAt) {
			delete(s.records, id)
			continue
		}
		if !sameApprovalInvocationScope(record.challenge.Request, scope) {
			continue
		}
		if found != nil {
			return ApprovalResumeState{}, ErrApprovalAmbiguous
		}
		found = record
	}
	if found == nil {
		return ApprovalResumeState{}, ErrApprovalNotFound
	}
	return ApprovalResumeState{Challenge: found.challenge, Granted: found.granted}, nil
}

// GetChallenge implements ApprovalInspector for local demonstrations and
// tests. The returned value contains only authorization scope metadata.
func (s *MemoryApprovalStore) GetChallenge(_ context.Context, challengeID string) (ApprovalChallenge, error) {
	if !validApprovalID(challengeID) {
		return ApprovalChallenge{}, ErrApprovalInvalid
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[challengeID]
	if !ok {
		return ApprovalChallenge{}, ErrApprovalNotFound
	}
	if record.consumed || !now.Before(record.challenge.ExpiresAt) {
		delete(s.records, challengeID)
		return ApprovalChallenge{}, ErrApprovalInvalid
	}
	return record.challenge, nil
}

func (s *MemoryApprovalStore) ListChallenges(_ context.Context, tenantID string, limit int) ([]ApprovalChallenge, error) {
	if strings.TrimSpace(tenantID) == "" || len(tenantID) > 255 || !utf8.ValidString(tenantID) || limit < 0 || limit > 100 {
		return nil, ErrApprovalInvalid
	}
	if limit == 0 {
		limit = 50
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]ApprovalChallenge, 0)
	for id, record := range s.records {
		if !now.Before(record.challenge.ExpiresAt) || record.consumed {
			delete(s.records, id)
			continue
		}
		if record.challenge.Request.TenantID == tenantID {
			result = append(result, record.challenge)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ExpiresAt.Before(result[j].ExpiresAt)
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *MemoryApprovalStore) now() time.Time {
	if s != nil && s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

func (s *MemoryApprovalStore) removeExpiredLocked(now time.Time) {
	for id, record := range s.records {
		if !now.Before(record.challenge.ExpiresAt) {
			delete(s.records, id)
		}
	}
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// CanonicalArgsHash normalizes JSON object key order before hashing. Arrays
// retain order, and malformed/non-JSON arguments are rejected.
func CanonicalArgsHash(raw []byte) (string, error) {
	if len(raw) > maxApprovalArgsBytes {
		return "", fmt.Errorf("tool arguments exceed %d bytes", maxApprovalArgsBytes)
	}
	if len(raw) > 0 && !utf8.Valid(raw) {
		return "", errors.New("tool arguments are not valid UTF-8")
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		raw = []byte("{}")
	}
	var value interface{}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var err error
	if value, err = decodeCanonicalJSONValue(decoder, 0); err != nil {
		return "", fmt.Errorf("invalid tool arguments: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err == nil {
		return "", errors.New("tool arguments contain multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("invalid trailing tool arguments: %w", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("canonicalize tool arguments: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return fmt.Sprintf("sha256:%x", digest[:]), nil
}

// decodeCanonicalJSONValue preserves json.Decoder's number semantics while
// rejecting duplicate object names. Silently choosing the first/last duplicate
// would let a signer and an executor bind different argument values.
func decodeCanonicalJSONValue(decoder *json.Decoder, depth int) (interface{}, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			if depth >= maxApprovalJSONDepth {
				return nil, fmt.Errorf("tool arguments exceed maximum JSON depth %d", maxApprovalJSONDepth)
			}
			object := make(map[string]interface{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON object key %q", key)
				}
				child, err := decodeCanonicalJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				if err != nil {
					return nil, err
				}
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			if depth >= maxApprovalJSONDepth {
				return nil, fmt.Errorf("tool arguments exceed maximum JSON depth %d", maxApprovalJSONDepth)
			}
			array := make([]interface{}, 0)
			for decoder.More() {
				child, err := decodeCanonicalJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				if err != nil {
					return nil, err
				}
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		default:
			return nil, fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	case string, bool, nil, json.Number:
		return value, nil
	default:
		return nil, fmt.Errorf("unexpected JSON token %T", token)
	}
}

func validateApprovalRequest(request ApprovalRequest) error {
	if err := validateApprovalInvocationScope(ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	}); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"tool": request.ToolName, "arguments hash": request.ArgsHash,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 255 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("approval %s is invalid", name)
		}
	}
	if len(request.ArgsHash) != len("sha256:")+sha256.Size*2 ||
		!strings.HasPrefix(request.ArgsHash, "sha256:") {
		return fmt.Errorf("approval arguments hash is invalid")
	}
	if _, err := hex.DecodeString(request.ArgsHash[len("sha256:"):]); err != nil {
		return fmt.Errorf("approval arguments hash is invalid")
	}
	return nil
}

func validateApprovalInvocationScope(scope ApprovalInvocationScope) error {
	for name, value := range map[string]string{
		"tenant": scope.TenantID, "user": scope.UserID,
		"session owner": scope.SessionOwnerID, "session": scope.SessionID,
		"invocation": scope.InvocationID,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 255 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("approval %s is invalid", name)
		}
	}
	return nil
}

func validApprovalID(value string) bool {
	if value == "" || len(value) > 128 || !utf8Safe(value) {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func validApprovalPrincipal(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 256 && utf8Safe(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func validApprovalToken(value string) bool {
	if value == "" || len(value) > 512 || !utf8Safe(value) {
		return false
	}
	// Built-in stores issue URL-safe base64 without padding. Restricting the
	// wire token to that alphabet prevents control characters or log-breaking
	// material from crossing the Worker boundary.
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == approvalTokenBytes
}

// ValidateApprovalToken checks the opaque capability at an input boundary
// without revealing its value. It is exported for HTTP/queue request
// validators; stores still validate again immediately before consumption.
func ValidateApprovalToken(value string) error {
	if !validApprovalToken(value) {
		return ErrApprovalInvalid
	}
	return nil
}

// ValidateApprovalChallenge checks the non-secret metadata returned by an
// approval store. Expiry is intentionally required to be present but is not
// compared with the worker's wall clock: durable stores own the authoritative
// clock and have already filtered expired rows at the read boundary.
func ValidateApprovalChallenge(challenge ApprovalChallenge) error {
	if !validApprovalID(challenge.ChallengeID) || challenge.ExpiresAt.IsZero() {
		return ErrApprovalInvalid
	}
	if err := validateApprovalRequest(challenge.Request); err != nil {
		return fmt.Errorf("%w: invalid challenge scope", ErrApprovalInvalid)
	}
	return nil
}

func utf8Safe(value string) bool {
	return utf8.ValidString(value)
}

func sameApprovalRequest(left, right ApprovalRequest) bool {
	return left == right
}

func sameApprovalInvocationScope(request ApprovalRequest, scope ApprovalInvocationScope) bool {
	return request.TenantID == scope.TenantID &&
		request.UserID == scope.UserID &&
		request.SessionOwnerID == scope.SessionOwnerID &&
		request.SessionID == scope.SessionID &&
		request.InvocationID == scope.InvocationID
}

// ApprovalRequiredError is returned to a caller that must obtain approval
// before retrying the exact same tool invocation.
type ApprovalRequiredError struct{ Challenge ApprovalChallenge }

func (e *ApprovalRequiredError) Error() string {
	if e == nil {
		return "tool approval is required"
	}
	return fmt.Sprintf("tool approval is required: challenge_id=%s", e.Challenge.ChallengeID)
}

// ApprovalState carries a challenge across Runner's derived contexts without
// leaking it into model/session payloads.
type ApprovalState struct {
	mu                sync.Mutex
	challenge         *ApprovalChallenge
	resumeChallengeID string
}

func NewApprovalState() *ApprovalState { return &ApprovalState{} }

func (s *ApprovalState) SetChallenge(challenge ApprovalChallenge) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.challenge = &challenge
	s.mu.Unlock()
}

func (s *ApprovalState) Challenge() (ApprovalChallenge, bool) {
	if s == nil {
		return ApprovalChallenge{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.challenge == nil {
		return ApprovalChallenge{}, false
	}
	return *s.challenge, true
}

// SetResumeChallengeID records the challenge selected by an atomic retry
// admission check. It is internal correlation metadata and is never serialized
// into model messages or Session events.
func (s *ApprovalState) SetResumeChallengeID(challengeID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.resumeChallengeID = challengeID
	s.mu.Unlock()
}

// ResumeChallengeID returns the challenge ID that the governance plugin must
// consume if this Runner invocation is resuming a pending tool call.
func (s *ApprovalState) ResumeChallengeID() (string, bool) {
	if s == nil {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resumeChallengeID, s.resumeChallengeID != ""
}

type approvalStateContextKey struct{}

func ContextWithApprovalState(ctx context.Context, state *ApprovalState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, approvalStateContextKey{}, state)
}

func approvalStateFromContext(ctx context.Context) *ApprovalState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(approvalStateContextKey{}).(*ApprovalState)
	return state
}

// ApprovalCapabilityFromContext is reserved for future custom runtime
// adapters. Built-in governance consumes capabilities only through Plugin.
func ApprovalCapabilityFromContext(ctx context.Context) (*ApprovalState, bool) {
	state := approvalStateFromContext(ctx)
	return state, state != nil
}
