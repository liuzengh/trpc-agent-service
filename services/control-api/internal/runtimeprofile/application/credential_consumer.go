package application

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const MaxCredentialUses = domain.MaxModelResources + domain.MaxToolResources + 2*domain.MaxKnowledgeResources + domain.MaxStorageResources

type CredentialUse struct {
	CredentialID   string `json:"credential_id"`
	Purpose        string `json:"purpose"`
	AudienceDigest string `json:"audience_digest"`
}
type CheckProfileCredentialsCommand struct {
	TenantID              string
	ProfileID             string
	ActorUserID           string
	ProfileRevisionNumber int64
	Uses                  []CredentialUse
}

// ExecutionAuthorizationVerifier must query the trusted Run/Attempt owner for
// CURRENT active lease/epoch and derive AllowedUses from its trusted Manifest.
// Token signature/expiry alone is insufficient. No permissive default exists.
type ExecutionAuthorizationVerifier interface {
	VerifyAttempt(context.Context, ExecutionAuthorizationRequest) (ExecutionAuthorization, error)
}
type ExecutionAuthorizationRequest struct {
	WorkloadIdentity string
	ExecutionToken   string
	ManifestID       string
	ManifestDigest   string
}
type ExecutionAuthorization struct {
	TenantID              string
	ProfileID             string
	ProfileRevisionNumber int64
	RunID                 string
	AttemptID             string
	WorkerID              string
	LeaseEpoch            int64
	ExpiresAt             time.Time
	ManifestID            string
	ManifestDigest        string
	AllowedUses           []CredentialUse
}
type ResolveAttemptCommand struct {
	Authorization ExecutionAuthorizationRequest
	Uses          []CredentialUse
}
type ResolvedCredential struct {
	Use                CredentialUse
	CredentialRevision int64
	Value              []byte
}

// CredentialBatch is INTERNAL ONLY, not a public management DTO or event.
type CredentialBatch struct {
	TenantID              string
	ProfileID             string
	ProfileRevisionNumber int64
	RunID                 string
	AttemptID             string
	WorkerID              string
	LeaseEpoch            int64
	ManifestID            string
	ManifestDigest        string
	Credentials           []ResolvedCredential
}

func (b *CredentialBatch) Clear() {
	for i := range b.Credentials {
		clear(b.Credentials[i].Value)
		b.Credentials[i].Value = nil
	}
}

func (s *Service) CheckUsable(ctx context.Context, command CheckProfileCredentialsCommand) error {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return err
	}
	if err := s.credentialDependencies(); err != nil {
		return err
	}
	return s.deps.Credentials.WithinProfile(ctx, command.TenantID, command.ProfileID, func(tx CredentialTransaction) error {
		_, err := s.checkedCredentialRecords(ctx, tx, command.TenantID, command.ProfileID, command.ProfileRevisionNumber, command.Uses)
		return err
	})
}
func (s *Service) checkedCredentialRecords(ctx context.Context, tx CredentialTransaction, tenant, profile string, number int64, uses []CredentialUse) ([]domain.ProfileCredential, error) {
	if len(uses) > MaxCredentialUses || number <= 0 {
		return nil, domain.ErrCredentialAssociation
	}
	revision, err := tx.GetRevision(ctx, number)
	if err != nil {
		return nil, err
	}
	revision, err = canonicalStoredRevision(revision)
	if err != nil {
		return nil, err
	}
	var spec domain.Spec
	if err := json.Unmarshal(revision.Spec, &spec); err != nil {
		return nil, errors.New("invalid canonical profile")
	}
	slots := map[string]credentialSlot{}
	for _, slot := range credentialSlots(spec) {
		if slot.ID != "" {
			slots[slot.ID] = slot
		}
	}
	seen := map[string]bool{}
	records := make([]domain.ProfileCredential, 0, len(uses))
	for _, use := range uses {
		slot, ok := slots[use.CredentialID]
		if !ok || seen[use.CredentialID] || use.Purpose != slot.Purpose || use.AudienceDigest != slot.AudienceDigest {
			return nil, domain.ErrCredentialAssociation
		}
		seen[use.CredentialID] = true
		record, err := tx.GetCredential(ctx, slot.ID)
		if err != nil {
			return nil, err
		}
		if err := checkAssociation(record, tenant, profile, slot); err != nil {
			return nil, err
		}
		if record.Status != domain.CredentialActive || len(record.Ciphertext) == 0 {
			return nil, domain.ErrCredentialUnavailable
		}
		records = append(records, record)
	}
	return records, nil
}
func (s *Service) ResolveForAttempt(ctx context.Context, command ResolveAttemptCommand) (CredentialBatch, error) {
	if err := s.credentialDependencies(); err != nil {
		return CredentialBatch{}, err
	}
	if s.deps.ExecutionVerifier == nil || command.Authorization.WorkloadIdentity == "" ||
		command.Authorization.ExecutionToken == "" || command.Authorization.ManifestID == "" || command.Authorization.ManifestDigest == "" {
		return CredentialBatch{}, ErrExecutionUnauthorized
	}
	auth, err := s.deps.ExecutionVerifier.VerifyAttempt(ctx, command.Authorization)
	if err != nil {
		return CredentialBatch{}, classifyExecutionVerification(err)
	}
	if auth.TenantID == "" || auth.ProfileID == "" || auth.RunID == "" || auth.AttemptID == "" ||
		auth.WorkerID != command.Authorization.WorkloadIdentity || auth.LeaseEpoch <= 0 ||
		!auth.ExpiresAt.After(s.deps.Now()) || auth.ManifestID != command.Authorization.ManifestID ||
		auth.ManifestDigest != command.Authorization.ManifestDigest || len(command.Uses) > MaxCredentialUses {
		return CredentialBatch{}, ErrExecutionUnauthorized
	}
	allowed := map[CredentialUse]bool{}
	for _, use := range auth.AllowedUses {
		allowed[use] = true
	}
	for _, use := range command.Uses {
		if !allowed[use] {
			return CredentialBatch{}, ErrExecutionUnauthorized
		}
	}
	result := CredentialBatch{TenantID: auth.TenantID, ProfileID: auth.ProfileID, ProfileRevisionNumber: auth.ProfileRevisionNumber, RunID: auth.RunID, AttemptID: auth.AttemptID,
		WorkerID: auth.WorkerID, LeaseEpoch: auth.LeaseEpoch, ManifestID: auth.ManifestID, ManifestDigest: auth.ManifestDigest}
	err = s.deps.Credentials.WithinProfile(ctx, auth.TenantID, auth.ProfileID, func(tx CredentialTransaction) error {
		// Acquiring the Profile lock can wait behind a write. A lease can be
		// revoked or fenced during that wait even while its token is unexpired.
		current, verifyErr := s.deps.ExecutionVerifier.VerifyAttempt(ctx, command.Authorization)
		if verifyErr != nil {
			return classifyExecutionVerification(verifyErr)
		}
		if !sameExecutionGrant(auth, current) || !current.ExpiresAt.After(s.deps.Now()) {
			return ErrExecutionUnauthorized
		}
		currentUses := make(map[CredentialUse]bool, len(current.AllowedUses))
		for _, use := range current.AllowedUses {
			currentUses[use] = true
		}
		for _, use := range command.Uses {
			if !currentUses[use] {
				return ErrExecutionUnauthorized
			}
		}
		records, err := s.checkedCredentialRecords(ctx, tx, auth.TenantID, auth.ProfileID, auth.ProfileRevisionNumber, command.Uses)
		if err != nil {
			return err
		}
		for i, record := range records {
			value, err := s.deps.Cipher.Decrypt(ctx, record.AssociatedData(), record.Ciphertext)
			if err != nil {
				return errors.New("resolve profile credential")
			}
			result.Credentials = append(result.Credentials, ResolvedCredential{Use: command.Uses[i], CredentialRevision: record.Revision, Value: value})
		}
		if !current.ExpiresAt.After(s.deps.Now()) {
			return ErrExecutionUnauthorized
		}
		return nil
	})
	if err != nil {
		result.Clear()
		return CredentialBatch{}, err
	}
	return result, nil
}

func sameExecutionGrant(a, b ExecutionAuthorization) bool {
	return a.TenantID == b.TenantID && a.ProfileID == b.ProfileID && a.ProfileRevisionNumber == b.ProfileRevisionNumber &&
		a.RunID == b.RunID && a.AttemptID == b.AttemptID && a.WorkerID == b.WorkerID && a.LeaseEpoch == b.LeaseEpoch &&
		a.ManifestID == b.ManifestID && a.ManifestDigest == b.ManifestDigest
}

// ResolveAttemptInput is the internal HTTP wire input. Tenant/Profile/Worker
// ownership is deliberately absent; it comes from trusted authentication and
// the online execution owner.
type ResolveAttemptInput struct {
	ExecutionToken string          `json:"execution_token"`
	ManifestID     string          `json:"manifest_id"`
	ManifestDigest string          `json:"manifest_digest"`
	Uses           []CredentialUse `json:"uses"`
}

func DecodeResolveAttempt(data []byte) (ResolveAttemptInput, error) {
	var input ResolveAttemptInput
	if err := decodeCredentialJSON(data, &input); err != nil {
		return ResolveAttemptInput{}, domain.ErrCredentialInput
	}
	if len(input.ExecutionToken) == 0 || len(input.ExecutionToken) > 8192 || len(input.ManifestID) == 0 ||
		len(input.ManifestID) > 128 || len(input.ManifestDigest) != 71 || input.Uses == nil || len(input.Uses) > MaxCredentialUses {
		return ResolveAttemptInput{}, domain.ErrCredentialInput
	}
	if !validSHA256Digest(input.ManifestDigest) {
		return ResolveAttemptInput{}, domain.ErrCredentialInput
	}
	for _, use := range input.Uses {
		if !credentialIDFormat.MatchString(use.CredentialID) || len(use.Purpose) == 0 || len(use.Purpose) > 64 ||
			!validSHA256Digest(use.AudienceDigest) {
			return ResolveAttemptInput{}, domain.ErrCredentialInput
		}
	}
	return input, nil
}

func validSHA256Digest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, digit := range value[7:] {
		if !(digit >= '0' && digit <= '9') && !(digit >= 'a' && digit <= 'f') {
			return false
		}
	}
	return true
}

// Only an explicit owner denial is permanent. Network, database, malformed
// dependency responses and unknown verifier failures are retryable dependency
// errors without exposing transport details or execution tokens.
func classifyExecutionVerification(err error) error {
	if errors.Is(err, ErrExecutionUnauthorized) {
		return ErrExecutionUnauthorized
	}
	return ErrExecutionDependencyUnavailable
}
