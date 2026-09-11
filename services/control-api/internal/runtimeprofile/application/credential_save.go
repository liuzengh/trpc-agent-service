package application

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const receiptMACPurpose = "profile-write-idempotency-v1"
const associationMACPurpose = "profile-credential-association-v1"

type SaveCredentialDraftCommand struct {
	TenantID       string
	ProfileID      string
	ActorUserID    string
	IdempotencyKey string
	Write          ProfileWrite
}
type DraftWriteResult struct {
	ProfileID     string    `json:"profile_id"`
	DraftRevision int64     `json:"draft_revision"`
	UpdatedAt     time.Time `json:"updated_at"`
}
type draftWriteReceipt struct {
	OwnerRequired bool             `json:"owner_required"`
	Result        DraftWriteResult `json:"result"`
}

var credentialIDFormat = regexp.MustCompile("^crd_[0-9a-f]{32}$")

func (s *Service) credentialDependencies() error {
	if err := s.validate(); err != nil {
		return err
	}
	if s.deps.Credentials == nil || s.deps.Cipher == nil || s.deps.OwnerAccess == nil || s.deps.NewCredentialID == nil {
		return errors.New("runtime profile credential dependencies are incomplete")
	}
	return nil
}
func (s *Service) authorizeOwner(ctx context.Context, tenant, user string) error {
	allowed, err := s.deps.OwnerAccess.IsActiveOwner(ctx, tenant, user)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrTenantForbidden
	}
	return nil
}
func validIdempotencyKey(key string) bool {
	if len(key) < 1 || len(key) > 128 {
		return false
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func (s *Service) requestMAC(command any) (string, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return "", domain.ErrCredentialInput
	}
	defer clear(body)
	if len(body) > domain.MaxDocumentBytes {
		return "", domain.ErrCredentialInput
	}
	return s.deps.Cipher.MAC(receiptMACPurpose, body), nil
}
func sameMAC(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

func (s *Service) SaveCredentialDraft(ctx context.Context, command SaveCredentialDraftCommand) (DraftWriteResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return DraftWriteResult{}, err
	}
	if err := s.credentialDependencies(); err != nil {
		return DraftWriteResult{}, err
	}
	if !validIdempotencyKey(command.IdempotencyKey) {
		return DraftWriteResult{}, domain.ErrCredentialInput
	}
	// Validate typed direct callers too, then detach all caller-owned maps and values.
	// Raw HTTP object-shape checks are performed by DecodeProfileWrite.
	encoded, err := json.Marshal(command.Write)
	if err != nil {
		return DraftWriteResult{}, domain.ErrCredentialInput
	}
	defer clear(encoded)
	if err := command.Write.validate(); err != nil {
		return DraftWriteResult{}, err
	}
	var input ProfileWrite
	if err := json.Unmarshal(encoded, &input); err != nil {
		return DraftWriteResult{}, domain.ErrCredentialInput
	}
	command.Write = input
	mac, err := s.requestMAC(struct {
		Operation string
		Command   SaveCredentialDraftCommand
	}{"save-draft", command})
	if err != nil {
		return DraftWriteResult{}, err
	}
	var result DraftWriteResult
	err = s.deps.Credentials.WithinProfile(ctx, command.TenantID, command.ProfileID, func(tx CredentialTransaction) error {
		if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
			return err
		}
		receipt, found, err := tx.FindReceipt(ctx, command.ActorUserID, command.IdempotencyKey)
		if err != nil {
			return err
		}
		if found {
			if !sameMAC(receipt.RequestMAC, mac) {
				return ErrCredentialIdempotencyConflict
			}
			var stored draftWriteReceipt
			if err := json.Unmarshal(receipt.Result, &stored); err != nil {
				return errors.New("invalid profile write receipt")
			}
			if stored.OwnerRequired {
				if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
					return err
				}
			}
			result = stored.Result
			return nil
		}
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		if draft.Revision != input.ExpectedDraftRevision {
			return ErrDraftRevisionConflict
		}
		var previous domain.Spec
		if err := json.Unmarshal(draft.Spec, &previous); err != nil {
			return errors.New("invalid stored profile draft")
		}
		next, err := specFromConfig(input.Config)
		if err != nil {
			return err
		}
		if err := s.checkManaged(ctx, command.TenantID, next); err != nil {
			return err
		}
		if err := s.bindManagedStorageCredentialTargets(ctx, command.TenantID, input, &next, previous); err != nil {
			return err
		}
		// DSNs carry a password and destination. Resolve their non-secret part before
		// comparing immutable purposes; do not persist the original URI.
		values := make(map[string][]byte)
		defer func() {
			for _, v := range values {
				clear(v)
			}
		}()
		for category, resources := range input.Credentials {
			for name, purposes := range resources {
				for purpose, action := range purposes {
					if action.Action != "replace" {
						continue
					}
					key := category + "/" + name + "/" + purpose
					if category == "storage" && purpose == "dsn" {
						destination, password, err := ParseStorageCredential(*action.Value)
						if err != nil {
							return err
						}
						configured := input.Config.Storage[name].Destination
						if configured != nil && *configured != destination {
							clear(password)
							return domain.ErrCredentialAssociation
						}
						r := next.Storage[name]
						r.Destination = destination
						next.Storage[name] = r
						values[key] = password
					} else {
						values[key] = []byte(*action.Value)
					}
				}
			}
		}
		oldSlots := make(map[string]credentialSlot)
		for _, slot := range credentialSlots(previous) {
			oldSlots[slot.key()] = slot
		}
		newSlots := credentialSlots(next)
		newKeys := make(map[string]bool)
		ownerRequired := false
		for _, slot := range newSlots {
			newKeys[slot.key()] = true
		}
		for _, slot := range oldSlots {
			if slot.ID != "" && !newKeys[slot.key()] {
				ownerRequired = true
			}
		}
		for _, resources := range input.Credentials {
			for _, purposes := range resources {
				for _, action := range purposes {
					if action.Action != "keep" {
						ownerRequired = true
					}
				}
			}
		}
		if ownerRequired {
			if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
				return err
			}
		}
		for _, slot := range newSlots {
			action := input.Credentials[slot.Category][slot.Name][slot.Purpose]
			if action.Action == "" {
				action.Action = "keep"
			}
			old := oldSlots[slot.key()]
			switch action.Action {
			case "keep":
				if old.ID == "" {
					continue
				}
				if old.AudienceDigest != slot.AudienceDigest {
					return domain.ErrCredentialAssociation
				}
				record, err := tx.GetCredential(ctx, old.ID)
				if err != nil {
					return err
				}
				if err := checkAssociation(record, command.TenantID, command.ProfileID, old); err != nil {
					return err
				}
				setCredentialID(&next, slot, old.ID)
			case "clear":
				// Removing the draft association is not a live revocation.
			case "replace":
				id, err := s.deps.NewCredentialID()
				if err != nil {
					return errors.New("generate profile credential identity")
				}
				if !credentialIDFormat.MatchString(id) {
					return errors.New("invalid generated credential identity")
				}
				now := s.deps.Now().UTC()
				record := domain.ProfileCredential{ID: id, TenantID: command.TenantID, ProfileID: command.ProfileID,
					Category: slot.Category, ResourceName: slot.Name, Purpose: slot.Purpose, AudienceDigest: slot.AudienceDigest,
					Revision: 1, Status: domain.CredentialActive, CreatedBy: command.ActorUserID, UpdatedBy: command.ActorUserID,
					CreatedAt: now, UpdatedAt: now}
				record.Ciphertext, err = s.deps.Cipher.Encrypt(ctx, record.AssociatedData(), values[slot.key()])
				if err != nil {
					return errors.New("encrypt profile credential")
				}
				if err := tx.InsertCredential(ctx, record); err != nil {
					return err
				}
				setCredentialID(&next, slot, id)
			}
		}
		storedSpec, err := json.Marshal(next)
		if err != nil {
			return domain.ErrCredentialInput
		}
		if !domain.ValidateDraftForStorage(storedSpec, input.ExpectedDraftRevision).Valid {
			return domain.ErrCredentialInput
		}
		draft.Spec = storedSpec
		draft.Revision++
		draft.UpdatedBy = command.ActorUserID
		draft.UpdatedAt = s.deps.Now().UTC()
		if err := tx.SaveDraft(ctx, draft, input.ExpectedDraftRevision); err != nil {
			return err
		}
		result = DraftWriteResult{ProfileID: command.ProfileID, DraftRevision: draft.Revision, UpdatedAt: draft.UpdatedAt}
		outcome, err := json.Marshal(draftWriteReceipt{OwnerRequired: ownerRequired, Result: result})
		if err != nil {
			return err
		}
		return tx.InsertReceipt(ctx, CredentialReceipt{ActorUserID: command.ActorUserID, Key: command.IdempotencyKey, RequestMAC: mac, Result: outcome, CreatedAt: draft.UpdatedAt})
	})
	if err != nil {
		return DraftWriteResult{}, err
	}
	return result, nil
}

func checkAssociation(record domain.ProfileCredential, tenant, profile string, slot credentialSlot) error {
	if record.TenantID != tenant || record.ProfileID != profile || record.ID != slot.ID ||
		record.Category != slot.Category || record.ResourceName != slot.Name || record.Purpose != slot.Purpose ||
		record.AudienceDigest != slot.AudienceDigest {
		return domain.ErrCredentialAssociation
	}
	return nil
}
