package application

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type PublishedCredentialTarget struct {
	ProfileRevisionNumber int64  `json:"profile_revision_number"`
	Category              string `json:"category"`
	ResourceName          string `json:"resource_name"`
	PurposeField          string `json:"purpose_field"`
	AssociationToken      string `json:"association_token"`
}
type CredentialUpdate struct {
	Target                     PublishedCredentialTarget `json:"target"`
	Action                     string                    `json:"action"`
	ExpectedCredentialRevision int64                     `json:"expected_credential_revision"`
	Value                      *string                   `json:"value,omitempty"`
}
type UpdateUsedCredentialCommand struct {
	TenantID       string
	ProfileID      string
	ActorUserID    string
	IdempotencyKey string
	Update         CredentialUpdate
}
type CredentialUpdateResult struct {
	CredentialRevision int64  `json:"credential_revision"`
	Status             string `json:"status"`
}

func DecodeCredentialUpdate(data []byte) (CredentialUpdate, error) {
	var input CredentialUpdate
	if err := decodeCredentialJSON(data, &input); err != nil {
		return CredentialUpdate{}, err
	}
	if err := input.validate(); err != nil {
		return CredentialUpdate{}, err
	}
	return input, nil
}

var associationTokenFormat = regexp.MustCompile("^[0-9a-f]{64}$")

func (u CredentialUpdate) validate() error {
	if u.Target.ProfileRevisionNumber <= 0 || !resourceName.MatchString(u.Target.ResourceName) ||
		!associationTokenFormat.MatchString(u.Target.AssociationToken) ||
		!validPublishedCredentialPurpose(u.Target.Category, u.Target.PurposeField) {
		return domain.ErrCredentialInput
	}
	if u.Action != "replace" && u.Action != "clear" {
		return domain.ErrCredentialInput
	}
	return (CredentialAction{Action: u.Action, ExpectedCredentialRevision: u.ExpectedCredentialRevision, Value: u.Value}).validate(true)
}

// The wire target uses only platform-owned category/purpose pairs. The
// immutable Revision and association token authorize a specific instance later.
func validPublishedCredentialPurpose(category, purpose string) bool {
	switch category {
	case "models":
		return purpose == "api_key"
	case "tools":
		return purpose == "bearer_token"
	case "knowledge":
		return purpose == "qdrant_api_key" || purpose == "embedding_api_key"
	case "storage":
		return purpose == "dsn" || purpose == "dsn_password" || purpose == "access_key_id" || purpose == "secret_access_key"
	default:
		return false
	}
}

func (s *Service) UpdateUsedProfileCredential(ctx context.Context, command UpdateUsedCredentialCommand) (CredentialUpdateResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return CredentialUpdateResult{}, err
	}
	if err := s.credentialDependencies(); err != nil {
		return CredentialUpdateResult{}, err
	}
	if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
		return CredentialUpdateResult{}, err
	}
	if !validIdempotencyKey(command.IdempotencyKey) {
		return CredentialUpdateResult{}, domain.ErrCredentialInput
	}
	if err := command.Update.validate(); err != nil {
		return CredentialUpdateResult{}, err
	}
	// Detach the write-only pointer before comparing or consuming its value.
	if command.Update.Value != nil {
		value := *command.Update.Value
		command.Update.Value = &value
	}
	mac, err := s.requestMAC(struct {
		Operation string
		Command   UpdateUsedCredentialCommand
	}{"live-update", command})
	if err != nil {
		return CredentialUpdateResult{}, err
	}
	var result CredentialUpdateResult
	err = s.deps.Credentials.WithinProfile(ctx, command.TenantID, command.ProfileID, func(tx CredentialTransaction) error {
		if err := s.authorizeOwner(ctx, command.TenantID, command.ActorUserID); err != nil {
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
			if err := json.Unmarshal(receipt.Result, &result); err != nil {
				return errors.New("invalid profile credential receipt")
			}
			return nil
		}
		target := command.Update.Target
		revision, err := tx.GetRevision(ctx, target.ProfileRevisionNumber)
		if err != nil {
			return err
		}
		revision, err = canonicalStoredRevision(revision)
		if err != nil {
			return err
		}
		var spec domain.Spec
		if err := json.Unmarshal(revision.Spec, &spec); err != nil {
			return errors.New("invalid canonical profile")
		}
		var selected credentialSlot
		found = false
		for _, slot := range credentialSlots(spec) {
			if slot.Category == target.Category && slot.Name == target.ResourceName && slot.Purpose == target.PurposeField {
				selected = slot
				found = true
				break
			}
		}
		if !found || selected.ID == "" || !sameMAC(target.AssociationToken, s.associationToken(command.TenantID, command.ProfileID, revision.ID, selected)) {
			return domain.ErrCredentialAssociation
		}
		record, err := tx.GetCredential(ctx, selected.ID)
		if err != nil {
			return err
		}
		if err := checkAssociation(record, command.TenantID, command.ProfileID, selected); err != nil {
			return err
		}
		if record.Status != domain.CredentialActive {
			return domain.ErrCredentialUnavailable
		}
		if record.Revision != command.Update.ExpectedCredentialRevision {
			return domain.ErrCredentialConflict
		}
		if command.Update.Action == "clear" {
			record.Status = domain.CredentialCleared
			record.Ciphertext = nil
		} else {
			value := []byte(*command.Update.Value)
			defer clear(value)
			if selected.Category == "storage" && selected.Purpose == "dsn" {
				destination, password, err := ParseStorageCredential(*command.Update.Value)
				if err != nil {
					return err
				}
				defer clear(password)
				r := spec.Storage[selected.Name]
				if destination != r.Destination {
					return domain.ErrCredentialAssociation
				}
				value = password
			}
			record.Ciphertext, err = s.deps.Cipher.Encrypt(ctx, record.AssociatedData(), value)
			if err != nil {
				return errors.New("encrypt profile credential")
			}
		}
		record.Revision++
		record.UpdatedBy = command.ActorUserID
		record.UpdatedAt = s.deps.Now().UTC()
		if err := tx.UpdateCredential(ctx, record, command.Update.ExpectedCredentialRevision); err != nil {
			return err
		}
		result = CredentialUpdateResult{CredentialRevision: record.Revision, Status: record.Status}
		outcome, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return tx.InsertReceipt(ctx, CredentialReceipt{ActorUserID: command.ActorUserID, Key: command.IdempotencyKey, RequestMAC: mac, Result: outcome, CreatedAt: record.UpdatedAt})
	})
	if err != nil {
		return CredentialUpdateResult{}, err
	}
	return result, nil
}
