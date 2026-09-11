package application

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type CredentialState struct {
	Configured         bool   `json:"configured"`
	Status             string `json:"status"`
	CredentialRevision int64  `json:"credential_revision"`
	AssociationToken   string `json:"association_token,omitempty"`
}
type CredentialStates map[string]map[string]map[string]CredentialState

// ProfileRead is the only public representation of credential-bearing content.
// Its digest identifies the internal Canonical Spec, not this redacted view.
type ProfileRead struct {
	ID                        string           `json:"id,omitempty"`
	TenantID                  string           `json:"tenant_id"`
	ProfileID                 string           `json:"profile_id"`
	DraftRevision             int64            `json:"draft_revision,omitempty"`
	RevisionNumber            int64            `json:"revision_number,omitempty"`
	SourceDraftRevision       int64            `json:"source_draft_revision,omitempty"`
	SchemaVersion             string           `json:"schema_version"`
	CredentialProtocolVersion string           `json:"credential_protocol_version"`
	Config                    ProfileConfig    `json:"config"`
	CredentialStates          CredentialStates `json:"credential_states,omitempty"`
	SpecDigest                string           `json:"spec_digest,omitempty"`
	UpdatedBy                 string           `json:"updated_by,omitempty"`
	UpdatedAt                 *time.Time       `json:"updated_at,omitempty"`
	PublishedBy               string           `json:"published_by,omitempty"`
	PublishedAt               *time.Time       `json:"published_at,omitempty"`
}

func (s *Service) associationToken(tenant, profile, revision string, slot credentialSlot) string {
	payload, _ := json.Marshal([]string{tenant, profile, revision, slot.Category, slot.Name, slot.Purpose, slot.ID, slot.AudienceDigest})
	return s.deps.Cipher.MAC(associationMACPurpose, payload)
}
func (s *Service) credentialStates(ctx context.Context, tx CredentialTransaction, tenant, profile, revision string, spec domain.Spec) (CredentialStates, error) {
	states := CredentialStates{}
	for _, slot := range credentialSlots(spec) {
		state := CredentialState{Status: "unconfigured"}
		if slot.ID != "" {
			record, err := tx.GetCredential(ctx, slot.ID)
			if err != nil {
				return nil, err
			}
			if err := checkAssociation(record, tenant, profile, slot); err != nil {
				return nil, err
			}
			state.Configured = record.Status == domain.CredentialActive
			state.Status = record.Status
			state.CredentialRevision = record.Revision
			state.AssociationToken = s.associationToken(tenant, profile, revision, slot)
		}
		if states[slot.Category] == nil {
			states[slot.Category] = map[string]map[string]CredentialState{}
		}
		if states[slot.Category][slot.Name] == nil {
			states[slot.Category][slot.Name] = map[string]CredentialState{}
		}
		states[slot.Category][slot.Name][slot.Purpose] = state
	}
	return states, nil
}
func (s *Service) GetCredentialDraft(ctx context.Context, tenant, profile, user string) (ProfileRead, error) {
	if err := s.authorize(ctx, tenant, user); err != nil {
		return ProfileRead{}, err
	}
	if err := s.credentialDependencies(); err != nil {
		return ProfileRead{}, err
	}
	var result ProfileRead
	err := s.deps.Credentials.WithinProfile(ctx, tenant, profile, func(tx CredentialTransaction) error {
		draft, err := tx.GetDraft(ctx)
		if err != nil {
			return err
		}
		if !domain.ValidateDraftForStorage(draft.Spec, draft.Revision).Valid {
			return errors.New("invalid stored profile draft")
		}
		var spec domain.Spec
		if err := json.Unmarshal(draft.Spec, &spec); err != nil {
			return errors.New("invalid stored profile draft")
		}
		states, err := s.credentialStates(ctx, tx, tenant, profile, "draft:"+strconv.FormatInt(draft.Revision, 10), spec)
		if err != nil {
			return err
		}
		result = ProfileRead{TenantID: tenant, ProfileID: profile, DraftRevision: draft.Revision,
			SchemaVersion: domain.SchemaVersionV1, CredentialProtocolVersion: domain.CredentialProtocolVersionV1,
			Config: configFromSpec(spec), CredentialStates: states, UpdatedBy: draft.UpdatedBy, UpdatedAt: &draft.UpdatedAt}
		return nil
	})
	if err != nil {
		return ProfileRead{}, err
	}
	return result, nil
}

// PublishedProfileRead excludes dynamic credential status so publication
// retries return the same immutable redacted result.
func PublishedProfileRead(revision domain.ProfileRevision) (ProfileRead, error) {
	verified, err := canonicalStoredRevision(revision)
	if err != nil {
		return ProfileRead{}, err
	}
	var spec domain.Spec
	if err := json.Unmarshal(verified.Spec, &spec); err != nil {
		return ProfileRead{}, errors.New("invalid canonical profile")
	}
	return ProfileRead{ID: verified.ID, TenantID: verified.TenantID, ProfileID: verified.ProfileID,
		RevisionNumber: verified.RevisionNumber, SourceDraftRevision: verified.SourceDraftRevision,
		SchemaVersion: verified.SchemaVersion, CredentialProtocolVersion: spec.CredentialProtocolVersion,
		Config: configFromSpec(spec), SpecDigest: verified.SpecDigest, PublishedBy: verified.PublishedBy, PublishedAt: &verified.PublishedAt}, nil
}
func (s *Service) GetCredentialRevision(ctx context.Context, tenant, profile, user string, number int64) (ProfileRead, error) {
	if err := s.authorize(ctx, tenant, user); err != nil {
		return ProfileRead{}, err
	}
	if err := s.credentialDependencies(); err != nil {
		return ProfileRead{}, err
	}
	if number <= 0 {
		return ProfileRead{}, ErrProfileRevisionNotFound
	}
	var result ProfileRead
	err := s.deps.Credentials.WithinProfile(ctx, tenant, profile, func(tx CredentialTransaction) error {
		revision, err := tx.GetRevision(ctx, number)
		if err != nil {
			return err
		}
		result, err = PublishedProfileRead(revision)
		if err != nil {
			return err
		}
		var spec domain.Spec
		if err := json.Unmarshal(revision.Spec, &spec); err != nil {
			return errors.New("invalid canonical profile")
		}
		result.CredentialStates, err = s.credentialStates(ctx, tx, tenant, profile, revision.ID, spec)
		return err
	})
	if err != nil {
		return ProfileRead{}, err
	}
	return result, nil
}
