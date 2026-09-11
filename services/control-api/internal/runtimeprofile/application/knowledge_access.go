package application

import (
	"context"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

type OwnerKnowledgeCredentialsCommand struct {
	CheckProfileCredentialsCommand
	ResourceName string
}

// ResolveKnowledgeForOwner is an internal publication-based port, not a secret HTTP API.
func (s *Service) ResolveKnowledgeForOwner(ctx context.Context, c OwnerKnowledgeCredentialsCommand) (CredentialBatch, error) {
	if err := s.credentialDependencies(); err != nil {
		return CredentialBatch{}, err
	}
	if err := s.authorizeOwner(ctx, c.TenantID, c.ActorUserID); err != nil {
		return CredentialBatch{}, err
	}
	if c.ResourceName == "" || len(c.Uses) != 2 || c.Uses[0].Purpose != "qdrant_api_key" || c.Uses[1].Purpose != "embedding_api_key" {
		return CredentialBatch{}, domain.ErrCredentialAssociation
	}
	result := CredentialBatch{TenantID: c.TenantID, ProfileID: c.ProfileID, ProfileRevisionNumber: c.ProfileRevisionNumber}
	err := s.deps.Credentials.WithinProfile(ctx, c.TenantID, c.ProfileID, func(tx CredentialTransaction) error {
		if err := s.authorizeOwner(ctx, c.TenantID, c.ActorUserID); err != nil {
			return err
		}
		records, err := s.checkedCredentialRecords(ctx, tx, c.TenantID, c.ProfileID, c.ProfileRevisionNumber, c.Uses)
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Category != "knowledge" || r.ResourceName != c.ResourceName {
				return domain.ErrCredentialAssociation
			}
		}
		for i, r := range records {
			value, err := s.deps.Cipher.Decrypt(ctx, r.AssociatedData(), r.Ciphertext)
			if err != nil {
				return errors.New("resolve knowledge credential")
			}
			result.Credentials = append(result.Credentials, ResolvedCredential{Use: c.Uses[i], CredentialRevision: r.Revision, Value: value})
		}
		return nil
	})
	if err != nil {
		result.Clear()
		return CredentialBatch{}, err
	}
	return result, nil
}
