package application

import (
	"context"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"regexp"
)

// Final proof grants only immutable Manifest Artifact credentials, never model keys or a live lease.
type ResolveFinalArtifactInput struct {
	Final                 executionv1.FinalRequest `json:"final"`
	TenantID              string                   `json:"tenant_id"`
	ManifestID            string                   `json:"manifest_id"`
	ManifestDigest        string                   `json:"manifest_digest"`
	DeploymentID          string                   `json:"deployment_id"`
	DeploymentRevisionID  string                   `json:"deployment_revision_id"`
	ProfileID             string                   `json:"profile_id"`
	ProfileRevisionNumber int64                    `json:"profile_revision_number"`
}
type FinalArtifactAuthorizer interface {
	AuthorizeFinalArtifact(context.Context, string, ResolveFinalArtifactInput) ([]CredentialUse, error)
}

func (v ResolveFinalArtifactInput) Validate() error {
	if v.Final.Validate() != nil || !validSHA256Digest(v.ManifestDigest) || v.ProfileRevisionNumber < 1 || v.ProfileRevisionNumber > 9007199254740991 {
		return domain.ErrCredentialInput
	}
	for _, id := range []string{v.TenantID, v.ManifestID, v.DeploymentID, v.DeploymentRevisionID, v.ProfileID} {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`).MatchString(id) {
			return domain.ErrCredentialInput
		}
	}
	return nil
}
func DecodeResolveFinalArtifact(b []byte) (ResolveFinalArtifactInput, error) {
	var v ResolveFinalArtifactInput
	if e := decodeCredentialJSON(b, &v); e != nil {
		return v, domain.ErrCredentialInput
	}
	return v, v.Validate()
}
func (s *Service) ResolveForFinalArtifact(ctx context.Context, worker string, in ResolveFinalArtifactInput) (CredentialBatch, error) {
	if e := in.Validate(); e != nil {
		return CredentialBatch{}, e
	}
	if worker == "" || s.deps.FinalArtifacts == nil {
		return CredentialBatch{}, ErrExecutionUnauthorized
	}
	if e := s.credentialDependencies(); e != nil {
		return CredentialBatch{}, e
	}
	uses, e := s.deps.FinalArtifacts.AuthorizeFinalArtifact(ctx, worker, in)
	if e != nil {
		return CredentialBatch{}, classifyExecutionVerification(e)
	}
	if len(uses) != 2 || uses[0].Purpose != "access_key_id" || uses[1].Purpose != "secret_access_key" || uses[0].CredentialID == uses[1].CredentialID || uses[0].AudienceDigest != uses[1].AudienceDigest {
		return CredentialBatch{}, ErrExecutionUnauthorized
	}
	out := CredentialBatch{TenantID: in.TenantID, ProfileID: in.ProfileID, ProfileRevisionNumber: in.ProfileRevisionNumber, RunID: in.Final.RunID, AttemptID: in.Final.AttemptID, WorkerID: worker, ManifestID: in.ManifestID, ManifestDigest: in.ManifestDigest}
	e = s.deps.Credentials.WithinProfile(ctx, in.TenantID, in.ProfileID, func(tx CredentialTransaction) error {
		records, e := s.checkedCredentialRecords(ctx, tx, in.TenantID, in.ProfileID, in.ProfileRevisionNumber, uses)
		if e != nil {
			return e
		}
		for i, r := range records {
			v, e := s.deps.Cipher.Decrypt(ctx, r.AssociatedData(), r.Ciphertext)
			if e != nil {
				return domain.ErrCredentialUnavailable
			}
			out.Credentials = append(out.Credentials, ResolvedCredential{Use: uses[i], CredentialRevision: r.Revision, Value: v})
		}
		return nil
	})
	if e != nil {
		out.Clear()
		return CredentialBatch{}, e
	}
	return out, nil
}
