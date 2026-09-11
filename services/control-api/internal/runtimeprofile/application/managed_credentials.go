package application

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// ManagedCredentialTargetResolver returns only the digest of a tenant-authorized,
// immutable Memory or Redis Session target. Profile never receives connection secrets.
type ManagedCredentialTargetResolver interface {
	ResolveManagedCredentialAudience(context.Context, string, string, uint64, string) (string, error)
}

func (s *Service) bindManagedStorageCredentialTargets(ctx context.Context, tenant string, input ProfileWrite, next *domain.Spec, previous domain.Spec) error {
	for name, r := range next.Storage {
		if !r.Kind.Managed() {
			continue
		}

		old := previous.Storage[name]
		if old.Kind != r.Kind {
			old = domain.StorageResource{}
		}
		needed := false
		purposes := map[string]string{"dsn_password": old.DSNCredentialID}
		if r.Kind == domain.StorageKindManagedArtifact {
			purposes = map[string]string{"access_key_id": old.AccessKeyIDCredentialID, "secret_access_key": old.SecretAccessKeyCredentialID}
		}
		for purpose, id := range purposes {
			a := input.Credentials["storage"][name][purpose]
			if a.Action == "replace" || (a.Action != "clear" && id != "") {
				needed = true
			}
		}
		if !needed {
			continue
		}
		if s.deps.ManagedCredentialTargets == nil {
			return ErrManagedBackend
		}
		digest, err := s.deps.ManagedCredentialTargets.ResolveManagedCredentialAudience(ctx, tenant, r.BackendID, r.BackendRevision, r.Kind.Role())
		if err != nil || !validSHA256Digest(digest) {
			return ErrManagedBackend
		}
		r.CredentialAudienceDigest = digest
		next.Storage[name] = r
	}
	for name, r := range next.Knowledge {
		if r.Kind != domain.KnowledgeKindManaged {
			continue
		}
		a := input.Credentials["knowledge"][name]["qdrant_api_key"]
		old := previous.Knowledge[name]
		if old.Kind != r.Kind {
			old = domain.KnowledgeResource{}
		}
		if a.Action == "clear" || (a.Action != "replace" && old.QdrantAPIKeyCredentialID == "") {
			continue
		}
		if s.deps.ManagedCredentialTargets == nil {
			return ErrManagedBackend
		}
		digest, err := s.deps.ManagedCredentialTargets.ResolveManagedCredentialAudience(ctx, tenant, r.BackendID, r.BackendRevision, "knowledge")
		if err != nil || !validSHA256Digest(digest) {
			return ErrManagedBackend
		}
		r.CredentialAudienceDigest = digest
		next.Knowledge[name] = r
	}
	return nil
}
