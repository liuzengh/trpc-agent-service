package application

import (
	"errors"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func canonicalStoredRevision(revision domain.ProfileRevision) (domain.ProfileRevision, error) {
	canonical, report := domain.ValidateForPublication(revision.Spec, revision.SourceDraftRevision)
	if !report.Valid || canonical.Digest != revision.SpecDigest ||
		canonical.SchemaVersion != revision.SchemaVersion {
		return domain.ProfileRevision{}, errors.New("stored runtime profile revision failed integrity validation")
	}
	revision.Spec = canonical.Document
	return revision, nil
}
