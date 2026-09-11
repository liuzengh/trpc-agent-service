package application

import (
	"errors"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func canonicalStoredVersion(version domain.AgentVersion) (domain.AgentVersion, error) {
	canonical, report := domain.ValidateForPublication(version.Spec, version.SourceDraftRevision)
	if !report.Valid || canonical.Digest != version.SpecDigest ||
		canonical.SchemaVersion != version.SchemaVersion {
		return domain.AgentVersion{}, errors.New("stored agent version failed integrity validation")
	}
	version.Spec = canonical.Document
	return version, nil
}
