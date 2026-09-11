package wire

import (
	"encoding/json"
	"github.com/gowebpki/jcs"
	events "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	values "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

func Decode(raw []byte) (domain.Publication, error) {
	e, err := events.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		return domain.Publication{}, err
	}
	digest, err := events.ManifestEventDigest(raw)
	if err != nil {
		return domain.Publication{}, err
	}
	envelope, err := json.Marshal(e.Manifest)
	if err != nil {
		return domain.Publication{}, err
	}
	envelope, err = jcs.Transform(envelope)
	if err != nil {
		return domain.Publication{}, err
	}
	return domain.Publication{EventID: e.EventID, EventDigest: digest, TenantID: e.TenantID, ManifestID: e.Manifest.ID, DeploymentRevisionID: e.DeploymentRevisionID, ContentDigest: e.Manifest.ContentDigest, EnvelopeDigest: values.Digest(envelope), Envelope: envelope}, nil
}
