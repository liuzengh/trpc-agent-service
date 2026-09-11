package application

import (
	"context"
	"errors"
	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	"time"
)

var ErrManifestDistributionUnavailable = errors.New("manifest distribution unavailable")
var ErrManifestOutboxLeaseLost = errors.New("manifest outbox lease lost")

type ManifestClaim struct {
	TenantID, EventID, DeploymentID, SchemaVersion, PayloadDigest, ClaimToken string
	RevisionNumber                                                            int64
	Attempt                                                                   int
	Payload                                                                   []byte
}
type ManifestOutbox interface {
	ClaimManifest(context.Context) (ManifestClaim, bool, error)
	FinishManifest(context.Context, ManifestClaim, bool, string, time.Duration) error
}
type ManifestPublisher interface {
	PublishManifest(context.Context, string, []byte) error
}
type ManifestRelay struct {
	outbox    ManifestOutbox
	publisher ManifestPublisher
}

func NewManifestRelay(outbox ManifestOutbox, publisher ManifestPublisher) (*ManifestRelay, error) {
	if outbox == nil || publisher == nil {
		return nil, ErrManifestDistributionUnavailable
	}
	return &ManifestRelay{outbox, publisher}, nil
}
func (r *ManifestRelay) Step(ctx context.Context) (bool, error) {
	claim, found, err := r.outbox.ClaimManifest(ctx)
	if err != nil || !found {
		return found, err
	}
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(claim.Payload)
	if err != nil && !errors.Is(err, controleventsv1.ErrInvalidManifestEvent) {
		return true, err
	}
	digest, digestErr := controleventsv1.ManifestEventDigest(claim.Payload)
	if err != nil || digestErr != nil || digest != claim.PayloadDigest || event.EventID != claim.EventID || event.TenantID != claim.TenantID || event.DeploymentID != claim.DeploymentID || event.RevisionNumber != claim.RevisionNumber || claim.SchemaVersion != "v1" {
		return true, r.outbox.FinishManifest(ctx, claim, false, "MANIFEST_OUTBOX_INTEGRITY", 0)
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = r.publisher.PublishManifest(publishCtx, claim.EventID, claim.Payload)
	cancel()
	if err != nil {
		delay := time.Second << min(max(claim.Attempt-1, 0), 6)
		return true, r.outbox.FinishManifest(ctx, claim, false, "MANIFEST_PUBLISH_UNAVAILABLE", min(delay, time.Minute))
	}
	return true, r.outbox.FinishManifest(ctx, claim, true, "", 0)
}
func (r *ManifestRelay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		found, err := r.Step(ctx)
		if err == nil && found {
			continue
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
