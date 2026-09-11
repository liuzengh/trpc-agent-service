package wire

import (
	protocol "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"time"
)

func Decode(raw []byte) (domain.Requested, error) {
	e, err := protocol.DecodeRunRequested(raw)
	if err != nil {
		return domain.Requested{}, err
	}
	canonical, err := protocol.EncodeRunRequested(e)
	if err != nil {
		return domain.Requested{}, err
	}
	eventDigest := domain.Digest(canonical)
	// Event identity is a separate receipt key. A new EventID pointing to an
	// identical logical Run replays that Run rather than allocating a sequence.
	e.EventID = "logical-run"
	canonical, err = protocol.EncodeRunRequested(e)
	if err != nil {
		return domain.Requested{}, err
	}
	stamp, err := time.Parse(time.RFC3339Nano, e.Input.ReceivedAt)
	if err != nil {
		return domain.Requested{}, err
	}
	original, err := protocol.DecodeRunRequested(raw)
	if err != nil {
		return domain.Requested{}, err
	}
	r := domain.Requested{EventID: original.EventID, EventDigest: eventDigest, RunDigest: domain.Digest(canonical), RunID: e.RunID, AdmissionID: e.AdmissionID, Route: domain.Route{TenantID: e.Route.TenantID, Provider: e.Route.Provider, AccountID: e.Route.AccountID, BindingID: e.Route.BindingID, DeploymentRevisionID: e.Route.DeploymentRevisionID, ManifestRef: e.Route.ManifestRef, ManifestDigest: e.Route.ManifestDigest, Generation: e.Route.Generation}, Input: domain.Input{ConversationID: e.Input.ConversationID, ThreadID: e.Input.ThreadID, SenderID: e.Input.SenderID, Text: e.Input.Text, SourceDigest: e.Input.SourceDigest, ReceivedAt: stamp}, UsagePolicy: domain.UsagePolicy{Revision: e.UsagePolicy.Revision, Enabled: e.UsagePolicy.Enabled, MaxConcurrentRuns: int(e.UsagePolicy.MaxConcurrentRuns), TokenPeriodSeconds: e.UsagePolicy.TokenPeriodSeconds, TokenLimit: e.UsagePolicy.TokenLimit, TokenReservationPerRun: e.UsagePolicy.TokenReservationPerRun, InputMicrosPerMillionTokens: e.UsagePolicy.InputMicrosPerMillionTokens, OutputMicrosPerMillionTokens: e.UsagePolicy.OutputMicrosPerMillionTokens}}
	return r, r.Validate()
}
