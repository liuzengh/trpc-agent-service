package postgresadapter

import (
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	"time"
)

// runRequestedPayload is an outbound Adapter conversion. Domain structs do not
// import transport DTOs, and only schema-validated normalized addresses leave the
// Gateway on the durable execution event.
func runRequestedPayload(c domain.Acceptance) ([]byte, error) {
	reply, err := wire.DecodeReplyContext(c.Input.Key.Provider, c.Input.ReplyContext)
	if err != nil {
		return nil, err
	}
	policy := dto.UsagePolicy{}
	if c.Policy != nil {
		policy = dto.UsagePolicy{Revision: c.Policy.Revision, Enabled: c.Policy.Enabled, MaxConcurrentRuns: int64(c.Policy.Execution.MaxConcurrentRuns), TokenPeriodSeconds: c.Policy.Tokens.PeriodSeconds, TokenLimit: c.Policy.Tokens.Limit, TokenReservationPerRun: c.Policy.Tokens.ReservationPerRun, InputMicrosPerMillionTokens: c.Policy.Tokens.InputMicrosPerMTok, OutputMicrosPerMillionTokens: c.Policy.Tokens.OutputMicrosPerMTok}
	}
	return wire.EncodeRunRequested(dto.RunRequested{
		SchemaVersion: 1, EventID: c.Receipt.AdmissionID, AdmissionID: c.Receipt.AdmissionID, RunID: c.Receipt.RunID,
		Route:       dto.RouteSnapshot{Provider: c.Route.Provider, AccountID: c.Route.AccountID, TenantID: c.Route.TenantID, BindingID: c.Route.BindingID, Generation: c.Route.Generation, DeploymentRevisionID: c.Route.DeploymentRevisionID, ManifestRef: c.Route.ManifestRef, ManifestDigest: c.Route.ManifestDigest},
		Input:       dto.Inbound{Key: dto.EventKey{Provider: c.Input.Key.Provider, AccountID: c.Input.Key.AccountID, EventID: c.Input.Key.EventID}, Kind: c.Input.Kind, ConversationID: c.Input.ConversationID, ThreadID: c.Input.ThreadID, SenderID: c.Input.SenderID, Text: c.Input.Text, SourceDigest: c.Input.SourceDigest, ReceivedAt: c.Input.ReceivedAt.Format(time.RFC3339Nano), ReplyContext: reply},
		UsagePolicy: policy,
	})
}
