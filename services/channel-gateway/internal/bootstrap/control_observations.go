package bootstrap

import (
	"context"

	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	observations "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/accountobservations"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

func (a *App) reportAccounts(ctx context.Context) error {
	return observations.New(accountObservationSource{a}, a.control).Run(ctx)
}

type accountObservationSource struct{ app *App }

// Composition translates read models, while Connection's reporting use case
// owns scheduling, sequencing, batching, retry budgets and shutdown diagnostics.
func (b accountObservationSource) Observations() []c.Observation {
	a := b.app
	out := []c.Observation{}
	for _, account := range a.catalog.Accounts() {
		state, reason := "CONFIG_APPLIED", "NONE"
		var owner *int64
		if !a.catalog.Ready() {
			state, reason = "ERROR", "SOURCE_UNAVAILABLE"
		} else if !account.Enabled {
			state = "DISABLED"
		} else if account.Provider == "telegram" {
			s := a.telegram.Status(account.ID)
			state, reason = s.State, s.Reason
			if s.OwnerEpoch > 0 {
				v := s.OwnerEpoch
				owner = &v
			}
			if state == "" || s.Revision != account.ConnectionRevision {
				state, reason = "CONNECTING", "REGISTRATION_PENDING"
			}
		} else {
			s, ok := a.connections.Status(account.ID)
			if ok && s.Revision == account.ConnectionRevision {
				state, reason = wecomObservation(s)
				if s.Epoch > 0 {
					v := s.Epoch
					owner = &v
				}
			} else {
				state = "CONNECTING"
			}
		}
		out = append(out, c.Observation{ReceiveMode: account.ReceiveMode(), ScopeID: a.controlConfig.ScopeID, SourceEpoch: a.controlConfig.SourceEpoch, TenantID: account.TenantID, AccountID: account.ID, Provider: account.Provider, ConnectionRevision: account.ConnectionRevision, InstanceID: a.instanceID, InstanceEpoch: a.instanceEpoch, State: state, Reason: reason, OwnerEpoch: owner})
	}
	return out
}
func wecomObservation(s connection.Status) (string, string) {
	if s.Ready {
		return "READY", "NONE"
	}
	switch s.Reason {
	case connection.ReasonSource:
		return "ERROR", "SOURCE_UNAVAILABLE"
	case connection.ReasonCredential:
		return "ERROR", "CREDENTIAL_UNAVAILABLE"
	case connection.ReasonInvalid:
		return "ERROR", "CONFIG_INVALID"
	case connection.ReasonLost, connection.ReasonReplaced:
		return "ERROR", "OWNERSHIP_LOST"
	case connection.ReasonShutdown:
		return "ERROR", "SHUTDOWN"
	}
	if s.Phase == connection.PhaseDisabled {
		return "DISABLED", "NONE"
	}
	if s.Phase == connection.PhaseFailed || s.Phase == connection.PhaseBlocked {
		return "ERROR", "PROVIDER_UNAVAILABLE"
	}
	return "CONNECTING", "NONE"
}
