package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/channelstest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// One row in each of the three scopes a scan can see: another tenant, another
// binding of the same tenant, and the consumer's own binding. The identifiers
// are chosen so both foreign rows sort ahead of the own row under the scans'
// tenant_id, id ordering, which is what makes a LIMIT of one a real test: a
// scan that filtered its page in Go would return nothing here.
const (
	foreignTenant = "tenant-a"
	scopeTenant   = "tenant-b"
	scopeBinding  = "binding-own"
)

func scopedRequest(tenantID, bindingID, suffix string) channels.AcceptRequest {
	request := channelstest.Request(tenantID, suffix, "session-"+suffix, "event-"+suffix)
	request.Envelope.ChannelBindingID = bindingID
	return request
}

// seedScopes accepts one Run in each scope and returns them in scan order.
func seedScopes(
	t *testing.T, ctx context.Context, store channels.Store,
) []channels.AcceptRequest {
	t.Helper()
	requests := []channels.AcceptRequest{
		scopedRequest(foreignTenant, "binding-main", "aaa"),
		scopedRequest(scopeTenant, "binding-other", "bbb"),
		scopedRequest(scopeTenant, scopeBinding, "ccc"),
	}
	for _, request := range requests {
		_, err := store.Accept(
			ctx, tenant.TenantContext{TenantID: request.Envelope.TenantID}, request)
		require.NoError(t, err)
	}
	return requests
}

func claimSeeded(
	t *testing.T, ctx context.Context, store channels.Store, request channels.AcceptRequest,
) channels.RunClaim {
	t.Helper()
	claim, ok, err := store.ClaimNextRun(ctx,
		tenant.TenantContext{TenantID: request.Envelope.TenantID},
		request.Envelope.SessionKey(),
		channels.ClaimRunRequest{
			ClaimToken: "claim-" + request.IDs.RunID,
			ClaimedBy:  "worker-main",
			Now:        request.Now,
		})
	require.NoError(t, err)
	require.True(t, ok)
	return claim
}

func runStatus(
	t *testing.T, ctx context.Context, store channels.Store, tenantID, runID string,
) channels.RunStatus {
	t.Helper()
	run, err := store.GetRun(ctx, tenant.TenantContext{TenantID: tenantID}, runID)
	require.NoError(t, err)
	return run.Status
}

func TestIntegrationScopedRunScansSeeOneBindingOnly(t *testing.T) {
	dsn := requireDSN(t)
	store := newIsolatedStore(t, dsn)
	ctx, cancel := setupContext()
	defer cancel()
	requests := seedScopes(t, ctx, store)
	now := requests[0].Now
	scope := channels.ScanScope{TenantID: scopeTenant, BindingID: scopeBinding}

	platform, err := store.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: now, StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, platform, 3, "the zero scope is still the platform scan")

	scoped, err := store.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: now, StaleAfter: time.Minute, Scope: scope, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.RunDispatch{{
		RunRef: channels.RunRef{TenantID: scopeTenant, RunID: "run-ccc"},
	}}, scoped, "the two rows sorting ahead belong to someone else")

	for _, request := range requests {
		claimSeeded(t, ctx, store, request)
	}
	expired := now.Add(2 * time.Minute)
	recovered, err := store.RecoverRuns(ctx, channels.RecoverRequest{
		Now: expired, Scope: scope, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.RunRecovery{{
		RunRef:  channels.RunRef{TenantID: scopeTenant, RunID: "run-ccc"},
		Outcome: channels.RequeueRetried,
	}}, recovered)
	require.Equal(t, channels.RunRunning, runStatus(t, ctx, store, foreignTenant, "run-aaa"),
		"another tenant's expired claim is not this consumer's to requeue")
	require.Equal(t, channels.RunRunning, runStatus(t, ctx, store, scopeTenant, "run-bbb"),
		"another binding of the same tenant is just as foreign")
}

func TestIntegrationScopedOutboxScansSeeOneBindingOnly(t *testing.T) {
	dsn := requireDSN(t)
	store := newIsolatedStore(t, dsn)
	ctx, cancel := setupContext()
	defer cancel()
	requests := seedScopes(t, ctx, store)
	now := requests[0].Now
	scope := channels.ScanScope{TenantID: scopeTenant, BindingID: scopeBinding}

	outboxIDs := map[string]string{"run-aaa": "out-aaa", "run-bbb": "out-bbb", "run-ccc": "out-ccc"}
	for _, request := range requests {
		claim := claimSeeded(t, ctx, store, request)
		_, err := store.FinishRun(ctx,
			tenant.TenantContext{TenantID: request.Envelope.TenantID},
			channels.FinishRunRequest{
				Token: channels.RunToken{
					RunID:      claim.Run.RunID,
					ClaimToken: claim.Run.ClaimToken,
				},
				Status:     channels.RunSucceeded,
				RevisionID: "revision-main",
				Stats:      channels.RunStats{EventCount: 2, OutputParts: 1},
				Outbox: []channels.OutboxDraft{{
					OutboxID:        outboxIDs[request.IDs.RunID],
					ClientMessageID: "client-" + request.IDs.RunID,
					MaxAttempts:     3,
					Message:         channels.OutboundMessage{Text: "answer"},
					DeliveryTarget:  request.Envelope.DeliveryTarget,
				}},
				Now: now.Add(time.Second),
			})
		require.NoError(t, err)
	}
	due := now.Add(time.Minute)

	platform, err := store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now: due, StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, platform, 3, "the zero scope is still the platform scan")

	scoped, err := store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now: due, StaleAfter: time.Minute, Scope: scope, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.OutboxDispatch{{
		OutboxRef: channels.OutboxRef{TenantID: scopeTenant, OutboxID: "out-ccc"},
		Channel:   channels.ChannelFeishu,
	}}, scoped)

	for _, request := range requests {
		outboxID := outboxIDs[request.IDs.RunID]
		_, ok, err := store.ClaimOutbox(ctx,
			tenant.TenantContext{TenantID: request.Envelope.TenantID},
			channels.ClaimOutboxRequest{
				OutboxID:    outboxID,
				SendToken:   "send-" + outboxID,
				SentBy:      "sender-main",
				SendTimeout: 20 * time.Second,
				Now:         due,
			})
		require.NoError(t, err)
		require.True(t, ok)
	}
	expired := due.Add(time.Minute)
	recovered, err := store.RecoverOutbox(ctx, channels.RecoverRequest{
		Now: expired, Scope: scope, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.OutboxRecovery{{
		OutboxRef: channels.OutboxRef{TenantID: scopeTenant, OutboxID: "out-ccc"},
		Outcome:   channels.RequeueRetried,
	}}, recovered)
	for _, foreign := range []struct{ tenantID, outboxID string }{
		{foreignTenant, "out-aaa"}, {scopeTenant, "out-bbb"},
	} {
		part, err := store.GetOutboxPart(
			ctx, tenant.TenantContext{TenantID: foreign.tenantID}, foreign.outboxID)
		require.NoError(t, err)
		require.Equal(t, channels.OutboxSending, part.Status,
			"a foreign send in flight is not this consumer's to take back")
		require.False(t, part.DuplicateRisk)
	}
}
