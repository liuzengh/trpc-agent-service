package bootstrap

import (
	"context"
	"testing"
	"time"

	deliverypg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
)

// Only the synthetic Telegram account in the local HTTP fixture is eligible.
// This is not a production credential owner or a fallback configured by App.New.
type telegramRunnerFixtureEligibility struct{}

func (telegramRunnerFixtureEligibility) InspectAccount(ctx context.Context, k app.AccountKey) (app.SendEligibility, error) {
	if err := ctx.Err(); err != nil {
		return app.SendEligibility{}, err
	}
	return app.SendEligibility{Eligible: k.Provider == "telegram" && k.AccountID == "account"}, nil
}
func startDeliveryRunnerFixture(t *testing.T, ctx context.Context, ledger *deliverypg.Store, dispatcher *app.Dispatcher, eligibility app.AccountEligibility, instance, provider string) func() {
	t.Helper()
	maintenance, err := app.NewMaintainer(ledger, app.MaintenanceOptions{PollInterval: 10 * time.Millisecond, OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := app.NewRunner(ledger, eligibility, dispatcher, maintenance, app.RunnerOptions{InstanceID: instance, Providers: []string{provider}, PollInterval: 10 * time.Millisecond, OperationTimeout: 3 * time.Second, ClaimLease: 5 * time.Second, DrainTimeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		runner.Quiesce()
		drain, stop := context.WithTimeout(context.Background(), 4*time.Second)
		defer stop()
		if err := runner.Drain(drain); err != nil {
			t.Error(err)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-drain.Done():
			t.Error("Runner shutdown exceeded deadline")
		}
	}
	t.Cleanup(stop)
	return stop
}
