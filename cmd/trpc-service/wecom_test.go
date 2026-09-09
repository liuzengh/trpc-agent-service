package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// env is one process environment, as a getenv.
func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// enabledEnv is a complete channel configuration.
func enabledEnv() map[string]string {
	return map[string]string{
		wecomEnabledEnvVar: "true",
		wecomTenantEnvVar:  "tenant-a",
		wecomAppEnvVar:     "app-a",
		wecomBindingEnvVar: "binding-a",
		wecomBotEnvVar:     "bot-a",
	}
}

func TestWeComIsDisabledUntilItIsAskedFor(t *testing.T) {
	config, err := loadWeComConfig(env(nil))
	require.NoError(t, err)
	require.False(t, config.enabled)
	require.Equal(t, "disabled", config.describe())

	// Disabled reads nothing else, so an incomplete configuration beside it is
	// not an error and no Secret is resolved.
	config, err = loadWeComConfig(env(map[string]string{wecomTenantEnvVar: "tenant-a"}))
	require.NoError(t, err)
	require.False(t, config.enabled)

	_, err = loadWeComConfig(env(map[string]string{wecomEnabledEnvVar: "TRUE"}))
	require.ErrorIs(t, err, errWeComConfig)

	values := enabledEnv()
	delete(values, wecomBotEnvVar)
	_, err = loadWeComConfig(env(values))
	require.ErrorIs(t, err, errWeComConfig)

	config, err = loadWeComConfig(env(enabledEnv()))
	require.NoError(t, err)
	require.True(t, config.enabled)
	require.Equal(t, wecomSecretRef, config.binding.SecretRef,
		"the reference is the process own, never configuration")
	require.NotContains(t, config.describe(), config.binding.BotID)
}

// The channel is refused rather than degraded when the storage it needs is not
// there, and refusing costs no connection and no Secret.
func TestWeComRequiresDurableStorage(t *testing.T) {
	channel, err := startWeComChannel(context.Background(),
		storageConfig{profile: profileInMemory}, telemetry.Config{},
		&storageStack{}, nil, env(enabledEnv()))
	require.ErrorIs(t, err, errWeComConfig)
	require.Nil(t, channel)

	// Disabled under the same profile is not an error: it is the default.
	channel, err = startWeComChannel(context.Background(),
		storageConfig{profile: profileInMemory}, telemetry.Config{},
		&storageStack{}, nil, env(nil))
	require.NoError(t, err)
	require.Nil(t, channel)
	require.NoError(t, channel.stop())
}

// The flush happens after both halves have returned, and it is the channel that
// owns it: a record made by the last message this process handled must not be
// dropped because the exporter was closed first.
func TestWeComChannelFlushesWhatItsLoopsRecorded(t *testing.T) {
	exports := make(chan string, 4)
	collector := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case exports <- r.URL.Path:
			default:
			}
		}))
	defer collector.Close()

	observer, err := telemetry.Open(context.Background(),
		telemetry.Config{Enabled: true, Endpoint: collector.URL})
	require.NoError(t, err)
	stages, err := observer.ChannelRecorder(telemetry.Binding{
		TenantID:  "tenant-a",
		AppID:     "app-a",
		BindingID: "binding-a",
		Channel:   channels.ChannelWeCom,
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := &wecomChannel{
		cancel:   cancel,
		failures: make(chan error, 2),
		observer: observer,
	}
	channel.start(runCtx, "consumer", func(ctx context.Context) error {
		<-ctx.Done()
		_, span := stages.Start(ctx, telemetry.StageDeliver)
		span.End(telemetry.Result{Outcome: telemetry.OutcomeSucceeded, RequestID: "req-1"})
		return ctx.Err()
	})

	require.NoError(t, channel.stop())
	require.NotEmpty(t, exports, "stop must export what the loop recorded on its way out")
}

// The bot Secret is granted to one tenant and one reference, in code. Nothing
// else in the process may name it, and the refusal says nothing about which
// half was wrong.
func TestWeComSecretGrantIsExact(t *testing.T) {
	authorizer := wecomSecretAuthorizer{tenantID: "tenant-a", ref: wecomSecretRef}
	require.NoError(t, authorizer.AuthorizeSecretRef("tenant-a", wecomSecretRef))
	for _, refused := range []struct{ tenantID, ref string }{
		{"tenant-b", wecomSecretRef},
		{"tenant-a", "env:TRPC_SERVICE_WECOM_BOT_SECRET_2"},
		{"tenant-a", "env:OTHER_SECRET"},
		{"tenant-a", ""},
		{"", wecomSecretRef},
	} {
		require.ErrorIs(t,
			authorizer.AuthorizeSecretRef(refused.tenantID, refused.ref),
			security.ErrNotEntitled)
	}

	// And the platform table cannot grant it however it is spelled, which is
	// why the grant above exists at all.
	_, err := security.NewEntitlements(security.Grant{
		TenantID:   "tenant-a",
		SecretRefs: []string{wecomSecretRef},
	})
	require.Error(t, err)
}

// A revision that names its own session backend would put this conversation
// somewhere the durability promise does not cover.
func TestWeComRefusesARevisionWithItsOwnBackend(t *testing.T) {
	require.NoError(t, checkWeComRevision(tenant.AgentRevision{}))
	err := checkWeComRevision(tenant.AgentRevision{
		AgentAppID: "app-a",
		Config:     tenant.RevisionConfig{BackendProfileID: "profile-a"},
	})
	require.ErrorIs(t, err, errWeComConfig)
}

// A channel that has failed terminally stops the process. Rejected credentials
// and a takeover by another process both end the connection for good, and a
// service that kept serving HTTP afterwards would look healthy while answering
// no message at all.
func TestWeComFailureStopsTheProcess(t *testing.T) {
	failure := errors.New("the connection was taken over")
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := &wecomChannel{cancel: cancel, failures: make(chan error, 2)}
	channel.start(runCtx, "connection", func(context.Context) error { return failure })

	server := &fakeHTTPServerLifecycle{}
	err := waitForStop(context.Background(), make(chan error), channel.failed(), server, time.Second)
	require.ErrorIs(t, err, failure, "the reason the process exited must survive the shutdown")
	// Drained rather than cut: HTTP is still healthy here, and a response still
	// in flight holds a runtime lease the cleanup below it then waits for.
	require.Equal(t, 1, server.shutdownCalls)
	require.Zero(t, server.closeCalls)
	// Reported once. stop runs after this, and finds nothing left to join.
	require.NoError(t, channel.stop())

	// Disabled is a nil channel, which never fails and never shortens the wait.
	var disabled *wecomChannel
	require.Nil(t, disabled.failed())
	server = &fakeHTTPServerLifecycle{}
	signalCtx, signalled := context.WithCancel(context.Background())
	signalled()
	require.NoError(t,
		waitForStop(signalCtx, make(chan error), disabled.failed(), server, time.Second))
	require.Equal(t, 1, server.shutdownCalls)
	require.Zero(t, server.closeCalls)
}
