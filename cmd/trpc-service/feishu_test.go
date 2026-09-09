package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// feishuEnabledEnv is a complete channel configuration. Its tenant and binding
// match enabledEnv, so the two collide unless a case changes one of them.
func feishuEnabledEnv() map[string]string {
	return map[string]string{
		feishuEnabledEnvVar:     "true",
		feishuTenantEnvVar:      "tenant-a",
		feishuAppEnvVar:         "app-a",
		feishuBindingEnvVar:     "binding-a",
		feishuExternalAppEnvVar: "cli_a1b2c3d4",
	}
}

// mergedEnv is one process environment built from several channels' variables.
func mergedEnv(parts ...map[string]string) func(string) string {
	values := map[string]string{}
	for _, part := range parts {
		for name, value := range part {
			values[name] = value
		}
	}
	return env(values)
}

// withValue returns values with one variable changed.
func withValue(values map[string]string, name, value string) map[string]string {
	values[name] = value
	return values
}

func TestFeishuIsDisabledUntilItIsAskedFor(t *testing.T) {
	var asked []string
	config, err := loadFeishuConfig(func(name string) string {
		asked = append(asked, name)
		return ""
	})
	require.NoError(t, err)
	require.False(t, config.enabled)
	require.Equal(t, "disabled", config.describe())
	// The default reads one variable and stops: no Secret is resolved, and an
	// incomplete configuration left beside it is not an error.
	require.Equal(t, []string{feishuEnabledEnvVar}, asked)

	_, err = loadFeishuConfig(env(map[string]string{feishuEnabledEnvVar: "yes"}))
	require.ErrorIs(t, err, errFeishuConfig)

	values := feishuEnabledEnv()
	delete(values, feishuExternalAppEnvVar)
	_, err = loadFeishuConfig(env(values))
	require.ErrorIs(t, err, errFeishuConfig)

	config, err = loadFeishuConfig(env(feishuEnabledEnv()))
	require.NoError(t, err)
	require.True(t, config.enabled)
	require.Equal(t, feishuSecretRef, config.binding.SecretRef,
		"the reference is the process own, never configuration")
	require.NotContains(t, config.describe(), config.binding.AppID)
}

// The channel is refused rather than degraded when the storage it needs is not
// there, and refusing costs no connection and no Secret.
func TestFeishuRequiresDurableStorage(t *testing.T) {
	channel, err := startFeishuChannel(context.Background(),
		storageConfig{profile: profileInMemory}, telemetry.Config{},
		&storageStack{}, nil, env(feishuEnabledEnv()))
	require.ErrorIs(t, err, errFeishuConfig)
	require.Nil(t, channel)

	channel, err = startFeishuChannel(context.Background(),
		storageConfig{profile: profileInMemory}, telemetry.Config{},
		&storageStack{}, nil, env(nil))
	require.NoError(t, err)
	require.Nil(t, channel)
	require.NoError(t, channel.stop())
}

// One binding's durable rows belong to one channel: the Store's uniqueness and
// its scan scope are (tenant, binding), so two enabled channels sharing a pair
// would claim each other's messages and then refuse to execute them.
func TestTwoChannelsMayNotShareOneBinding(t *testing.T) {
	cases := []struct {
		name     string
		getenv   func(string) string
		rejected bool
	}{
		{"the same tenant and binding", mergedEnv(enabledEnv(), feishuEnabledEnv()), true},
		{"another tenant", mergedEnv(enabledEnv(),
			withValue(feishuEnabledEnv(), feishuTenantEnvVar, "tenant-b")), false},
		{"another binding", mergedEnv(enabledEnv(),
			withValue(feishuEnabledEnv(), feishuBindingEnvVar, "binding-b")), false},
		{"a disabled peer", mergedEnv(
			withValue(enabledEnv(), wecomEnabledEnvVar, "false"), feishuEnabledEnv()), false},
		{"a disabled channel", mergedEnv(enabledEnv(),
			withValue(feishuEnabledEnv(), feishuEnabledEnvVar, "false")), false},
		{"neither channel", env(nil), false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := checkChannelBindings(test.getenv)
			if !test.rejected {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errFeishuConfig)
			require.Contains(t, err.Error(), feishuBindingEnvVar)
		})
	}
	// A configuration neither channel could start on is reported as itself.
	require.ErrorIs(t,
		checkChannelBindings(env(map[string]string{wecomEnabledEnvVar: "TRUE"})), errWeComConfig)
}

// A revision that names its own session backend would put this conversation
// somewhere the durability promise does not cover.
func TestFeishuRefusesARevisionWithItsOwnBackend(t *testing.T) {
	require.NoError(t, checkFeishuRevision(tenant.AgentRevision{}))
	require.ErrorIs(t, checkFeishuRevision(tenant.AgentRevision{
		AgentAppID: "app-a",
		Config:     tenant.RevisionConfig{BackendProfileID: "profile-a"},
	}), errFeishuConfig)
}

// A channel that has failed terminally stops the process, and the second
// channel is no different from the first: HTTP is still healthy, so it is
// drained rather than cut, and the reason survives the shutdown.
func TestFeishuFailureStopsTheProcess(t *testing.T) {
	failure := errors.New("the connection was taken over")
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := &feishuChannel{cancel: cancel, failures: make(chan error, 1)}
	channel.wg.Add(1)
	go func() {
		defer channel.wg.Done()
		channel.failures <- failure
	}()

	server := &fakeHTTPServerLifecycle{}
	err := waitForStop(context.Background(),
		make(chan error), nil, server, time.Second, channel.failed())
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, server.shutdownCalls)
	require.Zero(t, server.closeCalls)

	// Disabled is a nil channel, which never fails and never shortens the wait.
	var disabled *feishuChannel
	require.Nil(t, disabled.failed())
	require.NoError(t, disabled.stop())
	server = &fakeHTTPServerLifecycle{}
	signalCtx, signalled := context.WithCancel(context.Background())
	signalled()
	require.NoError(t, waitForStop(signalCtx,
		make(chan error), nil, server, time.Second, disabled.failed()))
	require.Equal(t, 1, server.shutdownCalls)
}
