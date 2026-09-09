package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	channelspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/postgres"
	channeltext "github.com/liuzengh/trpc-agent-service/trpcservice/channels/text"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// This file wires one Feishu bot into the process, on the same terms as the WeCom one
// next to it: off unless an operator turns it on, and turning it on takes a whole
// static configuration decided server-side rather than a flag.
const (
	feishuEnabledEnvVar = "TRPC_SERVICE_FEISHU_ENABLED"
	feishuTenantEnvVar  = "TRPC_SERVICE_FEISHU_TENANT_ID"
	feishuAppEnvVar     = "TRPC_SERVICE_FEISHU_AGENT_APP_ID"
	feishuBindingEnvVar = "TRPC_SERVICE_FEISHU_BINDING_ID"

	feishuExternalAppEnvVar = "TRPC_SERVICE_FEISHU_APP_ID"
)

// feishuSecretRef is fixed rather than configurable, for the reason given at
// wecomSecretRef: TRPC_SERVICE_ is the reserved namespace no tenant may point at, and
// this grant is the operator's static binding rather than tenant configuration.
const feishuSecretRef = "env:TRPC_SERVICE_FEISHU_APP_SECRET"

// channelSecretAuthorizer is the one-tenant-one-reference grant both channels use.
type channelSecretAuthorizer = wecomSecretAuthorizer

// errFeishuConfig is the sentinel behind every refusal in this file.
var errFeishuConfig = errors.New("feishu: invalid configuration")

type feishuConfig struct {
	enabled bool
	binding feishu.Binding
}

func loadFeishuConfig(getenv func(string) string) (feishuConfig, error) {
	enabled, err := parseFeishuEnabled(getenv(feishuEnabledEnvVar))
	if err != nil || !enabled {
		return feishuConfig{}, err
	}
	binding := feishu.Binding{
		TenantID:   getenv(feishuTenantEnvVar),
		AgentAppID: getenv(feishuAppEnvVar),
		BindingID:  getenv(feishuBindingEnvVar),
		AppID:      getenv(feishuExternalAppEnvVar),
		SecretRef:  feishuSecretRef,
	}
	if err := binding.Validate(); err != nil {
		return feishuConfig{}, fmt.Errorf("%w: %w", errFeishuConfig, err)
	}
	return feishuConfig{enabled: true, binding: binding}, nil
}

func parseFeishuEnabled(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf(
			"%w: unknown %s %q (want \"true\" or \"false\"; leave it unset for \"false\")",
			errFeishuConfig, feishuEnabledEnvVar, value)
	}
}

func (c feishuConfig) describe() string {
	if !c.enabled {
		return "disabled"
	}
	return fmt.Sprintf("enabled for tenant %q app %q binding %q",
		c.binding.TenantID, c.binding.AgentAppID, c.binding.BindingID)
}

func checkChannelBindings(getenv func(string) string) error {
	wecomCfg, err := loadWeComConfig(getenv)
	if err != nil {
		return err
	}
	feishuCfg, err := loadFeishuConfig(getenv)
	if err != nil {
		return err
	}
	if !wecomCfg.enabled || !feishuCfg.enabled {
		return nil
	}
	if wecomCfg.binding.TenantID != feishuCfg.binding.TenantID ||
		wecomCfg.binding.BindingID != feishuCfg.binding.BindingID {
		return nil
	}
	return fmt.Errorf(
		"%w: the wecom and feishu channels are both enabled for tenant %q binding %q, "+
			"and one binding's durable rows belong to one channel; give them different "+
			"%s values",
		errFeishuConfig, feishuCfg.binding.TenantID, feishuCfg.binding.BindingID,
		feishuBindingEnvVar)
}

// feishuChannel is the running channel: one durable consumer, which owns the
// long connection through the adapter it was given.
type feishuChannel struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	failures chan error

	observer *telemetry.Telemetry
}

func startFeishuChannel(
	ctx context.Context,
	cfg storageConfig,
	observability telemetry.Config,
	stack *storageStack,
	runs *sessionrun.Service,
	getenv func(string) string,
) (*feishuChannel, error) {
	channelCfg, err := loadFeishuConfig(getenv)
	if err != nil {
		return nil, err
	}

	log.Printf("channel feishu %s", channelCfg.describe())
	if !channelCfg.enabled {
		return nil, nil
	}

	if cfg.profile != profilePostgres || stack.pool == nil {
		return nil, fmt.Errorf("%w: %s=true requires %s=%q",
			errFeishuConfig, feishuEnabledEnvVar, storageProfileEnvVar, profilePostgres)
	}
	if err := channelspostgres.Migrate(ctx, stack.pool); err != nil {
		return nil, sessionbackend.Scrub(fmt.Errorf("migrate channel storage: %w", err), cfg.dsn)
	}
	store, err := channelspostgres.New(stack.pool)
	if err != nil {
		return nil, err
	}

	binding := channelCfg.binding

	published, err := stack.repository.ResolveRevision(
		ctx, tenant.TenantContext{TenantID: binding.TenantID}, binding.AgentAppID, "")
	if err != nil {
		return nil, fmt.Errorf("resolve the published revision of the configured feishu app: %w", err)
	}
	if err := checkFeishuRevision(published); err != nil {
		return nil, err
	}

	client, err := feishu.New(ctx, feishu.Config{
		Binding:    binding,
		Authorizer: channelSecretAuthorizer{tenantID: binding.TenantID, ref: binding.SecretRef},
		Getenv:     getenv,
	})
	if err != nil {
		return nil, err
	}
	adapter, err := feishu.NewAdapter(client, func() {

		log.Print("channel feishu connected")
	})
	if err != nil {
		return nil, err
	}
	observer, err := telemetry.Open(ctx, observability)
	if err != nil {
		return nil, err
	}
	consumer, err := channeltext.New(channeltext.Config{
		Identity: channels.BindingIdentity{
			TenantID:   binding.TenantID,
			AgentAppID: binding.AgentAppID,
			BindingID:  binding.BindingID,
			Channel:    channels.ChannelFeishu,
		},
		Adapter:   adapter,
		Store:     store,
		Runs:      runs,
		Revisions: feishuRevisionCheck(stack.repository),
		Telemetry: observer,
	})
	if err != nil {
		return nil, errors.Join(err, observer.Shutdown(ctx))
	}

	runCtx, cancel := context.WithCancel(context.Background())
	channel := &feishuChannel{
		cancel:   cancel,
		failures: make(chan error, 1),
		observer: observer,
	}

	channel.wg.Add(1)
	go func() {
		defer channel.wg.Done()
		defer channel.cancel()
		err := consumer.Run(runCtx)
		if errors.Is(err, context.Canceled) {
			return
		}

		log.Printf("channel feishu consumer stopped: %v", err)
		channel.failures <- fmt.Errorf("feishu consumer: %w", err)
	}()
	return channel, nil
}

func (f *feishuChannel) failed() <-chan error {
	if f == nil {
		return nil
	}
	return f.failures
}

func (f *feishuChannel) stop() error {
	if f == nil {
		return nil
	}
	f.cancel()
	f.wg.Wait()
	close(f.failures)
	var errs []error
	for err := range f.failures {
		errs = append(errs, err)
	}
	errs = append(errs, f.observer.Shutdown(context.Background()))
	return errors.Join(errs...)
}

func feishuRevisionCheck(repository tenant.Repository) channeltext.RevisionCheck {
	return func(ctx context.Context, tenantID, appID, revisionID string) error {
		revision, err := repository.GetRevision(
			ctx, tenant.TenantContext{TenantID: tenantID}, appID, revisionID)
		if err != nil {
			return err
		}
		return checkFeishuRevision(revision)
	}
}

func checkFeishuRevision(revision tenant.AgentRevision) error {
	if revision.Config.BackendProfileID != "" {
		return fmt.Errorf(
			"%w: the feishu channel supports only the process default session backend, "+
				"and app %q resolves to a revision naming a backend profile",
			errFeishuConfig, revision.AgentAppID)
	}
	return nil
}
