package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	channelspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/postgres"
	channeltext "github.com/liuzengh/trpc-agent-service/trpcservice/channels/text"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// This file wires one WeCom bot into the process.
//
// It is off unless an operator turns it on, and turning it on takes a whole
// static configuration rather than a flag: a tenant, an app, a binding and a
// bot account, all decided server-side. None of it can be set through the admin
// API, because an inbound channel decides which tenant an unsolicited message
// is charged to — that mapping belongs to whoever deployed the process, not to
// the tenants it serves.
const (
	wecomEnabledEnvVar = "TRPC_SERVICE_WECOM_ENABLED"
	wecomTenantEnvVar  = "TRPC_SERVICE_WECOM_TENANT_ID"
	wecomAppEnvVar     = "TRPC_SERVICE_WECOM_APP_ID"
	wecomBindingEnvVar = "TRPC_SERVICE_WECOM_BINDING_ID"
	wecomBotEnvVar     = "TRPC_SERVICE_WECOM_BOT_ID"
)

// wecomSecretRef names the bot Secret, and it is fixed rather than configurable.
//
// TRPC_SERVICE_ is the reserved namespace no tenant may point at: the
// entitlement table refuses those references by construction, so that a
// published revision cannot name a credential belonging to this process. This
// reference is not tenant configuration — it is the static binding of the
// operator — so it is granted below, in code, to exactly one tenant, and never
// through Entitlements. A configurable variable name would put the choice of
// which reserved variable to read back into configuration.
const wecomSecretRef = "env:TRPC_SERVICE_WECOM_BOT_SECRET"

// errWeComConfig is the sentinel behind every refusal in this file, so a caller
// can tell a misconfigured channel from an unreachable one.
var errWeComConfig = errors.New("wecom: invalid configuration")

// wecomConfig is the channel configuration of one process.
type wecomConfig struct {
	enabled bool
	binding wecom.Binding
}

// loadWeComConfig reads that configuration. Disabled is the default and reads
// nothing further: a process that does not run the channel neither requires the
// other variables nor resolves a bot Secret.
func loadWeComConfig(getenv func(string) string) (wecomConfig, error) {
	enabled, err := parseWeComEnabled(getenv(wecomEnabledEnvVar))
	if err != nil || !enabled {
		return wecomConfig{}, err
	}
	binding := wecom.Binding{
		TenantID:   getenv(wecomTenantEnvVar),
		AgentAppID: getenv(wecomAppEnvVar),
		BindingID:  getenv(wecomBindingEnvVar),
		BotID:      getenv(wecomBotEnvVar),
		SecretRef:  wecomSecretRef,
	}
	if err := binding.Validate(); err != nil {
		return wecomConfig{}, fmt.Errorf("%w: %w", errWeComConfig, err)
	}
	return wecomConfig{enabled: true, binding: binding}, nil
}

// parseWeComEnabled matches exactly, like the storage profile does. An operator
// who wrote TRUE or yes gets a refusal rather than a process that quietly did
// not connect the bot it was deployed to serve.
func parseWeComEnabled(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf(
			"%w: unknown %s %q (want \"true\" or \"false\"; leave it unset for \"false\")",
			errWeComConfig, wecomEnabledEnvVar, value)
	}
}

// describe reports what was configured, for the startup log. It names the
// platform ids this log already carries elsewhere and nothing else: the bot id
// is an external account identifier and the Secret is never rendered at all.
func (c wecomConfig) describe() string {
	if !c.enabled {
		return "disabled"
	}
	return fmt.Sprintf("enabled for tenant %q app %q binding %q",
		c.binding.TenantID, c.binding.AgentAppID, c.binding.BindingID)
}

// wecomSecretAuthorizer entitles one tenant to one reference, exactly.
//
// It is the private grant described at wecomSecretRef, and it is a value with
// one method so that there is nothing to widen: no table, no pattern, and no
// way to add a second reference after startup.
type wecomSecretAuthorizer struct {
	tenantID string
	ref      string
}

// AuthorizeSecretRef implements security.SecretRefAuthorizer. It returns the
// platform sentinel, which names neither the tenant nor the reference: a caller
// able to tell a wrong tenant from a wrong reference would have a probe for
// this grant built out of nothing but refusals.
func (a wecomSecretAuthorizer) AuthorizeSecretRef(tenantID string, ref string) error {
	if a.tenantID == "" || a.ref == "" || tenantID != a.tenantID || ref != a.ref {
		return security.ErrNotEntitled
	}
	return nil
}

// wecomChannel is the running channel: one long connection and one durable
// consumer, started together and stopped together.
type wecomChannel struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	failures chan error
	// observer is nil unless telemetry was turned on. It is owned here because
	// the consumer below is the only thing in this process that records: a
	// provider outliving its one recorder would have nothing left to flush.
	observer *telemetry.Telemetry
}

// startWeComChannel starts the channel, or reports that it is off. A nil
// channel is the disabled case and stop accepts it, so the caller has one path
// either way.
//
// ctx bounds startup only — the migration and the control-plane read. The
// channel itself runs on a context of its own; see below.
func startWeComChannel(
	ctx context.Context,
	cfg storageConfig,
	observability telemetry.Config,
	stack *storageStack,
	runs *sessionrun.Service,
	getenv func(string) string,
) (*wecomChannel, error) {
	channelCfg, err := loadWeComConfig(getenv)
	if err != nil {
		return nil, err
	}
	// Safe to log: describe renders platform ids and never a credential.
	log.Printf("channel wecom %s", channelCfg.describe())
	if !channelCfg.enabled {
		return nil, nil
	}

	// The channel is durable or it is nothing. Every promise it makes — accept
	// once, execute once, send at most once, survive a restart — is a row in a
	// database, and under the in-memory profile those rows, the sessions and the
	// revision pins all vanish with the process. Refused rather than degraded.
	if cfg.profile != profilePostgres || stack.pool == nil {
		return nil, fmt.Errorf("%w: %s=true requires %s=%q",
			errWeComConfig, wecomEnabledEnvVar, storageProfileEnvVar, profilePostgres)
	}
	// Only when enabled, and on the pool the control plane already uses: a
	// process that does not serve this channel creates none of its tables, and
	// one that does keeps them in the same database and the same schema as the
	// tenants and the pins they refer to.
	if err := channelspostgres.Migrate(ctx, stack.pool); err != nil {
		return nil, sessionbackend.Scrub(fmt.Errorf("migrate channel storage: %w", err), cfg.dsn)
	}
	store, err := channelspostgres.New(stack.pool)
	if err != nil {
		return nil, err
	}

	binding := channelCfg.binding
	// The same refusal the consumer applies before every execution, applied once
	// at startup: a misconfigured app should be a process that does not start,
	// not a bot that connects and then answers nothing.
	published, err := stack.repository.ResolveRevision(
		ctx, tenant.TenantContext{TenantID: binding.TenantID}, binding.AgentAppID, "")
	if err != nil {
		return nil, fmt.Errorf("resolve the published revision of the configured wecom app: %w", err)
	}
	if err := checkWeComRevision(published); err != nil {
		return nil, err
	}

	client, err := wecom.New(wecom.Config{
		Binding: binding,
		// The private grant, and the only authorizer that will ever answer yes
		// for this reference.
		Authorizer: wecomSecretAuthorizer{tenantID: binding.TenantID, ref: binding.SecretRef},
		Getenv:     getenv,
	})
	if err != nil {
		return nil, err
	}
	// The platform half. Its identity comes from the binding the client
	// validated; the consumer below is given the one this file read from the
	// environment, and refuses to start if the two are not the same binding.
	adapter, err := wecom.NewAdapter(client)
	if err != nil {
		return nil, err
	}
	// Built only for a channel that is going to run, and after the refusals
	// above: a process that will not serve this bot opens no exporter.
	observer, err := telemetry.Open(ctx, observability)
	if err != nil {
		return nil, err
	}
	consumer, err := channeltext.New(channeltext.Config{
		Identity: channels.BindingIdentity{
			TenantID:   binding.TenantID,
			AgentAppID: binding.AgentAppID,
			BindingID:  binding.BindingID,
			Channel:    channels.ChannelWeCom,
		},
		Adapter:   adapter,
		Store:     store,
		Runs:      runs,
		Revisions: wecomRevisionCheck(stack.repository),
		Telemetry: observer,
	})
	if err != nil {
		return nil, errors.Join(err, observer.Shutdown(ctx))
	}

	// A context of its own, rooted at Background rather than at the startup
	// deadline above, so the channel lives until stop ends it.
	runCtx, cancel := context.WithCancel(context.Background())
	channel := &wecomChannel{
		cancel:   cancel,
		failures: make(chan error, 2),
		observer: observer,
	}
	channel.start(runCtx, "connection", client.Run)
	channel.start(runCtx, "consumer", consumer.Run)
	return channel, nil
}

// start runs one half of the channel, and takes the other half down when it
// ends.
//
// Neither half is useful alone: a consumer polling storage for messages a dead
// connection will never deliver has nothing to do, and a connection accepting
// messages no consumer records would read them off the wire and lose them.
func (w *wecomChannel) start(ctx context.Context, name string, run func(context.Context) error) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer w.cancel()
		err := run(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		// Safe to log: both halves promise a sentinel or a fixed description,
		// never a message, an identifier, a credential or a backend error.
		log.Printf("channel wecom %s stopped: %v", name, err)
		w.failures <- fmt.Errorf("wecom %s: %w", name, err)
	}()
}

// failed reports the first terminal failure of either half, so that a channel
// that has stopped serving stops the process instead of leaving it listening
// with a dead bot. A disabled channel is a nil receiver and returns a nil
// channel, which blocks forever: the caller then has the same select either way.
func (w *wecomChannel) failed() <-chan error {
	if w == nil {
		return nil
	}
	return w.failures
}

// stop ends the channel and waits for both halves to return.
//
// Waiting is the point. The consumer executes through the Runtimes and writes
// through the shared pool, so a shutdown that closed either while it was still
// running would lose the answer it was in the middle of recording.
func (w *wecomChannel) stop() error {
	if w == nil {
		return nil
	}
	w.cancel()
	w.wg.Wait()
	close(w.failures)
	var errs []error
	for err := range w.failures {
		errs = append(errs, err)
	}
	// Flushed after both halves have returned, so the last stage of the last
	// message it handled is queued before the queue is drained. Shutdown is
	// bounded on its own, because a collector that has gone away must not hold
	// the process open.
	errs = append(errs, w.observer.Shutdown(context.Background()))
	return errors.Join(errs...)
}

// wecomRevisionCheck asks the control plane whether one resolved revision may
// execute on this channel.
//
// It is asked again before every execution, not only at startup, because
// publishing is live: an app repointed at a per-tenant backend profile while
// this process runs would otherwise keep answering WeCom out of a session store
// this channel cannot recover from.
func wecomRevisionCheck(repository tenant.Repository) channeltext.RevisionCheck {
	return func(ctx context.Context, tenantID, appID, revisionID string) error {
		revision, err := repository.GetRevision(
			ctx, tenant.TenantContext{TenantID: tenantID}, appID, revisionID)
		if err != nil {
			return err
		}
		return checkWeComRevision(revision)
	}
}

// checkWeComRevision refuses a revision this channel cannot serve durably.
//
// Only the process default backend is supported. A backend profile puts one
// revision on a different session store, while this channel records its Runs
// and its answers in the process database — so the conversation history and the
// durable channel state would sit in two places, one restart away from
// disagreeing about what the bot has already said.
func checkWeComRevision(revision tenant.AgentRevision) error {
	if revision.Config.BackendProfileID != "" {
		return fmt.Errorf(
			"%w: the wecom channel supports only the process default session backend, "+
				"and app %q resolves to a revision naming a backend profile",
			errWeComConfig, revision.AgentAppID)
	}
	return nil
}
