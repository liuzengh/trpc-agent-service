package main

import (
	"errors"

	channeloutbound "github.com/liuzengh/trpc-agent-service/trpcservice/channels/outbound"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const defaultChannelReplyOwner = "channel"

// replyRuntime owns the durable Reply Outbox sender and its provider clients.
// It is constructed only by a process that also owns the Channel role. This
// keeps WeCom's binding-scoped long connections out of horizontally scaled
// Worker processes.
type replyRuntime struct {
	providers *channeloutbound.Resolver
	sender    *worker.ReplySender
}

func newReplyRuntime(
	store *postgres.Store,
	redisClient *platformredis.Client,
	owner string,
	wecomResolver channeloutbound.WeComMessageSenderResolver,
	secrets platformsecret.SecretProvider,
	metrics *platformmetrics.Recorder,
) (*replyRuntime, error) {
	if store == nil || redisClient == nil || owner == "" || secrets == nil {
		return nil, errors.New("reply runtime dependencies are required")
	}
	limiter, err := platformredis.NewReplyRateLimiter(
		redisClient,
		defaultReplyRateLimit,
		defaultReplyRateLimitWindow,
	)
	if err != nil {
		return nil, err
	}
	providers, err := channeloutbound.NewResolver(store, secrets, limiter, wecomResolver)
	if err != nil {
		return nil, err
	}
	sender, err := worker.NewReplySender(
		store,
		store.ResolveReplyTarget,
		providers.ResolveReplyProvider,
		worker.ReplySenderOptions{Owner: owner, Metrics: metrics},
	)
	if err != nil {
		return nil, errors.Join(err, providers.Close())
	}
	return &replyRuntime{providers: providers, sender: sender}, nil
}

func (r *replyRuntime) close() error {
	if r == nil || r.providers == nil {
		return nil
	}
	return r.providers.Close()
}

func channelReplyOwner(config serviceConfig) string {
	if config.WorkerID != "" {
		return config.WorkerID
	}
	return defaultChannelReplyOwner
}
