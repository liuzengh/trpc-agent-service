package config

// RuntimeBackends prevents non-executing roles from opening Session or queue
// clients solely because a shared ConfigMap contains their settings.
type RuntimeBackends struct {
	Session         SessionConfig
	Coordinator     CoordinatorConfig
	PollCoordinator CoordinatorConfig
	Idempotency     IdempotencyConfig
	Queue           QueueConfig
	Quota           QuotaConfig
}

func LoadRuntimeBackends(roles Roles, hasMCPPoller bool) (RuntimeBackends, error) {
	result := RuntimeBackends{Session: SessionConfig{Backend: SessionBackendInMemory}, Coordinator: CoordinatorConfig{Backend: CoordinatorBackendLocal}, PollCoordinator: CoordinatorConfig{Backend: CoordinatorBackendLocal}, Idempotency: IdempotencyConfig{Backend: IdempotencyBackendLocal, CompletedTTL: defaultIdempotencyCompletedTTL}, Queue: QueueConfig{Backend: QueueBackendMemory}, Quota: QuotaConfig{Backend: QuotaBackendLocal}}
	var err error
	if roles.Worker || roles.Jobs {
		result.Session, err = LoadSessionConfigFromEnv()
		if err != nil {
			return result, err
		}
	}
	if roles.Worker {
		result.Coordinator, err = LoadCoordinatorConfigFromEnv()
		if err != nil {
			return result, err
		}
		result.Idempotency, err = LoadIdempotencyConfigFromEnv()
		if err != nil {
			return result, err
		}
	}
	if roles.Gateway && hasMCPPoller {
		result.PollCoordinator, err = LoadCoordinatorConfigFromEnv()
		if err != nil {
			return result, err
		}
		result.PollCoordinator.RedisPrefix += ":channel-poll"
	}
	if roles.Worker || roles.Relay {
		result.Queue, err = LoadQueueConfigFromEnv()
		if err != nil {
			return result, err
		}
	}
	if roles.Worker || roles.Gateway {
		result.Quota, err = LoadQuotaConfigFromEnv()
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
