package admin

import (
	"context"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"time"
)

func (s *Service) workerDependencies(ctx context.Context, tenant, appID string) ([]DependencyCheck, []console.WorkerView) {
	workers, err := s.consoleStore.WorkerObservations(ctx, tenant)
	if err != nil || len(workers) == 0 {
		return nil, nil
	}
	var binding controlplane.BackendBinding
	if appID != "" {
		bindings, err := s.repository.ListBackendBindings(ctx, tenant, appID)
		if err == nil {
			for _, candidate := range bindings {
				if candidate.ResourceType == "session" && candidate.MigrationState == "active" && (candidate.AppID == "" || candidate.AppID == appID) && (binding.ID == "" || candidate.AppID != "") {
					binding = candidate
				}
			}
		}
	}
	encoded, _ := json.Marshal(binding.Config)
	items := []DependencyCheck{{Component: "worker", State: "ready", Source: "shared_worker_record", ObservedAt: time.Now().UTC()}}
	for _, component := range []string{"session", "queue", "quota", "sandbox"} {
		state := "ready"
		observed := time.Now().UTC()
		for _, worker := range workers {
			one := "unknown"
			for _, check := range worker.Checks {
				matched := check.Component == component && check.TenantID == ""
				if component == "session" && appID != "" {
					matched = binding.ID != "" && binding.BackendType == "startup_config" && matched
					if binding.ID != "" && binding.BackendType != "startup_config" {
						matched = check.Component == "session_binding" && check.BindingID == binding.ID && check.TenantID == tenant && check.ConfigHash == console.Hash(string(encoded))
					}
				}
				if matched {
					one = check.State
					if check.ObservedAt.Before(observed) {
						observed = check.ObservedAt
					}
					break
				}
			}
			if one == "unavailable" {
				state = "unavailable"
			} else if one != "ready" && state != "unavailable" {
				state = "unknown"
			}
		}
		items = append(items, DependencyCheck{Component: component, State: state, Source: "shared_worker_record", ObservedAt: observed})
	}
	return items, workers
}
