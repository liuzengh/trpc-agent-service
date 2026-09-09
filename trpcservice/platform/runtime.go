package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RuntimeLifecycle interface {
	Acquire() (func(), bool)
	Done() <-chan struct{}
	IsClosing() bool
}

type ComponentRole string
type ComponentLifecycle string

const (
	ComponentGateway     ComponentRole      = "gateway"
	ComponentWorker      ComponentRole      = "worker"
	ComponentDependency  ComponentRole      = "dependency"
	LifecycleHealthy     ComponentLifecycle = "healthy"
	LifecycleDegraded    ComponentLifecycle = "degraded"
	LifecycleUnavailable ComponentLifecycle = "unavailable"
	LifecycleClosing     ComponentLifecycle = "closing"
	LifecycleError       ComponentLifecycle = "error"
)

type RuntimeComponentStatus struct {
	ID        string             `json:"id"`
	Role      ComponentRole      `json:"role"`
	Available bool               `json:"available"`
	Lifecycle ComponentLifecycle `json:"lifecycle"`
	Active    int64              `json:"active_executions"`
	Completed int64              `json:"completed_executions"`
	Failed    int64              `json:"failed_executions"`
}

type RuntimeFaultConfiguration struct {
	Scenario string `json:"scenario"`
	DelayMS  int    `json:"delay_ms"`
}

type runtimeError struct {
	code string
	err  error
}

func (e *runtimeError) Error() string { return e.code }
func (e *runtimeError) Unwrap() error { return e.err }

type Runtime struct {
	platform       ControlPlaneStore
	worker         *StatelessWorker
	life           RuntimeLifecycle
	gates          sessionGates
	global         runtimeCounters
	counterMu      sync.Mutex
	tenantCounters map[string]*runtimeCounters
	faultMu        sync.RWMutex
	faults         map[string]RuntimeFaultConfiguration
	leases         SessionLeaseManager
}

type runtimeCounters struct{ active, complete, failed atomic.Int64 }

// StatelessWorker is the Stage 1 execution boundary. It owns no Tenant,
// Deployment, or Session state and delegates only the resolved request.
type StatelessWorker struct {
	runner    RunnerAdapter
	available atomic.Bool
	lastError atomic.Bool
}

func NewStatelessWorker(runner RunnerAdapter) *StatelessWorker {
	worker := &StatelessWorker{runner: runner}
	worker.available.Store(true)
	return worker
}

func (w *StatelessWorker) Execute(ctx context.Context, request GatewayRequest) (GatewayResponse, error) {
	result, err := w.runner.Run(ctx, runnerRequestFromGateway(request))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			w.lastError.Store(true)
			w.available.Store(false)
		} else if !errors.Is(err, context.Canceled) {
			w.lastError.Store(true)
		}
		return GatewayResponse{}, err
	}
	w.lastError.Store(false)
	return GatewayResponse{
		SessionID:   request.SessionID,
		Output:      result.Output,
		UsageTokens: result.UsageTokens,
		UsageKnown:  result.UsageKnown,
	}, nil
}

func (w *StatelessWorker) ExecuteEvents(ctx context.Context, request GatewayRequest) (<-chan RuntimeEvent, error) {
	streaming, ok := w.runner.(StreamingRunnerAdapter)
	if !ok {
		events := make(chan RuntimeEvent, 4)
		go func() {
			defer close(events)
			response, err := w.Execute(ctx, request)
			if err != nil {
				eventType := "run.failed"
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					eventType = "run.cancelled"
				}
				events <- RuntimeEvent{Type: eventType, Data: runtimeEventData(runnerRequestFromGateway(request), map[string]string{"error": err.Error()})}
				return
			}
			data := runtimeEventData(runnerRequestFromGateway(request), map[string]string{"delta": response.Output, "output": response.Output})
			if response.UsageKnown {
				data["usage_known"] = "true"
				data["usage_tokens"] = strconv.FormatInt(response.UsageTokens, 10)
			}
			events <- RuntimeEvent{Type: "message.delta", Data: data}
			events <- RuntimeEvent{Type: "message.completed", Data: data}
			events <- RuntimeEvent{Type: "run.completed", Data: runtimeEventData(runnerRequestFromGateway(request), data)}
		}()
		return events, nil
	}
	return streaming.RunEvents(ctx, runnerRequestFromGateway(request))
}

func runnerRequestFromGateway(request GatewayRequest) RunnerRequest {
	return RunnerRequest{
		TenantID: request.TenantID, AppID: request.AppID, SessionID: request.SessionID, UserID: request.UserID,
		Channel: request.Channel, ExternalSubject: request.ExternalSubject, Input: request.Input, RequestID: request.RequestID,
		TraceID: request.TraceID, TraceParent: request.TraceParent, DeploymentID: request.DeploymentID, VersionID: request.VersionID, PolicyRevision: request.PolicyRevision,
		FencingToken: request.FencingToken, Version: request.Version,
	}
}

func NewRuntime(platform ControlPlaneStore, runner RunnerAdapter, life RuntimeLifecycle) *Runtime {
	if runner == nil {
		runner = EchoRunner{}
	}
	return &Runtime{platform: platform, worker: NewStatelessWorker(runner), life: life, gates: sessionGates{items: make(map[string]*sessionGate)}, tenantCounters: make(map[string]*runtimeCounters), faults: make(map[string]RuntimeFaultConfiguration)}
}

func (rt *Runtime) SetSessionLeaseManager(manager SessionLeaseManager) { rt.leases = manager }

func (rt *Runtime) acquireSession(ctx context.Context, tenantID, sessionID string) (uint64, <-chan struct{}, func(), error) {
	if rt.leases != nil {
		lease, err := rt.leases.Acquire(ctx, tenantID, sessionID)
		if err != nil {
			return 0, nil, nil, err
		}
		return lease.FencingToken, lease.Lost, lease.Release, nil
	}
	release, err := rt.gates.acquire(ctx, tenantID+"\x00"+sessionID)
	return 0, nil, release, err
}

func (rt *Runtime) SetWorkerAvailable(available bool) { rt.worker.available.Store(available) }

func (rt *Runtime) SetFault(tenantID, appID string, config RuntimeFaultConfiguration) {
	rt.faultMu.Lock()
	defer rt.faultMu.Unlock()
	if config.Scenario == "none" {
		delete(rt.faults, runtimeFaultKey(tenantID, appID))
		return
	}
	rt.faults[runtimeFaultKey(tenantID, appID)] = config
}

func (rt *Runtime) applyFault(ctx context.Context, tenantID, appID string) error {
	rt.faultMu.RLock()
	config := rt.faults[runtimeFaultKey(tenantID, appID)]
	rt.faultMu.RUnlock()
	switch config.Scenario {
	case "", "none":
		return nil
	case "runner_error", "tool_error":
		return &runtimeError{code: config.Scenario}
	case "runner_delay":
		if config.DelayMS <= 0 || config.DelayMS > 5000 {
			return &runtimeError{code: "invalid_runtime_fault"}
		}
		timer := time.NewTimer(time.Duration(config.DelayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return &runtimeError{code: "request_cancelled", err: ctx.Err()}
		case <-timer.C:
			return nil
		}
	default:
		return &runtimeError{code: "invalid_runtime_fault"}
	}
}

func runtimeFaultKey(tenantID, appID string) string { return tenantID + "\x00" + appID }

func (rt *Runtime) Close() error {
	var runnerErr error
	if streaming, ok := rt.worker.runner.(StreamingRunnerAdapter); ok {
		runnerErr = streaming.Close()
	}
	if rt.leases != nil {
		if err := rt.leases.Close(); runnerErr == nil {
			runnerErr = err
		}
	}
	return runnerErr
}

func (rt *Runtime) RetireVersion(ref DeploymentVersionRef) error {
	if streaming, ok := rt.worker.runner.(interface {
		RetireVersion(DeploymentVersionRef) error
	}); ok {
		return streaming.RetireVersion(ref)
	}
	return nil
}

func (rt *Runtime) Stream(ctx context.Context, tenant TenantContext, request GatewayRequest) (<-chan RuntimeEvent, error) {
	if tenant.TenantID == "" {
		return nil, &runtimeError{code: "tenant_context_missing"}
	}
	if !canOperate(tenant.Role) {
		return nil, &runtimeError{code: "forbidden"}
	}
	if !rt.worker.available.Load() {
		return nil, &runtimeError{code: "worker_unavailable"}
	}
	if err := rt.applyFault(ctx, tenant.TenantID, request.AppID); err != nil {
		return nil, err
	}
	if request.AppID == "" || request.SessionID == "" || request.Input == "" {
		return nil, &runtimeError{code: "invalid_request"}
	}
	deployment, found, err := rt.platform.routeDeployment(ctx, tenant.TenantID, request.AppID, request.RequestID)
	if err != nil {
		return nil, &runtimeError{code: "control_plane_unavailable", err: err}
	}
	if !found {
		return nil, &runtimeError{code: "active_deployment_not_found"}
	}
	if version, found, err := rt.platform.DeploymentVersion(ctx, DeploymentVersionRef{TenantID: tenant.TenantID, VersionID: deployment.VersionID}); err != nil {
		return nil, &runtimeError{code: "control_plane_unavailable", err: err}
	} else if found {
		request.Version = &version
	}
	request.TenantID, request.DeploymentID, request.VersionID, request.UserID = tenant.TenantID, deployment.ID, deployment.VersionID, tenant.UserID
	streamCtx, cancel := context.WithCancel(ctx)
	streamCtx = context.WithValue(streamCtx, governanceExternalCompletionContextKey{}, true)
	var releaseLife func()
	if rt.life != nil {
		var ok bool
		releaseLife, ok = rt.life.Acquire()
		if !ok {
			cancel()
			return nil, &runtimeError{code: "service_closing"}
		}
		go func() {
			select {
			case <-rt.life.Done():
				cancel()
			case <-streamCtx.Done():
			}
		}()
	}
	fencingToken, leaseLost, releaseGate, err := rt.acquireSession(streamCtx, tenant.TenantID, request.SessionID)
	if err != nil {
		if releaseLife != nil {
			releaseLife()
		}
		cancel()
		return nil, &runtimeError{code: "request_cancelled", err: err}
	}
	request.FencingToken = fencingToken
	if leaseLost != nil {
		go func() {
			select {
			case <-leaseLost:
				cancel()
			case <-streamCtx.Done():
			}
		}()
	}
	events, err := rt.worker.ExecuteEvents(streamCtx, request)
	if err != nil {
		releaseGate()
		if releaseLife != nil {
			releaseLife()
		}
		cancel()
		return nil, err
	}
	counters := rt.countersFor(tenant.TenantID)
	rt.global.active.Add(1)
	counters.active.Add(1)
	terminalEvent := ""
	output := make(chan RuntimeEvent, 4)
	go func() {
		defer close(output)
		defer releaseGate()
		if releaseLife != nil {
			defer releaseLife()
		}
		defer cancel()
		defer func() {
			if terminalEvent == "run.failed" || terminalEvent == "run.cancelled" || streamCtx.Err() != nil {
				rt.global.failed.Add(1)
				counters.failed.Add(1)
			} else {
				rt.global.complete.Add(1)
				counters.complete.Add(1)
			}
			rt.global.active.Add(-1)
			counters.active.Add(-1)
		}()
		if fencingToken > 0 {
			select {
			case <-streamCtx.Done():
				return
			case output <- RuntimeEvent{Type: "session.lease.acquired", Data: map[string]string{"fencing_token": fmt.Sprint(fencingToken)}}:
			}
		}
		for event := range events {
			data := make(map[string]string, len(event.Data)+1)
			for key, value := range event.Data {
				data[key] = value
			}
			event.Data = data
			if fencingToken > 0 {
				event.Data["fencing_token"] = fmt.Sprint(fencingToken)
			}
			switch event.Type {
			case "run.completed", "run.failed", "run.cancelled":
				terminalEvent = event.Type
			}
			select {
			case <-streamCtx.Done():
				terminalEvent = "run.cancelled"
				select {
				case output <- RuntimeEvent{Type: "run.cancelled", Data: map[string]string{"error": "request_cancelled", "fencing_token": fmt.Sprint(fencingToken)}}:
				default:
				}
				return
			case output <- event:
			}
		}
		if streamCtx.Err() != nil && terminalEvent == "" {
			terminalEvent = "run.cancelled"
			select {
			case output <- RuntimeEvent{Type: "run.cancelled", Data: map[string]string{"error": "request_cancelled", "fencing_token": fmt.Sprint(fencingToken)}}:
			default:
			}
		}
	}()
	return output, nil
}

func (rt *Runtime) Handle(ctx context.Context, tenant TenantContext, request GatewayRequest) (GatewayResponse, error) {
	if tenant.TenantID == "" {
		return GatewayResponse{}, &runtimeError{code: "tenant_context_missing"}
	}
	if !canOperate(tenant.Role) {
		return GatewayResponse{}, &runtimeError{code: "forbidden"}
	}
	if !rt.worker.available.Load() {
		return GatewayResponse{}, &runtimeError{code: "worker_unavailable"}
	}
	if err := rt.applyFault(ctx, tenant.TenantID, request.AppID); err != nil {
		return GatewayResponse{}, err
	}
	if request.AppID == "" || request.SessionID == "" || request.Input == "" {
		return GatewayResponse{}, &runtimeError{code: "invalid_request"}
	}
	deployment, found, err := rt.platform.routeDeployment(ctx, tenant.TenantID, request.AppID, request.RequestID)
	if err != nil {
		return GatewayResponse{}, &runtimeError{code: "control_plane_unavailable", err: err}
	}
	if !found {
		return GatewayResponse{}, &runtimeError{code: "active_deployment_not_found"}
	}
	if version, found, err := rt.platform.DeploymentVersion(ctx, DeploymentVersionRef{TenantID: tenant.TenantID, VersionID: deployment.VersionID}); err != nil {
		return GatewayResponse{}, &runtimeError{code: "control_plane_unavailable", err: err}
	} else if found {
		request.Version = &version
	}
	request.DeploymentID, request.VersionID = deployment.ID, deployment.VersionID
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var releaseLife func()
	if rt.life != nil {
		var ok bool
		releaseLife, ok = rt.life.Acquire()
		if !ok {
			return GatewayResponse{}, &runtimeError{code: "service_closing"}
		}
		defer releaseLife()
		go func() {
			select {
			case <-rt.life.Done():
				cancel()
			case <-runCtx.Done():
			}
		}()
	}
	fencingToken, leaseLost, releaseGate, err := rt.acquireSession(runCtx, tenant.TenantID, request.SessionID)
	if err != nil {
		return GatewayResponse{}, &runtimeError{code: "request_cancelled", err: err}
	}
	defer releaseGate()
	request.FencingToken = fencingToken
	if leaseLost != nil {
		go func() {
			select {
			case <-leaseLost:
				cancel()
			case <-runCtx.Done():
			}
		}()
	}

	if err := runCtx.Err(); err != nil {
		return GatewayResponse{}, &runtimeError{code: "request_cancelled", err: err}
	}
	if rt.life != nil && rt.life.IsClosing() {
		return GatewayResponse{}, &runtimeError{code: "request_cancelled", err: context.Canceled}
	}
	counters := rt.countersFor(tenant.TenantID)
	rt.global.active.Add(1)
	counters.active.Add(1)
	defer rt.global.active.Add(-1)
	defer counters.active.Add(-1)
	request.TenantID, request.UserID = tenant.TenantID, tenant.UserID
	result, err := rt.worker.Execute(runCtx, request)
	if err != nil {
		var runtimeErr *runtimeError
		if errors.As(err, &runtimeErr) {
			return GatewayResponse{}, runtimeErr
		}
		rt.global.failed.Add(1)
		counters.failed.Add(1)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || runCtx.Err() != nil {
			return GatewayResponse{}, &runtimeError{code: "request_cancelled", err: err}
		}
		return GatewayResponse{}, &runtimeError{code: "runner_error", err: err}
	}
	rt.global.complete.Add(1)
	counters.complete.Add(1)
	return result, nil
}

func (rt *Runtime) Status() []RuntimeComponentStatus {
	return rt.status(&rt.global)
}

func (rt *Runtime) StatusFor(tenant TenantContext) []RuntimeComponentStatus {
	return rt.status(rt.countersFor(tenant.TenantID))
}

func (rt *Runtime) status(counters *runtimeCounters) []RuntimeComponentStatus {
	lifecycle := LifecycleHealthy
	if rt.life != nil && rt.life.IsClosing() {
		lifecycle = LifecycleClosing
	}
	workerLifecycle := lifecycle
	if !rt.worker.available.Load() {
		workerLifecycle = LifecycleUnavailable
	} else if lifecycle == LifecycleHealthy && rt.worker.lastError.Load() {
		workerLifecycle = LifecycleError
	}
	active, complete, failed := counters.active.Load(), counters.complete.Load(), counters.failed.Load()
	return []RuntimeComponentStatus{
		{ID: "gateway-local", Role: ComponentGateway, Available: lifecycle == LifecycleHealthy, Lifecycle: lifecycle, Active: active, Completed: complete, Failed: failed},
		{ID: "worker-local", Role: ComponentWorker, Available: workerLifecycle == LifecycleHealthy, Lifecycle: workerLifecycle, Active: active, Completed: complete, Failed: failed},
	}
}

func (rt *Runtime) countersFor(tenantID string) *runtimeCounters {
	rt.counterMu.Lock()
	defer rt.counterMu.Unlock()
	counters := rt.tenantCounters[tenantID]
	if counters == nil {
		counters = &runtimeCounters{}
		rt.tenantCounters[tenantID] = counters
	}
	return counters
}

func (p *SnapshotControlPlane) activeDeployment(ctx context.Context, tenantID, appID string) (Deployment, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Deployment{}, false, p.persistenceErr
	}
	for _, deployment := range p.deployments {
		if deployment.TenantID == tenantID && deployment.AgentAppID == appID && deployment.Status == DeploymentActive && deployment.VersionID != "" {
			return deployment, true, nil
		}
	}
	return Deployment{}, false, nil
}

func (p *SnapshotControlPlane) routeDeployment(ctx context.Context, tenantID, appID, requestID string) (Deployment, bool, error) {
	deployment, found, err := p.activeDeployment(ctx, tenantID, appID)
	if err != nil || !found {
		return deployment, false, err
	}
	if deployment.TargetVersionID == "" || deployment.CurrentVersionID == "" || deployment.GrayPercentage <= 0 || deployment.GrayPercentage >= 100 || requestID == "" {
		return deployment, true, nil
	}
	if uint64(grayBucket(requestID)) < uint64(deployment.GrayPercentage) {
		deployment.VersionID = deployment.TargetVersionID
	}
	return deployment, true, nil
}

func grayBucket(requestID string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(requestID))
	return int(hash.Sum32() % 100)
}

type sessionGate struct {
	token chan struct{}
	refs  int
}
type sessionGates struct {
	mu    sync.Mutex
	items map[string]*sessionGate
}

func (g *sessionGates) acquire(ctx context.Context, key string) (func(), error) {
	g.mu.Lock()
	gate := g.items[key]
	if gate == nil {
		gate = &sessionGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		g.items[key] = gate
	}
	gate.refs++
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		g.releaseRef(key, gate)
		return nil, ctx.Err()
	case <-gate.token:
		var once sync.Once
		return func() { once.Do(func() { gate.token <- struct{}{}; g.releaseRef(key, gate) }) }, nil
	}
}

func (g *sessionGates) releaseRef(key string, gate *sessionGate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gate.refs--
	if gate.refs == 0 {
		delete(g.items, key)
	}
}

func (h *AdminHandler) handleRoutedRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	var request GatewayRequest
	if err := decodeStrict(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "app_id, session_id, and input are required")
		return
	}
	requestID := r.Header.Get("Idempotency-Key")
	if !validIdempotencyKey(requestID) {
		requestID = time.Now().UTC().Format("20060102150405.000000000")
	}
	request.RequestID = requestID
	store, releaseStore, err := h.acquireStore(r.Context(), tenant.TenantID)
	if err != nil {
		code, message := "storage_unavailable", "tenant storage is unavailable"
		if errors.Is(err, errControlPlaneUnavailable) {
			code, message = "control_plane_unavailable", "control plane is unavailable"
		} else if errors.Is(err, ErrTenantMigrating) {
			code, message = "tenant_storage_migrating", "tenant storage is temporarily read-only during migration"
		} else if h.life != nil && h.life.IsClosing() {
			code, message = "service_closing", "service is closing"
		}
		writeError(w, http.StatusServiceUnavailable, code, message)
		return
	}
	defer releaseStore()
	input, _ := json.Marshal(map[string]string{"app_id": request.AppID, "input": request.Input})
	if err := store.AppendSessionEvent(r.Context(), SessionEvent{TenantID: tenant.TenantID, SessionID: request.SessionID, IdempotencyKey: requestID + ":input", Type: "message.input", Payload: input}); err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage_error", "session event could not be persisted")
		return
	}
	response, err := h.runtime.Handle(r.Context(), tenant, request)
	if err != nil {
		failureCtx, cancelFailure := context.WithTimeout(h.failureCtx, 2*time.Second)
		_ = store.AppendSessionEvent(failureCtx, SessionEvent{TenantID: tenant.TenantID, SessionID: request.SessionID, IdempotencyKey: requestID + ":failed", Type: "run.failed", Payload: []byte(err.Error())})
		cancelFailure()
		code := err.Error()
		status := http.StatusBadGateway
		switch code {
		case "invalid_request":
			status = http.StatusBadRequest
		case "forbidden":
			status = http.StatusForbidden
		case "active_deployment_not_found":
			status = http.StatusNotFound
		case "request_cancelled":
			status = http.StatusRequestTimeout
		case "service_closing", "worker_unavailable", "control_plane_unavailable":
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, code, "routed execution failed")
		return
	}
	output, _ := json.Marshal(map[string]string{"output": response.Output})
	if err := store.AppendSessionEvent(r.Context(), SessionEvent{TenantID: tenant.TenantID, SessionID: request.SessionID, IdempotencyKey: requestID + ":output", Type: "message.output", Payload: output}); err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage_error", "session event could not be persisted")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *AdminHandler) handleRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if tenant.Role == RoleViewer {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	items := h.runtime.StatusFor(tenant)
	store, releaseStore, err := h.acquireStore(r.Context(), tenant.TenantID)
	if err != nil {
		if errors.Is(err, errControlPlaneUnavailable) {
			writeControlPlaneError(w, err)
			return
		}
		items = appendDependencyStatus(items, "dependency-storage", LifecycleUnavailable)
	} else {
		defer releaseStore()
		healthCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		health := store.Health(healthCtx)
		cancel()
		lifecycle := LifecycleHealthy
		if health.Status == "unavailable" {
			lifecycle = LifecycleUnavailable
		} else if health.Status != "healthy" && health.Status != "" {
			lifecycle = LifecycleDegraded
		}
		items = appendDependencyStatus(items, "dependency-"+health.Backend, lifecycle)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func appendDependencyStatus(items []RuntimeComponentStatus, id string, lifecycle ComponentLifecycle) []RuntimeComponentStatus {
	available := lifecycle == LifecycleHealthy
	return append(items, RuntimeComponentStatus{ID: id, Role: ComponentDependency, Available: available, Lifecycle: lifecycle})
}
