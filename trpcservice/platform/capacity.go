package platform

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type capacityRunRequest struct {
	AgentAppID                string `json:"agent_app_id"`
	Concurrency               int    `json:"concurrency"`
	Runs                      int    `json:"runs"`
	TimeoutMS                 int    `json:"timeout_ms"`
	PeakIMCallbacksPerSecond  int64  `json:"peak_im_callbacks_per_second"`
	AverageTokensPerSession   int64  `json:"average_tokens_per_session"`
	RedisOperationsPerSession int64  `json:"redis_operations_per_session"`
	SQLOperationsPerSession   int64  `json:"sql_operations_per_session"`
	HeadroomPercent           int    `json:"headroom_percent"`
}

type CapacityTestResult struct {
	ID                       string    `json:"id"`
	RequestID                string    `json:"request_id"`
	TraceID                  string    `json:"trace_id"`
	TenantID                 string    `json:"tenant_id"`
	AgentAppID               string    `json:"agent_app_id"`
	Status                   string    `json:"status"`
	Concurrency              int       `json:"concurrency"`
	Runs                     int       `json:"runs"`
	Completed                int       `json:"completed"`
	Failed                   int       `json:"failed"`
	Active                   int       `json:"active"`
	SafeConcurrency          int       `json:"safe_concurrency"`
	ThroughputPerSecond      float64   `json:"throughput_per_second"`
	ModelLatencyMS           int64     `json:"model_latency_ms"`
	ToolLatencyMS            int64     `json:"tool_latency_ms"`
	StorageLatencyMS         int64     `json:"storage_latency_ms"`
	EstimatedTokens          int64     `json:"estimated_tokens"`
	EstimatedCost            float64   `json:"estimated_cost"`
	FirstBottleneck          string    `json:"first_bottleneck"`
	SessionsPerNode          int       `json:"sessions_per_node"`
	RecommendedWorkerNodes   int       `json:"recommended_worker_nodes"`
	AverageTokensPerSession  int64     `json:"average_tokens_per_session"`
	TokenThroughputPerSecond float64   `json:"token_throughput_per_second"`
	IMCallbackPeakQPS        float64   `json:"im_callback_peak_qps"`
	RedisQPS                 float64   `json:"redis_qps"`
	SQLQPS                   float64   `json:"sql_qps"`
	HeadroomPercent          int       `json:"headroom_percent"`
	StartedAt                time.Time `json:"started_at"`
	CompletedAt              time.Time `json:"completed_at,omitempty"`
	Error                    string    `json:"error,omitempty"`
}

type capacityRun struct {
	tenantID string
	cancel   context.CancelFunc
	result   CapacityTestResult
}

const maxCapacityConcurrency = 10
const maxCapacityRuns = 100
const minCapacityTimeoutMS = 100
const maxCapacityTimeoutMS = 5000
const capacityRunDelay = 25 * time.Millisecond
const defaultCapacityHeadroomPercent = 30

type capacityRunner struct{}

func (capacityRunner) Run(ctx context.Context, _ RunnerRequest) (RunnerResponse, error) {
	timer := time.NewTimer(capacityRunDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return RunnerResponse{}, ctx.Err()
	case <-timer.C:
		return RunnerResponse{Output: "capacity smoke test"}, nil
	}
}

func (h *AdminHandler) handleCapacity(w http.ResponseWriter, r *http.Request, tenant TenantContext, parts []string) {
	if len(parts) == 0 && r.Method == http.MethodPost {
		h.startCapacityRun(w, r, tenant)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		result, ok := h.capacityResult(tenant.TenantID, parts[0])
		if !ok {
			writeError(w, http.StatusNotFound, "capacity_run_not_found", "capacity run was not found")
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		run, ok := h.capacityRunFor(tenant.TenantID, parts[0])
		if !ok {
			writeError(w, http.StatusNotFound, "capacity_run_not_found", "capacity run was not found")
			return
		}
		run.cancel()
		writeJSON(w, http.StatusAccepted, h.capacityResultForRun(run))
		return
	}
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "capacity operation is not supported")
}

func (h *AdminHandler) startCapacityRun(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request capacityRunRequest
	if err := decodeStrict(r, &request); err != nil || !validResourceID(request.AgentAppID) ||
		request.Concurrency < 1 || request.Concurrency > maxCapacityConcurrency || request.Runs < 1 || request.Runs > maxCapacityRuns ||
		request.TimeoutMS < minCapacityTimeoutMS || request.TimeoutMS > maxCapacityTimeoutMS ||
		request.PeakIMCallbacksPerSecond < 0 || request.PeakIMCallbacksPerSecond > 1_000_000 ||
		request.AverageTokensPerSession < 0 || request.AverageTokensPerSession > 10_000_000 ||
		request.RedisOperationsPerSession < 0 || request.RedisOperationsPerSession > 10_000 ||
		request.SQLOperationsPerSession < 0 || request.SQLOperationsPerSession > 10_000 ||
		request.HeadroomPercent < 0 || request.HeadroomPercent > 90 {
		writeError(w, http.StatusBadRequest, "invalid_capacity_request", "capacity test inputs are invalid")
		return
	}
	if request.HeadroomPercent == 0 {
		request.HeadroomPercent = defaultCapacityHeadroomPercent
	}
	if _, exists, err := h.platform.app(r.Context(), tenant.TenantID, request.AgentAppID); err != nil || !exists {
		if writeControlPlaneError(w, err) {
			return
		}
		writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
		return
	}
	if _, exists, err := h.platform.activeDeployment(r.Context(), tenant.TenantID, request.AgentAppID); err != nil || !exists {
		if writeControlPlaneError(w, err) {
			return
		}
		writeError(w, http.StatusConflict, "active_deployment_not_found", "active Deployment is required")
		return
	}
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if !validIdempotencyKey(requestID) {
		requestID = newRequestID()
	}
	runID := "capacity-" + newRequestID()
	governanceResult, err := h.governance.Evaluate(h.capacityCtx, GovernanceRequest{
		TenantID: tenant.TenantID, AgentAppID: request.AgentAppID, UserID: tenant.UserID,
		SessionID: runID, RequestID: requestID, Input: "capacity smoke test",
	})
	if err != nil {
		writeCapacityGovernanceError(w, err)
		return
	}
	if err := h.governance.RecordSpan(GovernanceRequest{
		TenantID: tenant.TenantID, AgentAppID: request.AgentAppID, UserID: tenant.UserID,
		SessionID: runID, RequestID: requestID, PolicyRevision: governanceResult.PolicyRevision,
	}, governanceResult.TraceID, "capacity.start", "ok"); err != nil {
		_, _ = completeGovernance(h.capacityCtx, h.governance, GovernanceCompletion{
			TenantID: tenant.TenantID, AgentAppID: request.AgentAppID, RequestID: requestID,
			UserID: tenant.UserID, SessionID: runID, ErrorType: "audit_unavailable",
		})
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	policy, _, err := h.governance.Policy(r.Context(), tenant.TenantID, request.AgentAppID)
	if writeControlPlaneError(w, err) {
		return
	}
	timeout := time.Duration(request.TimeoutMS) * time.Millisecond
	if policy.RuntimeTimeoutMS > 0 && policy.runtimeTimeout() < timeout {
		timeout = policy.runtimeTimeout()
	}
	runCtx, cancel := context.WithTimeout(h.capacityCtx, timeout)
	run := &capacityRun{
		tenantID: tenant.TenantID, cancel: cancel,
		result: CapacityTestResult{
			ID: runID, RequestID: requestID, TraceID: governanceResult.TraceID, TenantID: tenant.TenantID,
			AgentAppID: request.AgentAppID, Status: "running", Concurrency: request.Concurrency,
			Runs: request.Runs, AverageTokensPerSession: request.AverageTokensPerSession,
			IMCallbackPeakQPS: float64(request.PeakIMCallbacksPerSecond),
			RedisQPS:          float64(request.PeakIMCallbacksPerSecond * request.RedisOperationsPerSession),
			SQLQPS:            float64(request.PeakIMCallbacksPerSecond * request.SQLOperationsPerSession),
			HeadroomPercent:   request.HeadroomPercent, StartedAt: time.Now().UTC(),
		},
	}
	h.capacityMu.Lock()
	h.capacityRuns[runID] = run
	h.capacityMu.Unlock()
	h.capacityWG.Add(1)
	go func() {
		defer h.capacityWG.Done()
		h.executeCapacityRun(runCtx, run, tenant, policy)
	}()
	writeJSON(w, http.StatusAccepted, h.capacityResultForRun(run))
}

func (h *AdminHandler) executeCapacityRun(ctx context.Context, run *capacityRun, tenant TenantContext, policy TenantPolicy) {
	runtime := NewRuntime(h.platform, capacityRunner{}, h.life)
	slots := make(chan struct{}, run.result.Concurrency)
	var failed, completed, active atomic.Int64
	var totalLatency atomic.Int64
	var wg sync.WaitGroup
	started := time.Now()
	for index := 0; index < run.result.Runs; index++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			active.Add(1)
			defer active.Add(-1)
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return
			}
			runStart := time.Now()
			_, err := runtime.Handle(ctx, tenant, GatewayRequest{
				AppID: run.result.AgentAppID, SessionID: run.result.ID + "-" + strconv.Itoa(index), Input: "capacity smoke test",
				RequestID: run.result.RequestID + "-" + newRequestID(),
			})
			latency := time.Since(runStart).Milliseconds()
			for {
				current := totalLatency.Load()
				if totalLatency.CompareAndSwap(current, current+latency) {
					break
				}
			}
			if err != nil {
				failed.Add(1)
				return
			}
			completed.Add(1)
		}(index)
	}
	wg.Wait()
	elapsed := time.Since(started)
	storageLatency := h.capacityStorageLatency(tenant)
	result := run.result
	result.Completed = int(completed.Load())
	result.Failed = int(failed.Load())
	result.Active = int(active.Load())
	result.ModelLatencyMS = totalLatency.Load() / int64(max(result.Completed, 1))
	result.ToolLatencyMS = 0
	result.StorageLatencyMS = storageLatency
	if result.AverageTokensPerSession == 0 {
		result.AverageTokensPerSession = policy.EstimatedTokensPerRun
	}
	result.EstimatedTokens = result.AverageTokensPerSession * int64(result.Completed)
	result.EstimatedCost = policy.CostPerToken * float64(result.EstimatedTokens)
	result.SafeConcurrency = result.Concurrency
	if elapsed > 0 {
		result.ThroughputPerSecond = float64(result.Completed) / elapsed.Seconds()
	}
	result.TokenThroughputPerSecond = result.IMCallbackPeakQPS * float64(result.AverageTokensPerSession)
	if result.ModelLatencyMS > 0 {
		budget := maxDuration(0, elapsed-time.Duration(storageLatency)*time.Millisecond)
		latency := time.Duration(max(result.ModelLatencyMS, 1)) * time.Millisecond
		if budget >= latency {
			result.SafeConcurrency = min(result.Concurrency, int(budget/latency))
		}
	}
	result.FirstBottleneck = "none"
	if result.Failed > 0 || result.Completed < result.Runs {
		result.FirstBottleneck = "runtime"
	} else if storageLatency >= int64(minCapacityTimeoutMS) {
		result.FirstBottleneck = "storage"
	} else if result.SafeConcurrency < result.Concurrency {
		result.FirstBottleneck = "model"
	}
	headroomFactor := 1 - float64(result.HeadroomPercent)/100
	result.SessionsPerNode = max(1, int(math.Floor(float64(result.SafeConcurrency)*headroomFactor)))
	effectiveThroughput := result.ThroughputPerSecond * headroomFactor
	if result.IMCallbackPeakQPS > 0 && effectiveThroughput > 0 {
		result.RecommendedWorkerNodes = max(1, int(math.Ceil(result.IMCallbackPeakQPS/effectiveThroughput)))
	} else {
		result.RecommendedWorkerNodes = 1
	}
	result.CompletedAt = time.Now().UTC()
	errorType, cancelled := "", false
	if ctx.Err() != nil {
		result.Status, errorType, cancelled = "cancelled", "cancelled", true
		result.Error = "capacity test was cancelled"
	} else if result.Failed > 0 {
		result.Status, errorType = "failed", "capacity_failed"
		result.Error = "one or more deterministic runs failed"
	} else {
		result.Status = "completed"
	}
	_, _ = completeGovernance(h.capacityCtx, h.governance, GovernanceCompletion{
		TenantID: tenant.TenantID, AgentAppID: result.AgentAppID, RequestID: result.RequestID,
		UserID: tenant.UserID, SessionID: result.ID, Output: "capacity smoke test", NoUsage: true,
		ErrorType: errorType, Cancelled: cancelled,
	})
	_ = h.governance.RecordSpan(GovernanceRequest{
		TenantID: tenant.TenantID, AgentAppID: result.AgentAppID, UserID: tenant.UserID,
		SessionID: result.ID, RequestID: result.RequestID,
	}, result.TraceID, "capacity.finish", statusSpan(errorType))
	h.capacityMu.Lock()
	run.result = result
	h.capacityMu.Unlock()
}

func (h *AdminHandler) capacityStorageLatency(tenant TenantContext) int64 {
	store, release, err := h.acquireStore(h.capacityCtx, tenant.TenantID)
	if err != nil {
		return minCapacityTimeoutMS
	}
	defer release()
	started := time.Now()
	health := store.Health(h.capacityCtx)
	if health.Status != "healthy" {
		return minCapacityTimeoutMS
	}
	return time.Since(started).Milliseconds()
}

func (h *AdminHandler) capacityResultForRun(run *capacityRun) CapacityTestResult {
	h.capacityMu.Lock()
	defer h.capacityMu.Unlock()
	return run.result
}

func (h *AdminHandler) capacityResult(tenantID, id string) (CapacityTestResult, bool) {
	run, ok := h.capacityRunFor(tenantID, id)
	if !ok {
		return CapacityTestResult{}, false
	}
	return h.capacityResultForRun(run), true
}

func (h *AdminHandler) capacityRunFor(tenantID, id string) (*capacityRun, bool) {
	h.capacityMu.Lock()
	defer h.capacityMu.Unlock()
	run, ok := h.capacityRuns[id]
	return run, ok && run.tenantID == tenantID
}

func writeCapacityGovernanceError(w http.ResponseWriter, err error) {
	var governanceErr *GovernanceError
	if errors.As(err, &governanceErr) && (governanceErr.Code == "tenant_rate_limited" || governanceErr.Code == "budget_exceeded") {
		writeError(w, http.StatusTooManyRequests, governanceErr.Code, "tenant governance limit exceeded")
		return
	}
	writeError(w, http.StatusServiceUnavailable, "capacity_unavailable", "capacity test could not be admitted")
}

func statusSpan(errorType string) string {
	if errorType == "" {
		return "ok"
	}
	if errorType == "cancelled" {
		return "cancelled"
	}
	return "error"
}

func maxDuration(value, other time.Duration) time.Duration {
	if value > other {
		return value
	}
	return other
}
