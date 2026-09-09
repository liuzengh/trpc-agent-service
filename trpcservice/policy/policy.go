// Package policy implements tenant identity, budget, visibility, execution, and approval rules.
package policy

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	ErrIdentityDenied   = errors.New("policy: identity denied")
	ErrBudgetExceeded   = errors.New("policy: budget exceeded")
	ErrToolDenied       = errors.New("policy: tool denied")
	ErrApprovalRequired = errors.New("policy: tool approval required")
)

type Request struct {
	TenantID, AppID, UserID, RequestID   string
	ExternalUserID, ConversationID       string
	AllowedUsers, AllowedChats           []string
	Policy                               tenant.ToolPolicy
	EstimatedTokens, EstimatedCostMicros int64
}
type IdentityAuthorizer interface {
	Authorize(context.Context, Request) error
}
type IdentityAuthorizerFunc func(context.Context, Request) error

func (authorize IdentityAuthorizerFunc) Authorize(ctx context.Context, request Request) error {
	return authorize(ctx, request)
}

// AuthenticatedIdentityAuthorizer marks the boundary where a verified channel
// identity has already been mapped to the canonical tenant user ID.
type AuthenticatedIdentityAuthorizer struct{}

func (AuthenticatedIdentityAuthorizer) Authorize(_ context.Context, request Request) error {
	if request.TenantID == "" || request.AppID == "" || request.UserID == "" {
		return ErrIdentityDenied
	}
	binding := tenant.ChannelBinding{AllowedUsers: request.AllowedUsers, AllowedChats: request.AllowedChats}
	if !binding.AllowsIdentity(request.ExternalUserID, request.ConversationID) {
		return ErrIdentityDenied
	}
	return nil
}

type BudgetStore interface {
	Reserve(context.Context, Request) error
}
type BudgetReconciler interface {
	Reconcile(context.Context, Request, int64, int64) error
}
type ApprovalStore interface {
	Approved(context.Context, string, string, string) bool
}
type ApprovalWaiter interface {
	Wait(context.Context, string, string, string) bool
}

type Controls struct {
	Visibility trpctool.FilterFunc
	Execution  trpctool.FilterFunc
	Permission trpctool.PermissionPolicy
}
type Engine struct {
	Identity           IdentityAuthorizer
	Budgets            BudgetStore
	Approvals          ApprovalStore
	CostMicrosPerToken int64
}

type requestContextKey struct{}
type invocationContextKey struct{}
type ContextRequest struct {
	Engine      *Engine
	Request     Request
	Invocations *InvocationState
}

// InvocationState assigns a deterministic ordinal to tool calls made during
// one Runner execution. The ordinal is included in external idempotency keys
// so two calls to the same tool in one request cannot collide.
type InvocationState struct{ next atomic.Uint64 }

func (state *InvocationState) Next() uint64 {
	if state == nil {
		return 0
	}
	return state.next.Add(1)
}

func WithRequest(ctx context.Context, engine *Engine, request Request) context.Context {
	// Provider identifiers and the full ACL are needed only for the pre-run
	// identity decision. Do not expose them to tool wrappers through context.
	request.ExternalUserID = ""
	request.ConversationID = ""
	request.AllowedUsers = nil
	request.AllowedChats = nil
	return context.WithValue(ctx, requestContextKey{}, ContextRequest{Engine: engine, Request: request, Invocations: &InvocationState{}})
}
func FromContext(ctx context.Context) (ContextRequest, bool) {
	value, ok := ctx.Value(requestContextKey{}).(ContextRequest)
	return value, ok
}

func WithInvocation(ctx context.Context, ordinal uint64) context.Context {
	return context.WithValue(ctx, invocationContextKey{}, ordinal)
}

func InvocationFromContext(ctx context.Context) uint64 {
	value, _ := ctx.Value(invocationContextKey{}).(uint64)
	return value
}

// Evaluate applies identity and budget before creating all three upstream tool controls.
func (engine *Engine) Evaluate(ctx context.Context, request Request) (Controls, error) {
	if request.TenantID == "" || request.AppID == "" || request.UserID == "" || request.RequestID == "" {
		return Controls{}, errors.New("policy: complete request scope is required")
	}
	if engine == nil {
		return Controls{}, errors.New("policy: engine is required")
	}
	if engine.Identity == nil {
		return Controls{}, errors.New("policy: identity authorizer is required")
	}
	if err := engine.Identity.Authorize(ctx, request); err != nil {
		return Controls{}, errors.Join(ErrIdentityDenied, err)
	}
	if engine.Budgets != nil {
		if err := engine.Budgets.Reserve(ctx, request); err != nil {
			return Controls{}, errors.Join(ErrBudgetExceeded, err)
		}
	}
	visible := nameSet(request.Policy.Allow)
	denied := nameSet(request.Policy.Deny)
	approval := nameSet(request.Policy.RequireApproval)
	allowed := func(name string) bool {
		if name == "" {
			return false
		}
		if _, blocked := denied[name]; blocked {
			return false
		}
		if len(visible) == 0 {
			return false
		}
		_, ok := visible[name]
		return ok
	}
	approved := func(ctx context.Context, name string) bool {
		if _, dangerous := approval[name]; !dangerous {
			return true
		}
		return engine.Approvals != nil && engine.Approvals.Approved(ctx, request.TenantID, request.RequestID, name)
	}
	visibility := func(_ context.Context, tool trpctool.Tool) bool { return allowed(toolName(tool)) }
	execution := func(_ context.Context, tool trpctool.Tool) bool {
		name := toolName(tool)
		_, dangerous := approval[name]
		return allowed(name) && !dangerous
	}
	permission := trpctool.PermissionPolicyFunc(func(ctx context.Context, pending *trpctool.PermissionRequest) (trpctool.PermissionDecision, error) {
		name := ""
		if pending != nil {
			name = pending.ToolName
		}
		if !allowed(name) {
			return trpctool.DenyPermission("tool is not allowed for this tenant"), nil
		}
		if !approved(ctx, name) {
			return trpctool.AskPermission("tenant approval is required"), nil
		}
		return trpctool.AllowPermission(), nil
	})
	return Controls{Visibility: visibility, Execution: execution, Permission: permission}, nil
}

func (engine *Engine) RequiresApproval(request Request, toolName string) bool {
	if engine == nil {
		return true
	}
	for _, name := range request.Policy.RequireApproval {
		if name == toolName {
			return engine.Approvals == nil || !engine.Approvals.Approved(context.Background(), request.TenantID, request.RequestID, toolName)
		}
	}
	return false
}

func (engine *Engine) WaitApproval(ctx context.Context, request Request, toolName string) error {
	if !engine.RequiresApproval(request, toolName) {
		return nil
	}
	waiter, ok := engine.Approvals.(ApprovalWaiter)
	if !ok || !waiter.Wait(ctx, request.TenantID, request.RequestID, toolName) {
		return ErrApprovalRequired
	}
	return nil
}

func (engine *Engine) Grant(tenantID, requestID, toolName string) bool {
	store, ok := engine.Approvals.(interface {
		Grant(string, string, string) bool
	})
	if !ok || tenantID == "" || requestID == "" || toolName == "" {
		return false
	}
	return store.Grant(tenantID, requestID, toolName)
}

func (engine *Engine) EstimateCost(tokens int64) int64 {
	if engine == nil || engine.CostMicrosPerToken <= 0 || tokens <= 0 {
		return 0
	}
	return tokens * engine.CostMicrosPerToken
}

// EstimateModelCost calculates version-pinned prompt/completion cost. Each
// non-empty component rounds up to one micro so small calls are never free.
func EstimateModelCost(pricing tenant.ModelPricing, promptTokens, completionTokens int64) int64 {
	inputCost := tokenCost(promptTokens, pricing.InputMicrosPerMillion)
	outputCost := tokenCost(completionTokens, pricing.OutputMicrosPerMillion)
	if inputCost > math.MaxInt64-outputCost {
		return math.MaxInt64
	}
	return inputCost + outputCost
}

func tokenCost(tokens, rate int64) int64 {
	if tokens <= 0 || rate <= 0 {
		return 0
	}
	if tokens > (math.MaxInt64-999999)/rate {
		return math.MaxInt64
	}
	return (tokens*rate + 999999) / 1000000
}

func (engine *Engine) Reconcile(ctx context.Context, request Request, actualTokens, actualCostMicros int64) error {
	if request.Policy.RequestTokenBudget > 0 && actualTokens > request.Policy.RequestTokenBudget {
		return ErrBudgetExceeded
	}
	if request.Policy.MonthlyCostBudgetCents > 0 && actualCostMicros <= 0 {
		return errors.New("policy: cost estimator is required for monthly budget")
	}
	if reconciler, ok := engine.Budgets.(BudgetReconciler); ok {
		if err := reconciler.Reconcile(ctx, request, actualTokens, actualCostMicros); err != nil {
			return errors.Join(ErrBudgetExceeded, err)
		}
	}
	return nil
}

// AuthorizeDirect is the final non-negotiable check used by guarded tool handlers.
func (engine *Engine) AuthorizeDirect(ctx context.Context, request Request, toolName string) error {
	controls, err := engine.Evaluate(ctx, request)
	if err != nil {
		return err
	}
	stub := namedTool(toolName)
	if !controls.Visibility(ctx, stub) {
		return ErrToolDenied
	}
	decision, err := controls.Permission.CheckToolPermission(ctx, &trpctool.PermissionRequest{Tool: stub, ToolName: toolName, Declaration: stub.Declaration()})
	if err != nil {
		return err
	}
	switch decision.Action {
	case trpctool.PermissionActionAllow:
		return nil
	case trpctool.PermissionActionAsk:
		return ErrApprovalRequired
	default:
		return ErrToolDenied
	}
}

type MemoryBudget struct {
	mu       sync.Mutex
	used     map[string]int64
	requests map[string]budgetReservation
	now      func() time.Time
}
type budgetReservation struct {
	period string
	cost   int64
}

func NewMemoryBudget() *MemoryBudget {
	return &MemoryBudget{used: make(map[string]int64), requests: make(map[string]budgetReservation), now: time.Now}
}
func (store *MemoryBudget) Reserve(_ context.Context, request Request) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := request.TenantID + "\x00" + request.RequestID
	if _, ok := store.requests[key]; ok {
		return nil
	}
	if request.Policy.RequestTokenBudget > 0 && request.EstimatedTokens > request.Policy.RequestTokenBudget {
		return ErrBudgetExceeded
	}
	if request.Policy.MonthlyCostBudgetCents > 0 {
		if request.EstimatedCostMicros <= 0 {
			return errors.New("policy: positive estimated cost is required for monthly budget")
		}
		limit := request.Policy.MonthlyCostBudgetCents * 10000
		period := store.period(request.TenantID)
		if store.used[period]+request.EstimatedCostMicros > limit {
			return ErrBudgetExceeded
		}
		store.used[period] += request.EstimatedCostMicros
		store.requests[key] = budgetReservation{period: period, cost: request.EstimatedCostMicros}
		return nil
	}
	store.requests[key] = budgetReservation{}
	return nil
}

func (store *MemoryBudget) Reconcile(_ context.Context, request Request, actualTokens, actualCostMicros int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if request.Policy.RequestTokenBudget > 0 && actualTokens > request.Policy.RequestTokenBudget {
		return ErrBudgetExceeded
	}
	key := request.TenantID + "\x00" + request.RequestID
	reserved, ok := store.requests[key]
	if !ok {
		return errors.New("policy: budget was not reserved")
	}
	if request.Policy.MonthlyCostBudgetCents > 0 {
		limit := request.Policy.MonthlyCostBudgetCents * 10000
		updated := store.used[reserved.period] - reserved.cost + actualCostMicros
		if updated > limit {
			return ErrBudgetExceeded
		}
		store.used[reserved.period] = updated
		store.requests[key] = budgetReservation{period: reserved.period, cost: actualCostMicros}
	}
	return nil
}
func (store *MemoryBudget) period(tenantID string) string {
	now := time.Now()
	if store.now != nil {
		now = store.now()
	}
	return tenantID + "\x00" + now.UTC().Format("2006-01")
}

type MemoryApprovals struct {
	mu       sync.RWMutex
	approved map[string]struct{}
	waiters  map[string]chan struct{}
}

func NewMemoryApprovals() *MemoryApprovals {
	return &MemoryApprovals{approved: make(map[string]struct{}), waiters: make(map[string]chan struct{})}
}
func (store *MemoryApprovals) Grant(tenantID, requestID, toolName string) bool {
	if store == nil || tenantID == "" || requestID == "" || toolName == "" {
		return false
	}
	key := tenantID + "\x00" + requestID + "\x00" + toolName
	store.mu.Lock()
	store.approved[key] = struct{}{}
	if waiter := store.waiters[key]; waiter != nil {
		close(waiter)
		delete(store.waiters, key)
	}
	store.mu.Unlock()
	return true
}
func (store *MemoryApprovals) Approved(_ context.Context, tenantID, requestID, toolName string) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	_, ok := store.approved[tenantID+"\x00"+requestID+"\x00"+toolName]
	return ok
}
func (store *MemoryApprovals) Wait(ctx context.Context, tenantID, requestID, toolName string) bool {
	key := tenantID + "\x00" + requestID + "\x00" + toolName
	store.mu.Lock()
	if _, ok := store.approved[key]; ok {
		store.mu.Unlock()
		return true
	}
	waiter := store.waiters[key]
	if waiter == nil {
		waiter = make(chan struct{})
		store.waiters[key] = waiter
	}
	store.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-waiter:
		return true
	}
}

type namedTool string

func (tool namedTool) Declaration() *trpctool.Declaration {
	return &trpctool.Declaration{Name: string(tool)}
}
func toolName(tool trpctool.Tool) string {
	if tool == nil || tool.Declaration() == nil {
		return ""
	}
	return tool.Declaration().Name
}
func nameSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
