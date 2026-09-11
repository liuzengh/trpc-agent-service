package governance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"

	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	ErrUserDenied             = errors.New("IM user is not allowed for this tenant binding")
	ErrInputTooLarge          = errors.New("message exceeds tenant input budget")
	ErrRateLimited            = errors.New("tenant request rate exceeded")
	ErrRateLimiterUnavailable = coordination.ErrRateLimiterUnavailable
	ErrBudgetExceeded         = budget.ErrBudgetExceeded
	approvalPattern           = regexp.MustCompile(`(?i)(?:^|\s)#approve:([a-f0-9-]{16,64})(?:\s|$)`)
)

const (
	maxAttachmentCount     = 8
	maxAttachmentTypeRunes = 64
	maxAttachmentNameRunes = 256
	maxAttachmentMIMERunes = 128
	maxPendingApprovals    = 10000
	maxLLMCallsPerRun      = 8
)

type requestContextKey struct{}

type RequestContext struct {
	TenantID      string
	ConfigVersion string
	AppNamespace  string
	UserID        string
	SessionID     string
	TurnID        string
	DedupKey      string
	SenderID      string
	Channel       string
	BindingID     string
	AgentName     string
	RequestID     string
	ApprovalNonce string
	AuditPolicy   config.AuditPolicy
	AuditSecrets  []string
}

func WithRequestContext(ctx context.Context, value RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey{}, value)
}

func RequestContextFrom(ctx context.Context) (RequestContext, bool) {
	value, ok := ctx.Value(requestContextKey{}).(RequestContext)
	return value, ok
}

type costBucket struct {
	month    string
	reserved float64
}

// Filter performs inbound user authorization, message-size enforcement and a
// shared tenant fixed-window request check before any model or storage call.
// The production constructor uses a persistent model-call ledger for monthly
// budget admission; the legacy constructor keeps the old in-process estimate
// for tests and explicitly local/demo deployments.
type Filter struct {
	mu           sync.Mutex
	costs        map[string]costBucket
	now          func() time.Time
	rateLimiter  coordination.RateLimiter
	legacyBudget bool
}

func NewFilter() *Filter {
	return &Filter{
		costs: make(map[string]costBucket), now: time.Now,
		rateLimiter: coordination.NewInMemory(), legacyBudget: true,
	}
}

// NewFilterWithRateLimiter constructs the production path. Monthly budget
// admission is performed per actual model call by the persistent ledger; the
// filter only validates the message and consumes the shared request quota.
func NewFilterWithRateLimiter(limiter coordination.RateLimiter) *Filter {
	if limiter == nil {
		limiter = coordination.NewInMemory()
	}
	return &Filter{costs: make(map[string]costBucket), now: time.Now, rateLimiter: limiter}
}

func (f *Filter) CheckInbound(tenant config.TenantConfig, binding config.ChannelConfig, msg domain.InboundMessage) error {
	return f.CheckInboundContext(context.Background(), tenant, binding, msg)
}

func (f *Filter) CheckInboundContext(ctx context.Context, tenant config.TenantConfig, binding config.ChannelConfig, msg domain.InboundMessage) error {
	if len(binding.AllowedUsers) > 0 && !contains(binding.AllowedUsers, msg.ExternalUserID) {
		return ErrUserDenied
	}
	if err := validateAttachmentMetadata(msg.Attachments); err != nil {
		return err
	}
	prompt := domain.UserPrompt(msg.Text, msg.Attachments, msg.Scope, domain.SenderIdentity(msg))
	if len([]rune(prompt)) > tenant.Budget.MaxInputChars {
		return ErrInputTooLarge
	}
	if f.rateLimiter != nil {
		allowed, _, err := f.rateLimiter.Allow(ctx, tenant.TenantID, tenant.Budget.RequestsPerMinute)
		if err != nil {
			return ErrRateLimiterUnavailable
		}
		if !allowed {
			return ErrRateLimited
		}
	}
	if !f.legacyBudget {
		return nil
	}
	now := f.now().UTC()
	month := now.Format("2006-01")
	estimatedCost := estimateMaxCost(tenant, prompt)
	f.mu.Lock()
	defer f.mu.Unlock()
	cost := f.costs[tenant.TenantID]
	if cost.month != month {
		cost = costBucket{month: month}
	}
	if tenant.Budget.MonthlyCostUSD > 0 && cost.reserved+estimatedCost > tenant.Budget.MonthlyCostUSD {
		return ErrBudgetExceeded
	}
	cost.reserved += estimatedCost
	f.costs[tenant.TenantID] = cost
	return nil
}

func validateAttachmentMetadata(attachments []domain.Attachment) error {
	if len(attachments) > maxAttachmentCount {
		return fmt.Errorf("%w: at most %d attachments are accepted", ErrInputTooLarge, maxAttachmentCount)
	}
	for _, attachment := range attachments {
		if len([]rune(attachment.Type)) > maxAttachmentTypeRunes ||
			len([]rune(attachment.Name)) > maxAttachmentNameRunes ||
			len([]rune(attachment.MimeType)) > maxAttachmentMIMERunes {
			return fmt.Errorf("%w: attachment metadata is too large", ErrInputTooLarge)
		}
	}
	return nil
}

func estimateMaxCost(tenant config.TenantConfig, text string) float64 {
	// Four runes/token remains a tokenizer approximation, but reserve the full
	// configured eight-call Agent envelope rather than only one model turn.
	// Production still settles against provider usage in an atomic ledger.
	inputTokens := (len([]rune(text)) + 3) / 4
	perCall := float64(inputTokens)*tenant.Model.InputPrice + float64(tenant.Model.MaxTokens)*tenant.Model.OutputPrice
	return float64(maxLLMCallsPerRun) * perCall / 1_000_000
}

func ExtractApproval(text string) (cleanText, nonce string) {
	match := approvalPattern.FindStringSubmatch(text)
	if len(match) != 2 {
		return text, ""
	}
	clean := approvalPattern.ReplaceAllString(text, " ")
	return strings.TrimSpace(clean), strings.ToLower(match[1])
}

// ToolFilter is the model-visible allowlist. PermissionPolicy below repeats
// the check at execution time, because visibility is not an authorization
// boundary and framework tools may be injected dynamically.
func ToolFilter(policy config.ToolPolicy) tool.FilterFunc {
	allowed := stringSet(policy.Allow)
	denied := stringSet(policy.Deny)
	return func(_ context.Context, candidate tool.Tool) bool {
		if candidate == nil || candidate.Declaration() == nil {
			return false
		}
		name := candidate.Declaration().Name
		if _, blocked := denied[name]; blocked {
			return false
		}
		_, ok := allowed[name]
		return ok
	}
}

type ToolDecision struct {
	Request       RequestContext
	ToolName      string
	Decision      string
	Reason        string
	ArgumentsHash string
}

type ToolDecisionObserver func(context.Context, ToolDecision)

func PermissionPolicy(policy config.ToolPolicy, approvals ApprovalBackend) tool.PermissionPolicyFunc {
	return PermissionPolicyObserved(policy, approvals, nil)
}

func PermissionPolicyObserved(policy config.ToolPolicy, approvals ApprovalBackend, observer ToolDecisionObserver) tool.PermissionPolicyFunc {
	allowed := stringSet(policy.Allow)
	denied := stringSet(policy.Deny)
	dangerous := stringSet(policy.RequireConfirm)
	return func(ctx context.Context, req *tool.PermissionRequest) (tool.PermissionDecision, error) {
		decide := func(decision tool.PermissionDecision) (tool.PermissionDecision, error) {
			if observer != nil {
				rc, _ := RequestContextFrom(ctx)
				name := ""
				var args []byte
				if req != nil {
					name = req.ToolName
					args = req.Arguments
				}
				observer(ctx, ToolDecision{Request: rc, ToolName: name, Decision: string(decision.Action), Reason: decision.Reason, ArgumentsHash: ArgumentsHash(args)})
			}
			return decision, nil
		}
		if req == nil {
			return decide(tool.DenyPermission("missing tool permission request"))
		}
		if _, blocked := denied[req.ToolName]; blocked {
			return decide(tool.DenyPermission("tool denied by tenant policy"))
		}
		if _, ok := allowed[req.ToolName]; !ok {
			return decide(tool.DenyPermission("tool is not in the tenant allowlist"))
		}
		if _, needsApproval := dangerous[req.ToolName]; !needsApproval {
			return decide(tool.AllowPermission())
		}
		rc, ok := RequestContextFrom(ctx)
		if !ok || approvals == nil {
			return decide(tool.DenyPermission("approval context is unavailable"))
		}
		scope := approvalScope(rc, req.ToolName, req.Arguments)
		approved, err := approvals.ConsumeApproval(ctx, rc.ApprovalNonce, scope)
		if err != nil {
			return decide(tool.DenyPermission("approval storage is temporarily unavailable"))
		}
		if approved {
			return decide(tool.AllowPermission())
		}
		nonce, err := approvals.IssueApproval(ctx, scope, 5*time.Minute)
		if err != nil {
			return decide(tool.DenyPermission("approval token is temporarily unavailable"))
		}
		return decide(tool.AskPermission("confirmation required; resend the request with #approve:" + nonce))
	}
}

func approvalScope(rc RequestContext, toolName string, args []byte) string {
	normalized := normalizeJSON(args)
	h := sha256.Sum256(normalized)
	parts := []string{rc.TenantID, rc.ConfigVersion, rc.AppNamespace, rc.UserID, rc.SenderID,
		rc.SessionID, rc.Channel, rc.BindingID, toolName, hex.EncodeToString(h[:])}
	// JSON encodes field boundaries unambiguously, even for custom callers
	// supplying separator characters in a scope component.
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}

func normalizeJSON(raw []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	// json.Decoder accepts a valid value followed by another valid value. The
	// old json.Unmarshal-based behavior rejected that input, so preserve the
	// fail-open-to-original-bytes behavior for every trailing token too.
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return raw
	}
	if encoded, err := json.Marshal(value); err == nil {
		return encoded
	}
	return raw
}

func ArgumentsHash(raw []byte) string {
	h := sha256.Sum256(normalizeJSON(raw))
	return hex.EncodeToString(h[:])
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func stringSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item] = struct{}{}
	}
	return set
}

// SortedToolNames makes policy decisions deterministic in audit/config views.
func SortedToolNames(policy config.ToolPolicy) []string {
	names := append([]string(nil), policy.Allow...)
	sort.Strings(names)
	return names
}
