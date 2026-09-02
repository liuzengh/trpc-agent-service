// Package governance provides tenant-level Filter strategies: sensitive-data
// redaction (tool args/results), budget limiting (token quota) and IM user
// permission checks. Redaction runs as a framework plugin (BeforeTool /
// AfterTool); budget and permission are checked by the worker where the
// tenant/user context is available.
package governance

import (
	"context"
	"regexp"

	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// Sensitive-data patterns (redaction). Order matters: long digit runs (ID
// card 18-digit / bank card 16-19-digit) first, then the shorter phone and
// email — so an ID number's inner 11-digit substring is not mis-masked as a
// phone number.
var redactPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\d{16,19}`),                                       // ID card / bank card
	regexp.MustCompile(`1[3-9]\d{9}`),                                     // mobile phone
	regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), // email
}

// Redact masks sensitive data (ID card, bank card, phone, email) in text.
func Redact(text string) string {
	out := text
	for _, re := range redactPatterns {
		out = re.ReplaceAllStringFunc(out, func(s string) string {
			if len(s) <= 4 {
				return "***"
			}
			return s[:3] + "***" + s[len(s)-2:]
		})
	}
	return out
}

// RedactionFilter is a framework plugin that redacts tool arguments before
// execution and tool results after, so sensitive data never reaches the model
// or the audit log verbatim.
type RedactionFilter struct{}

// NewRedactionFilter returns a redaction plugin.
func NewRedactionFilter() *RedactionFilter { return &RedactionFilter{} }

// Name implements plugin.Plugin.
func (f *RedactionFilter) Name() string { return "governance-redaction" }

// Register implements plugin.Plugin.
func (f *RedactionFilter) Register(r *plugin.Registry) {
	r.BeforeTool(f.beforeTool)
	r.AfterTool(f.afterTool)
}

func (f *RedactionFilter) beforeTool(_ context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
	if args == nil || len(args.Arguments) == 0 {
		return nil, nil
	}
	redacted := Redact(string(args.Arguments))
	if redacted == string(args.Arguments) {
		return nil, nil
	}
	return &tool.BeforeToolResult{ModifiedArguments: []byte(redacted)}, nil
}

func (f *RedactionFilter) afterTool(_ context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
	if args == nil || args.Result == nil {
		return nil, nil
	}
	if s, ok := args.Result.(string); ok && s != Redact(s) {
		return &tool.AfterToolResult{CustomResult: Redact(s)}, nil
	}
	return nil, nil
}

// Budget enforces a per-tenant token quota. Usage resolves the tenant's
// cumulative token consumption (nil disables the check).
type Budget struct {
	QuotaTokens int64
	Usage       func(ctx context.Context, tenantID string) (int64, error)
}

// Exceeded reports whether the tenant has reached its token quota.
func (b *Budget) Exceeded(ctx context.Context, tenantID string) (bool, error) {
	if b == nil || b.Usage == nil || b.QuotaTokens <= 0 {
		return false, nil
	}
	used, err := b.Usage(ctx, tenantID)
	if err != nil {
		return false, err
	}
	return used >= b.QuotaTokens, nil
}

// Permission gates which IM users may use a tenant's agents. Allowed returns
// false to deny; a nil Allowed allows everyone (default-open).
type Permission struct {
	Allowed func(ctx context.Context, tenantID, userID string) bool
}

// Check reports whether the user is allowed.
func (p *Permission) Check(ctx context.Context, tenantID, userID string) bool {
	if p == nil || p.Allowed == nil {
		return true
	}
	return p.Allowed(ctx, tenantID, userID)
}
