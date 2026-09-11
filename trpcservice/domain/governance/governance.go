// Package governance provides tenant-level Filter strategies: sensitive-data
// redaction of tool arguments and results, enforced as a framework plugin
// (BeforeTool / AfterTool). The broader tenant policy (budget, IM allow-list,
// tool whitelist, forced approvals) is resolved by the worker from the
// tenant's quota / audit_policy config.
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
