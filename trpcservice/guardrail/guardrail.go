// Package guardrail enforces the per-tenant governance policy of proposal
// doc 3.5: an input check before the model call and a streaming tripwire
// over reply chunks. The Gateway is the single enforcement point; tool-level
// policies are second-phase work hanging off the framework Callbacks.
package guardrail

import (
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// User-facing replies when a policy fires.
const (
	RejectionText = "您的消息被租户安全策略拦截，请调整后重试。"
	CutoffText    = "回复因触发租户安全策略已被截断。"
)

// Rule names used as audit/metric labels.
const (
	RuleLength  = "length"
	RuleKeyword = "keyword"
)

// CheckInput validates user text against the policy before the model call.
// It returns the fired rule (RuleLength or RuleKeyword), or "" when allowed.
func CheckInput(p tenant.Guardrails, text string) string {
	if p.MaxInputBytes > 0 && len(text) > p.MaxInputBytes {
		return RuleLength
	}
	if matchAny(p.BlockedKeywords, text) {
		return RuleKeyword
	}
	return ""
}

// matchAny reports whether any keyword occurs in text, case-insensitively.
func matchAny(keywords []string, text string) bool {
	if len(keywords) == 0 {
		return false
	}
	lower := strings.ToLower(text)
	for _, kw := range keywords {
		if strings.Contains(lower, strings.ToLower(kw)) {
			return true
		}
	}
	return false
}

// StreamChecker is the streaming output tripwire. Reply chunks arrive in
// arbitrary slices, so a keyword may straddle two chunks; the checker keeps
// a tail window as long as the longest keyword so split matches still fire.
type StreamChecker struct {
	keywords []string
	maxLen   int
	tail     string
	tripped  string
}

// NewStreamChecker builds a tripwire for the policy's output keywords. A
// policy without output keywords yields a checker that never trips.
func NewStreamChecker(p tenant.Guardrails) *StreamChecker {
	s := &StreamChecker{keywords: p.OutputBlockedKeywords}
	for _, kw := range s.keywords {
		if n := len(strings.ToLower(kw)); n > s.maxLen {
			s.maxLen = n
		}
	}
	return s
}

// Add feeds one reply chunk and returns the keyword that tripped the wire
// ("" while clean). Once tripped, every later Add keeps returning it so the
// caller can stop forwarding without re-checking.
func (s *StreamChecker) Add(chunk string) string {
	if s.tripped != "" {
		return s.tripped
	}
	if len(s.keywords) == 0 {
		return ""
	}
	window := strings.ToLower(s.tail + chunk)
	for _, kw := range s.keywords {
		if strings.Contains(window, strings.ToLower(kw)) {
			s.tripped = kw
			return kw
		}
	}
	if len(window) > s.maxLen {
		window = window[len(window)-s.maxLen:]
	}
	s.tail = window
	return ""
}

// Tripped reports the keyword that fired, or "" when the stream stayed clean.
func (s *StreamChecker) Tripped() string {
	return s.tripped
}
