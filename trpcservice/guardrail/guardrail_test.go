package guardrail

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestCheckInput(t *testing.T) {
	p := tenant.Guardrails{
		MaxInputBytes:   64,
		BlockedKeywords: []string{"Secret", "内部代号"},
	}
	cases := []struct {
		name, text, want string
	}{
		{"clean", "hello", ""},
		{"too long", strings.Repeat("a", 65), RuleLength},
		{"keyword case-insensitive", "the SECRET is", RuleKeyword},
		{"keyword unicode", "提到内部代号了", RuleKeyword},
		{"length wins first", strings.Repeat("secret", 12), RuleLength},
	}
	for _, c := range cases {
		if got := CheckInput(p, c.text); got != c.want {
			t.Fatalf("%s: CheckInput = %q, want %q", c.name, got, c.want)
		}
	}
	if got := CheckInput(tenant.Guardrails{}, strings.Repeat("a", 1000)); got != "" {
		t.Fatalf("zero policy must allow everything, got %q", got)
	}
}

func TestStreamCheckerStraddlingKeyword(t *testing.T) {
	p := tenant.Guardrails{OutputBlockedKeywords: []string{"badword"}}
	s := NewStreamChecker(p)
	// Split the keyword across three chunks; the tail window must bridge it.
	if kw := s.Add("this is a ba"); kw != "" {
		t.Fatalf("premature trip on %q", kw)
	}
	if kw := s.Add("dw"); kw != "" {
		t.Fatalf("partial keyword must not trip yet, got %q", kw)
	}
	if kw := s.Add("ord and more"); kw != "badword" {
		t.Fatalf("straddled keyword not detected, got %q", kw)
	}
	if kw := s.Add("more text"); kw != "badword" {
		t.Fatalf("tripped checker must stay tripped, got %q", kw)
	}
	if s.Tripped() != "badword" {
		t.Fatalf("Tripped = %q", s.Tripped())
	}
}

func TestStreamCheckerCleanAndNoPolicy(t *testing.T) {
	s := NewStreamChecker(tenant.Guardrails{OutputBlockedKeywords: []string{"leak"}})
	for _, chunk := range []string{"all ", "good ", "news ", "le", "arning is fun"} {
		if kw := s.Add(chunk); kw != "" {
			t.Fatalf("false positive on %q via %q", chunk, kw)
		}
	}
	// "le" + "arning" must not match "leak"; a real keyword still fires later.
	if kw := s.Add(" but leak"); kw != "leak" {
		t.Fatalf("late keyword missed, got %q", kw)
	}

	none := NewStreamChecker(tenant.Guardrails{})
	if kw := none.Add("anything leak"); kw != "" {
		t.Fatalf("no-policy checker tripped on %q", kw)
	}
}
