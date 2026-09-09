// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestBoundedMaskingPreservesRegexpReplacementSemantics(t *testing.T) {
	tests := []struct {
		pattern, source, replacement string
	}{
		{`(a)(b)?`, "aa ab", `${1}-$2-$$-$0-$01-$3-$1x`},
		{`(?P<name>a)|(?P<name>b)`, "abcab", `$name-${name}-$$name`},
		{`(a)`, "aba", `${1$1}-${1}-${}-$$$1-$$$$1-$`},
		{`(a)`, "a", "$\u00e9-${\u00e9}-$1000000000-${01}-$!-$1"},
		{`(?m)^(a*)$`, "aa\na\nb", `${1}${1}`},
		{`\B(a)`, "ba a ca", `<${1}>`},
		{`\b(a)`, "ba a ca", `<${1}>`},
		{`a*`, "baaab", `$$`},
		{`(a*)`, "baaab", `${1}x`},
		{`(?:)`, "\u00e9a", `_`},
		{`(a){0}`, "aa", `$1x`},
		{`a|$`, "a", `_`},
		{`a*`, "", `empty`},
		{`b`, "aaa", `none`},
	}
	for _, test := range tests {
		t.Run(test.pattern+"/"+test.replacement, func(t *testing.T) {
			re := regexp.MustCompile(test.pattern)
			want := re.ReplaceAllString(test.source, test.replacement)
			got, err := replaceMaskingString(context.Background(), re, test.source, test.replacement, 1024, 4096)
			if err != nil || got != want {
				t.Fatalf("bounded replacement = %q, %v; standard library = %q", got, err, want)
			}
		})
	}
}

func TestMaskingRejectsExpansionBeforeLaterShrinkingRule(t *testing.T) {
	rules := make([]tenant.MaskingRule, 8)
	for index := range rules {
		rules[index] = tenant.MaskingRule{Type: "custom", Pattern: `(a)`, Replace: `${1}${1}`}
	}
	rules = append(rules, tenant.MaskingRule{Type: "custom", Pattern: `a+`, Replace: "safe"})
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{DataMasking: rules}})
	got, err := filter.maskSensitiveData(context.Background(), "a", 64)
	if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("expanding chain = %#v, %v; want fail-closed size error", got, err)
	}
}

func TestMaskingBoundsAggregateJSONOutput(t *testing.T) {
	for _, length := range []int{24, 25} {
		filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
			DataMasking: []tenant.MaskingRule{{Type: "custom", Pattern: `a`, Replace: strings.Repeat("x", length)}},
		}})
		got, err := filter.maskSensitiveData(context.Background(), map[string]interface{}{"x": "a", "y": "a"}, 64)
		if length == 25 {
			if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
				t.Fatalf("aggregate overflow = %#v, %v; want fail-closed size error", got, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(got)
		if err != nil || len(encoded) != 63 {
			t.Fatalf("bounded aggregate = %s, %v; want 63 bytes", encoded, err)
		}
	}
}

func TestMaskingBoundsEncodedExpansion(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "custom", Pattern: `a`, Replace: strings.Repeat("<", 11)}},
	}})
	got, err := filter.maskSensitiveData(context.Background(), "a", 64)
	if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("HTML-escaped overflow = %#v, %v; want fail-closed size error", got, err)
	}
}

func TestMaskingAggregateBudgetUsesDeterministicFieldOrder(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{
			{Type: "custom", Pattern: `y+`, Replace: ""},
			{Type: "custom", Pattern: `x`, Replace: strings.Repeat("a", 40)},
		},
	}})
	for index := 0; index < 64; index++ {
		input := map[string]interface{}{"a": strings.Repeat("y", 20), "b": "x"}
		got, err := filter.maskSensitiveData(context.Background(), input, 64)
		if err != nil || got == nil {
			t.Fatalf("iteration %d changed deterministic budget admission: %#v, %v", index, got, err)
		}
		input = map[string]interface{}{"a": "x", "b": strings.Repeat("y", 20)}
		got, err = filter.maskSensitiveData(context.Background(), input, 64)
		if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
			t.Fatalf("iteration %d ignored intermediate aggregate budget: %#v, %v", index, got, err)
		}
	}
}

func TestMaskingBoundsSingleCaptureExpansionAndMatchIndices(t *testing.T) {
	tests := []struct {
		name, pattern, source, replacement string
		limit, matchBudget                 int
	}{
		{"repeated capture", `(a+)`, strings.Repeat("a", 32), strings.Repeat("${1}", 256), 64, 4096},
		{"capture index storage", `(a)`, strings.Repeat("a", 32), "x", 64, 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := replaceMaskingString(context.Background(), regexp.MustCompile(test.pattern), test.source, test.replacement, test.limit, test.matchBudget)
			if got != "" || err == nil {
				t.Fatalf("bounded replacement = %q, %v; want size error", got, err)
			}
		})
	}
}

type maskingInputMarshaler struct {
	Payload string
	called  *bool
}

func (value maskingInputMarshaler) MarshalJSON() ([]byte, error) {
	*value.called = true
	return json.Marshal(value.Payload)
}

func TestMaskingChecksInputBeforeJSONEncoding(t *testing.T) {
	called := false
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	got, err := filter.maskSensitiveData(context.Background(), maskingInputMarshaler{
		Payload: strings.Repeat("x", 65), called: &called,
	}, 64)
	if called || got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("oversized input = %#v, %v, marshaler called = %v", got, err, called)
	}
}

func TestMaskingBoundsRawJSONNormalizationAndDepth(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	inputs := []json.RawMessage{
		json.RawMessage(`"` + strings.Repeat("\xff", 22) + `"`),
		json.RawMessage(strings.Repeat("[", maxMaskingDepth+1) + `"x"` + strings.Repeat("]", maxMaskingDepth+1)),
	}
	for index, input := range inputs {
		limit := 64
		if index == 1 {
			limit = 256
		}
		got, err := filter.maskSensitiveData(context.Background(), input, limit)
		if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
			t.Fatalf("raw JSON case %d = %#v, %v; want fail-closed error", index, got, err)
		}
	}
}

func TestMaskingHonorsCanceledContext(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := filter.AfterToolInvocation(ctx, "lookup", "alice@example.com", nil)
	if got != nil || !errors.Is(err, ErrUnsafeToolOutput) {
		t.Fatalf("canceled masking = %#v, %v; want fail-closed error", got, err)
	}
}

func TestMaskingJSONSizeMatchesEncodingJSON(t *testing.T) {
	inputs := []interface{}{
		"", "ascii", "\b\f\n\r\t\x00\x1f\\\"<>&", "\u00e9\u2028\u2029\ufffd\xff",
		json.Number("18446744073709551615"), json.Number("1e400"), true, false, nil,
		map[string]interface{}{"<key>": []interface{}{"\xff", true, nil, json.Number("0")}},
	}
	for _, input := range inputs {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := maskingJSONSize(input, len(encoded), 0)
		if err != nil || got != len(encoded) {
			t.Fatalf("JSON size(%#v) = %d, %v; want %d", input, got, err, len(encoded))
		}
		if _, err := maskingJSONSize(input, len(encoded)-1, 0); err == nil {
			t.Fatalf("JSON size(%#v) accepted one byte over budget", input)
		}
	}
}
