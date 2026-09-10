package redaction

import (
	"strings"
	"testing"
)

func TestCompileProtectsMandatoryAndStrictFields(t *testing.T) {
	basic, err := Compile(Config{Level: LevelBasic})
	if err != nil {
		t.Fatal(err)
	}
	if !basic.RedactKey("model.api-key") || basic.RedactKey("external_user_id") {
		t.Fatalf("basic key policy mismatch")
	}
	strict, err := Compile(Config{Level: LevelStrict})
	if err != nil {
		t.Fatal(err)
	}
	if !strict.RedactKey("external_user_id") {
		t.Fatal("strict policy did not redact PII")
	}
	value := strict.RedactText("authorization=canary bearer abc.def postgres://app:canary@db")
	if strings.Contains(value, "canary") || strings.Contains(value, "abc.def") {
		t.Fatalf("mandatory text redaction leaked value: %q", value)
	}
}

func TestCompileAddsCustomRules(t *testing.T) {
	program, err := Compile(Config{Rules: []Rule{{
		ID:           "customer-reference",
		KeyFragments: []string{"customer_ref"},
		TextPattern:  `CUST-[0-9]{6}`,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !program.RedactKey("customer-ref") {
		t.Fatal("custom key matcher was not compiled")
	}
	if got := program.RedactText("reference CUST-123456"); got != "reference "+Replacement {
		t.Fatalf("custom text matcher mismatch: %q", got)
	}
}

func TestCompileRejectsUnsafeRuleDefinitions(t *testing.T) {
	for _, rules := range [][]Rule{
		{{ID: "empty"}},
		{{ID: "duplicate", KeyFragments: []string{"one"}}, {ID: "duplicate", KeyFragments: []string{"two"}}},
		{{ID: "broken", TextPattern: "("}},
		{{ID: "bad key", KeyFragments: []string{"one"}}},
	} {
		if _, err := Compile(Config{Rules: rules}); err == nil {
			t.Fatalf("expected invalid rules to be rejected: %#v", rules)
		}
	}
}

func TestContextCarriesOnlyCompiledProgram(t *testing.T) {
	program, err := Compile(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if scoped, ok := ProgramFromContext(ContextWithProgram(nil, program)); !ok || scoped != program {
		t.Fatalf("context program=%p ok=%t", scoped, ok)
	}
}
