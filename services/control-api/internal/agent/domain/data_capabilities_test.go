package domain_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const dataBase = `{"schema_version":"v1","root":"assistant","requirements":{"models":{"primary":{"capabilities":["chat"]},"summary":{"capabilities":["chat"]}},"tools":{},"knowledge":{}},"nodes":{"assistant":{"kind":"llm","instruction":"Answer","model_slot":"primary","tool_slots":[],"knowledge_slots":[]}}}`

func dataDoc(t *testing.T, f func(map[string]any, map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(dataBase), &m); err != nil {
		t.Fatal(err)
	}
	n := m["nodes"].(map[string]any)["assistant"].(map[string]any)
	f(m, n)
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestRuntimeDataValidSchemaAndDomain(t *testing.T) {
	schema := compilePublicSchema(t)
	for _, preload := range []int64{-1, 0, 1, 9007199254740991} {
		t.Run(strconv.FormatInt(preload, 10), func(t *testing.T) {
			b := dataDoc(t, func(m, n map[string]any) {
				m["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_slot": "summary", "event_threshold": 4}}
				n["memory"] = map[string]any{"tools": []string{"memory_update", "memory_add", "memory_delete", "memory_clear", "memory_search", "memory_load"}, "preload_limit": preload}
				n["artifact"] = map[string]any{"enabled": true}
				n["add_session_summary"] = true
			})
			instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			if err = schema.Validate(instance); err != nil {
				t.Fatal(err)
			}
			c, r := domain.ValidateForPublication(b, 1)
			if !r.Valid {
				t.Fatal(r)
			}
			var s domain.Spec
			if err = json.Unmarshal(c.Document, &s); err != nil {
				t.Fatal(err)
			}
			if s.Runtime.Summary.ModelSlot != "summary" || *s.Nodes["assistant"].Memory.PreloadLimit != preload || !s.Nodes["assistant"].Memory.Enabled() || !s.Nodes["assistant"].Artifact.Enabled || !*s.Nodes["assistant"].AddSessionSummary {
				t.Fatal(string(c.Document))
			}
			for _, d := range r.Diagnostics {
				if d.Code == "AGENT_SPEC_UNUSED_MODEL_SLOT" {
					t.Fatal("summary model excluded from closure", d)
				}
			}
			again, rr := domain.ValidateForPublication(c.Document, 1)
			if !rr.Valid || c.Digest != again.Digest || !bytes.Equal(c.Document, again.Document) {
				t.Fatal("canonical not stable")
			}
		})
	}
}
func TestRuntimeDataInvalidSchemaAndDomain(t *testing.T) {
	tests := map[string]func(map[string]any, map[string]any){
		"unknown-tool":     func(m, n map[string]any) { n["memory"] = map[string]any{"tools": []string{"memory_execute"}} },
		"duplicate-tool":   func(m, n map[string]any) { n["memory"] = map[string]any{"tools": []string{"memory_add", "memory_add"}} },
		"null-memory":      func(m, n map[string]any) { n["memory"] = nil },
		"missing-tools":    func(m, n map[string]any) { n["memory"] = map[string]any{"preload_limit": 1} },
		"negative-preload": func(m, n map[string]any) { n["memory"] = map[string]any{"tools": []string{}, "preload_limit": -2} },
		"unsafe-preload": func(m, n map[string]any) {
			n["memory"] = map[string]any{"tools": []string{}, "preload_limit": 9007199254740992}
		},
		"fraction-preload": func(m, n map[string]any) { n["memory"] = map[string]any{"tools": []string{}, "preload_limit": 1.5} },
		"foreign-subject":  func(m, n map[string]any) { n["memory"] = map[string]any{"tools": []string{}, "subject_id": "other"} },
		"artifact-auto-tools": func(m, n map[string]any) {
			n["artifact"] = map[string]any{"enabled": true, "tools": []string{"delete"}}
		},
		"artifact-null": func(m, n map[string]any) { n["artifact"] = nil },
		"summary-null":  func(m, n map[string]any) { m["runtime"] = map[string]any{"summary": nil} },
		"runtime-null":  func(m, n map[string]any) { m["runtime"] = nil },
		"disabled-with-options": func(m, n map[string]any) {
			m["runtime"] = map[string]any{"summary": map[string]any{"enabled": false, "model_slot": "summary"}}
		},
		"summary-no-threshold": func(m, n map[string]any) {
			m["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_slot": "summary"}}
		},
		"summary-zero": func(m, n map[string]any) {
			m["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_slot": "summary", "event_threshold": 0}}
		},
		"consume-null": func(m, n map[string]any) { n["add_session_summary"] = nil },
		"non-llm": func(m, n map[string]any) {
			m["nodes"].(map[string]any)["root"] = map[string]any{"kind": "sequence", "children": []string{"assistant"}, "memory": map[string]any{"tools": []string{}}}
			m["root"] = "root"
		},
	}
	schema := compilePublicSchema(t)
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			b := dataDoc(t, f)
			v, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			if schema.Validate(v) == nil {
				t.Fatal("schema accepted")
			}
			if c, r := domain.ValidateForPublication(b, 1); r.Valid || len(c.Document) > 0 {
				t.Fatal("domain accepted", r)
			}
		})
	}
}
func TestRuntimeDataSemanticDependencies(t *testing.T) {
	for _, slot := range []string{"missing", "primary"} {
		b := dataDoc(t, func(m, n map[string]any) {
			m["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_slot": slot, "event_threshold": 1}}
			if slot == "primary" {
				m["requirements"].(map[string]any)["models"].(map[string]any)["primary"] = map[string]any{"capabilities": []string{"embedding"}}
			}
		})
		if _, r := domain.ValidateForPublication(b, 1); r.Valid {
			t.Fatal("bad summary slot accepted")
		}
	}
	b := dataDoc(t, func(m, n map[string]any) { n["add_session_summary"] = true })
	if _, r := domain.ValidateForPublication(b, 1); r.Valid {
		t.Fatal("consumption without generation")
	}
}
func TestMemoryResourceEnablement(t *testing.T) {
	for _, v := range []int64{-1, 0, 1} {
		m := &domain.Memory{Tools: []string{}, PreloadLimit: &v}
		if m.Enabled() != (v != 0) {
			t.Fatal(v)
		}
	}
	if (*domain.Memory)(nil).Enabled() || (&domain.Memory{Tools: []string{}}).Enabled() {
		t.Fatal("empty enabled")
	}
	if !(&domain.Memory{Tools: []string{"memory_load"}}).Enabled() {
		t.Fatal("tool did not enable memory")
	}
}
func TestLegacyAgentCanonicalAndDigestUnchanged(t *testing.T) {
	raw, err := os.ReadFile("testdata/p0-legacy-canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]struct {
		Canonical string `json:"canonical"`
		Digest    string `json:"digest"`
	}
	if err = json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	for name, want := range golden {
		t.Run(name, func(t *testing.T) {
			c, r := domain.ValidateForPublication(readFixture(t, fixturePath(t, "valid", name)), 1)
			if !r.Valid || string(c.Document) != want.Canonical || c.Digest != want.Digest {
				t.Fatal("legacy bytes/digest changed", r)
			}
		})
	}
}
func TestRuntimeDataIntegerLexemesAndDisabledDefaults(t *testing.T) {
	raw := dataDoc(t, func(m, n map[string]any) {
		m["runtime"] = map[string]any{"summary": map[string]any{"enabled": false}}
		n["memory"] = map[string]any{"tools": []string{}, "preload_limit": 0}
		n["artifact"] = map[string]any{"enabled": false}
		n["add_session_summary"] = false
	})
	c, r := domain.ValidateForPublication(bytes.ReplaceAll(raw, []byte(`"preload_limit":0`), []byte(`"preload_limit":0.0`)), 1)
	if !r.Valid {
		t.Fatal(r)
	}
	var s domain.Spec
	_ = json.Unmarshal(c.Document, &s)
	if s.Nodes["assistant"].Memory.Enabled() {
		t.Fatal("empty behavior enables memory")
	}
}
