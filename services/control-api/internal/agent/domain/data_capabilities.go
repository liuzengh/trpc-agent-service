package domain

import (
	"encoding/json"
	"sort"
	"strconv"
)

// Runtime contains explicit session-wide configuration, never storage targets.
type Runtime struct {
	Summary *Summary `json:"summary,omitempty"`
}
type Summary struct {
	Enabled        bool   `json:"enabled"`
	ModelSlot      string `json:"model_slot,omitempty"`
	EventThreshold *int64 `json:"event_threshold,omitempty"`
}
type Memory struct {
	Tools []string `json:"tools"`
	// SDK semantics: -1 loads all, 0 disables preload, positive counts entries.
	PreloadLimit *int64 `json:"preload_limit,omitempty"`
}
type Artifact struct {
	Enabled bool `json:"enabled"`
}

// Enabled is the resource-closure rule, not permission inferred from Profile.
func (m *Memory) Enabled() bool {
	return m != nil && (len(m.Tools) > 0 || (m.PreloadLimit != nil && *m.PreloadLimit != 0))
}
func validMemoryTool(name string) bool {
	switch name {
	case "memory_add", "memory_update", "memory_delete", "memory_clear", "memory_search", "memory_load":
		return true
	}
	return false
}
func dataObject(value any, pointer string, fields, required []string, d *[]Diagnostic) (map[string]any, bool) {
	o, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, d)
		return nil, false
	}
	validateAllowedFields(o, pointer, fields, d)
	requireFields(o, pointer, required, d)
	return o, true
}
func dataBool(o map[string]any, k, p string, d *[]Diagnostic) (bool, bool) {
	v, exists := o[k]
	if !exists {
		return false, false
	}
	b, ok := v.(bool)
	if !ok {
		addTypeDiagnostic(p+"/"+k, d)
	}
	return b, ok
}
func dataInteger(o map[string]any, k, p string, min int64, d *[]Diagnostic) {
	if v, exists := o[k]; exists {
		n, ok := integerValue(v)
		if !ok {
			addTypeDiagnostic(p+"/"+k, d)
		} else if n < min || n > 9007199254740991 {
			addLimitDiagnostic(p+"/"+k, d)
		}
	}
}
func validateRuntime(value any, d *[]Diagnostic) {
	o, ok := dataObject(value, "/runtime", []string{"summary"}, nil, d)
	if !ok {
		return
	}
	value, exists := o["summary"]
	if !exists {
		return
	}
	p := "/runtime/summary"
	s, ok := dataObject(value, p, []string{"enabled", "model_slot", "event_threshold"}, []string{"enabled"}, d)
	if !ok {
		return
	}
	enabled, valid := dataBool(s, "enabled", p, d)
	if !valid {
		return
	}
	if !enabled {
		validateAllowedFields(s, p, []string{"enabled"}, d)
		return
	}
	requireFields(s, p, []string{"model_slot", "event_threshold"}, d)
	validateIdentifierField(s, "model_slot", p, d)
	dataInteger(s, "event_threshold", p, 1, d)
}
func validateNodeData(p string, node map[string]any, d *[]Diagnostic) {
	validateWorkspaceShape(p, node, d)
	if value, exists := node["memory"]; exists {
		q := p + "/memory"
		if o, ok := dataObject(value, q, []string{"tools", "preload_limit"}, []string{"tools"}, d); ok {
			if tools, exists := o["tools"]; exists {
				validateStringArray(tools, q+"/tools", 0, 6, capabilityPattern, "", d)
				if a, ok := tools.([]any); ok {
					for i, v := range a {
						if name, ok := v.(string); ok && !validMemoryTool(name) {
							*d = append(*d, Diagnostic{Code: "AGENT_SPEC_MEMORY_TOOL_UNSUPPORTED", Severity: SeverityError, Pointer: q + "/tools/" + strconv.Itoa(i), Message: "memory tool is not supported"})
						}
					}
				}
			}
			dataInteger(o, "preload_limit", q, -1, d)
		}
	}
	if value, exists := node["artifact"]; exists {
		q := p + "/artifact"
		if o, ok := dataObject(value, q, []string{"enabled"}, []string{"enabled"}, d); ok {
			dataBool(o, "enabled", q, d)
		}
	}
	dataBool(node, "add_session_summary", p, d)
}
func validateDataSemantics(s Spec, d *[]Diagnostic) {
	enabled := s.Runtime != nil && s.Runtime.Summary != nil && s.Runtime.Summary.Enabled
	if enabled {
		summary := s.Runtime.Summary
		requirement, ok := s.Requirements.Models[summary.ModelSlot]
		if !ok {
			*d = append(*d, Diagnostic{Code: "AGENT_SPEC_MODEL_SLOT_NOT_FOUND", Severity: SeverityError, Pointer: "/runtime/summary/model_slot", Message: "summary model slot is not declared"})
		} else {
			chat := false
			for _, c := range requirement.Capabilities {
				if c == "chat" {
					chat = true
				}
			}
			if !chat {
				*d = append(*d, Diagnostic{Code: "AGENT_SPEC_SUMMARY_MODEL_CAPABILITY", Severity: SeverityError, Pointer: "/runtime/summary/model_slot", Message: "summary model must require chat capability"})
			}
		}
	}
	for _, id := range sortedNodeIDs(s.Nodes) {
		n := s.Nodes[id]
		if n.AddSessionSummary != nil && *n.AddSessionSummary && !enabled {
			*d = append(*d, nodeDiagnostic("AGENT_SPEC_SUMMARY_NOT_ENABLED", SeverityError, "/nodes/"+escapeJSONPointer(id)+"/add_session_summary", id, "summary consumption requires enabled runtime summary"))
		}
	}
}
func cloneRuntime(r *Runtime) *Runtime {
	if r == nil {
		return nil
	}
	out := *r
	if r.Summary != nil {
		s := *r.Summary
		if s.EventThreshold != nil {
			v := *s.EventThreshold
			s.EventThreshold = &v
		}
		out.Summary = &s
	}
	return &out
}
func cloneNodeData(n Node) Node {
	if n.Memory != nil {
		m := *n.Memory
		m.Tools = append([]string{}, m.Tools...)
		sort.Strings(m.Tools)
		if m.PreloadLimit != nil {
			v := *m.PreloadLimit
			m.PreloadLimit = &v
		}
		n.Memory = &m
	}
	if n.Artifact != nil {
		a := *n.Artifact
		n.Artifact = &a
	}
	if n.AddSessionSummary != nil {
		v := *n.AddSessionSummary
		n.AddSessionSummary = &v
	}
	return n
}
func normalizeDataIntegers(root map[string]any) {
	normalize := func(o map[string]any, k string) {
		if v, ok := integerValue(o[k]); ok {
			o[k] = json.Number(strconv.FormatInt(v, 10))
		}
	}
	runtime, _ := root["runtime"].(map[string]any)
	summary, _ := runtime["summary"].(map[string]any)
	normalize(summary, "event_threshold")
	nodes, _ := root["nodes"].(map[string]any)
	for _, v := range nodes {
		node, _ := v.(map[string]any)
		memory, _ := node["memory"].(map[string]any)
		normalize(memory, "preload_limit")
	}
}
