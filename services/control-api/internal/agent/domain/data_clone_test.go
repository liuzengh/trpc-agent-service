package domain

import "testing"

func TestNormalizeDataOwnsPointers(t *testing.T) {
	v := int64(1)
	b := true
	in := Spec{Runtime: &Runtime{Summary: &Summary{Enabled: true, ModelSlot: "model", EventThreshold: &v}}, Nodes: map[string]Node{"n": {Kind: NodeKindLLM, Memory: &Memory{Tools: []string{"memory_update", "memory_add"}, PreloadLimit: &v}, Artifact: &Artifact{Enabled: true}, AddSessionSummary: &b}}}
	out := normalizeSpec(in)
	*out.Runtime.Summary.EventThreshold = 2
	n := out.Nodes["n"]
	n.Memory.Tools[0] = "changed"
	*n.Memory.PreloadLimit = 3
	n.Artifact.Enabled = false
	*n.AddSessionSummary = false
	if v != 1 || !b || in.Nodes["n"].Memory.Tools[0] != "memory_update" || !in.Nodes["n"].Artifact.Enabled {
		t.Fatal("normalization aliased source")
	}
}
