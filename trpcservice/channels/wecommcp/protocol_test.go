package wecommcp

import (
	"encoding/json"
	"reflect"
	"testing"

	mcp "trpc.group/trpc-go/trpc-mcp-go"
)

func TestToolPayloadEncodings(t *testing.T) {
	text := mcp.NewTextContent(`{"errcode":0,"success":true}`)
	for name, result := range map[string]*mcp.CallToolResult{
		"structured":   {StructuredContent: map[string]any{"errcode": 0, "success": true}},
		"text value":   {Content: []mcp.Content{text}},
		"text pointer": {Content: []mcp.Content{&text}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := toolPayload(result)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if json.Unmarshal(raw, &got) != nil || !reflect.DeepEqual(got, map[string]any{"errcode": float64(0), "success": true}) {
				t.Fatalf("unexpected normalized payload: %s", raw)
			}
		})
	}
	for _, result := range []*mcp.CallToolResult{
		{Content: []mcp.Content{mcp.NewTextContent("not JSON")}},
		{Content: []mcp.Content{text, text}},
	} {
		raw, err := toolPayload(result)
		if err != nil {
			t.Fatal(err)
		}
		var got []json.RawMessage
		if json.Unmarshal(raw, &got) != nil || len(got) != len(result.Content) {
			t.Fatal("opaque content should remain an array")
		}
	}
	if _, err := toolPayload(&mcp.CallToolResult{StructuredContent: make(chan int)}); err == nil {
		t.Fatal("invalid structured content accepted")
	}
}

func TestEndpointHashRemainsStable(t *testing.T) {
	if endpointHash("abc") != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatal("persisted channel fingerprints must not change")
	}
}
