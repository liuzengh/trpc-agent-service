package trpcagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

type failedArtifact struct{ artifact.Service }

func (failedArtifact) SaveArtifact(context.Context, artifact.SessionInfo, string, *artifact.Artifact) (int, error) {
	return 0, errors.New("backend-secret-never-expose")
}
func TestArtifactAndMemoryToolsComposeThroughSDK(t *testing.T) {
	names := append(append([]string{}, ArtifactToolNames...), memory.AddToolName)
	f, s := newMemoryHTTPFixture(t, names,
		memoryHTTPRound{tool: "artifact_save", args: `{"name":"note.txt","content_base64":"aGVsbG8=","mime_type":"text/plain"}`},
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"remember independently"}`},
		memoryHTTPRound{tool: "artifact_load", args: `{"name":"note.txt","version":0}`},
		memoryHTTPRound{final: "saved and read", before: func(req map[string]any) error {
			var got ArtifactResult
			if err := decodeMemoryHTTPTool(req, 2, &got); err != nil {
				return err
			}
			if string(got.Content) != "hello" || got.Version != 0 || got.SizeBytes != 5 {
				return fmt.Errorf("wrong artifact read")
			}
			return nil
		}})
	req := memoryHTTPRequest(s.URL, []string{memory.AddToolName})
	req.Artifact = &ArtifactConfig{Service: inmemory.NewService(), MaxBytes: 4096}
	result, err := executeMemoryHTTP(t, req)
	if err != nil || result.FinalText != "saved and read" || result.Memory == nil {
		t.Fatalf("result=%+v err=%v problems=%v calls=%d", result, err, f.problems, f.calls)
	}
	f.check(t, 4)
}
func TestArtifactBackendErrorCannotBecomeSuccessfulFinal(t *testing.T) {
	_, s := newMemoryHTTPFixture(t, ArtifactToolNames, memoryHTTPRound{tool: "artifact_save", args: `{"name":"note.txt","content_base64":"aGVsbG8=","mime_type":"text/plain"}`}, memoryHTTPRound{final: "pretend saved"})
	req := testRequest(s.URL)
	req.MaxToolCalls = 10
	req.Artifact = &ArtifactConfig{Service: failedArtifact{}, MaxBytes: 4096}
	result, err := executeMemoryHTTP(t, req)
	if err == nil || result.FinalText != "" || len(result.Snapshot) > 0 {
		t.Fatalf("backend failure accepted: %+v %v", result, err)
	}
}
func TestArtifactFilenameBoundary(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../x", "a/b", "a\\b", "a\r\nx", "a\x00b", " padded"} {
		if ValidArtifactName(name) {
			t.Fatalf("accepted %q", name)
		}
	}
	if !ValidArtifactName("报告.txt") {
		t.Fatal("valid Unicode filename rejected")
	}
}

func TestArtifactMIMEBoundary(t *testing.T) {
	for _, v := range []string{"", strings.Repeat("x", 257), "text/plain\x00", "text/plain\r\n"} {
		if ValidArtifactMIME(v) {
			t.Fatal("accepted invalid MIME")
		}
	}
	if !ValidArtifactMIME("application/octet-stream") {
		t.Fatal("rejected normal MIME")
	}
}
