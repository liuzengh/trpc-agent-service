package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

func TestWorkspaceProfilePublicInput(t *testing.T) {
	r := writeRoot(t)
	writeConfig(r)["executors"] = map[string]any{"shell": map[string]any{"kind": "sdk_sandbox"}}
	got, err := application.DecodeProfileWrite(writeBytes(t, r))
	if err != nil || got.Config.Executors["shell"].Kind != "sdk_sandbox" {
		t.Fatal(err, got.Config)
	}
	for _, v := range []any{nil, map[string]any{"kind": "host"}, map[string]any{"kind": "sdk_sandbox", "env": map[string]any{}}, map[string]any{"kind": "sdk_sandbox", "url": "https://host"}} {
		writeConfig(r)["executors"] = map[string]any{"shell": v}
		assertWriteRejected(t, writeBytes(t, r))
	}
}

func TestWorkspaceProfileSaveReadRoundTrip(t *testing.T) {
	h := newCredentialHarness(t)
	command := modelCredentialCommand(1, "workspace-save", "private-value")
	command.Write.Config.Executors = map[string]domain.ExecutorResource{"shell": {Kind: "sdk_sandbox"}}
	h.save(t, command)
	if h.spec(t).Executors["shell"].Kind != "sdk_sandbox" {
		t.Fatal("stored executor missing")
	}
	public, err := h.service.GetCredentialDraft(context.Background(), "tnt_a", "rpf_a", "usr_owner")
	if err != nil {
		t.Fatal(err)
	}
	if public.Config.Executors["shell"].Kind != "sdk_sandbox" {
		t.Fatal("read projection missing")
	}
	b, _ := json.Marshal(public)
	if bytes.Contains(b, []byte("private-value")) {
		t.Fatal("secret leak")
	}
}
