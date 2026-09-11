package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestMediaDownloadUsesProviderPathAndRejectsTraversal(t *testing.T) {
	var downloads atomic.Int32
	var traversal atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bottest-token/getFile":
			p := "documents/file.txt"
			if traversal.Load() {
				p = "../private"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": p, "file_size": 5}})
		case "/file/bottest-token/documents/file.txt":
			downloads.Add(1)
			_, _ = w.Write([]byte("hello"))
		default:
			t.Error("unexpected download destination")
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	raw, _ := json.Marshal(bindingConfig{BotTokenRef: "token", WebhookSecretRef: "webhook", APIBaseURL: server.URL, AttachmentsEnabled: true})
	b := controlplane.ChannelBinding{ID: "binding", TenantID: "tenant", ChannelType: "telegram", Status: "active", Version: 2, Config: raw}
	a, _ := New(secret.StaticStore{"token": "test-token"}, server.Client())
	ref := channels.MediaReference{FileID: "opaque-id", BindingVersion: 2, Size: 5}
	data, err := a.DownloadMedia(context.Background(), b, ref, 100)
	if err != nil || string(data) != "hello" {
		t.Fatalf("download=%q err=%v", data, err)
	}
	traversal.Store(true)
	if _, err := a.DownloadMedia(context.Background(), b, ref, 100); err == nil {
		t.Fatal("path traversal accepted")
	}
	if downloads.Load() != 1 {
		t.Fatal("unsafe path was fetched")
	}
	production, _ := New(secret.StaticStore{"token": "test-token"}, nil)
	if _, err := production.DownloadMedia(context.Background(), b, ref, 100); err == nil || strings.Contains(err.Error(), "test-token") {
		t.Fatal("custom endpoint or credential leak accepted")
	}
}
