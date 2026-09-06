package telegram

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestEditedUpdateIsMarkedAndKnownMediaIsNotSilentlyDropped(t *testing.T) {
	adapter, _ := New(secret.StaticStore{"secret://webhook": "webhook"}, nil)
	for _, tc := range []struct {
		kind, body string
		edited     bool
	}{
		{"text", `"edited_message":{"text":"批准 apr_test"`, true},
		{"voice", `"message":{"voice":{"file_id":"test"}`, false},
		{"video", `"message":{"video":{"file_id":"test"}`, false},
		{"file", `"message":{"document":{"file_id":"test"}`, false},
	} {
		body := `{"update_id":1,` + tc.body + `,"from":{"id":42},"chat":{"id":42,"type":"private"}}}`
		req := httptest.NewRequest(http.MethodPost, "/callback", strings.NewReader(body))
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook")
		result, err := adapter.Callback(context.Background(), testBinding(t, ""), req)
		if err != nil || len(result.Messages) != 1 || result.Messages[0].MessageType != tc.kind || result.Messages[0].Edited != tc.edited {
			t.Fatalf("%s: %+v %v", tc.kind, result, err)
		}
	}
}
