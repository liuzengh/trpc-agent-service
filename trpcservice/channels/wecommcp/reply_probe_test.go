package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplyProbeSendsFixedMarkerOnlyOnceEvenAfterUnknownResult(t *testing.T) {
	fixture, err := os.ReadFile("testdata/group_reply.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "receipt", true: "unknown"}[fail], func(t *testing.T) {
			const endpoint = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=secret-canary"
			const group = "private-group"
			dir := t.TempDir()
			calls, sends := 0, 0
			client := &http.Client{Transport: sampleRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				var rpc struct {
					ID     any    `json:"id"`
					Method string `json:"method"`
					Params struct {
						Name      string         `json:"name"`
						Arguments map[string]any `json:"arguments"`
					} `json:"params"`
				}
				if req.Method != http.MethodPost || json.NewDecoder(req.Body).Decode(&rpc) != nil {
					t.Error("unexpected probe request")
					return nil, errors.New("unexpected request")
				}
				if _, err := os.Stat(attemptPath(endpoint, group, dir)); err != nil {
					t.Error("network used before recording attempt")
				}
				var result any
				switch rpc.Method {
				case "initialize":
					result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "test", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
				case "notifications/initialized":
					return &http.Response{StatusCode: 202, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
				case "tools/call":
					want, _ := json.Marshal(map[string]any{"chat_id": group, "msg_type": "markdown", "markdown": map[string]any{"content": TestReplyMarker}})
					actual, _ := json.Marshal(rpc.Params.Arguments)
					if rpc.Params.Name != replyTool || string(want) != string(actual) {
						t.Error("probe changed recipient, tool or fixed text")
					}
					sends++
					if fail {
						return nil, errors.New("ambiguous transport failure secret-canary")
					}
					result = map[string]any{"content": []any{map[string]any{"type": "text", "text": string(fixture)}}}
				default:
					t.Error("unexpected RPC method")
					return nil, errors.New("unexpected method")
				}
				encoded, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": result})
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(encoded)))}, nil
			})}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			report, err := sendGroupReply(ctx, endpoint, client, group, dir)
			if (err != nil) != fail {
				t.Fatalf("unexpected outcome: %v", err)
			}
			if !fail && (report.Status["errcode"] != json.Number("0") || report.Status["success"] != true) {
				t.Fatal("provider acknowledgement not reported")
			}
			if err != nil && strings.Contains(err.Error(), "secret-canary") {
				t.Fatal("unsafe error")
			}
			encoded, _ := json.Marshal(report)
			for _, private := range []string{"secret-canary", group, "private-receipt"} {
				if strings.Contains(string(encoded), private) {
					t.Fatal("private data in report")
				}
			}
			if _, err := sendGroupReply(ctx, endpoint, client, group, dir); err == nil {
				t.Fatal("repeat invocation sent again")
			}
			if calls != 3 || sends != 1 {
				t.Fatalf("unexpected calls=%d sends=%d", calls, sends)
			}
			file := attemptPath(endpoint, group, dir)
			info, err := os.Stat(file)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("attempt record is not private")
			}
			data, _ := os.ReadFile(file)
			if strings.Contains(string(data), "secret-canary") || strings.Contains(string(data), group) {
				t.Fatal("attempt record must store only fingerprints")
			}
		})
	}
}

func TestReplyAttemptIsExclusive(t *testing.T) {
	dir := t.TempDir()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if claimReplyAttempt("endpoint", "group", dir, time.Now()) == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal("more than one sender claimed the fixed marker")
	}
}

func TestReplyVerificationWindowIsBoundToAttemptAndGroup(t *testing.T) {
	now := time.Date(2026, 9, 6, 8, 10, 20, 0, time.UTC)
	dir := t.TempDir()
	if err := claimReplyAttempt("endpoint", "group", dir, now); err != nil {
		t.Fatal(err)
	}
	args, err := replyWindow("endpoint", "group", dir, now.Add(time.Second))
	if err != nil || len(args) != 3 || args["chat_id"] != "group" || args["begin_time"] != "2026-09-06 16:09:20" || args["end_time"] != "2026-09-06 16:12:20" {
		t.Fatalf("unexpected reply window: %v %v", args, err)
	}
	for _, tc := range []struct {
		endpoint, group string
		now             time.Time
	}{
		{"other", "group", now}, {"endpoint", "other", now},
		{"endpoint", "group", now.Add(time.Hour)}, {"endpoint", "group", now.Add(-time.Minute)},
	} {
		if _, err := replyWindow(tc.endpoint, tc.group, dir, tc.now); err == nil {
			t.Fatal("out-of-scope reply verification allowed")
		}
	}
}
