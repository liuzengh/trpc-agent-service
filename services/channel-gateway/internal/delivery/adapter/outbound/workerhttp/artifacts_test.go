package workerhttp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestReplyArtifactBindsIdentityAndChecksBytes(t *testing.T) {
	body := []byte{0, 255, 1, 10}
	hash := sha256.Sum256(body)
	a := d.Attachment{Name: "report.bin", MIMEType: "application/octet-stream", SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(hash[:])}
	i := d.Intent{ID: "intent", AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1, Text: "file", Attachments: []d.Attachment{a}, Deadline: time.Now().Add(time.Minute)}
	for _, mode := range []string{"ok", "hash", "bytes", "mime", "length", "conflict", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/internal/v1/reply-artifacts" || r.Method != "POST" {
					t.Error("wrong endpoint")
				}
				var req map[string]any
				if json.NewDecoder(r.Body).Decode(&req) != nil || len(req) != 5 || req["version"] != float64(0) || req["intent_id"] != i.ID || req["completion_id"] != i.CompletionID || req["run_id"] != i.RunID || req["name"] != a.Name {
					t.Error("unbound request", req)
				}
				if mode == "conflict" {
					w.WriteHeader(409)
					return
				}
				if mode == "redirect" {
					w.Header().Set("Location", "http://example.invalid")
					w.WriteHeader(302)
					return
				}
				w.Header().Set("Content-Type", a.MIMEType)
				w.Header().Set("Content-Length", fmt.Sprint(len(body)))
				w.Header().Set("X-Content-SHA256", a.SHA256)
				if mode == "hash" {
					w.Header().Set("X-Content-SHA256", "wrong")
				}
				if mode == "mime" {
					w.Header().Set("Content-Type", "text/plain")
				}
				if mode == "length" {
					w.Header().Set("Content-Length", "5")
				}
				if mode == "bytes" {
					_, _ = w.Write([]byte{1, 2, 3, 4})
				} else {
					_, _ = w.Write(body)
				}
			}))
			defer s.Close()
			u, _ := url.Parse(s.URL)
			c := &Client{base: u, http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
			got, err := c.ReadReplyArtifact(context.Background(), i, a)
			if (mode == "ok") != (err == nil) {
				t.Fatal(mode, err)
			}
			if mode == "ok" && string(got) != string(body) {
				t.Fatal("bytes changed")
			}
			wrong := a
			wrong.Version = 1
			if _, e := c.ReadReplyArtifact(context.Background(), i, wrong); e == nil || calls != 1 {
				t.Fatal("unlisted version read", calls, e)
			}
		})
	}
}
