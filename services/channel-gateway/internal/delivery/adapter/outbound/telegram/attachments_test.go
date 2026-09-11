package telegramadapter_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/go-telegram/bot"
	tg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/telegram"
	a "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fileReader struct {
	data  []byte
	err   error
	calls int
}

func (r *fileReader) ReadReplyArtifact(_ context.Context, i d.Intent, f d.Attachment) ([]byte, error) {
	r.calls++
	return r.data, r.err
}

var _ a.ArtifactReader = (*fileReader)(nil)

func TestSendDocumentActualMultipartAndFailureCertainty(t *testing.T) {
	body := []byte{0, 255, 12, 10, 65}
	hash := sha256.Sum256(body)
	for _, mode := range []string{"ok", "read-fail", "corrupt", "reject", "bad-receipt"} {
		t.Run(mode, func(t *testing.T) {
			req, attempt := request(t)
			doc := d.Attachment{Name: "report.bin", MIMEType: "application/octet-stream", SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(hash[:])}
			req.Claim.Intent.Attachments = []d.Attachment{doc}
			parts, e := d.Plan(req.Claim.Target, req.Claim.Intent)
			if e != nil {
				t.Fatal(e)
			}
			req.Claim.Part.Index = 1
			req.Claim.Part.ID = d.PartID(req.Claim.Intent.ID, 1)
			req.Claim.Part.Text = parts[1]
			req.RequestDigest, e = d.RequestDigest(req.Claim)
			if e != nil {
				t.Fatal(e)
			}
			attempt.Intent = req.Claim.Intent
			attempt.PartID = req.Claim.Part.ID
			attempt.Text = parts[1]
			attempt.RequestDigest = req.RequestDigest
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/bot100:synthetic-fixture-not-a-token/sendDocument" {
					t.Error(r.URL.Path)
				}
				if e := r.ParseMultipartForm(1 << 20); e != nil {
					t.Error(e)
					return
				}
				defer r.MultipartForm.RemoveAll()
				f, h, e := r.FormFile("document")
				if e != nil {
					t.Error(e)
					return
				}
				defer f.Close()
				got, _ := io.ReadAll(f)
				if string(got) != string(body) || h.Filename != doc.Name || r.FormValue("chat_id") != "-10012" || r.FormValue("message_thread_id") != "7" || r.FormValue("reply_parameters") == "" {
					t.Error("lost file/original target")
				}
				w.Header().Set("Content-Type", "application/json")
				if mode == "reject" {
					w.WriteHeader(403)
					_, _ = w.Write([]byte(`{"ok":false,"error_code":403,"description":"forbidden"}`))
					return
				}
				if mode == "bad-receipt" {
					_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":5,"chat":{"id":2}}}`))
					return
				}
				_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":5,"message_thread_id":7,"chat":{"id":-10012},"document":{"file_id":"received-file"}}}`))
			}))
			defer s.Close()
			reader := &fileReader{data: body}
			if mode == "read-fail" {
				reader.err = errors.New("offline")
			}
			if mode == "corrupt" {
				reader.data = []byte("wrong")
			}
			p, e := tg.NewProvider(map[string]*bot.Bot{"account-1": configured(t, s.URL)}, reader)
			if e != nil {
				t.Fatal(e)
			}
			r, e := p.Reserve(context.Background(), req)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Release()
			if reader.calls != 0 {
				t.Fatal("reserve fetched bytes")
			}
			result := r.SendFinal(context.Background(), attempt)
			want := d.CertaintyAccepted
			switch mode {
			case "read-fail", "corrupt":
				want = d.CertaintyNotSent
			case "reject":
				want = d.CertaintyRejected
			case "bad-receipt":
				want = d.CertaintyUnknown
			}
			if result.Certainty != want {
				t.Fatal(result)
			}
			if (mode == "read-fail" || mode == "corrupt") && calls != 0 {
				t.Fatal("sent unverified bytes")
			}
			if mode == "ok" && (calls != 1 || result.ProviderMessageID != "5") {
				t.Fatal(result, calls)
			}
		})
	}
}
