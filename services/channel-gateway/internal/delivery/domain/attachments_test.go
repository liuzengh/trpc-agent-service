package domain_test

import (
	"errors"
	"fmt"
	contract "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"strings"
	"testing"
)

func TestAttachmentPlanAndDigest(t *testing.T) {
	i := intent()
	a := d.Attachment{Name: "report.txt", Version: 0, MIMEType: "text/plain", SizeBytes: 3, SHA256: strings.Repeat("a", 64)}
	i.Attachments = []d.Attachment{a}
	w := wire(i)
	w.Content.Attachments = []dto.ReplyAttachment{{Name: a.Name, Version: a.Version, MimeType: a.MIMEType, SizeBytes: a.SizeBytes, Sha256: a.SHA256}}
	got, e := d.IntentDigest(i)
	want, we := contract.ReplyIntentDigest(w)
	if e != nil || we != nil || got != want {
		t.Fatal(got, want, e, we)
	}
	raw, e := contract.EncodeReplyIntent(w)
	if e != nil || !strings.Contains(string(raw), `"version":0`) {
		t.Fatal(string(raw), e)
	}
	target := d.Target{TenantID: "tenant", Provider: "telegram", AccountID: "account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "100", SourceEventID: "event", ReceivedAt: i.Deadline}
	parts, e := d.Plan(target, i)
	if e != nil || len(parts) != 2 || parts[0] != i.Text {
		t.Fatal(parts, e)
	}
	if doc, ok := d.AttachmentAt(target, i, 1); !ok || doc != a {
		t.Fatal(doc, ok)
	}
	if _, ok := d.AttachmentAt(target, i, 0); ok {
		t.Fatal("text treated as document")
	}
	changed := i
	changed.Attachments = append([]d.Attachment(nil), i.Attachments...)
	changed.Attachments[0].Version = 1
	other, _ := d.IntentDigest(changed)
	if got == other {
		t.Fatal("version not pinned")
	}
	for _, name := range []string{"../report.txt", "a/b", "a\\b", ".", "..", "bad\nname"} {
		changed.Attachments[0] = a
		changed.Attachments[0].Name = name
		if changed.Validate() == nil {
			t.Fatal("invalid name", name)
		}
	}
	i.Attachments = append(i.Attachments, a)
	if i.Validate() == nil {
		t.Fatal("duplicate accepted")
	}
}

func TestAttachmentMIMEUnicodeMatchesWire(t *testing.T) {
	i := intent()
	mime := "text/plain;x=\"" + strings.Repeat("a", 239) + "é\""
	if len(mime) != 256 {
		t.Fatal(len(mime))
	}
	i.Attachments = []d.Attachment{{Name: "report.txt", Version: 0, MIMEType: mime, SizeBytes: 1, SHA256: strings.Repeat("a", 64)}}
	if err := i.Validate(); err != nil {
		t.Fatal(err)
	}
	w := wire(i)
	w.Content.Attachments = []dto.ReplyAttachment{{Name: "report.txt", Version: 0, MimeType: mime, SizeBytes: 1, Sha256: strings.Repeat("a", 64)}}
	b, err := contract.EncodeReplyIntent(w)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = contract.DecodeReplyIntent(b); err != nil {
		t.Fatal(err)
	}
}
func TestAttachmentPlanRespectsExistingStoragePartCount(t *testing.T) {
	i := intent()
	target := d.Target{TenantID: "tenant", Provider: "telegram", AccountID: "account", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "100", SourceEventID: "event", ReceivedAt: i.Deadline}
	for j := 0; j < 63; j++ {
		i.Attachments = append(i.Attachments, d.Attachment{Name: fmt.Sprintf("report-%d.txt", j), Version: 0, MIMEType: "text/plain", SizeBytes: 1, SHA256: strings.Repeat("a", 64)})
	}
	parts, err := d.Plan(target, i)
	if err != nil || len(parts) != 64 {
		t.Fatal(len(parts), err)
	}
	i.Attachments = append(i.Attachments, d.Attachment{Name: "report-last.txt", Version: 0, MIMEType: "text/plain", SizeBytes: 1, SHA256: strings.Repeat("a", 64)})
	if _, err = d.Plan(target, i); !errors.Is(err, d.ErrCapacity) {
		t.Fatal("SQL capacity exceeded by an accepted plan", err)
	}
}
