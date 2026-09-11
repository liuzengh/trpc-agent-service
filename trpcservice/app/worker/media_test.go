package worker

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmem "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// pngAttachment returns a real 1x1 PNG so the pipeline sees actual image data.
func pngAttachment(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{G: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

// fakeOutbox records appends so a test can assert on the receipt and its
// idempotency key.
type fakeOutbox struct {
	appends []*bus.Message
	keys    []string
	err     error
}

func (f *fakeOutbox) Append(_ context.Context, m *bus.Message, msgKey string) error {
	if f.err != nil {
		return f.err
	}
	f.appends = append(f.appends, m)
	f.keys = append(f.keys, msgKey)
	return nil
}

// fakeAuditor captures audit entries synchronously (the real recorder batches).
type fakeAuditor struct {
	entries []audit.Entry
}

func (f *fakeAuditor) Record(e audit.Entry)                                { f.entries = append(f.entries, e) }
func (f *fakeAuditor) RecordUsage(context.Context, audit.UsageEntry) error { return nil }
func (f *fakeAuditor) Close() error                                        { return nil }

func mediaMessage(refs ...bus.MediaRef) *bus.Message {
	content := model.NewUserMessage("看下这个")
	return &bus.Message{
		ID: "m-1", TenantID: "t1", AgentID: "a1", SessionID: "s1",
		Channel: "wecom", UserID: "u1", Content: &content, Media: refs,
	}
}

// TestReportUnreadableMediaReceiptAndAudit covers the "explicit receipt + audit"
// requirement: when the platform could not read an attachment the user is told
// (durably, once) and the audit trail explains the missing content.
func TestReportUnreadableMediaReceiptAndAudit(t *testing.T) {
	outbox := &fakeOutbox{}
	auditor := &fakeAuditor{}
	w := &Worker{outbox: outbox, auditor: auditor}

	w.reportUnreadableMedia(context.Background(), mediaMessage(
		bus.MediaRef{Kind: "image", Name: "ok.png", Size: 128},
		bus.MediaRef{Kind: "file", Name: "report.pdf", FetchError: "download failed"},
	))

	if len(outbox.appends) != 1 {
		t.Fatalf("receipts = %d, want exactly one", len(outbox.appends))
	}
	receipt := outbox.appends[0]
	if receipt.SessionID != "s1" || receipt.TenantID != "t1" || receipt.Channel != "wecom" {
		t.Errorf("receipt must be addressed to the originating session: %+v", receipt)
	}
	if receipt.ReplyTo != "m-1" {
		t.Errorf("receipt.ReplyTo = %q, want the inbound message id", receipt.ReplyTo)
	}
	body := receipt.Content.Content
	if !strings.Contains(body, "report.pdf") {
		t.Errorf("receipt must name the unreadable attachment: %q", body)
	}
	if strings.Contains(body, "ok.png") {
		t.Errorf("receipt must not mention a readable attachment: %q", body)
	}
	if !strings.Contains(body, "未能读取") {
		t.Errorf("receipt must say the attachment could not be read: %q", body)
	}
	// The key scopes the receipt to the inbound message, so a redelivery cannot
	// send it twice.
	if outbox.keys[0] != "m-1"+mediaReceiptSuffix {
		t.Errorf("receipt idempotency key = %q", outbox.keys[0])
	}
	if len(auditor.entries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(auditor.entries))
	}
	entry := auditor.entries[0]
	if entry.ErrorType != errAttachmentUnreadable || entry.Decision != audit.DecisionFailed {
		t.Errorf("audit entry must class the failure: %+v", entry)
	}
	if entry.TenantID != "t1" || entry.SessionID != "s1" || entry.UserID != "u1" {
		t.Errorf("audit entry must carry the full surface: %+v", entry)
	}
}

// TestReportUnreadableMediaStaysQuietWhenEverythingArrived: a success notice on
// every image would be noise, and the model already received the bytes.
func TestReportUnreadableMediaStaysQuietWhenEverythingArrived(t *testing.T) {
	outbox := &fakeOutbox{}
	auditor := &fakeAuditor{}
	w := &Worker{outbox: outbox, auditor: auditor}

	w.reportUnreadableMedia(context.Background(), mediaMessage(
		bus.MediaRef{Kind: "image", Name: "ok.png", Size: 128},
	))
	w.reportUnreadableMedia(context.Background(), mediaMessage())
	plain := mediaMessage()
	plain.Media = nil
	w.reportUnreadableMedia(context.Background(), plain)

	if len(outbox.appends) != 0 || len(auditor.entries) != 0 {
		t.Errorf("readable attachments must not produce receipts/audit rows: %d/%d",
			len(outbox.appends), len(auditor.entries))
	}
}

// TestReportUnreadableMediaSurvivesOutboxFailure: a missing receipt must not
// escalate into a failed turn — the audit row and the model's manifest line
// still record the gap.
func TestReportUnreadableMediaSurvivesOutboxFailure(t *testing.T) {
	w := &Worker{outbox: &fakeOutbox{err: errors.New("outbox down")}, auditor: &fakeAuditor{}}
	w.reportUnreadableMedia(context.Background(), mediaMessage(
		bus.MediaRef{Kind: "file", Name: "a.pdf", FetchError: "boom"},
	))
}

// TestExternalizeSessionContentArchivesInlineAttachments proves the "attachment
// bytes land in artifact storage" half of the media pipeline: the inline payload
// is replaced by a pinned artifact reference in the persisted session event, and
// a later read hydrates the bytes back. Without this an inbound image would be
// base64-inlined into session_events forever.
func TestExternalizeSessionContentArchivesInlineAttachments(t *testing.T) {
	ctx := context.Background()
	inner := inmemory.NewSessionService()
	artifacts := artifactinmem.NewService()
	meter := &artifactUsage{inner: artifacts}
	w := &Worker{}

	svc := w.externalizeSessionContent(inner, meter)
	if svc == inner {
		t.Fatal("externalization must wrap the backend when an artifact service is present")
	}
	if got := w.externalizeSessionContent(inner, nil); got != inner {
		t.Error("without an artifact service the backend must be used as-is")
	}

	data := pngAttachment(t)
	key := session.Key{AppName: "t1", UserID: "u1", SessionID: "s1"}
	sess, err := svc.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	msg := model.NewUserMessage("这是什么")
	msg.AddImageData(data, "", "png")
	evt := event.NewResponseEvent("inv-1", "u1", &model.Response{
		Choices: []model.Choice{{Message: msg}},
	})
	if err := svc.AppendEvent(ctx, sess, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}

	info := artifact.SessionInfo{AppName: "t1", UserID: "u1", SessionID: "s1"}
	keys, err := artifacts.ListArtifactKeys(ctx, info)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("archived artifacts = %v, want 1", keys)
	}
	if meter.count() != 1 {
		t.Errorf("artifact usage count = %d, want 1 (externalized payloads count too)", meter.count())
	}

	// The persisted event must keep the reference, not the bytes.
	stored, err := inner.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("read raw session: %v", err)
	}
	part := firstImagePart(t, stored)
	if part.ContentRef == nil || part.ContentRef.ArtifactName == "" {
		t.Fatalf("persisted part must carry an artifact reference: %+v", part)
	}
	if part.Image != nil && len(part.Image.Data) != 0 {
		t.Error("persisted part must not keep the inline bytes")
	}
	if part.ContentRef.SizeBytes != int64(len(data)) {
		t.Errorf("reference size = %d, want %d", part.ContentRef.SizeBytes, len(data))
	}

	// Reading through the wrapper restores the bytes for the next turn.
	hydrated, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("read hydrated session: %v", err)
	}
	hydratedPart := firstImagePart(t, hydrated)
	if hydratedPart.Image == nil || !bytes.Equal(hydratedPart.Image.Data, data) {
		t.Error("hydrated read must restore the attachment bytes")
	}
}

// firstImagePart returns the first image content part of a session's events.
func firstImagePart(t *testing.T, sess *session.Session) model.ContentPart {
	t.Helper()
	if sess == nil {
		t.Fatal("session is nil")
	}
	for _, evt := range sess.Events {
		if evt.Response == nil {
			continue
		}
		for _, choice := range evt.Response.Choices {
			for _, part := range choice.Message.ContentParts {
				if part.Type == model.ContentTypeImage && (part.Image != nil || part.ContentRef != nil) {
					return part
				}
			}
		}
	}
	t.Fatalf("no image content part in %d events", len(sess.Events))
	return model.ContentPart{}
}
