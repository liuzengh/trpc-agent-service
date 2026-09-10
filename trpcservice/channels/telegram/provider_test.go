package telegram

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type providerBot struct {
	message        *models.Message
	err            error
	params         *bot.SendMessageParams
	photoParams    *bot.SendPhotoParams
	documentParams *bot.SendDocumentParams
	calls          int
	photoCalls     int
	documentCalls  int
}

func (b *providerBot) Start(context.Context) {}
func (b *providerBot) GetMe(context.Context) (*models.User, error) {
	return &models.User{ID: 1, IsBot: true}, nil
}
func (b *providerBot) SendMessage(_ context.Context, params *bot.SendMessageParams) (*models.Message, error) {
	b.calls++
	b.params = params
	return b.message, b.err
}
func (b *providerBot) SendPhoto(_ context.Context, params *bot.SendPhotoParams) (*models.Message, error) {
	b.photoCalls++
	b.photoParams = params
	return b.message, b.err
}
func (b *providerBot) SendDocument(_ context.Context, params *bot.SendDocumentParams) (*models.Message, error) {
	b.documentCalls++
	b.documentParams = params
	return b.message, b.err
}

type providerAttachmentReader struct {
	content   attachment.Content
	err       error
	tenantID  string
	eventID   string
	reference attachment.Reference
	calls     int
}

func (reader *providerAttachmentReader) Load(_ context.Context, tenantID, eventID string, reference attachment.Reference) (attachment.Content, error) {
	reader.calls++
	reader.tenantID = tenantID
	reader.eventID = eventID
	reader.reference = reference
	if reader.err != nil {
		return attachment.Content{}, reader.err
	}
	return reader.content.Clone(), nil
}

func TestProviderUsesStableReceiptAndReconcile(t *testing.T) {
	botClient := &providerBot{message: &models.Message{ID: 42}}
	provider, err := NewProvider(botClient, 99, 7)
	if err != nil {
		t.Fatal(err)
	}
	reply := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, Payload: "hello"}
	receipt, err := provider.Deliver(context.Background(), reply)
	if err != nil || receipt != "42" || botClient.calls != 1 {
		t.Fatalf("deliver = %q calls=%d err=%v", receipt, botClient.calls, err)
	}
	if _, err := provider.Deliver(context.Background(), reply); err != nil || botClient.calls != 1 {
		t.Fatalf("duplicate deliver calls=%d err=%v", botClient.calls, err)
	}
	status, reconciled, err := provider.Reconcile(context.Background(), reply)
	if err != nil || status != outbox.DeliveryAccepted || reconciled != "42" {
		t.Fatalf("reconcile = %s/%q err=%v", status, reconciled, err)
	}
	if botClient.params.ChatID != int64(99) || botClient.params.MessageThreadID != 7 || botClient.params.Text != "hello" {
		t.Fatalf("send params = %+v", botClient.params)
	}
}

func TestProviderDeliversImageAttachmentNatively(t *testing.T) {
	data := []byte("png")
	reference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", data)
	botClient := &providerBot{message: &models.Message{ID: 77}}
	reader := &providerAttachmentReader{content: attachment.Content{Data: data}}
	provider, err := NewProvider(botClient, 99, 7, WithAttachmentReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	reply := runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-image", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
	}
	receipt, err := provider.Deliver(context.Background(), reply)
	if err != nil || receipt != "77" {
		t.Fatalf("image deliver = %q, %v", receipt, err)
	}
	if botClient.photoCalls != 1 || botClient.calls != 0 || reader.calls != 1 {
		t.Fatalf("calls text=%d photo=%d reader=%d", botClient.calls, botClient.photoCalls, reader.calls)
	}
	if reader.tenantID != reply.TenantID || reader.eventID != reply.EventID || reader.reference != reference {
		t.Fatalf("reader args = %q %q %+v", reader.tenantID, reader.eventID, reader.reference)
	}
	if botClient.photoParams.ChatID != int64(99) || botClient.photoParams.MessageThreadID != 7 || botClient.photoParams.Caption != "caption" {
		t.Fatalf("photo params = %+v", botClient.photoParams)
	}
	upload, ok := botClient.photoParams.Photo.(*models.InputFileUpload)
	if !ok || upload.Filename != "chart.png" {
		t.Fatalf("photo upload = %#v", botClient.photoParams.Photo)
	}
	if got, err := io.ReadAll(upload.Data); err != nil || string(got) != string(data) {
		t.Fatalf("photo data = %q, %v", got, err)
	}
	if _, err := provider.Deliver(context.Background(), reply); err != nil || botClient.photoCalls != 1 {
		t.Fatalf("idempotent image delivery calls=%d err=%v", botClient.photoCalls, err)
	}
}

func TestProviderDeliversDocumentAttachmentNatively(t *testing.T) {
	data := []byte("document")
	reference := providerAttachmentReference(t, attachment.KindDocument, "application/pdf", "brief.pdf", data)
	botClient := &providerBot{message: &models.Message{ID: 88}}
	provider, err := NewProvider(botClient, 99, 0, WithAttachmentReader(&providerAttachmentReader{content: attachment.Content{Data: data}}))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-document", ReplyID: "reply-document", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindDocument, Payload: "caption", Attachment: reference, Fallback: "[document attachment: brief.pdf]",
	})
	if err != nil || receipt != "88" || botClient.documentCalls != 1 || botClient.calls != 0 {
		t.Fatalf("document deliver = %q text=%d document=%d err=%v", receipt, botClient.calls, botClient.documentCalls, err)
	}
	if botClient.documentParams.Caption != "caption" {
		t.Fatalf("document caption = %q", botClient.documentParams.Caption)
	}
	upload, ok := botClient.documentParams.Document.(*models.InputFileUpload)
	if !ok || upload.Filename != "brief.pdf" {
		t.Fatalf("document upload = %#v", botClient.documentParams.Document)
	}
}

func TestProviderFallsBackForUnsupportedOrUnavailableMedia(t *testing.T) {
	data := []byte("mp4")
	reference := providerAttachmentReference(t, attachment.KindVideo, "video/mp4", "clip.mp4", data)
	botClient := &providerBot{message: &models.Message{ID: 99}}
	provider, err := NewProvider(botClient, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-video", ReplyID: "reply-video", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindVideo, Payload: "caption", Attachment: reference, Fallback: "[video attachment: clip.mp4]",
	})
	if err != nil || receipt != "99" || botClient.calls != 1 || botClient.photoCalls != 0 || botClient.params.Text != "[video attachment: clip.mp4]" {
		t.Fatalf("fallback deliver = %q text=%d photo=%d params=%+v err=%v", receipt, botClient.calls, botClient.photoCalls, botClient.params, err)
	}
}

func TestProviderImageWithoutReaderFallsBackToText(t *testing.T) {
	imageData := []byte("png")
	imageReference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", imageData)
	botClient := &providerBot{message: &models.Message{ID: 11}}
	provider, err := NewProvider(botClient, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-image", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: imageReference, Fallback: "[image attachment: chart.png]",
	})
	if err != nil || receipt != "11" || botClient.photoCalls != 0 || botClient.calls != 1 || botClient.params.Text != "[image attachment: chart.png]" {
		t.Fatalf("fallback receipt=%q text=%d photo=%d params=%+v err=%v", receipt, botClient.calls, botClient.photoCalls, botClient.params, err)
	}
}

func TestProviderMissingAttachmentFallsBackToText(t *testing.T) {
	imageData := []byte("png")
	imageReference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", imageData)
	botClient := &providerBot{message: &models.Message{ID: 12}}
	reader := &providerAttachmentReader{err: runtimestorage.ErrNotFound}
	provider, err := NewProvider(botClient, 9, 0, WithAttachmentReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-missing", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: imageReference, Fallback: "[image attachment: chart.png]",
	})
	if err != nil || receipt != "12" || botClient.calls != 1 || botClient.photoCalls != 0 {
		t.Fatalf("missing fallback receipt=%q text=%d photo=%d err=%v", receipt, botClient.calls, botClient.photoCalls, err)
	}
}

func TestProviderTamperedAttachmentFallsBackToText(t *testing.T) {
	imageData := []byte("png")
	imageReference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", imageData)
	botClient := &providerBot{message: &models.Message{ID: 13}}
	reader := &providerAttachmentReader{content: attachment.Content{Data: []byte("tampered")}}
	provider, err := NewProvider(botClient, 9, 0, WithAttachmentReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-tampered", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: imageReference, Fallback: "[image attachment: chart.png]",
	})
	if err != nil || receipt != "13" || botClient.calls != 1 || botClient.photoCalls != 0 {
		t.Fatalf("tampered fallback receipt=%q text=%d photo=%d err=%v", receipt, botClient.calls, botClient.photoCalls, err)
	}
}

func TestProviderDocumentEmptyNameUsesReferenceID(t *testing.T) {
	data := []byte("pdf")
	reference := providerAttachmentReference(t, attachment.KindDocument, "application/pdf", "", data)
	botClient := &providerBot{message: &models.Message{ID: 14}}
	provider, err := NewProvider(botClient, 9, 0, WithAttachmentReader(&providerAttachmentReader{content: attachment.Content{Data: data}}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-doc", ReplyID: "reply-doc-empty-name", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindDocument, Payload: "caption", Attachment: reference, Fallback: "[document attachment]",
	})
	if err != nil {
		t.Fatal(err)
	}
	upload, ok := botClient.documentParams.Document.(*models.InputFileUpload)
	if !ok || upload.Filename != reference.ID {
		t.Fatalf("document upload = %#v", botClient.documentParams.Document)
	}
}

func TestProviderPreservesAttachmentCancellation(t *testing.T) {
	data := []byte("png")
	reference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", data)
	reader := &providerAttachmentReader{err: context.Canceled}
	provider, err := NewProvider(&providerBot{message: &models.Message{ID: 1}}, 9, 0, WithAttachmentReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-image", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
	})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "canceled" || !deliveryErr.Retryable {
		t.Fatalf("canceled attachment = %#v", err)
	}
}

func TestProviderClassifiesMediaTimeoutAndInvalidContract(t *testing.T) {
	data := []byte("png")
	reference := providerAttachmentReference(t, attachment.KindImage, "image/png", "chart.png", data)
	timeoutProvider, err := NewProvider(&providerBot{message: &models.Message{ID: 1}}, 9, 0, WithAttachmentReader(&providerAttachmentReader{err: context.DeadlineExceeded}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = timeoutProvider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-timeout", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
	})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "timeout" || !deliveryErr.Retryable {
		t.Fatalf("timeout attachment = %#v", err)
	}

	sendTimeout, err := NewProvider(&providerBot{message: &models.Message{ID: 1}, err: context.DeadlineExceeded}, 9, 0, WithAttachmentReader(&providerAttachmentReader{content: attachment.Content{Data: data}}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = sendTimeout.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-send-timeout", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
	})
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "timeout" || !deliveryErr.Retryable {
		t.Fatalf("send timeout = %#v", err)
	}

	invalidProvider, err := NewProvider(&providerBot{message: &models.Message{ID: 1}}, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = invalidProvider.Deliver(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-invalid", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Fallback: "[image attachment]",
	})
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "invalid" || deliveryErr.Retryable {
		t.Fatalf("invalid media contract = %#v", err)
	}
}

func TestProviderRedactsAndClassifiesSendFailures(t *testing.T) {
	botClient := &providerBot{err: errors.New("secret provider response")}
	provider, err := NewProvider(botClient, 99, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, Payload: "hello"})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "provider_error" || !deliveryErr.Retryable {
		t.Fatalf("delivery error = %#v", err)
	}
}

func TestProviderValidationAndReceiptFailureBranches(t *testing.T) {
	client := &providerBot{message: &models.Message{ID: 1}}
	for name, chatID := range map[string]int64{"zero chat": 0, "valid chat": 9} {
		if name == "valid chat" {
			if _, err := NewProvider(client, chatID, -1); !errors.Is(err, outbox.ErrInvalid) {
				t.Fatalf("negative thread = %v", err)
			}
			continue
		}
		if _, err := NewProvider(client, chatID, 0); !errors.Is(err, outbox.ErrInvalid) {
			t.Fatalf("zero chat = %v", err)
		}
	}
	if _, err := NewProvider(nil, 9, 0); !errors.Is(err, outbox.ErrInvalid) {
		t.Fatalf("nil client = %v", err)
	}
	value := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-invalid", SegmentIndex: 0, Payload: "payload"}
	for name, botValue := range map[string]*models.Message{"nil receipt": nil, "zero receipt": {ID: 0}} {
		t.Run(name, func(t *testing.T) {
			provider, err := NewProvider(&providerBot{message: botValue}, 9, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Deliver(context.Background(), value)
			var deliveryErr *outbox.DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Class != "provider_invalid_receipt" || deliveryErr.Retryable {
				t.Fatalf("invalid receipt error = %#v", err)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	provider, err := NewProvider(&providerBot{err: context.Canceled}, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Deliver(canceled, value)
	var canceledErr *outbox.DeliveryError
	if !errors.As(err, &canceledErr) || canceledErr.Class != "canceled" || !canceledErr.Retryable {
		t.Fatalf("canceled delivery error = %#v", err)
	}
	var nilProvider *Provider
	if _, err := nilProvider.Deliver(context.Background(), value); err == nil {
		t.Fatal("nil provider deliver unexpectedly succeeded")
	}
	if status, _, err := nilProvider.Reconcile(context.Background(), value); err != nil || status != outbox.DeliveryUnknown {
		t.Fatalf("nil provider reconcile = %s/%v", status, err)
	}
	var nilContext context.Context
	if _, err := provider.Deliver(nilContext, value); err == nil {
		t.Fatal("nil context deliver unexpectedly succeeded")
	}
}

func providerAttachmentReference(t *testing.T, kind attachment.Kind, contentType, name string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-" + string(kind), Kind: kind, MIMEType: contentType, Name: name, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("attachment reference = %v", err)
	}
	return reference
}
