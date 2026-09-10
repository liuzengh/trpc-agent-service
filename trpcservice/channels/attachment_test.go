package channels_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestDetectMediaMIMETypeUsesOfficeFilename(t *testing.T) {
	data := []byte("PK\x03\x04not-an-office-file")
	for _, test := range []struct {
		filename string
		want     string
	}{
		{filename: "report.docx", want: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{filename: "archive.zip", want: "application/zip"},
	} {
		if got := channels.DetectMediaMIMEType(test.filename, data); got != test.want {
			t.Errorf("MIME type for %q = %q, want %q", test.filename, got, test.want)
		}
	}
}

func TestDetectMediaMIMETypeDoesNotTrustWrongImageExtension(t *testing.T) {
	if got := channels.DetectMediaMIMEType("payload.png", []byte("%PDF-1.7\n")); got != "application/pdf" {
		t.Fatalf("MIME type for wrong image extension = %q, want application/pdf", got)
	}
}

func TestArtifactIngestorKeepsUnknownImageAsImageMIME(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID: "tenant-a", AppID: "support", Channel: channels.ChannelWeCom,
		BindingID: "binding-1", BindingRevision: 1, ExternalMessageID: "message-unknown-image",
		Conversation: channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:  channels.MessageTypeImage,
	}, channels.ChannelMappingInput{ExternalSenderID: "user-1", ProviderSenderTarget: "user-1"})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}
	writer := &recordingArtifactWriter{ref: "artifact://inbound/unknown-image", owned: true}
	ingestor, err := channels.NewArtifactIngestor(recordingMediaDownloader{download: func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
		return channels.DownloadedMedia{
			Filename: "payload.bin", MIMEType: "application/octet-stream",
			Data: []byte{0x00, 0x01, 0x02, 0x03},
		}, nil
	}}, writer)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}
	if _, err := ingestor.Prepare(context.Background(), input, []channels.ProviderMediaRef{{
		Kind: channels.MessageTypeImage, Reference: "provider-unknown-image",
	}}); err != nil {
		t.Fatalf("prepare unknown image: %v", err)
	}
	if writer.artifact.MIMEType != "image/octet-stream" {
		t.Fatalf("unknown image MIME = %q, want image/octet-stream", writer.artifact.MIMEType)
	}
}

func TestArtifactIngestorMaterializesOnlyArtifactRefs(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID:          "tenant-a",
		AppID:             "support",
		Channel:           channels.ChannelWeCom,
		BindingID:         "binding-1",
		BindingRevision:   1,
		ExternalMessageID: "message-1",
		Conversation:      channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:       channels.MessageTypeImage,
	}, channels.ChannelMappingInput{
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-1",
	})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}

	writer := &recordingArtifactWriter{ref: "artifact://inbound/one", owned: true}
	ingestor, err := channels.NewArtifactIngestor(
		recordingMediaDownloader{download: func(_ context.Context, got channels.ChannelInput, media channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
			if got.ArtifactRefs != nil {
				t.Fatalf("downloader received pre-existing artifact refs: %#v", got.ArtifactRefs)
			}
			if media.Reference != "provider-media-1" {
				t.Fatalf("media reference = %q", media.Reference)
			}
			return channels.DownloadedMedia{Filename: "image.png", MIMEType: "image/png", Data: []byte("image")}, nil
		}},
		writer,
	)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}

	prepared, err := ingestor.Prepare(context.Background(), input, []channels.ProviderMediaRef{{
		Kind:      channels.MessageTypeImage,
		Reference: "provider-media-1",
	}})
	if err != nil {
		t.Fatalf("prepare artifact: %v", err)
	}
	if len(prepared.ArtifactRefs) != 1 || prepared.ArtifactRefs[0] != writer.ref {
		t.Fatalf("artifact refs = %#v", prepared.ArtifactRefs)
	}
	if string(writer.artifact.Data) != "image" || writer.artifact.Filename != "image.png" {
		t.Fatalf("written artifact = %#v", writer.artifact)
	}
}

func TestArtifactIngestorRejectsOversizedMediaBeforeWrite(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID:          "tenant-a",
		AppID:             "support",
		Channel:           channels.ChannelFeishu,
		BindingID:         "binding-1",
		BindingRevision:   1,
		ExternalMessageID: "message-1",
		Conversation:      channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:       channels.MessageTypeFile,
	}, channels.ChannelMappingInput{
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-1",
	})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}

	writer := &recordingArtifactWriter{ref: "artifact://inbound/one", owned: true}
	ingestor, err := channels.NewArtifactIngestor(
		recordingMediaDownloader{download: func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
			return channels.DownloadedMedia{Data: []byte("12345")}, nil
		}},
		writer,
	)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}
	if err := ingestor.WithMaxBytes(4); err != nil {
		t.Fatalf("set artifact limit: %v", err)
	}
	if _, err := ingestor.Prepare(context.Background(), input, []channels.ProviderMediaRef{{
		Kind:      channels.MessageTypeFile,
		Reference: "provider-media-1",
	}}); err == nil {
		t.Fatal("oversized media was accepted")
	}
	if writer.called {
		t.Fatal("writer was called for oversized media")
	}
}

func TestArtifactIngestorPinnedPreparationCompensatesPartialBatch(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID: "tenant-a", AppID: "support", Channel: channels.ChannelWeCom,
		BindingID: "binding-1", BindingRevision: 1, ExternalMessageID: "message-2",
		Conversation: channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:  channels.MessageTypeMixed, Text: "hello",
	}, channels.ChannelMappingInput{ExternalSenderID: "user-1", ProviderSenderTarget: "user-1"})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}
	writer := &recordingArtifactWriter{ref: "artifact://inbound/one@0", owned: true}
	count := 0
	ingestor, err := channels.NewArtifactIngestor(recordingMediaDownloader{download: func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
		count++
		if count == 2 {
			return channels.DownloadedMedia{}, errors.New("second media failed")
		}
		return channels.DownloadedMedia{Filename: "one.png", MIMEType: "image/png", Data: []byte("one")}, nil
	}}, writer)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}
	_, _, err = ingestor.PreparePinned(context.Background(), input, []channels.ProviderMediaRef{
		{Kind: channels.MessageTypeImage, Reference: "provider-1"},
		{Kind: channels.MessageTypeImage, Reference: "provider-2"},
	}, "v-canary")
	if err == nil {
		t.Fatal("partial media preparation succeeded")
	}
	if len(writer.deleted) != 1 || writer.deleted[0].ConfigVersion != "v-canary" {
		t.Fatalf("compensated artifacts = %#v", writer.deleted)
	}
}

func TestArtifactIngestorDoesNotCompensateSharedArtifact(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID: "tenant-a", AppID: "support", Channel: channels.ChannelWeCom,
		BindingID: "binding-1", BindingRevision: 1, ExternalMessageID: "message-shared",
		Conversation: channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:  channels.MessageTypeImage,
	}, channels.ChannelMappingInput{ExternalSenderID: "user-1", ProviderSenderTarget: "user-1"})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}
	writer := &recordingArtifactWriter{ref: "artifact://inbound/shared@0", owned: false}
	ingestor, err := channels.NewArtifactIngestor(
		recordingMediaDownloader{download: func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
			return channels.DownloadedMedia{Filename: "shared.png", MIMEType: "image/png", Data: []byte("shared")}, nil
		}},
		writer,
	)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}
	_, cleanup, err := ingestor.PreparePinned(context.Background(), input, []channels.ProviderMediaRef{{
		Kind: channels.MessageTypeImage, Reference: "provider-shared",
	}}, "v-canary")
	if err != nil {
		t.Fatalf("prepare shared artifact: %v", err)
	}
	if err := cleanup(context.Background()); err != nil {
		t.Fatalf("cleanup shared artifact: %v", err)
	}
	if len(writer.deleted) != 0 {
		t.Fatalf("shared artifacts were compensated: %#v", writer.deleted)
	}
}

type recordingArtifactWriter struct {
	ref      string
	owned    bool
	artifact channels.InboundArtifact
	called   bool
	deleted  []channels.InboundArtifact
}

type recordingMediaDownloader struct {
	download func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error)
}

func (d recordingMediaDownloader) Download(
	ctx context.Context,
	input channels.ChannelInput,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	return d.download(ctx, input, media)
}

func (w *recordingArtifactWriter) WriteInboundArtifact(_ context.Context, artifact channels.InboundArtifact) (string, bool, error) {
	w.called = true
	w.artifact = artifact
	return w.ref, w.owned, nil
}

func (w *recordingArtifactWriter) DeleteInboundArtifact(_ context.Context, artifact channels.InboundArtifact, _ string) error {
	w.deleted = append(w.deleted, artifact)
	return nil
}

var _ channels.ArtifactWriter = (*recordingArtifactWriter)(nil)
var _ channels.ArtifactCompensator = (*recordingArtifactWriter)(nil)
