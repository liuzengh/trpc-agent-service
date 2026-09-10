package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	attachmentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestDecodeAESKeyAndDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	plain := append([]byte(strings.Repeat("x", 16)), []byte{0, 0, 0, 3}...)
	plain = append(plain, []byte("abcRID")...)
	block, _ := aes.NewCipher(key)
	padded := append([]byte(nil), plain...)
	n := aes.BlockSize - len(padded)%aes.BlockSize
	padded = append(padded, bytes.Repeat([]byte{byte(n)}, n)...)
	encrypted := make([]byte, len(padded))
	iv := key[:aes.BlockSize]
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	h := &Handler{key: key, receiveID: "RID"}
	got, err := h.decrypt(base64.StdEncoding.EncodeToString(encrypted))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(plain[20:23]) {
		t.Fatalf("got %q", got)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	stub := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	staticTarget := staticTestTarget(t)
	for _, config := range []Config{
		{},
		{Dispatcher: stub, MaxBodyBytes: -1},
		{Dispatcher: stub, ExecutionTimeout: -1},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1"},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: "bad", Target: channels.RoutingTarget{}},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: channels.RoutingTarget{}, Candidates: &dynamicCandidateConsumer{}},
		{Dispatcher: stub, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: staticTarget, Attachments: attachmentmemory.New(), MediaDownloader: &fakeWeComMediaDownloader{}},
	} {
		if handler, err := New(config); handler != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid config = handler %v, err %v", handler, err)
		}
	}
}

func TestSignatureAndInvalidPadding(t *testing.T) {
	h := &Handler{token: "token"}
	if h.validSignature("", "1", "2", "3") {
		t.Fatal("empty signature accepted")
	}
	if h.validSignature("bad", "1", "2", "3") {
		t.Fatal("bad signature accepted")
	}
	if _, err := unpad([]byte{1, 2}); err == nil {
		t.Fatal("invalid padding accepted")
	}
	if _, err := decodeAESKey("bad"); err == nil {
		t.Fatal("invalid AES key accepted")
	}
	if (&Handler{}).validSignature("a", "b", "c", "d") {
		t.Fatal("unconfigured handler accepted signature")
	}
}

func TestHandlerVerifyStaticCallbackState(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	ciphertext := encryptCallbackTestPayload(t, handler.static.key, handler.static.receiveID, []byte("static callback"))
	request := callbackVerificationRequest("token", "/", ciphertext)
	plain, state, err := handler.verify(request, ciphertext)
	if err != nil || string(plain) != "static callback" || state.token != handler.static.token || state.receiveID != handler.static.receiveID || state.agentID != handler.static.agentID || !bytes.Equal(state.key, handler.static.key) {
		t.Fatalf("verify = plain %q state %+v err %v", plain, state, err)
	}
	query := request.URL.Query()
	query.Set("msg_signature", "bad")
	request.URL.RawQuery = query.Encode()
	if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
		t.Fatalf("bad static signature error = %v", err)
	}
}

func TestHandlerVerifyDynamicCandidateBoundaries(t *testing.T) {
	t.Run("rejects malformed route and lookup failure", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		request.URL.Path = "/wecom/callback"
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
			t.Fatalf("malformed route error = %v", err)
		}
		request.URL.Path = "/wecom/callback/verify-route"
		consumer.lookupErr = errors.New("candidate lookup failed")
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) {
			t.Fatalf("lookup error = %v", err)
		}
	})

	t.Run("skips failed candidate and returns verified target", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.candidates = []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}, {Channel: channels.ChannelWeCom}}
		handler.credentials = &sequenceCredentialResolver{values: []Credentials{
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
			{CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
		}}
		plain, state, err := handler.verify(request, ciphertext)
		if err != nil || string(plain) != "dynamic callback" || state.principal.TenantID() == "" || consumer.consumeCalls != 2 {
			t.Fatalf("verify = plain %q state %+v consumes %d err %v", plain, state, consumer.consumeCalls, err)
		}
		target, ok := state.principal.RoutingTarget()
		if !ok || target.BindingID != consumer.binding.BindingID {
			t.Fatalf("principal target = %+v, ok=%t", target, ok)
		}
	})

	t.Run("rejects when every candidate fails verification", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.candidates = []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}, {Channel: channels.ChannelWeCom}}
		handler.credentials = &sequenceCredentialResolver{values: []Credentials{
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
			{CallbackToken: "wrong-token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))},
		}}
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, ErrVerification) || consumer.consumeCalls != 2 {
			t.Fatalf("exhausted candidates error = %v consumes %d", err, consumer.consumeCalls)
		}
	})

	t.Run("propagates cancellation", func(t *testing.T) {
		handler, consumer, ciphertext, request := newDynamicVerifyFixture(t)
		consumer.consumeErrs = []error{context.Canceled}
		if _, _, err := handler.verify(request, ciphertext); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	})
}

func TestHandlerRejectsUnsupportedMethodsAndRoutes(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	methodResponse := httptest.NewRecorder()
	handler.ServeHTTP(methodResponse, httptest.NewRequest(http.MethodPut, "/", nil))
	if methodResponse.Code != http.StatusMethodNotAllowed || methodResponse.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("method response = %d allow %q", methodResponse.Code, methodResponse.Header().Get("Allow"))
	}
	routeResponse := httptest.NewRecorder()
	handler.routeKey = "expected"
	handler.ServeHTTP(routeResponse, httptest.NewRequest(http.MethodGet, "/wecom/callback/other", nil))
	if routeResponse.Code != http.StatusNotFound {
		t.Fatalf("route response = %d", routeResponse.Code)
	}
}

func TestProviderCachesTokenAndDeliversText(t *testing.T) {
	var tokenCalls, sendCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/cgi-bin/gettoken" {
			tokenCalls++
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"secret-token","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/cgi-bin/message/send" {
			sendCalls++
			if r.URL.Query().Get("access_token") != "secret-token" {
				t.Errorf("token missing")
			}
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"m-1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "app-secret", BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return time.Unix(100, 0).UTC() }}
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"}}
	if id, err := p.Deliver(context.Background(), value); err != nil || id != "m-1" {
		t.Fatalf("deliver = %q, %v", id, err)
	}
	if _, err := p.Deliver(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 1 || sendCalls != 2 {
		t.Fatalf("calls token=%d send=%d", tokenCalls, sendCalls)
	}
}

func TestProviderDeliversGroupChat(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/cgi-bin/gettoken" {
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/cgi-bin/message/send" {
			_ = json.NewDecoder(r.Body).Decode(&payload)
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"group-1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "group", ReceiverID: "chat-1"}}
	if id, err := p.Deliver(context.Background(), value); err != nil || id != "group-1" {
		t.Fatalf("group deliver = %q, %v", id, err)
	}
	if payload["chatid"] != "chat-1" || payload["touser"] != nil {
		t.Fatalf("group payload = %#v", payload)
	}
}

func TestProviderSendsMediaReplyFallbackText(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/cgi-bin/gettoken" {
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":3600}`)
			return
		}
		if r.URL.Path == "/cgi-bin/message/send" {
			_ = json.NewDecoder(r.Body).Decode(&payload)
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"media-fallback-1"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
	value := storage.ReplyOutbox{
		Payload: "caption", Kind: storage.ReplyKindImage,
		Attachment: wecomReplyReference(t, attachment.KindImage, "image/png", "chart.png", []byte("png")),
		Fallback:   "[image attachment: chart.png]",
		ReplyTarget: storage.ReplyTarget{
			ConversationKind: "direct",
			ReceiverID:       "user-1",
		},
	}
	if id, err := p.Deliver(context.Background(), value); err != nil || id != "media-fallback-1" {
		t.Fatalf("media fallback deliver = %q, %v", id, err)
	}
	text, ok := payload["text"].(map[string]any)
	if payload["msgtype"] != "text" || !ok || text["content"] != "[image attachment: chart.png]" {
		t.Fatalf("media fallback payload = %#v", payload)
	}
}

//nolint:gocyclo // Covers the complete native upload/send contract for both supported media kinds.
func TestProviderSendsNativeMediaReply(t *testing.T) {
	for _, test := range []struct {
		name           string
		kind           storage.ReplyKind
		attachmentKind attachment.Kind
		mimeType       string
		fileName       string
		data           []byte
		uploadType     string
		messageType    string
		fallback       string
	}{
		{name: "image", kind: storage.ReplyKindImage, attachmentKind: attachment.KindImage, mimeType: "image/png", fileName: "chart.png", data: []byte("png"), uploadType: "image", messageType: "image", fallback: "[image attachment: chart.png]"},
		{name: "document", kind: storage.ReplyKindDocument, attachmentKind: attachment.KindDocument, mimeType: "application/pdf", fileName: "brief.pdf", data: []byte("pdf"), uploadType: "file", messageType: "file", fallback: "[document attachment: brief.pdf]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var payload map[string]any
			var uploadCalls int
			var sendCalls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/cgi-bin/gettoken":
					_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":3600}`)
				case "/cgi-bin/media/upload":
					uploadCalls++
					if r.URL.Query().Get("access_token") != "token" || r.URL.Query().Get("type") != test.uploadType {
						t.Errorf("upload query = %s", r.URL.RawQuery)
					}
					if err := r.ParseMultipartForm(1 << 20); err != nil {
						t.Errorf("parse upload = %v", err)
						return
					}
					files := r.MultipartForm.File["media"]
					if len(files) != 1 || files[0].Filename != test.fileName {
						t.Errorf("upload files = %#v", files)
						return
					}
					file, err := files[0].Open()
					if err != nil {
						t.Errorf("open upload = %v", err)
						return
					}
					defer func() { _ = file.Close() }()
					data, err := io.ReadAll(file)
					if err != nil || string(data) != string(test.data) {
						t.Errorf("upload data = %q, %v", data, err)
						return
					}
					_, _ = io.WriteString(w, `{"errcode":0,"media_id":"uploaded-`+test.messageType+`"}`)
				case "/cgi-bin/message/send":
					sendCalls++
					if r.URL.Query().Get("access_token") != "token" {
						t.Errorf("send query = %s", r.URL.RawQuery)
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Errorf("decode send payload = %v", err)
					}
					_, _ = io.WriteString(w, `{"errcode":0,"msgid":"native-`+test.messageType+`-1"}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			reference := wecomReplyReference(t, test.attachmentKind, test.mimeType, test.fileName, test.data)
			reader := &providerAttachmentReader{content: attachment.Content{Data: test.data}}
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client(), Attachments: reader}
			reply := storage.ReplyOutbox{
				TenantID: "tenant-a", EventID: "event-1", ReplyID: "reply-native-" + test.messageType,
				SegmentIndex: 0, SegmentCount: 1, Kind: test.kind, Payload: "caption",
				Attachment: reference, Fallback: test.fallback,
				ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"},
			}

			receipt, err := provider.Deliver(context.Background(), reply)
			if err != nil || receipt != "native-"+test.messageType+"-1" {
				t.Fatalf("native deliver = %q, %v", receipt, err)
			}
			if uploadCalls != 1 || sendCalls != 1 || reader.calls != 1 {
				t.Fatalf("calls upload=%d send=%d reader=%d", uploadCalls, sendCalls, reader.calls)
			}
			if reader.tenantID != reply.TenantID || reader.eventID != reply.EventID || reader.reference != reference {
				t.Fatalf("reader args = %q %q %+v", reader.tenantID, reader.eventID, reader.reference)
			}
			media, ok := payload[test.messageType].(map[string]any)
			if payload["msgtype"] != test.messageType || payload["touser"] != "user-1" || payload["agentid"] != float64(1) || !ok || media["media_id"] != "uploaded-"+test.messageType {
				t.Fatalf("native payload = %#v", payload)
			}
		})
	}
}

func TestProviderPreservesNativeAttachmentCancellation(t *testing.T) {
	data := []byte("png")
	reference := wecomReplyReference(t, attachment.KindImage, "image/png", "chart.png", data)
	provider := &Provider{
		CorpID: "corp", AgentID: "1", AppSecret: "secret",
		Attachments: &providerAttachmentReader{err: context.Canceled},
		token:       "cached", tokenExpiry: time.Now().Add(time.Hour),
	}
	_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-image", SegmentIndex: 0, SegmentCount: 1,
		Kind: storage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
		ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"},
	})
	assertDeliveryErrorClass(t, err, "canceled", true)
}

func TestHTTPMediaDownloaderFetchesMediaWithVerifiedBindingContext(t *testing.T) {
	var tokenCalls int
	var mediaCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			tokenCalls++
			if r.URL.Query().Get("corpid") != "corp" || r.URL.Query().Get("corpsecret") != "secret" {
				t.Errorf("token query = %s", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":3600}`)
		case "/cgi-bin/media/get":
			mediaCalls++
			if r.URL.Query().Get("access_token") != "token" || r.URL.Query().Get("media_id") != "media-1" {
				t.Errorf("media query = %s", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, "media-bytes")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &HTTPMediaDownloader{BaseURL: server.URL, HTTPClient: server.Client(), Now: func() time.Time { return time.Unix(100, 0).UTC() }}
	request := MediaDownloadRequest{TenantID: "tenant-a", BindingID: "binding-1", CorpID: "corp", AgentID: "1", AppSecret: "secret", MediaID: "media-1", Kind: attachment.KindImage, MIMEType: "image/jpeg", MaximumBytes: 1 << 20}
	for i := 0; i < 2; i++ {
		body, err := downloader.Download(context.Background(), request)
		if err != nil {
			t.Fatalf("download = %v", err)
		}
		data, err := io.ReadAll(body)
		closeErr := body.Close()
		if err != nil || closeErr != nil || string(data) != "media-bytes" {
			t.Fatalf("download body = %q read=%v close=%v", data, err, closeErr)
		}
	}
	if tokenCalls != 1 || mediaCalls != 2 {
		t.Fatalf("calls token=%d media=%d", tokenCalls, mediaCalls)
	}
}

func TestHTTPMediaDownloaderRejectsInvalidOrProviderErrorResponses(t *testing.T) {
	if _, err := (*HTTPMediaDownloader)(nil).Download(context.Background(), MediaDownloadRequest{}); !errors.Is(err, ErrAttachment) {
		t.Fatalf("nil downloader error = %v", err)
	}

	for _, test := range []struct {
		name string
		kind attachment.Kind
		mime string
		body string
	}{
		{name: "provider error JSON", kind: attachment.KindImage, mime: "image/jpeg", body: `{"errcode":40003}`},
		{name: "successful JSON is not document media", kind: attachment.KindDocument, mime: "application/octet-stream", body: `{"errcode":0}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cgi-bin/gettoken" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"errcode":0,"access_token":"token","expires_in":3600}`)
					return
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()

			downloader := &HTTPMediaDownloader{BaseURL: server.URL, HTTPClient: server.Client()}
			request := MediaDownloadRequest{TenantID: "tenant-a", BindingID: "binding-1", CorpID: "corp", AgentID: "1", AppSecret: "secret", MediaID: "media-1", Kind: test.kind, MIMEType: test.mime, MaximumBytes: 1 << 20}
			body, err := downloader.Download(context.Background(), request)
			if body != nil || !errors.Is(err, ErrAttachment) {
				t.Fatalf("provider JSON download = body %v err %v", body, err)
			}
		})
	}
}

func TestWeComMediaHelpersCoverFamiliesAndFallbacks(t *testing.T) {
	for _, test := range []struct {
		name    string
		message inboundXML
		want    wecomAttachment
	}{
		{name: "file", message: inboundXML{MsgType: " file ", MediaID: " media-file ", FileName: "bad/name"}, want: wecomAttachment{mediaID: "media-file", kind: attachment.KindDocument, mimeType: wecomDefaultFileMIME, name: "media-file"}},
		{name: "voice", message: inboundXML{MsgType: "voice", MediaID: "media-voice", Format: "speex"}, want: wecomAttachment{mediaID: "media-voice", kind: attachment.KindAudio, mimeType: "audio/speex", name: "media-voice.speex"}},
		{name: "video", message: inboundXML{MsgType: "video", MediaID: "media-video"}, want: wecomAttachment{mediaID: "media-video", kind: attachment.KindVideo, mimeType: wecomDefaultVideoMIME, name: "media-video.mp4"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := wecomAttachmentDescriptor(test.message)
			if err != nil || got != test.want {
				t.Fatalf("descriptor = %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
	if _, err := wecomAttachmentDescriptor(inboundXML{MsgType: "location", MediaID: "media"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsupported descriptor = %v", err)
	}
	for _, test := range []struct {
		format, mime, suffix string
	}{
		{format: "mp3", mime: "audio/mpeg", suffix: ".mp3"},
		{format: "wav", mime: "audio/wav", suffix: ".wav"},
		{format: "m4a", mime: "audio/mp4", suffix: ".m4a"},
		{format: "ogg", mime: "audio/ogg", suffix: ".ogg"},
		{format: "unknown", mime: wecomDefaultVoiceMIME, suffix: wecomDefaultVoiceSuffix},
	} {
		if mime, suffix := voiceMIME(test.format); mime != test.mime || suffix != test.suffix {
			t.Fatalf("voiceMIME(%q) = %q/%q, want %q/%q", test.format, mime, suffix, test.mime, test.suffix)
		}
	}
	if got := mediaName(" report.pdf ", "media-file", ".bin"); got != "report.pdf" {
		t.Fatalf("mediaName kept name = %q", got)
	}
	long := strings.Repeat("x", wecomMediaContentRunes)
	if got := wecomMediaContent(attachment.Reference{Kind: attachment.KindDocument, Name: long}); got != "[wecom document attachment]" {
		t.Fatalf("long media content = %q", got)
	}
	if got, err := normalizeAttachmentBytes(1); err != nil || got != 1 {
		t.Fatalf("normalizeAttachmentBytes = %d, %v", got, err)
	}
	if _, err := normalizeAttachmentBytes(maximumAttachmentBytes + 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized attachment limit = %v", err)
	}
}

func TestHTTPMediaDownloaderValidatesRequestAndResponseBoundaries(t *testing.T) {
	normalized, err := normalizeMediaDownloadRequest(MediaDownloadRequest{
		TenantID: " tenant-a ", BindingID: " binding-1 ", CorpID: " corp ", AgentID: " 1 ", AppSecret: " secret ",
		MediaID: " media-1 ", Kind: attachment.KindDocument, MIMEType: " APPLICATION/PDF ",
	})
	if err != nil || normalized.TenantID != "tenant-a" || normalized.MIMEType != "application/pdf" || normalized.MaximumBytes != defaultAttachmentBytes {
		t.Fatalf("normalized request = %+v, %v", normalized, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*MediaDownloadRequest)
	}{
		{name: "missing tenant", mutate: func(request *MediaDownloadRequest) { request.TenantID = "" }},
		{name: "invalid kind", mutate: func(request *MediaDownloadRequest) { request.Kind = "sticker" }},
		{name: "control value", mutate: func(request *MediaDownloadRequest) { request.MediaID = "bad\nmedia" }},
		{name: "oversized maximum", mutate: func(request *MediaDownloadRequest) { request.MaximumBytes = maximumAttachmentBytes + 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := normalized
			test.mutate(&request)
			if _, err := normalizeMediaDownloadRequest(request); !errors.Is(err, ErrAttachment) {
				t.Fatalf("normalizeMediaDownloadRequest accepted %+v: %v", request, err)
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&HTTPMediaDownloader{}).Download(canceled, normalized); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled download = %v", err)
	}
	if _, err := readWeComMediaResponse(context.Background(), nil, normalized); !errors.Is(err, ErrAttachment) {
		t.Fatalf("nil response = %v", err)
	}
	if _, err := readWeComMediaResponse(context.Background(), &http.Response{}, normalized); !errors.Is(err, ErrAttachment) {
		t.Fatalf("nil body = %v", err)
	}
	for _, test := range []struct {
		name        string
		status      int
		contentType string
		body        string
		kind        attachment.Kind
		maximum     int64
	}{
		{name: "bad status", status: http.StatusBadGateway, contentType: "application/octet-stream", body: "x", kind: attachment.KindDocument, maximum: 8},
		{name: "empty body", status: http.StatusOK, contentType: "application/octet-stream", kind: attachment.KindDocument, maximum: 8},
		{name: "too large", status: http.StatusOK, contentType: "application/octet-stream", body: "123456", kind: attachment.KindDocument, maximum: 5},
		{name: "json suffix", status: http.StatusOK, contentType: "application/problem+json", body: "{}", kind: attachment.KindDocument, maximum: 8},
		{name: "media mismatch", status: http.StatusOK, contentType: "image/png", body: "png", kind: attachment.KindDocument, maximum: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{StatusCode: test.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(test.body))}
			response.Header.Set("Content-Type", test.contentType)
			request := normalized
			request.Kind = test.kind
			request.MaximumBytes = test.maximum
			if data, err := readWeComMediaResponse(context.Background(), response, request); data != nil || !errors.Is(err, ErrAttachment) {
				t.Fatalf("readWeComMediaResponse = %q, %v", data, err)
			}
		})
	}
	if providerJSONError("not a media type", nil) {
		t.Fatal("non-JSON invalid content type was rejected as provider JSON")
	}
	for _, test := range []struct {
		kind        attachment.Kind
		contentType string
		want        bool
	}{
		{kind: attachment.KindImage, contentType: "image/png", want: true},
		{kind: attachment.KindVideo, contentType: "video/mp4", want: true},
		{kind: attachment.KindAudio, contentType: "audio/mpeg", want: true},
		{kind: attachment.KindDocument, contentType: "application/pdf", want: true},
		{kind: attachment.KindDocument, contentType: "audio/mpeg"},
		{kind: attachment.Kind("unknown"), contentType: "application/octet-stream", want: true},
		{kind: attachment.Kind("unknown"), contentType: "text/plain"},
	} {
		if got := downloadContentTypeMatches(test.kind, test.contentType); got != test.want {
			t.Fatalf("downloadContentTypeMatches(%q, %q) = %t, want %t", test.kind, test.contentType, got, test.want)
		}
	}
}

func TestHandlerBuildsGroupMediaAndIngestFailures(t *testing.T) {
	target := staticTestTarget(t)
	principal, err := gateway.NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	state := callbackState{principal: principal, agentID: "1", appSecret: "app-secret"}
	data := []byte("document")
	downloader := &fakeWeComMediaDownloader{data: data}
	handler := &Handler{attachments: attachmentmemory.New(), mediaDownloader: downloader, maxAttachmentBytes: defaultAttachmentBytes}

	inbound, err := handler.buildInboundMessage(context.Background(), state, inboundXML{
		MsgID: "message-file", FromUserName: "user-1", ChatID: "chat-1", MsgType: "file", AgentID: "1", MediaID: "media-file", FileName: "brief.pdf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inbound.ConversationKind != channels.ConversationGroup || inbound.ExternalChatID != "chat-1" || inbound.Content != "[wecom document attachment: brief.pdf]" || len(inbound.Attachments) != 1 {
		t.Fatalf("group media inbound = %+v", inbound)
	}
	if downloader.request.BindingID != target.BindingID || downloader.request.CorpID != target.ProviderAccountID || downloader.request.AppSecret != "app-secret" {
		t.Fatalf("download request = %+v", downloader.request)
	}

	if _, err := handler.buildInboundMessage(context.Background(), state, inboundXML{MsgID: "message-bad", FromUserName: "user-1", MsgType: "location", MediaID: "media"}); !errors.Is(err, ErrAttachment) {
		t.Fatalf("invalid media build = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := handler.ingestAttachment(canceled, state, inboundXML{MsgID: "message-cancel", FromUserName: "user-1", MsgType: "image", MediaID: "media-image"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ingest = %v", err)
	}
	for _, test := range []struct {
		name       string
		downloader MediaDownloader
		want       error
	}{
		{name: "download canceled", downloader: wecomMediaDownloaderFunc(func(context.Context, MediaDownloadRequest) (io.ReadCloser, error) { return nil, context.Canceled }), want: context.Canceled},
		{name: "download redacted", downloader: wecomMediaDownloaderFunc(func(context.Context, MediaDownloadRequest) (io.ReadCloser, error) {
			return nil, errors.New("provider secret")
		}), want: ErrAttachment},
		{name: "nil reader", downloader: wecomMediaDownloaderFunc(func(context.Context, MediaDownloadRequest) (io.ReadCloser, error) { return nil, nil }), want: ErrAttachment},
		{name: "read error", downloader: wecomMediaDownloaderFunc(func(context.Context, MediaDownloadRequest) (io.ReadCloser, error) { return failingReadCloser{}, nil }), want: ErrAttachment},
		{name: "empty body", downloader: &fakeWeComMediaDownloader{}, want: ErrAttachment},
		{name: "too large", downloader: &fakeWeComMediaDownloader{data: []byte("123456")}, want: ErrAttachment},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &Handler{attachments: attachmentmemory.New(), mediaDownloader: test.downloader, maxAttachmentBytes: 5}
			_, err := h.ingestAttachment(context.Background(), state, inboundXML{MsgID: "message-" + test.name, FromUserName: "user-1", MsgType: "image", MediaID: "media-image"})
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("ingest error = %v, want %v", err, test.want)
			}
		})
	}
	storeCanceled, cancelStore := context.WithCancel(context.Background())
	downloader = &fakeWeComMediaDownloader{data: data}
	cancelingStore := wecomAttachmentStoreFunc(func(context.Context, string, attachment.Upload, io.Reader) (attachment.Reference, error) {
		cancelStore()
		return attachment.Reference{}, context.Canceled
	})
	h := &Handler{attachments: cancelingStore, mediaDownloader: downloader, maxAttachmentBytes: defaultAttachmentBytes}
	if _, err := h.ingestAttachment(storeCanceled, state, inboundXML{MsgID: "message-store-cancel", FromUserName: "user-1", MsgType: "image", MediaID: "media-image"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("store cancellation = %v", err)
	}
	h = &Handler{attachments: wecomAttachmentStoreFunc(func(context.Context, string, attachment.Upload, io.Reader) (attachment.Reference, error) {
		return attachment.Reference{}, errors.New("write failed")
	}), mediaDownloader: downloader, maxAttachmentBytes: defaultAttachmentBytes}
	if _, err := h.ingestAttachment(context.Background(), state, inboundXML{MsgID: "message-store", FromUserName: "user-1", MsgType: "image", MediaID: "media-image"}); !errors.Is(err, ErrAttachment) {
		t.Fatalf("store failure = %v", err)
	}
}

func TestHandlerAttachmentIDIncludesBindingScope(t *testing.T) {
	firstTarget := staticTestTarget(t)
	secondTarget := staticTestTarget(t)
	if firstTarget.BindingID == secondTarget.BindingID {
		t.Fatal("test requires distinct binding IDs")
	}
	firstPrincipal, err := gateway.NewChannelPrincipal(firstTarget)
	if err != nil {
		t.Fatal(err)
	}
	secondPrincipal, err := gateway.NewChannelPrincipal(secondTarget)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("wecom-image")
	handler := &Handler{attachments: attachmentmemory.New(), mediaDownloader: &fakeWeComMediaDownloader{data: data}, maxAttachmentBytes: defaultAttachmentBytes}
	message := inboundXML{MsgID: "message-same", FromUserName: "user-1", MsgType: "image", AgentID: "1", MediaID: "media-same"}

	first, err := handler.ingestAttachment(context.Background(), callbackState{principal: firstPrincipal, agentID: "1", appSecret: "app-secret"}, message)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.ingestAttachment(context.Background(), callbackState{principal: secondPrincipal, agentID: "1", appSecret: "app-secret"}, message)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("attachment IDs collided across bindings: %q", first.ID)
	}
	if first.ID != attachmentID(firstTarget.BindingID, "message-same", 0, "media-same") || second.ID != attachmentID(secondTarget.BindingID, "message-same", 0, "media-same") {
		t.Fatalf("binding-scoped IDs = %q and %q", first.ID, second.ID)
	}
}

func TestProviderNativeMediaErrorBranches(t *testing.T) {
	data := []byte("png")
	reference := wecomReplyReference(t, attachment.KindImage, "image/png", "chart.png", data)
	base := storage.ReplyOutbox{
		TenantID: "tenant-a", EventID: "event-image", ReplyID: "reply-image", SegmentIndex: 0, SegmentCount: 1,
		Kind: storage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
		ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"},
	}
	for _, test := range []struct {
		name      string
		reader    *providerAttachmentReader
		class     string
		retryable bool
	}{
		{name: "missing attachment", reader: &providerAttachmentReader{err: storage.ErrNotFound}, class: "invalid"},
		{name: "storage unavailable", reader: &providerAttachmentReader{err: errors.New("storage down")}, class: "unavailable", retryable: true},
		{name: "tampered attachment", reader: &providerAttachmentReader{content: attachment.Content{Data: []byte("tampered")}}, class: "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", Attachments: test.reader, token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), base)
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
		})
	}
	for _, test := range []struct {
		name      string
		status    int
		body      string
		class     string
		retryable bool
	}{
		{name: "upload unavailable", status: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "upload malformed", status: http.StatusOK, body: "not-json", class: "provider_error", retryable: true},
		{name: "upload rejected", status: http.StatusOK, body: `{"errcode":40003}`, class: "provider_error"},
		{name: "missing media id", status: http.StatusOK, body: `{"errcode":0}`, class: "provider_error", retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/cgi-bin/media/upload" {
					t.Fatalf("path = %s", r.URL.Path)
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client(), Attachments: &providerAttachmentReader{content: attachment.Content{Data: data}}, token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), base)
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
		})
	}
	transport := &Provider{
		CorpID: "corp", AgentID: "1", AppSecret: "secret",
		HTTPClient:  &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded })},
		Attachments: &providerAttachmentReader{content: attachment.Content{Data: data}},
		token:       "cached", tokenExpiry: time.Now().Add(time.Hour),
	}
	_, err := transport.Deliver(context.Background(), base)
	assertDeliveryErrorClass(t, err, "timeout", true)

	fallback := base
	fallback.Kind = storage.ReplyKindAudio
	fallback.Attachment = wecomReplyReference(t, attachment.KindAudio, "audio/mpeg", "voice.mp3", []byte("mp3"))
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cgi-bin/message/send" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		_, _ = io.WriteString(w, `{"errcode":0,"msgid":"audio-fallback"}`)
	}))
	defer server.Close()
	receipt, err := (&Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client(), token: "cached", tokenExpiry: time.Now().Add(time.Hour)}).Deliver(context.Background(), fallback)
	if err != nil || receipt != "audio-fallback" {
		t.Fatalf("audio fallback = %q, %v", receipt, err)
	}
	text, ok := payload["text"].(map[string]any)
	if payload["msgtype"] != "text" || !ok || text["content"] != fallback.Fallback {
		t.Fatalf("audio fallback payload = %#v", payload)
	}
}

func TestProviderValidatesBeforeReceiptReplay(t *testing.T) {
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", receipts: map[string]string{"tenant\x00reply\x000": "m-1"}}
	value := storage.ReplyOutbox{TenantID: "tenant", ReplyID: "reply", SegmentIndex: 0, Payload: "", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	_, err := p.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)
}

func TestProviderValidatesAgentIDBeforeReceiptReplay(t *testing.T) {
	p := &Provider{CorpID: "corp", AgentID: "not-canonical", AppSecret: "secret", receipts: map[string]string{"tenant\x00reply\x000": "m-1"}}
	value := storage.ReplyOutbox{TenantID: "tenant", ReplyID: "reply", SegmentIndex: 0, Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	_, err := p.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)
}

func TestProviderRejectsOversizedText(t *testing.T) {
	p := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "app-secret"}
	_, err := p.Deliver(context.Background(), storage.ReplyOutbox{Payload: strings.Repeat("界", 683), ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user-1"}})
	if err == nil {
		t.Fatal("oversized text was accepted")
	}
}

func TestProviderMapsTransportAndPayloadErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		response string
		status   int
		want     string
	}{
		{name: "malformed token", response: "not-json", status: http.StatusOK, want: "provider_error"},
		{name: "token rejected", response: `{"errcode":40014}`, status: http.StatusOK, want: "unauthenticated"},
		{name: "send malformed", response: `{"errcode":0,"access_token":"token","expires_in":3600}`, status: http.StatusOK, want: "provider_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(test.status)
				if request.URL.Path == "/cgi-bin/gettoken" && test.name == "send malformed" {
					_, _ = io.WriteString(writer, `{"errcode":0,"access_token":"token","expires_in":3600}`)
					return
				}
				_, _ = io.WriteString(writer, test.response)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			var deliveryErr *outbox.DeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Class != test.want {
				t.Fatalf("delivery error = %v", err)
			}
		})
	}
}

func TestProviderClassifiesDeliveryOutcomes(t *testing.T) {
	for _, test := range []struct {
		name       string
		code, http int
		class      string
		retryable  bool
	}{
		{name: "expired token", code: 42001, class: "unauthenticated", retryable: true},
		{name: "rate limited", code: 45009, class: "rate_limited", retryable: true},
		{name: "server error", http: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "provider rejection", code: 40003, http: http.StatusOK, class: "provider_error", retryable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			class, retryable := classifyWeCom(test.code, test.http)
			if class != test.class || retryable != test.retryable {
				t.Fatalf("classifyWeCom(%d, %d) = %q, %t", test.code, test.http, class, retryable)
			}
		})
	}
	provider := &Provider{}
	if status, _, err := provider.Reconcile(context.Background(), storage.ReplyOutbox{}); status != outbox.DeliveryUnknown || err != nil {
		t.Fatalf("reconcile = %q, %v", status, err)
	}
	bindingProvider := &BindingProvider{}
	if _, err := bindingProvider.Deliver(context.Background(), storage.ReplyOutbox{}); err == nil {
		t.Fatal("unconfigured binding provider delivered a reply")
	}
	if status, _, err := bindingProvider.Reconcile(context.Background(), storage.ReplyOutbox{}); status != outbox.DeliveryUnknown || err == nil {
		t.Fatalf("unconfigured binding provider reconcile = %q, %v", status, err)
	}
}

func TestProviderDefaultsAndContextCancellation(t *testing.T) {
	provider := &Provider{}
	if provider.baseURL() != "https://qyapi.weixin.qq.com" || provider.client() == nil {
		t.Fatalf("provider defaults are invalid")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret"}).Deliver(canceled, storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != "canceled" && deliveryErr.Class != "unavailable" {
		t.Fatalf("canceled delivery = %v", err)
	}
}

func TestProviderRejectsInvalidDeliveryInputs(t *testing.T) {
	valid := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", token: "token", tokenExpiry: time.Now().Add(time.Hour)}
	for _, test := range []struct {
		name     string
		provider *Provider
		context  context.Context
		value    storage.ReplyOutbox
	}{
		{name: "nil provider", context: context.Background(), value: valid},
		{name: "missing credentials", provider: &Provider{}, context: context.Background(), value: valid},
		{name: "nil context", provider: provider, value: valid},
		{name: "missing recipient", provider: provider, context: context.Background(), value: storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct"}}},
		{name: "empty payload", provider: provider, context: context.Background(), value: storage.ReplyOutbox{ReplyTarget: valid.ReplyTarget}},
		{name: "oversized payload", provider: provider, context: context.Background(), value: storage.ReplyOutbox{Payload: strings.Repeat("x", maximumTextBytes+1), ReplyTarget: valid.ReplyTarget}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.provider.Deliver(test.context, test.value)
			assertDeliveryErrorClass(t, err, "invalid", false)
		})
	}
}

func TestProviderHandlesSendFailuresAndInvalidatesRejectedToken(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		response  string
		class     string
		retryable bool
		clears    bool
	}{
		{name: "non successful status", status: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "malformed response", status: http.StatusOK, response: "not-json", class: "provider_error", retryable: true},
		{name: "missing message id", status: http.StatusOK, response: `{"errcode":0}`, class: "provider_error", retryable: true},
		{name: "expired token", status: http.StatusOK, response: `{"errcode":42001}`, class: "unauthenticated", retryable: true, clears: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/cgi-bin/message/send" {
					t.Fatalf("unexpected endpoint %s", request.URL.Path)
				}
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.response)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client(), token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
			if test.clears && (!provider.tokenExpiry.IsZero() || provider.token != "") {
				t.Fatal("rejected access token remained cached")
			}
		})
	}
}

func TestProviderMapsCanceledAndTimedOutTransport(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "canceled", err: context.Canceled, class: "canceled"},
		{name: "timeout", err: context.DeadlineExceeded, class: "timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, test.err })}, token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
			_, err := provider.Deliver(context.Background(), storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}})
			assertDeliveryErrorClass(t, err, test.class, true)
		})
	}
}

func TestHandlerAcceptsEncryptedTextWithRequestAndTraceIDs(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	handler := newCallbackTestHandler(t, dispatcher)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequest(t, "message-1", "user-1", "hello"))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "success" {
		t.Fatalf("callback response = %d %q", recorder.Code, recorder.Body.String())
	}
	select {
	case request := <-dispatcher.requests:
		if request.RequestID == "" || request.TraceID == "" {
			t.Fatalf("request trace fields = request_id %q trace_id %q", request.RequestID, request.TraceID)
		}
		if request.Message.Content != "hello" || request.Message.ExternalMessageID != "message-1" || request.Message.ExternalUserID != "user-1" {
			t.Fatalf("dispatch message = %+v", request.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("callback did not dispatch")
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
}

//nolint:gocyclo // Keeps the encrypted callback-to-attachment contract visible in one scenario.
func TestHandlerAcceptsEncryptedNativeMediaAsAttachment(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "media-route", "env/wecom", app.AppID)
	data := []byte("wecom-image")
	downloader := &fakeWeComMediaDownloader{data: data}
	handler, err := New(Config{
		Candidates:       &dynamicCandidateConsumer{binding: binding},
		Tenants:          dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:             dynamicAppRepository{value: app},
		Credentials:      dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:       dispatcher,
		Attachments:      attachmentmemory.New(),
		MediaDownloader:  downloader,
		MaxBodyBytes:     1 << 20,
		ExecutionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	request := callbackXMLRequestAtPath(t, "/wecom/callback/media-route", []byte("<xml><MsgId>message-media</MsgId><FromUserName>user-1</FromUserName><MsgType>image</MsgType><AgentID>1</AgentID><MediaId>media-image</MediaId><PicUrl>https://provider.example/token</PicUrl></xml>"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "success" {
		t.Fatalf("media callback response = %d %q", response.Code, response.Body.String())
	}
	select {
	case request := <-dispatcher.requests:
		if request.Principal.TenantID() != binding.TenantID {
			t.Fatalf("dispatch principal tenant = %q, want %q", request.Principal.TenantID(), binding.TenantID)
		}
		if request.Message.ContentType != gateway.ContentTypeMedia || request.Message.Content != "[wecom image attachment: media-image.jpg]" || len(request.Message.Attachments) != 1 {
			t.Fatalf("media dispatch message = %+v", request.Message)
		}
		reference := request.Message.Attachments[0]
		digest := sha256.Sum256(data)
		if reference.ID != attachmentID(binding.BindingID, "message-media", 0, "media-image") || reference.Kind != attachment.KindImage || reference.MIMEType != "image/jpeg" || reference.Name != "media-image.jpg" || reference.Provider != "wecom" || reference.ProviderID != "media-image" || reference.Size != int64(len(data)) || reference.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("media attachment reference = %+v", reference)
		}
		if downloader.request.MediaID != "media-image" || downloader.request.TenantID != binding.TenantID || downloader.request.BindingID != binding.BindingID ||
			downloader.request.CorpID != "corp" || downloader.request.AgentID != "1" || downloader.request.AppSecret != "app-secret" ||
			downloader.request.Kind != attachment.KindImage || downloader.request.MIMEType != "image/jpeg" || downloader.request.MaximumBytes != defaultAttachmentBytes {
			t.Fatalf("download request = %+v", downloader.request)
		}
	case <-time.After(time.Second):
		t.Fatal("media callback did not dispatch")
	}
}

func TestHandlerRejectsMediaWithoutConfiguredAttachmentBoundary(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	handler := newCallbackTestHandler(t, dispatcher)
	t.Cleanup(func() { _ = handler.Close() })

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackXMLRequestAtPath(t, "/", []byte("<xml><MsgId>message-media</MsgId><FromUserName>user-1</FromUserName><MsgType>image</MsgType><AgentID>1</AgentID><MediaId>media-image</MediaId></xml>")))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("media without attachment boundary response = %d", response.Code)
	}
	select {
	case request := <-dispatcher.requests:
		t.Fatalf("unsupported media reached dispatch: %+v", request)
	default:
	}
}

func TestHandlerCloseCancelsAndJoinsAcceptedDrain(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1), canceled: make(chan struct{})}
	handler := newCallbackTestHandler(t, dispatcher)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequest(t, "message-2", "user-2", "wait"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("callback response = %d", recorder.Code)
	}
	select {
	case <-dispatcher.requests:
	case <-time.After(time.Second):
		t.Fatal("callback did not reach dispatcher")
	}
	closed := make(chan error, 1)
	go func() { closed <- handler.Close() }()
	select {
	case <-dispatcher.canceled:
	case <-time.After(time.Second):
		t.Fatal("handler close did not cancel dispatch")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("handler close did not join dispatch")
	}
	shutdownResponse := httptest.NewRecorder()
	handler.ServeHTTP(shutdownResponse, callbackTestRequest(t, "message-3", "user-2", "after shutdown"))
	if shutdownResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown response = %d", shutdownResponse.Code)
	}
}

func TestHandlerAcceptedDrainOutlivesRequestCancellation(t *testing.T) {
	dispatched := make(chan context.Context, 1)
	canceled := make(chan struct{})
	dispatcher := dispatchFunc(func(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		dispatched <- ctx
		if request.Accepted != nil {
			request.Accepted <- struct{}{}
		}
		stream := make(chan gateway.DispatchEvent)
		go func() {
			<-ctx.Done()
			close(canceled)
			close(stream)
		}()
		return stream, nil
	})
	handler := newCallbackTestHandler(t, dispatcher)
	defer func() { _ = handler.Close() }()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request := callbackTestRequest(t, "message-request-cancel", "user", "hello").WithContext(requestCtx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("callback response = %d", response.Code)
	}
	var dispatchContext context.Context
	select {
	case dispatchContext = <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("callback did not reach dispatcher")
	}
	cancelRequest()
	if err := dispatchContext.Err(); err != nil {
		t.Fatalf("request cancellation canceled accepted drain: %v", err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler close did not cancel accepted drain")
	}
}

func TestHandlerAcknowledgesCompletedAndDuplicateDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code int
	}{
		{name: "completed synchronously", code: http.StatusOK},
		{name: "duplicate", err: gateway.ErrDuplicateMessage, code: http.StatusOK},
		{name: "unavailable", err: errors.New("dispatcher unavailable"), code: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newCallbackTestHandler(t, dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
				return nil, test.err
			}))
			defer func() { _ = handler.Close() }()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, callbackTestRequest(t, "message-"+test.name, "user", "hello"))
			if response.Code != test.code {
				t.Fatalf("callback response = %d", response.Code)
			}
		})
	}
}

func TestHandlerRejectsInvalidChallengeAndMessageShape(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	defer func() { _ = handler.Close() }()
	challenge := httptest.NewRequest(http.MethodGet, "/?msg_signature=bad&timestamp=1&nonce=2&echostr=bad", nil)
	challengeResponse := httptest.NewRecorder()
	handler.ServeHTTP(challengeResponse, challenge)
	if challengeResponse.Code != http.StatusForbidden {
		t.Fatalf("invalid challenge response = %d", challengeResponse.Code)
	}
	request := callbackTestRequest(t, "message", "user", "hello")
	request.Body = io.NopCloser(strings.NewReader(requestBodyWithTrailingXML(t, request)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("trailing XML response = %d", response.Code)
	}
}

func TestDynamicHandlerRoutesOnlyVerifiedBinding(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "route-key", "env/wecom", app.AppID)
	writer, err := audit.NewInMemory(binding.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	writeSignal := make(chan struct{}, 2)
	handler, err := New(Config{
		Candidates:  &dynamicCandidateConsumer{binding: binding},
		Tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:        dynamicAppRepository{value: app},
		Credentials: dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:  dispatcher,
		AuditWriter: signalingAuditWriter{Writer: writer, Signal: writeSignal},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, callbackTestRequestAtPath(t, "/wecom/callback/route-key", "message-dynamic", "user-1", "hello"))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "success" {
		t.Fatalf("dynamic callback response = %d %q", recorder.Code, recorder.Body.String())
	}
	select {
	case <-writeSignal:
	case <-time.After(time.Second):
		t.Fatal("accepted ingress audit was not appended")
	}
	var target channels.RoutingTarget
	var requestID, traceID string
	select {
	case request := <-dispatcher.requests:
		var ok bool
		target, ok = request.Principal.RoutingTarget()
		if !ok || request.Principal.TenantID() != binding.TenantID || target.BindingID != binding.BindingID || request.RequestID == "" || request.TraceID == "" {
			t.Fatalf("dynamic dispatch request = %+v", request)
		}
		requestID, traceID = request.RequestID, request.TraceID
	case <-time.After(time.Second):
		t.Fatal("verified dynamic callback did not dispatch")
	}
	staticHandler, err := New(Config{Dispatcher: dispatcher, Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: target})
	if err != nil {
		t.Fatalf("static handler = %v", err)
	}
	if staticHandler == nil {
		t.Fatal("static handler is nil")
	}
	_ = staticHandler.Close()
	assertIngressAudit(t, writer, 1, audit.EventIMIngressAccepted, audit.DecisionAccepted, "", requestID, traceID)
	assertDuplicateIngressAudit(t, target, writer)

	badSignature := callbackTestRequestAtPath(t, "/wecom/callback/route-key", "message-bad", "user-1", "hello")
	badQuery := badSignature.URL.Query()
	badQuery.Set("msg_signature", "bad")
	badSignature.URL.RawQuery = badQuery.Encode()
	badResponse := httptest.NewRecorder()
	handler.ServeHTTP(badResponse, badSignature)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("bad dynamic signature response = %d", badResponse.Code)
	}

	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, callbackTestRequestAtPath(t, "/wecom/callback", "message-unknown", "user-1", "hello"))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown route response = %d", unknown.Code)
	}
}

func TestHandlerTryAcceptedIngress(t *testing.T) {
	handler := &Handler{}
	message := inboundXML{FromUserName: "user"}
	accepted := make(chan struct{}, 1)
	accepted <- struct{}{}
	recorder := httptest.NewRecorder()
	if !handler.tryAcceptedIngress(accepted, recorder, context.Background(), gateway.Principal{}, message, "request", "trace") {
		t.Fatal("accepted ingress was not consumed")
	}
	if recorder.Code != http.StatusOK || recorder.Body.String() != "success" {
		t.Fatalf("accepted ingress response = %d %q", recorder.Code, recorder.Body.String())
	}
	if handler.tryAcceptedIngress(make(chan struct{}, 1), httptest.NewRecorder(), context.Background(), gateway.Principal{}, message, "request", "trace") {
		t.Fatal("empty acceptance channel was consumed")
	}
}

func assertIngressAudit(t *testing.T, writer audit.Reader, count int, eventType audit.EventType, decision audit.Decision, errorType, requestID, traceID string) {
	t.Helper()
	var events []audit.Event
	var err error
	deadline := time.Now().Add(time.Second)
	for {
		events, err = writer.List(context.Background(), audit.Query{})
		if err != nil {
			t.Fatalf("ingress audit = %+v, err=%v", events, err)
		}
		if len(events) == count || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(events) != count {
		t.Fatalf("ingress audit = %+v, err=%v", events, err)
	}
	var event audit.Event
	found := 0
	for _, candidate := range events {
		if candidate.EventType == eventType {
			event = candidate
			found++
		}
	}
	if found != 1 {
		t.Fatalf("ingress event type %q count = %d, events = %+v", eventType, found, events)
	}
	if event.EventType != eventType {
		t.Fatalf("ingress event type = %q, want %q", event.EventType, eventType)
	}
	if event.Decision != decision {
		t.Fatalf("ingress decision = %q, want %q", event.Decision, decision)
	}
	if event.ErrorType != errorType {
		t.Fatalf("ingress error type = %q, want %q", event.ErrorType, errorType)
	}
	if requestID != "" && event.RequestID != requestID {
		t.Fatalf("ingress request ID = %q, want %q", event.RequestID, requestID)
	}
	if traceID != "" && event.TraceID != traceID {
		t.Fatalf("ingress trace ID = %q, want %q", event.TraceID, traceID)
	}
}

func assertDuplicateIngressAudit(t *testing.T, target channels.RoutingTarget, writer audit.Writer) {
	t.Helper()
	handler, err := New(Config{Dispatcher: dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		return nil, gateway.ErrDuplicateMessage
	}), Token: "token", ReceiveID: "receive", AgentID: "1", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Target: target, AuditWriter: writer})
	if err != nil {
		t.Fatalf("duplicate handler = %v", err)
	}
	defer func() { _ = handler.Close() }()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackTestRequest(t, "message-duplicate", "user-1", "hello"))
	if response.Code != http.StatusOK || response.Body.String() != "success" {
		t.Fatalf("duplicate callback response = %d %q", response.Code, response.Body.String())
	}
	assertIngressAudit(t, writer.(audit.Reader), 2, audit.EventIMIngressDuplicate, audit.DecisionDuplicate, string(audit.ErrorDuplicate), "", "")
}

func TestHandlerRejectsMalformedMessages(t *testing.T) {
	handler := newCallbackTestHandler(t, &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)})
	t.Cleanup(func() { _ = handler.Close() })
	for _, body := range []string{"", "<xml></xml>", "<xml><Encrypt>bad</Encrypt></xml>"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		if response.Code != http.StatusForbidden && response.Code != http.StatusBadRequest {
			t.Fatalf("malformed body %q status = %d", body, response.Code)
		}
	}
}

func TestHandlerRejectsUnknownOrIncompleteMediaCallbacks(t *testing.T) {
	dispatcher := &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)}
	handler := newCallbackTestHandler(t, dispatcher)
	handler.attachments = attachmentmemory.New()
	handler.mediaDownloader = &fakeWeComMediaDownloader{data: []byte("media")}
	handler.maxAttachmentBytes = defaultAttachmentBytes
	t.Cleanup(func() { _ = handler.Close() })

	for _, message := range [][]byte{
		[]byte("<xml><MsgId>unknown-media</MsgId><FromUserName>user-1</FromUserName><MsgType>location</MsgType><AgentID>1</AgentID><MediaId>media-id</MediaId></xml>"),
		[]byte("<xml><MsgId>missing-media-id</MsgId><FromUserName>user-1</FromUserName><MsgType>file</MsgType><AgentID>1</AgentID><FileName>report.pdf</FileName></xml>"),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, callbackXMLRequestAtPath(t, "/", message))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("malformed media response = %d", response.Code)
		}
	}
	select {
	case request := <-dispatcher.requests:
		t.Fatalf("malformed media reached dispatch: %+v", request)
	default:
	}
}

func TestDynamicHandlerAnswersVerifiedChallenge(t *testing.T) {
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "challenge-key", "env/wecom", app.AppID)
	handler, err := New(Config{
		Candidates:  &dynamicCandidateConsumer{binding: binding},
		Tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		Apps:        dynamicAppRepository{value: app},
		Credentials: dynamicCredentials{values: map[string]Credentials{binding.SecretRef: {CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), AppSecret: "app-secret"}}},
		Dispatcher:  &callbackDispatchStub{requests: make(chan gateway.DispatchRequest, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", []byte("challenge"))
	parts := []string{"token", "123", "456", ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- WeCom requires SHA-1 callback signatures.
	request := httptest.NewRequest(http.MethodGet, "/wecom/callback/challenge-key?msg_signature="+hex.EncodeToString(sum[:])+"&timestamp=123&nonce=456&echostr="+url.QueryEscape(ciphertext), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "challenge" {
		t.Fatalf("challenge response = %d %q", response.Code, response.Body.String())
	}
}

func TestBindingProviderUsesActiveWeComBindingAndCachesProvider(t *testing.T) {
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "binding-provider-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", BindingKey: "wecom", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp", PublicRouteKeyDigest: routeDigest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SecretRef: "env/wecom", Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp", AgentID: "1", ReceiveID: "receive"}}, Status: channels.StatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup := &bindingLookupStub{binding: binding}
	credentials := &credentialResolverStub{credentials: Credentials{AppSecret: "app-secret"}}
	reader := &providerAttachmentReader{}
	provider := &BindingProvider{Bindings: lookup, Credentials: credentials, Attachments: reader}
	value := storage.ReplyOutbox{TenantID: binding.TenantID, ReplyTarget: storage.ReplyTarget{BindingID: binding.BindingID, ConversationKind: "direct", ReceiverID: "user-1"}}
	first, err := provider.provider(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.provider(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.CorpID != "corp" || first.AgentID != "1" {
		t.Fatalf("binding provider = %+v, cached=%t", first, first == second)
	}
	if first.Attachments != reader {
		t.Fatalf("binding provider attachment reader = %#v", first.Attachments)
	}
	if lookup.calls != 2 || credentials.calls != 2 {
		t.Fatalf("lookup=%d credentials=%d", lookup.calls, credentials.calls)
	}

	inactive := binding.Clone()
	inactive.Status = channels.StatusSuspended
	lookup.binding = &inactive
	provider.providers = nil
	_, err = provider.provider(context.Background(), value)
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Retryable || deliveryErr.Class != "invalid" {
		t.Fatalf("inactive binding error = %v", err)
	}
}

func TestBindingProviderPreservesRetryableResolutionErrorsAndRotatesSecrets(t *testing.T) {
	binding := dynamicTestBinding(t, "binding-provider-resolution", "env/wecom", "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	value := storage.ReplyOutbox{TenantID: binding.TenantID, ReplyTarget: storage.ReplyTarget{BindingID: binding.BindingID, ConversationKind: "direct", ReceiverID: "user-1"}}

	t.Run("binding cancellation is preserved", func(t *testing.T) {
		provider := &BindingProvider{Bindings: &bindingLookupStub{err: context.Canceled}, Credentials: &credentialResolverStub{}}
		if _, err := provider.provider(context.Background(), value); !errors.Is(err, context.Canceled) {
			t.Fatalf("provider error = %v", err)
		}
	})

	t.Run("credential failures are retryable unless invalid", func(t *testing.T) {
		for _, want := range []struct {
			name  string
			err   error
			class string
			retry bool
		}{
			{name: "canceled", err: context.Canceled},
			{name: "unavailable", err: errors.New("resolver unavailable"), class: "unavailable", retry: true},
			{name: "invalid secret ref", err: channels.ErrNotFound, class: "invalid", retry: false},
		} {
			t.Run(want.name, func(t *testing.T) {
				provider := &BindingProvider{Bindings: &bindingLookupStub{binding: binding}, Credentials: &credentialResolverStub{err: want.err}}
				_, err := provider.provider(context.Background(), value)
				if errors.Is(want.err, context.Canceled) {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("provider error = %v", err)
					}
					return
				}
				var deliveryErr *outbox.DeliveryError
				if !errors.As(err, &deliveryErr) || deliveryErr.Class != want.class || deliveryErr.Retryable != want.retry {
					t.Fatalf("provider error = %v", err)
				}
			})
		}
	})

	t.Run("secret rotation replaces cached provider", func(t *testing.T) {
		provider := &BindingProvider{Bindings: &bindingLookupStub{binding: binding}, Credentials: &sequenceCredentialResolver{values: []Credentials{{AppSecret: "old"}, {AppSecret: "new"}}}}
		first, err := provider.provider(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		second, err := provider.provider(context.Background(), value)
		if err != nil {
			t.Fatal(err)
		}
		if first == second || first.AppSecret != "old" || second.AppSecret != "new" {
			t.Fatalf("provider rotation = first %+v second %+v", first, second)
		}
	})
}

func TestHandlerRejectsBodyLargerThanConfiguredLimit(t *testing.T) {
	handler := &Handler{maxBodyBytes: 3}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("1234"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("response status = %d", response.Code)
	}
}

func TestProviderRejectsInvalidAgentAndMapsTokenFailures(t *testing.T) {
	value := storage.ReplyOutbox{Payload: "hello", ReplyTarget: storage.ReplyTarget{ConversationKind: "direct", ReceiverID: "user"}}
	invalidAgent := &Provider{CorpID: "corp", AgentID: "01", AppSecret: "secret", token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
	_, err := invalidAgent.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)

	for _, test := range []struct {
		name      string
		status    int
		body      string
		class     string
		retryable bool
	}{
		{name: "unavailable", status: http.StatusBadGateway, class: "unavailable", retryable: true},
		{name: "malformed", status: http.StatusOK, body: "bad-json", class: "provider_error", retryable: true},
		{name: "missing token", status: http.StatusOK, body: `{"errcode":0}`, class: "provider_error", retryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/cgi-bin/gettoken" {
					t.Fatalf("path = %s", request.URL.Path)
				}
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body)
			}))
			defer server.Close()
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: server.URL, HTTPClient: server.Client()}
			_, err := provider.accessToken(context.Background())
			assertDeliveryErrorClass(t, err, test.class, test.retryable)
		})
	}

	for _, test := range []struct {
		name  string
		err   error
		class string
	}{
		{name: "deadline", err: context.DeadlineExceeded, class: "timeout"},
		{name: "unavailable", err: errors.New("network unavailable"), class: "unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, test.err })}}
			_, err := provider.accessToken(context.Background())
			assertDeliveryErrorClass(t, err, test.class, true)
		})
	}

	invalidURL := &Provider{CorpID: "corp", AgentID: "1", AppSecret: "secret", BaseURL: "http://[::1", token: "cached", tokenExpiry: time.Now().Add(time.Hour)}
	_, err = invalidURL.Deliver(context.Background(), value)
	assertDeliveryErrorClass(t, err, "invalid", false)
}

func TestHandlerDrainsStreamAndRejectsCryptographicBoundaryFailures(t *testing.T) {
	stream := make(chan gateway.DispatchEvent, 1)
	stream <- gateway.DispatchEvent{}
	close(stream)
	handler := newCallbackTestHandler(t, dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		return stream, nil
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackTestRequest(t, "stream", "user", "hello"))
	if response.Code != http.StatusOK {
		t.Fatalf("stream response = %d", response.Code)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	if err := (*Handler)(nil).Close(); err != nil {
		t.Fatalf("nil close = %v", err)
	}
	if (*Handler)(nil).validSignature("signature", "timestamp", "nonce", "ciphertext") {
		t.Fatal("nil handler accepted a signature")
	}
	if _, err := (*Handler)(nil).decrypt("ciphertext"); !errors.Is(err, ErrVerification) {
		t.Fatalf("nil decrypt error = %v", err)
	}
	if _, err := decrypt(nil, "receive", "not-base64"); !errors.Is(err, ErrVerification) {
		t.Fatalf("invalid ciphertext error = %v", err)
	}
	if _, err := decrypt([]byte{1}, "receive", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, aes.BlockSize))); !errors.Is(err, ErrVerification) {
		t.Fatalf("invalid AES key error = %v", err)
	}
	key := bytes.Repeat([]byte{1}, 32)
	if _, err := decrypt(key, "receive", encryptCallbackTestPayload(t, key, "other", []byte("message"))); !errors.Is(err, ErrVerification) {
		t.Fatalf("receive ID mismatch error = %v", err)
	}
	if _, err := unpad(nil); !errors.Is(err, ErrVerification) {
		t.Fatalf("empty padding error = %v", err)
	}
}

type callbackDispatchStub struct {
	requests chan gateway.DispatchRequest
	canceled chan struct{}
}

type signalingAuditWriter struct {
	audit.Writer
	Signal chan<- struct{}
}

func (writer signalingAuditWriter) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	result, err := writer.Writer.Append(ctx, event)
	if err == nil {
		writer.Signal <- struct{}{}
	}
	return result, err
}

type dispatchFunc func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error)

func (dispatch dispatchFunc) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	return dispatch(ctx, request)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type fakeWeComMediaDownloader struct {
	data    []byte
	err     error
	request MediaDownloadRequest
	mediaID string
}

func (downloader *fakeWeComMediaDownloader) Download(ctx context.Context, request MediaDownloadRequest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	downloader.request = request
	downloader.mediaID = request.MediaID
	if downloader.err != nil {
		return nil, downloader.err
	}
	return io.NopCloser(bytes.NewReader(downloader.data)), nil
}

type wecomMediaDownloaderFunc func(context.Context, MediaDownloadRequest) (io.ReadCloser, error)

func (function wecomMediaDownloaderFunc) Download(ctx context.Context, request MediaDownloadRequest) (io.ReadCloser, error) {
	return function(ctx, request)
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (failingReadCloser) Close() error { return nil }

type wecomAttachmentStoreFunc func(context.Context, string, attachment.Upload, io.Reader) (attachment.Reference, error)

func (function wecomAttachmentStoreFunc) PutAttachment(ctx context.Context, tenantID string, upload attachment.Upload, content io.Reader) (attachment.Reference, error) {
	return function(ctx, tenantID, upload, content)
}

func (wecomAttachmentStoreFunc) Load(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
	return attachment.Content{}, storage.ErrNotFound
}

func (wecomAttachmentStoreFunc) BindAttachments(context.Context, string, string, []attachment.Reference) error {
	return storage.ErrNotFound
}

func (wecomAttachmentStoreFunc) CleanupAttachments(context.Context, string, time.Time) (int, error) {
	return 0, nil
}

func (wecomAttachmentStoreFunc) Close() error { return nil }

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

func assertDeliveryErrorClass(t *testing.T, err error, class string, retryable bool) {
	t.Helper()
	var deliveryErr *outbox.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Class != class || deliveryErr.Retryable != retryable {
		t.Fatalf("delivery error = %v, want class %q retryable %t", err, class, retryable)
	}
}

func wecomReplyReference(t *testing.T, kind attachment.Kind, contentType, name string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-" + string(kind), Kind: kind, MIMEType: contentType, Name: name, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("attachment reference = %v", err)
	}
	return reference
}

func requestBodyWithTrailingXML(t *testing.T, request *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body) + "<trailing/>"
}

func (stub *callbackDispatchStub) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	stub.requests <- request
	if request.Accepted != nil {
		request.Accepted <- struct{}{}
	}
	output := make(chan gateway.DispatchEvent)
	if stub.canceled == nil {
		close(output)
		return output, nil
	}
	go func() {
		<-ctx.Done()
		close(stub.canceled)
		close(output)
	}()
	return output, nil
}

func newCallbackTestHandler(t *testing.T, dispatcher gateway.DispatchService) *Handler {
	t.Helper()
	key := bytes.Repeat([]byte{1}, 32)
	baseCtx, cancel := context.WithCancel(context.Background())
	return &Handler{
		static:           &callbackState{token: "token", receiveID: "receive", agentID: "1", key: key},
		dispatcher:       dispatcher,
		maxBodyBytes:     1 << 20,
		executionTimeout: time.Minute,
		baseCtx:          baseCtx,
		cancel:           cancel,
	}
}

func callbackTestRequest(t *testing.T, messageID, userID, content string) *http.Request {
	return callbackTestRequestAtPath(t, "/", messageID, userID, content)
}

func callbackVerificationRequest(token, path, ciphertext string) *http.Request {
	timestamp, nonce := "123", "456"
	parts := []string{token, timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	query := url.Values{"msg_signature": {hex.EncodeToString(sum[:])}, "timestamp": {timestamp}, "nonce": {nonce}}
	return httptest.NewRequest(http.MethodGet, path+"?"+query.Encode(), nil)
}

func callbackTestRequestAtPath(t *testing.T, path, messageID, userID, content string) *http.Request {
	t.Helper()
	plain := []byte("<xml><MsgId>" + messageID + "</MsgId><FromUserName>" + userID + "</FromUserName><MsgType>text</MsgType><AgentID>1</AgentID><Content>" + content + "</Content></xml>")
	return callbackXMLRequestAtPath(t, path, plain)
}

func callbackXMLRequestAtPath(t *testing.T, path string, plain []byte) *http.Request {
	t.Helper()
	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", plain)
	request := callbackVerificationRequest("token", path, ciphertext)
	request.Method = http.MethodPost
	request.Body = io.NopCloser(strings.NewReader("<xml><Encrypt>" + ciphertext + "</Encrypt></xml>"))
	return request
}

func encryptCallbackTestPayload(t *testing.T, key []byte, receiveID string, message []byte) string {
	t.Helper()
	plain := append(bytes.Repeat([]byte{2}, 16), make([]byte, 4)...)
	binary.BigEndian.PutUint32(plain[16:20], uint32(len(message))) // #nosec G115 -- test payloads are bounded by the callback fixture.
	plain = append(plain, message...)
	plain = append(plain, receiveID...)
	padding := wecomBlockSize - len(plain)%wecomBlockSize
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	return base64.StdEncoding.EncodeToString(encrypted)
}

type bindingLookupStub struct {
	binding *channels.Binding
	calls   int
	err     error
}

func (stub *bindingLookupStub) Get(_ context.Context, _, _ string) (*channels.Binding, error) {
	stub.calls++
	if stub.err != nil {
		return nil, stub.err
	}
	value := stub.binding.Clone()
	return &value, nil
}

type credentialResolverStub struct {
	credentials Credentials
	calls       int
	err         error
}

func (stub *credentialResolverStub) Resolve(_ context.Context, _ channels.SecretScope) (Credentials, error) {
	stub.calls++
	if stub.err != nil {
		return Credentials{}, stub.err
	}
	return stub.credentials, nil
}

type dynamicCandidateConsumer struct{ binding *channels.Binding }

func (stub *dynamicCandidateConsumer) LookupCandidates(_ context.Context, channel channels.Channel, _ string) ([]channels.CandidateBindingContext, error) {
	if channel != channels.ChannelWeCom {
		return nil, errors.New("unexpected channel")
	}
	return []channels.CandidateBindingContext{{Channel: channel}}, nil
}
func (stub *dynamicCandidateConsumer) Get(_ context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if stub.binding == nil || stub.binding.TenantID != tenantID || stub.binding.BindingID != bindingID {
		return nil, channels.ErrNotFound
	}
	value := stub.binding.Clone()
	return &value, nil
}
func (stub *dynamicCandidateConsumer) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	value := stub.binding.Clone()
	return &value, nil
}

type verifyCandidateConsumer struct {
	binding      *channels.Binding
	candidates   []channels.CandidateBindingContext
	lookupErr    error
	consumeErrs  []error
	consumeCalls int
}

func (stub *verifyCandidateConsumer) LookupCandidates(_ context.Context, channel channels.Channel, _ string) ([]channels.CandidateBindingContext, error) {
	if stub.lookupErr != nil {
		return nil, stub.lookupErr
	}
	if channel != channels.ChannelWeCom {
		return nil, errors.New("unexpected channel")
	}
	return append([]channels.CandidateBindingContext(nil), stub.candidates...), nil
}
func (stub *verifyCandidateConsumer) Get(context.Context, string, string) (*channels.Binding, error) {
	return nil, errors.New("unsupported")
}
func (stub *verifyCandidateConsumer) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	index := stub.consumeCalls
	stub.consumeCalls++
	if index < len(stub.consumeErrs) && stub.consumeErrs[index] != nil {
		return nil, stub.consumeErrs[index]
	}
	value := stub.binding.Clone()
	return &value, nil
}

type sequenceCredentialResolver struct {
	values []Credentials
	calls  int
}

func (resolver *sequenceCredentialResolver) Resolve(context.Context, channels.SecretScope) (Credentials, error) {
	if resolver.calls >= len(resolver.values) {
		return Credentials{}, errors.New("unexpected credential resolution")
	}
	value := resolver.values[resolver.calls]
	resolver.calls++
	return value, nil
}

func newDynamicVerifyFixture(t *testing.T) (*Handler, *verifyCandidateConsumer, string, *http.Request) {
	t.Helper()
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "verify-route", "env/wecom", app.AppID)
	consumer := &verifyCandidateConsumer{binding: binding, candidates: []channels.CandidateBindingContext{{Channel: channels.ChannelWeCom}}}
	handler := &Handler{
		candidates:  consumer,
		tenants:     dynamicTenantRepository{value: dynamicTestTenant(t)},
		apps:        dynamicAppRepository{value: app},
		credentials: &sequenceCredentialResolver{values: []Credentials{{CallbackToken: "token", EncodingAESKey: base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))}}},
	}
	ciphertext := encryptCallbackTestPayload(t, bytes.Repeat([]byte{1}, 32), "receive", []byte("dynamic callback"))
	return handler, consumer, ciphertext, callbackVerificationRequest("token", "/wecom/callback/verify-route", ciphertext)
}

func staticTestTarget(t *testing.T) channels.RoutingTarget {
	t.Helper()
	app := dynamicTestApp(t, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	binding := dynamicTestBinding(t, "static-route", "env/wecom", app.AppID)
	target, err := channels.ResolveCandidateRoutingTarget(
		context.Background(),
		&dynamicCandidateConsumer{binding: binding},
		dynamicTenantRepository{value: dynamicTestTenant(t)},
		dynamicAppRepository{value: app},
		channels.CandidateBindingContext{Channel: channels.ChannelWeCom},
		func(context.Context, channels.Binding) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

type dynamicCredentials struct{ values map[string]Credentials }

func (resolver dynamicCredentials) Resolve(_ context.Context, scope channels.SecretScope) (Credentials, error) {
	credentials, ok := resolver.values[scope.SecretRef]
	if !ok {
		return Credentials{}, errors.New("credential not found")
	}
	return credentials, nil
}

type dynamicTenantRepository struct {
	tenant.Repository
	value *tenant.Tenant
}

func (repository dynamicTenantRepository) Get(_ context.Context, tenantID string) (*tenant.Tenant, error) {
	if repository.value == nil || repository.value.TenantID != tenantID {
		return nil, channels.ErrNotFound
	}
	value := repository.value.Clone()
	return &value, nil
}

type dynamicAppRepository struct {
	appmodel.Repository
	value *appmodel.App
}

func (repository dynamicAppRepository) Get(_ context.Context, tenantID, appID string) (*appmodel.App, error) {
	if repository.value == nil || repository.value.TenantID != tenantID || repository.value.AppID != appID {
		return nil, channels.ErrNotFound
	}
	value := repository.value.Clone()
	return &value, nil
}

func dynamicTestBinding(t *testing.T, routeKey, secretRef, appID string) *channels.Binding {
	t.Helper()
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, routeKey)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", BindingKey: "wecom", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp", PublicRouteKeyDigest: digest, AppID: appID, SecretRef: secretRef,
		Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp", AgentID: "1", ReceiveID: "receive"}}, Status: channels.StatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func dynamicTestTenant(t *testing.T) *tenant.Tenant {
	t.Helper()
	value, err := tenant.NewTenant(tenant.CreateInput{TenantKey: "dynamic", DisplayName: "Dynamic Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	value.TenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	return value
}

func dynamicTestApp(t *testing.T, tenantID string) *appmodel.App {
	t.Helper()
	value, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantID, AppKey: "dynamic", DisplayName: "Dynamic App", Description: "callback test"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	value.Status = appmodel.StatusActive
	value.CurrentRevision = &revision
	value.Version = 2
	value.UpdatedAt = value.CreatedAt.Add(time.Second)
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	return value
}
