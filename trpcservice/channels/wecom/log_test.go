package wecom

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	attachmentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type failingAuditWriter struct{}

func (failingAuditWriter) Append(context.Context, audit.Event) (audit.AppendResult, error) {
	return audit.AppendResult{}, errors.New("database password=secret")
}

func TestLogIngressAuditFailureIncludesCorrelation(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logIngressAuditFailure("request-1", "trace-1", errors.New("database password=secret"))
	entry := logs.FilterMessage("[wecom] ingress audit failed").All()[0]
	fields := entry.ContextMap()
	if fields["request_id"] != "request-1" || fields["trace_id"] != "trace-1" {
		t.Fatalf("correlation fields = %v", fields)
	}
	if fields["error_type"] != "audit_write_failed" || fields["error_class"] != "error" {
		t.Fatalf("stable error fields = %v", fields)
	}
	if fields["error"] != nil {
		t.Fatalf("unexpected raw error field: %v", fields["error"])
	}
}

func TestLogIngressFailureSkipsExpectedErrorsAndWarnsOnDeadline(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logIngressFailure("ingress failed", "request-1", "trace-1", "internal_error", nil)
	logIngressFailure("ingress failed", "request-1", "trace-1", "internal_error", context.Canceled)
	if logs.Len() != 0 {
		t.Fatalf("expected errors emitted logs: %v", logs.All())
	}

	logIngressFailure("ingress timed out", "request-1", "trace-1", "internal_error", context.DeadlineExceeded)
	entries := logs.FilterMessage("[wecom] ingress timed out").All()
	if len(entries) != 1 || entries[0].Level != zapcore.WarnLevel {
		t.Fatalf("timeout log = %v, want one warn entry", entries)
	}
}

func TestLogIngressBuildFailureClassifiesAttachment(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logIngressBuildFailure("request-1", "trace-1", ErrAttachment)
	entry := logs.FilterMessage("[wecom] ingress message build failed").All()[0]
	if got := entry.ContextMap()["error_type"]; got != ErrAttachment.Error() {
		t.Fatalf("error_type = %v, want %q", got, ErrAttachment.Error())
	}
}

func TestWriteIngressSuccessLogsAuditFailure(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	handler := &Handler{auditWriter: failingAuditWriter{}}
	response := httptest.NewRecorder()
	handler.writeIngressSuccess(response, context.Background(), gateway.Principal{}, inboundXML{FromUserName: "user-1"}, "request-1", "trace-1", audit.EventIMIngressAccepted, audit.DecisionAccepted, "")
	if response.Code != 503 {
		t.Fatalf("response status = %d, want 503", response.Code)
	}
	entry := logs.FilterMessage("[wecom] ingress audit failed").All()[0]
	if got := entry.ContextMap()["error_type"]; got != "audit_write_failed" {
		t.Fatalf("error_type = %v, want audit_write_failed", got)
	}
}

func TestHandleMessageLogsAttachmentBuildFailure(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	baseCtx, cancel := context.WithCancel(context.Background())
	handler := &Handler{
		static:             &callbackState{token: "token", receiveID: "receive", agentID: "1", key: bytes.Repeat([]byte{1}, 32)},
		dispatcher:         dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) { return nil, nil }),
		attachments:        attachmentmemory.New(),
		mediaDownloader:    &fakeWeComMediaDownloader{err: errors.New("provider password=secret")},
		maxBodyBytes:       1 << 20,
		maxAttachmentBytes: defaultAttachmentBytes,
		executionTimeout:   time.Second,
		baseCtx:            baseCtx,
		cancel:             cancel,
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callbackXMLRequestAtPath(t, "/", []byte("<xml><MsgId>message-media</MsgId><FromUserName>user-1</FromUserName><MsgType>image</MsgType><AgentID>1</AgentID><MediaId>media-image</MediaId></xml>")))
	if response.Code != 503 {
		t.Fatalf("response status = %d, want 503", response.Code)
	}
	waitForObservedMessage(t, logs, "[wecom] ingress message build failed")
	entry := logs.FilterMessage("[wecom] ingress message build failed").All()[0]
	if got := entry.ContextMap()["error_type"]; got != ErrAttachment.Error() {
		t.Fatalf("error_type = %v, want %q", got, ErrAttachment.Error())
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForObservedMessage(t *testing.T, logs *observer.ObservedLogs, message string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if logs.FilterMessage(message).Len() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("log message %q was not emitted", message)
}
