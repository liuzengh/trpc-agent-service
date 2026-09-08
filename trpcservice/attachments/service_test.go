package attachments

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type fakeDownload struct{ data []byte }

func (d fakeDownload) DownloadMedia(context.Context, controlplane.ChannelBinding, channels.MediaReference, int64) ([]byte, error) {
	return append([]byte(nil), d.data...), nil
}
func attachmentFixture(t *testing.T) (*Service, workqueue.AgentTask) {
	t.Helper()
	data := controlplane.DefaultBootstrapData()
	data.ChannelBindings = append(data.ChannelBindings, controlplane.ChannelBinding{ID: "attachment-binding", TenantID: "tutorial-tenant", AppID: "tutorial-app", ChannelType: "telegram", Status: "active", Version: 1, CallbackKey: "attachment-callback", Config: json.RawMessage(`{"attachments_enabled":true}`)})
	repo := controlplane.NewMemoryRepository(data)
	artifacts, _ := storage.NewArtifactRouter(repo, secret.StaticStore{})
	writer := audit.NewMemoryWriter()
	s, err := New(repo, artifacts, secret.StaticStore{}, writer)
	if err != nil {
		t.Fatal(err)
	}
	s.downloader = fakeDownload{data: []byte("untrusted user document")}
	t.Cleanup(func() { _ = artifacts.Close(); _ = repo.Close(); _ = writer.Close() })
	scope, _ := runtimecontext.NewScope("tutorial-tenant", "tutorial-app", "tutorial-revision-1", "telegram", "attachment-binding")
	return s, workqueue.AgentTask{Scope: scope, RequestID: "attachment-request", MessageID: "message", UserID: "user", SessionID: "session", Media: &runtimecontext.MediaReference{FileID: "file", Name: "../../private", BindingVersion: 1}}
}
func TestAttachmentImportIsImmutableAndSessionScoped(t *testing.T) {
	s, task := attachmentFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Import(ctx, task); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	id := attachmentID(task)
	versions, err := s.artifacts.ListVersions(ctx, artifact.SessionInfo{AppName: task.Scope.StorageScope, UserID: task.UserID, SessionID: task.SessionID}, id)
	if err != nil || len(versions) != 1 {
		t.Fatalf("duplicate artifact versions=%v err=%v", versions, err)
	}
	inv := &agentcore.Invocation{Session: session.NewSession(task.Scope.StorageScope, task.UserID, task.SessionID), RunOptions: agentcore.RunOptions{AppName: task.Scope.StorageScope}}
	result, err := s.read(agentcore.NewInvocationContext(ctx, inv), ReadInput{AttachmentID: id})
	if err != nil || result.Text != "untrusted user document" {
		t.Fatalf("read=%+v err=%v", result, err)
	}
	inv.Session = session.NewSession(task.Scope.StorageScope, "other-user", task.SessionID)
	if _, err := s.read(agentcore.NewInvocationContext(ctx, inv), ReadInput{AttachmentID: id}); err == nil {
		t.Fatal("cross-user attachment read")
	}
	inv.Session = session.NewSession(task.Scope.StorageScope, task.UserID, "other-session")
	if _, err := s.read(agentcore.NewInvocationContext(ctx, inv), ReadInput{AttachmentID: id}); err == nil {
		t.Fatal("cross-session attachment read")
	}
}
func TestAttachmentContentAllowlist(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("<html><script>unsafe</script></html>"), []byte("MZ\x00binary"), []byte("EICAR-STANDARD-ANTIVIRUS-TEST-FILE"), bytes.Repeat([]byte("x"), MaxBytes+1)} {
		if _, err := validateContent(data); err == nil {
			t.Fatal("unsafe content accepted")
		}
	}
	var pngData bytes.Buffer
	if err := png.Encode(&pngData, image.NewRGBA(image.Rect(0, 0, 4, 4))); err != nil {
		t.Fatal(err)
	}
	if mime, err := validateContent(pngData.Bytes()); err != nil || mime != "image/png" {
		t.Fatal("valid bounded PNG rejected")
	}
	s, task := attachmentFixture(t)
	s.downloader = fakeDownload{data: []byte("MZ\x00binary")}
	reply, err := s.Import(context.Background(), task)
	if err != nil || !strings.Contains(reply, "未导入") {
		t.Fatal("rejection feedback missing")
	}
	keys, err := s.artifacts.ListArtifactKeys(context.Background(), artifact.SessionInfo{AppName: task.Scope.StorageScope, UserID: task.UserID, SessionID: task.SessionID})
	if err != nil || len(keys) != 0 {
		t.Fatal("rejected file was persisted")
	}
}
