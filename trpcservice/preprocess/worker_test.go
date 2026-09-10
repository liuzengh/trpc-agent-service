package preprocess_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocessmemory "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

func TestWorkerDoesNotDispatchOpaqueProviderMediaReference(t *testing.T) {
	clock := time.Now().UTC().Truncate(time.Microsecond)
	store := preprocessmemory.New()
	payloads := messagingmemory.New()
	inbox, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("media-message"))
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal(preprocess.NormalizedInput{ExternalMessageID: "media-message", ExternalUserID: "external-user",
		ChannelBindingID: "binding", ExternalAccountID: "account", ConfigVersion: 1, MessageType: "image",
		MediaRefs: []channel.MediaRef{{ID: "provider-image-key", MessageID: "provider-message", Kind: "image"}}})
	sum := sha256.Sum256(content)
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: inbox.TenantID, RequestID: inbox.RequestID,
		PayloadRef: inbox.PayloadRef, ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingDispatcher{}
	worker := preprocess.Worker{Store: store, Payloads: payloads, Dispatcher: dispatcher, Owner: "worker",
		RetryDelay: time.Second, Now: func() time.Time { return clock }}
	if processed, err := worker.RunOnce(context.Background(), 10); err != nil || processed != 1 || len(dispatcher.requests) != 0 {
		t.Fatalf("processed=%d requests=%d err=%v", processed, len(dispatcher.requests), err)
	}
	if jobs, err := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "inspect", Now: clock.Add(500 * time.Millisecond), TTL: time.Second, Limit: 1}); err != nil || len(jobs) != 0 {
		t.Fatalf("job became runnable before retry delay: jobs=%#v err=%v", jobs, err)
	}
	jobs, err := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "inspect", Now: clock.Add(2 * time.Second), TTL: time.Second, Limit: 1})
	if err != nil || len(jobs) != 1 || jobs[0].RejectReason != "media_prepared_payload_unavailable" {
		t.Fatalf("recoverable media job=%#v err=%v", jobs, err)
	}
}

func TestWorkerStagesMediaAndDispatchesPreparedPayload(t *testing.T) {
	clock := time.Now().UTC().Truncate(time.Microsecond)
	store := preprocessmemory.New()
	payloads := messagingmemory.New()
	inbox, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("media-ready"))
	if err != nil {
		t.Fatal(err)
	}
	contentBytes := []byte("\x89PNG\r\n\x1a\n")
	content, _ := json.Marshal(preprocess.NormalizedInput{ExternalMessageID: "media-ready", ExternalUserID: "external-user",
		ChannelBindingID: "binding", ExternalAccountID: "account", ConfigVersion: 1, MessageType: "image",
		MediaRefs: []channel.MediaRef{{ID: "provider-image-key", MessageID: "provider-message", Kind: "image", ContentType: "image/png", Size: int64(len(contentBytes))}}})
	sum := sha256.Sum256(content)
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: inbox.TenantID, RequestID: inbox.RequestID,
		PayloadRef: inbox.PayloadRef, ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 7}); err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingDispatcher{}
	worker := preprocess.Worker{Store: store, Payloads: payloads, Dispatcher: dispatcher, Owner: "worker", Now: func() time.Time { return clock },
		Media:             &preprocess.MediaStager{Fetcher: mediaBytesFetcher{content: contentBytes}, Malware: scanner{}, DLP: scanner{}, Artifacts: artifactmemory.New()},
		ArtifactRetention: 30 * 24 * time.Hour}
	if processed, err := worker.RunOnce(context.Background(), 10); err != nil || processed != 1 || len(dispatcher.requests) != 1 {
		t.Fatalf("processed=%d requests=%d err=%v", processed, len(dispatcher.requests), err)
	}
	if !strings.HasPrefix(dispatcher.requests[0].PayloadRef, "prepared://") || dispatcher.requests[0].PayloadRef == inbox.PayloadRef {
		t.Fatalf("dispatch payload ref=%q raw=%q", dispatcher.requests[0].PayloadRef, inbox.PayloadRef)
	}
	prepared, err := payloads.GetPreparedPayload(context.Background(), inbox.TenantID, inbox.RequestID, dispatcher.requests[0].PayloadRef)
	if err != nil || prepared.KeyVersion != 7 || bytes.Contains(prepared.Content, []byte("provider-image-key")) || !bytes.Contains(prepared.Content, []byte("artifact://")) {
		t.Fatalf("prepared key version=%d content_len=%d err=%v", prepared.KeyVersion, len(prepared.Content), err)
	}
}

func TestWorkerTransitionsReadyAndRecoversDispatch(t *testing.T) {
	clock := time.Now().UTC().Truncate(time.Microsecond)
	store := preprocessmemory.New()
	payloads := messagingmemory.New()
	inbox, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("message-1"))
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal(preprocess.NormalizedText{ExternalMessageID: "message-1", ExternalUserID: "external-user", Text: "hello"})
	sum := sha256.Sum256(content)
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: inbox.TenantID, RequestID: inbox.RequestID,
		PayloadRef: inbox.PayloadRef, ContentDigest: hex.EncodeToString(sum[:]), Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingDispatcher{fail: true}
	worker := preprocess.Worker{Store: store, Payloads: payloads, Dispatcher: dispatcher, Owner: "worker", Now: func() time.Time { return clock }}
	if _, err := worker.RunOnce(context.Background(), 10); err == nil {
		t.Fatal("expected first dispatch failure")
	}
	ready, err := store.ListReadyForDispatch(context.Background(), 10)
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready=%#v err=%v", ready, err)
	}
	dispatcher.fail = false
	clock = clock.Add(31 * time.Second)
	if processed, err := worker.RunOnce(context.Background(), 10); err != nil || processed != 0 || len(dispatcher.requests) != 2 {
		t.Fatalf("processed=%d requests=%d err=%v", processed, len(dispatcher.requests), err)
	}
	ready, _ = store.ListReadyForDispatch(context.Background(), 10)
	if len(ready) != 0 {
		t.Fatalf("dispatched job remained ready=%#v", ready)
	}
}

func TestExpiredLeaseIsReclaimedAndCASProtected(t *testing.T) {
	store := preprocessmemory.New()
	if _, _, err := store.ClaimInboxAndSchedule(context.Background(), claim("message-2")); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first, _ := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "one", Now: now, TTL: time.Second, Limit: 1})
	second, _ := store.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "two", Now: now.Add(2 * time.Second), TTL: time.Second, Limit: 1})
	if len(first) != 1 || len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	if _, err := store.FinishReady(context.Background(), first[0]); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("stale lease finish=%v", err)
	}
	if _, err := store.FinishReady(context.Background(), second[0]); err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateClaimIgnoresTraceContextChanges(t *testing.T) {
	store := preprocessmemory.New()
	original := claim("message-trace-retry")
	original.TraceParent = "first-trace"
	inbox, job, err := store.ClaimInboxAndSchedule(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	retry := original
	retry.TraceParent = "retry-trace"
	retriedInbox, retriedJob, err := store.ClaimInboxAndSchedule(context.Background(), retry)
	if err != nil || retriedInbox.RequestID != inbox.RequestID || retriedJob.JobID != job.JobID || retriedJob.TraceParent != "first-trace" {
		t.Fatalf("retried inbox=%#v job=%#v err=%v", retriedInbox, retriedJob, err)
	}
}

func claim(messageID string) preprocess.ClaimRequest {
	key := messaging.InboxKey{TenantID: "tenant", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: messageID}
	return preprocess.ClaimRequest{Inbox: messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: "app", SessionID: "session",
		ExternalUserID: "external-user", PayloadDigest: string(make([]byte, 64)), KeyVersion: 1,
		InitialState: messaging.InboxPreprocessPending}, TenantVersion: 1, ConfigVersion: 1,
		ChannelBindingID: "binding", UserID: "user", TraceParent: "trace"}
}

type recordingDispatcher struct {
	fail     bool
	requests []gateway.DispatchRequest
}

type mediaBytesFetcher struct{ content []byte }

func (f mediaBytesFetcher) Fetch(context.Context, preprocess.MediaFetchRequest) (preprocess.MediaDownload, error) {
	return preprocess.MediaDownload{Body: io.NopCloser(bytes.NewReader(f.content)), ContentType: "image/png", DeclaredSize: int64(len(f.content))}, nil
}

type scanner struct{}

func (scanner) ScanMedia(context.Context, []byte, string) (preprocess.ScanResult, error) {
	return preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: "scan-1"}, nil
}

func (scanner) ScanMediaInput(context.Context, string, []byte, string) (preprocess.ScanResult, error) {
	return preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: "scan-1"}, nil
}

func (d *recordingDispatcher) Dispatch(_ context.Context, request gateway.DispatchRequest) (gateway.ExecutionHandle, error) {
	d.requests = append(d.requests, request)
	if d.fail {
		return gateway.ExecutionHandle{}, errors.New("broker unavailable")
	}
	return gateway.ExecutionHandle{RequestID: request.RequestID, Status: "accepted"}, nil
}
