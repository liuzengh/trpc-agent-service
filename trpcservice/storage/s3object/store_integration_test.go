package s3object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func integrationTenant(id string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: id, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "test", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "s3"}}
}

func integrationUpload(id string, body []byte) storage.ObjectUpload {
	digest := sha256.Sum256(body)
	return storage.ObjectUpload{ArtifactID: id, MIMEType: "text/plain", ExpectedSize: int64(len(body)), ExpectedSHA256: hex.EncodeToString(digest[:]), Body: bytes.NewReader(body)}
}

func TestMinioObjectStoreIntegration(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OBJECT_S3_INTEGRATION")) != "1" {
		t.Skip("set OBJECT_S3_INTEGRATION=1 to run the real MinIO gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := testinfra.NewObjectLab(t, "caelumc")
	lab.Start(ctx)
	initialPort := lab.Port(ctx)
	if lab.NetworkAlias() != "minio" || !lab.HostPortPublished() {
		t.Fatalf("object gate failed stage=endpoint_reachability category=unexpected_test_topology")
	}
	store, err := New(Config{Endpoint: lab.Endpoint(ctx), Region: "us-east-1", Bucket: lab.Bucket(), AccessKey: lab.AccessKey(), SecretKey: lab.SecretKey(), UsePathStyle: true, MaxObjectBytes: 1 << 20, PresignMaxTTL: 2 * time.Minute})
	if err != nil {
		t.Fatalf("object gate failed stage=s3_probe category=client_initialization_failed")
	}
	lab.CreateBucket(ctx)
	tc := integrationTenant("tenant-a")
	body := []byte("real MinIO object bytes")
	info, err := store.Put(ctx, tc, integrationUpload("artifact-a", body))
	if err != nil {
		t.Fatalf("object gate failed stage=put category=provider_request_failed")
	}
	if info.ObjectKey != "tenants/tenant-a/artifacts/artifact-a" || info.SizeBytes != int64(len(body)) || info.SHA256 == "" || info.MIMEType != "text/plain" {
		t.Fatalf("object gate failed stage=put category=metadata_mismatch")
	}
	head, err := store.Head(ctx, tc, "artifact-a")
	if err != nil {
		t.Fatalf("object gate failed stage=head category=provider_request_failed")
	}
	if head.SizeBytes != int64(len(body)) || head.SHA256 != info.SHA256 || head.MIMEType != "text/plain" {
		t.Fatalf("object gate failed stage=head category=metadata_mismatch")
	}
	reader, getInfo, err := store.Get(ctx, tc, "artifact-a")
	if err != nil {
		t.Fatalf("object gate failed stage=get category=provider_request_failed")
	}
	got, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || !bytes.Equal(got, body) || getInfo.SizeBytes != int64(len(body)) || getInfo.SHA256 != info.SHA256 || getInfo.MIMEType != "text/plain" {
		t.Fatalf("object gate failed stage=get category=bytes_metadata_mismatch")
	}
	urlValue, err := store.PresignedURL(ctx, tc, "artifact-a", time.Minute)
	if err != nil {
		t.Fatalf("object gate failed stage=presigned_url category=generation_failed")
	}
	response, err := boundedHTTPGet(ctx, urlValue)
	if err != nil {
		t.Fatalf("object gate failed stage=presigned_url category=access_failed")
	}
	urlBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || readErr != nil || !bytes.Equal(urlBody, body) {
		t.Fatalf("object gate failed stage=presigned_url category=actual_access_mismatch")
	}
	invalidURL := strings.Replace(urlValue, "X-Amz-Signature=", "X-Amz-Signature=0", 1)
	invalidResponse, err := boundedHTTPGet(ctx, invalidURL)
	if err != nil {
		t.Fatalf("object gate failed stage=presigned_url category=invalid_access_failed")
	}
	_ = invalidResponse.Body.Close()
	if invalidResponse.StatusCode == http.StatusOK {
		t.Fatalf("object gate failed stage=presigned_url category=invalid_signature_accepted")
	}
	shortURL, err := store.PresignedURL(ctx, tc, "artifact-a", time.Second)
	if err != nil {
		t.Fatalf("object gate failed stage=presigned_url category=expiry_generation_failed")
	}
	expiryTimer := time.NewTimer(1500 * time.Millisecond)
	select {
	case <-expiryTimer.C:
	case <-ctx.Done():
		if !expiryTimer.Stop() {
			<-expiryTimer.C
		}
		t.Fatalf("object gate failed stage=presigned_url category=expiry_wait_timeout")
	}
	expiredResponse, err := boundedHTTPGet(ctx, shortURL)
	if err != nil {
		t.Fatalf("object gate failed stage=presigned_url category=expired_access_failed")
	}
	_ = expiredResponse.Body.Close()
	if expiredResponse.StatusCode == http.StatusOK {
		t.Fatalf("object gate failed stage=presigned_url category=expired_signature_accepted")
	}
	otherTenant := integrationTenant("tenant-b")
	if _, err := store.Head(ctx, otherTenant, "artifact-a"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("object gate failed stage=tenant_isolation category=cross_tenant_access")
	}
	for _, artifactID := range []string{"", "/absolute", "../escape", `..\escape`, "tenants/tenant-a/artifacts/other"} {
		if _, err := store.Put(ctx, tc, integrationUpload(artifactID, body)); !errors.Is(err, storage.ErrObjectInvalid) {
			t.Fatalf("object gate failed stage=key_validation category=unsafe_key_accepted")
		}
	}
	overLimit := integrationUpload("over-limit", body)
	overLimit.ExpectedSize = 1<<20 + 1
	if _, err := store.Put(ctx, tc, overLimit); !errors.Is(err, storage.ErrObjectTooLarge) {
		t.Fatalf("object gate failed stage=put category=size_limit_not_enforced")
	}
	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	if _, err := store.Head(canceled, tc, "artifact-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("object gate failed stage=cancel category=context_not_honored")
	}
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, time.Nanosecond)
	<-deadlineCtx.Done()
	_, err = store.Head(deadlineCtx, tc, "artifact-a")
	deadlineCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("object gate failed stage=timeout category=context_not_honored")
	}
	lab.Stop(ctx)
	failureCtx, failureCancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = store.Head(failureCtx, tc, "artifact-a")
	failureCancel()
	providerFailureAccepted := errors.Is(err, storage.ErrObjectUnavailable) || errors.Is(err, context.DeadlineExceeded)
	if err == nil || !providerFailureAccepted || strings.Contains(err.Error(), lab.AccessKey()) || strings.Contains(err.Error(), lab.SecretKey()) {
		t.Fatalf("object gate failed stage=provider_error category=unsafe_error_or_missing_classification error_class=%s unavailable=%t canceled=%t deadline=%t", objectErrorClass(err), errors.Is(err, storage.ErrObjectUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded))
	}
	lab.Restart(ctx)
	if lab.Port(ctx) != initialPort {
		t.Fatalf("object gate failed stage=reconnect category=host_port_changed")
	}
	if _, err := store.Head(ctx, tc, "artifact-a"); err != nil {
		t.Fatalf("object gate failed stage=reconnect category=head_after_restart_%s", objectErrorClass(err))
	}
	if _, err := store.Put(ctx, tc, integrationUpload("artifact-b", body)); err != nil {
		t.Fatalf("object gate failed stage=reconnect category=put_after_restart_failed")
	}
	if err := store.Delete(ctx, tc, "artifact-a"); err != nil {
		t.Fatalf("object gate failed stage=delete category=first_delete_failed")
	}
	if err := store.Delete(ctx, tc, "artifact-a"); err != nil {
		t.Fatalf("object gate failed stage=delete category=idempotent_delete_failed")
	}
	if _, err := store.Head(ctx, tc, "artifact-a"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("object gate failed stage=delete category=deleted_object_still_visible")
	}
	if err := store.Delete(ctx, tc, "artifact-b"); err != nil {
		t.Fatalf("object gate failed stage=cleanup category=object_delete_failed")
	}
	lab.RemoveBucket(ctx)
	lab.Cleanup()
	if lab.RemainingResources(ctx) {
		t.Fatalf("object gate failed stage=cleanup category=owner_resources_remain")
	}
}

func boundedHTTPGet(ctx context.Context, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	return (&http.Client{Timeout: 5 * time.Second}).Do(req)
}

func objectErrorClass(err error) string {
	switch {
	case err == nil:
		return "nil"
	case errors.Is(err, storage.ErrObjectNotFound):
		return "not_found"
	case errors.Is(err, storage.ErrObjectUnavailable):
		return "unavailable"
	case errors.Is(err, storage.ErrObjectUnauthorized):
		return "unauthorized"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "other"
	}
}
