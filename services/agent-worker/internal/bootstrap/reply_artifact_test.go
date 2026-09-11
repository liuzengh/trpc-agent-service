package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type replyLedgerFixture struct {
	value   domain.AcceptedArtifact
	err     error
	request proof.ReplyArtifactRequest
	calls   int
}

func (s *replyLedgerFixture) AcceptedArtifact(_ context.Context, intent, run, completion, name string, version int) (domain.AcceptedArtifact, error) {
	s.calls++
	s.request = proof.ReplyArtifactRequest{IntentID: intent, RunID: run, CompletionID: completion, Name: name, Version: version}
	return s.value, s.err
}

type replyPlanFixture struct {
	plan  domain.Plan
	err   error
	route domain.Route
}

func (s *replyPlanFixture) Resolve(_ context.Context, r domain.Route) (domain.Plan, error) {
	s.route = r
	return s.plan, s.err
}

type replyCredentialsFixture struct {
	calls   int
	err     error
	request proof.ReplyArtifactRequest
	run     domain.Run
	plan    domain.Plan
}

func (s *replyCredentialsFixture) ResolveReplyArtifact(_ context.Context, r proof.ReplyArtifactRequest, accepted domain.AcceptedArtifact, p domain.Plan) (artifactstore.Credentials, error) {
	s.calls++
	s.request = r
	s.run = accepted.Run
	s.plan = p
	return artifactstore.Credentials{AccessKeyID: "private-access-key", SecretAccessKey: "private-secret-key"}, s.err
}

type replyStoreFixture struct {
	calls, closed int
	scope         artifact.SessionInfo
	name          string
	version       int
	value         *artifact.Artifact
	err           error
}

func (s *replyStoreFixture) LoadArtifact(_ context.Context, scope artifact.SessionInfo, name string, version *int) (*artifact.Artifact, error) {
	s.calls++
	s.scope = scope
	s.name = name
	if version == nil {
		panic("latest version forbidden")
	}
	s.version = *version
	return s.value, s.err
}
func (s *replyStoreFixture) Close() { s.closed++ }
func replyQueryFixture(t *testing.T) (replyArtifactQueries, proof.ReplyArtifactRequest, *replyLedgerFixture, *replyPlanFixture, *replyCredentialsFixture, *replyStoreFixture) {
	t.Helper()
	r := proof.ReplyArtifactRequest{IntentID: "intent", RunID: "run", CompletionID: "completion", Name: "report.txt", Version: 0}
	route := domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), DeploymentRevisionID: "revision"}
	run := domain.Run{Status: domain.Succeeded, Request: domain.Requested{RunID: r.RunID, Route: route}, SessionID: "accepted-session"}
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "artifact", BackendRevision: 1, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 2, MaxBytes: 4096}, S3: &datav1.S3Target{Endpoint: "http://127.0.0.1:9000", Bucket: "artifact-fixture", Region: "us-east-1", PathStyle: true, Versioning: "disabled"}}
	digest, e := b.Digest()
	if e != nil {
		t.Fatal(e)
	}
	p := domain.Plan{TenantID: route.TenantID, ManifestID: route.ManifestRef, ManifestDigest: route.ManifestDigest, DeploymentRevisionID: route.DeploymentRevisionID, ProfileID: "profile", ProfileRevision: 3, Artifact: &domain.ArtifactPlan{Backend: b, AccessKeyID: domain.CredentialUse{CredentialID: "access", Purpose: "access_key_id", AudienceDigest: digest}, SecretAccessKey: domain.CredentialUse{CredentialID: "secret", Purpose: "secret_access_key", AudienceDigest: digest}}}
	v := &artifact.Artifact{Data: []byte("accepted artifact\n中\x00"), MimeType: "application/octet-stream", Name: r.Name}
	meta := trpcagent.ArtifactMetadata(trpcagent.ArtifactSessionInfo(route.TenantID, run.SessionID), r.Name, r.Version, v)
	l := &replyLedgerFixture{value: domain.AcceptedArtifact{Run: run, Attachment: domain.Attachment{Name: r.Name, Version: 0, MimeType: meta.MimeType, SizeBytes: meta.SizeBytes, SHA256: meta.SHA256}}}
	m := &replyPlanFixture{plan: p}
	c := &replyCredentialsFixture{}
	s := &replyStoreFixture{value: v}
	q := replyArtifactQueries{ledger: l, manifests: m, credentials: c, open: func(_ context.Context, _ *pgxpool.Pool, got datav1.Snapshot, secrets artifactstore.Credentials, tenant string, scope artifact.SessionInfo) (replyArtifactStore, error) {
		if got.TenantID != route.TenantID || tenant != route.TenantID || scope != trpcagent.ArtifactSessionInfo(route.TenantID, run.SessionID) || secrets.AccessKeyID != "private-access-key" || secrets.SecretAccessKey != "private-secret-key" {
			t.Fatal("storage scope not from accepted run")
		}
		return s, nil
	}}
	return q, r, l, m, c, s
}
func TestReplyArtifactUsesAcceptedFinalScopeAndExactVersionZero(t *testing.T) {
	q, r, l, m, c, s := replyQueryFixture(t)
	want := bytes.Clone(s.value.Data)
	out, err := q.ReplyArtifact(context.Background(), r)
	if err != nil || !bytes.Equal(out.Content, want) || out.MimeType != l.value.Attachment.MimeType || out.SHA256 != l.value.Attachment.SHA256 || out.SizeBytes != len(want) {
		t.Fatal(out, err)
	}
	if l.request != r || m.route != l.value.Run.Request.Route || c.request != r || c.run.SessionID != l.value.Run.SessionID || c.plan.ProfileRevision != 3 || c.calls != 1 || s.calls != 1 || s.closed != 1 || s.version != 0 || s.name != r.Name {
		t.Fatal("accepted identity/version/resource lifecycle mismatch")
	}
}
func TestReplyArtifactRejectsBeforeCredentialResolution(t *testing.T) {
	for _, mode := range []string{"ledger missing", "ledger pending", "ledger conflict", "ledger dependency", "run not succeeded", "run id", "missing session", "attachment name", "attachment version", "tenant", "manifest", "digest", "revision", "artifact disabled", "backend tenant", "credential audience", "credential purpose", "capacity", "manifest dependency"} {
		t.Run(mode, func(t *testing.T) {
			q, r, l, m, c, s := replyQueryFixture(t)
			switch mode {
			case "ledger missing":
				l.err = domain.ErrNotFound
			case "ledger pending":
				l.err = domain.ErrNotReady
			case "ledger conflict":
				l.err = domain.ErrConflict
			case "ledger dependency":
				l.err = errors.New("postgres unavailable")
			case "run not succeeded":
				l.value.Run.Status = domain.Running
			case "run id":
				l.value.Run.Request.RunID = "other"
			case "missing session":
				l.value.Run.SessionID = ""
			case "attachment name":
				l.value.Attachment.Name = "other.txt"
			case "attachment version":
				l.value.Attachment.Version = 1
			case "tenant":
				m.plan.TenantID = "other"
			case "manifest":
				m.plan.ManifestID = "other"
			case "digest":
				m.plan.ManifestDigest = domain.Digest([]byte("other"))
			case "revision":
				m.plan.DeploymentRevisionID = "other"
			case "artifact disabled":
				m.plan.Artifact = nil
			case "backend tenant":
				m.plan.Artifact.Backend.TenantID = "other"
			case "credential audience":
				m.plan.Artifact.AccessKeyID.AudienceDigest = domain.Digest([]byte("other"))
			case "credential purpose":
				m.plan.Artifact.SecretAccessKey.Purpose = "api_key"
			case "capacity":
				l.value.Attachment.SizeBytes = 5000
			case "manifest dependency":
				m.err = errors.New("projection unavailable")
			}
			out, err := q.ReplyArtifact(context.Background(), r)
			if err == nil || len(out.Content) > 0 || c.calls != 0 || s.calls != 0 {
				t.Fatal("unauthorized downstream activity", err, c.calls, s.calls)
			}
		})
	}
}
func TestReplyArtifactReadIntegrityAndDependencyFailuresReturnNoBytes(t *testing.T) {
	for _, mode := range []string{"credential", "open", "load", "missing", "body", "mime", "size", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			q, r, _, _, c, s := replyQueryFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := 1
			switch mode {
			case "credential":
				c.err = errors.New("private credential diagnostic")
				closed = 0
			case "open":
				q.open = func(context.Context, *pgxpool.Pool, datav1.Snapshot, artifactstore.Credentials, string, artifact.SessionInfo) (replyArtifactStore, error) {
					return nil, errors.New("S3 unavailable")
				}
				closed = 0
			case "load":
				s.err = errors.New("read interrupted")
			case "missing":
				s.value = nil
			case "body":
				s.value.Data = []byte("corrupt")
			case "mime":
				s.value.MimeType = "text/plain"
			case "size":
				s.value.Data = append(s.value.Data, 0)
			case "cancel":
				cancel()
			}
			out, err := q.ReplyArtifact(ctx, r)
			if err == nil || len(out.Content) != 0 || s.closed != closed {
				t.Fatal("failed read response", err, s.closed)
			}
			if mode == "missing" && !errors.Is(err, httpadapter.ErrArtifactMissing) {
				t.Fatal(err)
			}
			if s.calls > 0 && s.value != nil && !bytes.Equal(s.value.Data, make([]byte, len(s.value.Data))) {
				t.Fatal("failure buffer retained")
			}
		})
	}
}
