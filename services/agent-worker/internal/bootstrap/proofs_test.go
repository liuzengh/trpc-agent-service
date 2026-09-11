package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type proofLedgerFixture struct {
	grants []domain.Grant
	calls  int
	err    error
	final  domain.Final
}

func (l *proofLedgerFixture) ActiveToken(context.Context, string, string, string, string) (domain.Grant, error) {
	if l.err != nil {
		return domain.Grant{}, l.err
	}
	g := l.grants[l.calls]
	l.calls++
	return g, nil
}
func (l *proofLedgerFixture) Final(context.Context, string) (domain.Final, error) {
	return l.final, l.err
}

type proofPlanFixture struct {
	plan domain.Plan
	err  error
}

func (p proofPlanFixture) Resolve(context.Context, domain.Route) (domain.Plan, error) {
	return p.plan, p.err
}

func TestAttemptProofRechecksGrantAndUsesImmutablePlan(t *testing.T) {
	digest := domain.Digest([]byte("manifest"))
	request := proof.AttemptRequest{WorkloadIdentity: "worker", ExecutionToken: "token", ManifestID: "manifest", ManifestDigest: digest}
	route := domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: digest, DeploymentRevisionID: "revision"}
	first := domain.Grant{Run: domain.Run{Request: domain.Requested{RunID: "run", Route: route}}, AttemptID: "attempt", WorkerID: "worker", Generation: 1, LeaseEpoch: 2, LeaseUntil: time.Now()}
	current := first
	current.LeaseUntil = current.LeaseUntil.Add(time.Minute)
	plan := domain.Plan{TenantID: "tenant", ManifestID: "manifest", ManifestDigest: digest, DeploymentRevisionID: "revision", ProfileID: "profile", ProfileRevision: 3, ModelCredential: domain.CredentialUse{CredentialID: "model", Purpose: "auth", AudienceDigest: domain.Digest([]byte("auth"))}, SessionCredential: domain.CredentialUse{CredentialID: "session", Purpose: "dsn", AudienceDigest: domain.Digest([]byte("dsn"))}}
	ledger := &proofLedgerFixture{grants: []domain.Grant{first, current}}
	q := proofQueries{ledger: ledger, manifests: proofPlanFixture{plan: plan}}
	got, err := q.VerifyAttempt(context.Background(), request)
	if err != nil || ledger.calls != 2 || got.ProfileRevisionNumber != 3 || len(got.AllowedUses) != 2 || !got.ExpiresAt.Equal(current.LeaseUntil) {
		t.Fatalf("proof = %+v, %v", got, err)
	}
	for name, change := range map[string]func(*domain.Grant){"attempt": func(g *domain.Grant) { g.AttemptID = "replacement" }, "epoch": func(g *domain.Grant) { g.LeaseEpoch++ }, "generation": func(g *domain.Grant) { g.Generation++ }} {
		t.Run(name, func(t *testing.T) {
			changed := current
			change(&changed)
			ledger := &proofLedgerFixture{grants: []domain.Grant{first, changed}}
			q := proofQueries{ledger: ledger, manifests: proofPlanFixture{plan: plan}}
			if _, err := q.VerifyAttempt(context.Background(), request); !errors.Is(err, httpadapter.ErrAttemptDenied) {
				t.Fatal("changed grant authorized")
			}
		})
	}
	for name, err := range map[string]error{"unsupported": application.ErrManifestUnsupported, "invalid": application.ErrManifestInvalid, "dependency": errors.New("offline")} {
		t.Run(name, func(t *testing.T) {
			ledger := &proofLedgerFixture{grants: []domain.Grant{first}}
			q := proofQueries{ledger: ledger, manifests: proofPlanFixture{err: err}}
			_, got := q.VerifyAttempt(context.Background(), request)
			want := httpadapter.ErrAttemptDenied
			if name == "dependency" {
				want = httpadapter.ErrUnavailable
			}
			if !errors.Is(got, want) {
				t.Fatalf("error = %v", got)
			}
		})
	}
}
func TestFinalProofRequiresEveryCommittedIdentityField(t *testing.T) {
	f := domain.Final{TenantID: "tenant", IntentID: "intent", Digest: domain.Digest([]byte("intent")), AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ManifestDigest: domain.Digest([]byte("manifest")), ExecutionGeneration: 2, Sequence: 1}
	ledger := &proofLedgerFixture{final: f}
	q := proofQueries{ledger: ledger}
	r := proof.FinalRequest{IntentID: f.IntentID, Digest: f.Digest, AdmissionID: f.AdmissionID, RunID: f.RunID, AttemptID: f.AttemptID, CompletionID: f.CompletionID, ExecutionGeneration: f.ExecutionGeneration, Sequence: f.Sequence}
	if got, err := q.VerifyFinal(context.Background(), r); err != nil || got.TenantID != f.TenantID || got.ManifestDigest != f.ManifestDigest {
		t.Fatalf("final = %+v, %v", got, err)
	}
	for name, change := range map[string]func(*proof.FinalRequest){"intent": func(r *proof.FinalRequest) { r.IntentID += "x" }, "digest": func(r *proof.FinalRequest) { r.Digest += "x" }, "admission": func(r *proof.FinalRequest) { r.AdmissionID += "x" }, "run": func(r *proof.FinalRequest) { r.RunID += "x" }, "attempt": func(r *proof.FinalRequest) { r.AttemptID += "x" }, "completion": func(r *proof.FinalRequest) { r.CompletionID += "x" }, "generation": func(r *proof.FinalRequest) { r.ExecutionGeneration++ }, "sequence": func(r *proof.FinalRequest) { r.Sequence++ }} {
		t.Run(name, func(t *testing.T) {
			copy := r
			change(&copy)
			if _, err := q.VerifyFinal(context.Background(), copy); !errors.Is(err, httpadapter.ErrFinalMismatch) {
				t.Fatal("different identity accepted")
			}
		})
	}
	ledger.err = domain.ErrNotFound
	if _, err := q.VerifyFinal(context.Background(), r); !errors.Is(err, httpadapter.ErrNotYet) {
		t.Fatal("absence classification")
	}
	ledger.err = errors.New("offline")
	if _, err := q.VerifyFinal(context.Background(), r); !errors.Is(err, httpadapter.ErrUnavailable) {
		t.Fatal("dependency classification")
	}
}
