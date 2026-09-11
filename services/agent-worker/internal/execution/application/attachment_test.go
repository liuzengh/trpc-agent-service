package application

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type attachmentRuntime struct {
	observationRuntime
	result     domain.RuntimeResult
	executeErr error
	staged     int
}

func (r *attachmentRuntime) Prepare(context.Context, domain.Grant, domain.Plan, func(context.Context) error) (AttemptRuntime, error) {
	return r, nil
}
func (r *attachmentRuntime) Execute(context.Context, []byte) (domain.RuntimeResult, error) {
	return r.result, r.executeErr
}
func (r *attachmentRuntime) Stage(context.Context, []byte) (domain.Candidate, error) {
	r.staged++
	return domain.Candidate{}, nil
}

type attachmentLedger struct {
	observationLedger
	finish   domain.Finish
	fenceErr error
}

func (l *attachmentLedger) Claim(context.Context, domain.ClaimRequest) (domain.Grant, error) {
	return domain.Grant{Run: l.run, AttemptID: "attempt", WorkerID: "worker", Generation: 1, LeaseEpoch: 1}, nil
}
func (l *attachmentLedger) Complete(_ context.Context, f domain.Finish) (domain.Completion, error) {
	l.completed++
	l.finish = f
	return domain.Completion{}, nil
}
func (l *attachmentLedger) Check(context.Context, domain.Grant) error { return l.fenceErr }

func TestProcessorFinalAttachmentAcceptance(t *testing.T) {
	for _, mode := range []string{"success", "invalid", "oversized", "execute_failure", "fenced"} {
		t.Run(mode, func(t *testing.T) {
			deadline := time.Now().Add(time.Minute)
			run := domain.Run{Request: domain.Requested{RunID: "run", AdmissionID: "admission", Route: domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: "digest", DeploymentRevisionID: "revision"}}, ExecutionDeadline: &deadline, ReplyDeadline: deadline, Policy: domain.Policy{RenewalInterval: time.Hour}}
			ledger := &attachmentLedger{observationLedger: observationLedger{run: run}}
			runtime := &attachmentRuntime{result: domain.RuntimeResult{FinalText: "final", Snapshot: []byte("snapshot"), Attachments: []domain.Attachment{{Name: "report.txt", Version: 2, MimeType: "text/plain", SizeBytes: 3, SHA256: strings.Repeat("a", 64)}}}}
			if mode == "invalid" {
				runtime.result.Attachments[0].Name = "../secret"
			}
			if mode == "oversized" {
				sample := runtime.result.Attachments[0]
				runtime.result.Attachments = make([]domain.Attachment, 5000)
				for i := range runtime.result.Attachments {
					a := sample
					a.Name = fmt.Sprintf("file-%04d-%s.txt", i, strings.Repeat("x", 100))
					runtime.result.Attachments[i] = a
				}
			}
			if mode == "execute_failure" {
				runtime.executeErr = ErrRuntimeFailed
			}
			if mode == "fenced" {
				ledger.fenceErr = domain.ErrFenced
			}
			p, e := NewProcessor(ledger, observationManifest{domain.Plan{TenantID: "tenant", ManifestID: "manifest", ManifestDigest: "digest", DeploymentRevisionID: "revision"}}, runtime, "worker", 1)
			if e != nil {
				t.Fatal(e)
			}
			e = p.Advance(context.Background(), run)
			if mode == "success" {
				if e != nil || ledger.completed != 1 || runtime.staged != 1 || !reflect.DeepEqual(ledger.finish.Attachments, runtime.result.Attachments) {
					t.Fatal(e, ledger, runtime)
				}
				runtime.result.Attachments[0].Version = 9
				if ledger.finish.Attachments[0].Version != 2 {
					t.Fatal("Finish aliases runtime slice")
				}
			} else {
				want := ErrRuntimeFailed
				if mode == "fenced" {
					want = domain.ErrFenced
				}
				if !errors.Is(e, want) || ledger.completed != 0 || runtime.staged != 0 {
					t.Fatal(e, ledger, runtime)
				}
			}
		})
	}
}
