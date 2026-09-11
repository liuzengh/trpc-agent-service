package postgresadapter

import (
	"context"
	"strings"

	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// AcceptedArtifact reads only the immutable, visible Final. A candidate's mere
// existence, a successful Run with pending/failed Memory, or a different version
// is not attachment authority. The caller cannot supply a tenant or session.
func (l *Ledger) AcceptedArtifact(ctx context.Context, intentID, runID, completionID, name string, version int) (domain.AcceptedArtifact, error) {
	var zero domain.AcceptedArtifact
	if strings.TrimSpace(intentID) == "" || strings.TrimSpace(runID) == "" || strings.TrimSpace(completionID) == "" || version < 0 {
		return zero, domain.ErrInvalid
	}
	// Reuse the owner Final proof's canonical payload/digest validation and gate.
	final, err := l.Final(ctx, intentID)
	if err != nil {
		return zero, err
	}
	if final.IntentID != intentID || final.RunID != runID || final.CompletionID != completionID {
		return zero, domain.ErrNotFound
	}
	// One joined read additionally binds every persisted identity and Memory gate.
	var attempt string
	var generation int64
	r, err := scanRun(l.pool.QueryRow(ctx, `SELECT `+prefixColumns("r", runColumns)+`,c.attempt_id,r.generation FROM execution_runs r JOIN execution_completions c ON c.tenant_id=r.tenant_id AND c.run_id=r.run_id JOIN execution_reply_outbox o ON o.tenant_id=c.tenant_id AND o.run_id=c.run_id AND o.intent_id=c.final_intent_id WHERE o.intent_id=$1 AND r.run_id=$2 AND c.completion_id=$3 AND r.tenant_id=$4 AND r.status='SUCCEEDED' AND c.status='SUCCEEDED' AND c.kind='ATTEMPT' AND c.reply_disposition='FINAL' AND c.memory_status IN ('','APPLIED') AND o.ready AND o.digest=$5 AND o.payload=$6`, intentID, runID, completionID, final.TenantID, final.Digest, final.Payload), &attempt, &generation)
	if err != nil {
		return zero, err
	}
	if r.Request.RunID != runID || r.Request.Route.TenantID != final.TenantID || r.Request.AdmissionID != final.AdmissionID || r.Request.Route.ManifestDigest != final.ManifestDigest || attempt != final.AttemptID || generation != final.ExecutionGeneration || r.CurrentAttemptID != attempt || final.Sequence != 1 {
		return zero, domain.ErrConflict
	}
	event, err := codec.DecodeReplyIntent(final.Payload)
	if err != nil {
		return zero, domain.ErrConflict
	}
	items := make([]domain.Attachment, len(event.Content.Attachments))
	for i, a := range event.Content.Attachments {
		if int64(int(a.Version)) != a.Version || int64(int(a.SizeBytes)) != a.SizeBytes {
			return zero, domain.ErrConflict
		}
		items[i] = domain.Attachment{Name: a.Name, Version: int(a.Version), MimeType: a.MimeType, SizeBytes: int(a.SizeBytes), SHA256: a.Sha256}
	}
	if domain.ValidateAttachments(items) != nil {
		return zero, domain.ErrConflict
	}
	for _, a := range items {
		if a.Name == name && a.Version == version {
			return domain.AcceptedArtifact{Run: r, Final: final, Attachment: a}, nil
		}
	}
	return zero, domain.ErrNotFound
}
