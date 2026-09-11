// Package summary keeps a session's rolling summary, in the background.
//
// The rules the approved plan pins down are all CAS-shaped:
//
//   - a summary covers a prefix of the event log (summary_covered_seq), and
//     the tail beyond it is never touched — history stays authoritative;
//   - the write is conditional on the summary_version that was read, so a
//     job that raced a newer summary (or a second worker) loses instead of
//     overwriting it;
//   - generation and covered sequence only move forward.
package summary

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/jobs"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

// KindSessionSummary is the job kind; Threshold is how many uncovered events
// make one due.
const (
	KindSessionSummary = "session_summary"
	Threshold          = 12
	// maxEventsPerRun bounds one summarization; a long backlog is summarized
	// in several runs, each covering the prefix it read.
	maxEventsPerRun = 200
)

// Service generates summaries with the tenant's own model profile.
type Service struct {
	db       *controlplane.DB
	resolver *secrets.Resolver
}

// New wires the service.
func New(db *controlplane.DB, resolver *secrets.Resolver) (*Service, error) {
	if db == nil || resolver == nil {
		return nil, errors.New("summary: db and secret resolver are required")
	}
	return &Service{db: db, resolver: resolver}, nil
}

// EnqueueIfDue is called inside the commit transaction: it counts the
// uncovered events and enqueues one summary job when the threshold is met.
// The idempotency key carries the summary version, so the same backlog
// cannot enqueue twice while a job for it is already pending.
func EnqueueIfDue(ctx context.Context, tx *controlplane.TxScope, tenantID string, sessionPK int64, summaryVersion uint32, coveredSeq uint32) error {
	var uncovered int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM session_events
		WHERE tenant_id = ? AND session_pk = ? AND seq > ?`,
		tenantID, sessionPK, coveredSeq).Scan(&uncovered); err != nil {
		return fmt.Errorf("summary: count uncovered events: %w", err)
	}
	if uncovered < Threshold {
		return nil
	}
	return jobs.Enqueue(ctx, controlplane.Scope{}, tx, tenantID, KindSessionSummary,
		fmt.Sprintf("summary:%d:v%d", sessionPK, summaryVersion),
		map[string]any{"session_pk": sessionPK})
}

// Summarize is the job body. It reads the session and its uncovered events,
// asks the tenant's model for a summary, and applies it with a CAS on
// summary_version.
func (s *Service) Summarize(ctx context.Context, tenantID string, sessionPK int64) error {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return err
	}
	var (
		existing   sql.NullString
		coveredSeq uint32
		summaryVer uint32
		revisionID int64
	)
	row, err := scope.QueryRow(ctx, `
		SELECT summary, summary_covered_seq, summary_version, revision_id
		FROM sessions WHERE tenant_id = ? AND session_pk = ?`, tenantID, sessionPK)
	if err != nil {
		return err
	}
	switch err := row.Scan(&existing, &coveredSeq, &summaryVer, &revisionID); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("summary: load session: %w", err)
	}

	events, maxSeq, err := s.loadUncovered(ctx, scope, tenantID, sessionPK, coveredSeq)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	// The model profile the session was fixed to, resolved the same way the
	// worker resolves one: a reference through the allowlist, never a
	// plaintext key in a row.
	modelName, baseURL, apiKey, err := s.modelFor(ctx, scope, tenantID, revisionID)
	if err != nil {
		return err
	}
	llm := openai.New(modelName, openai.WithAPIKey(apiKey), openai.WithBaseURL(baseURL))
	prompt := buildPrompt(existing.String, events)
	req := &model.Request{
		Messages:         []model.Message{model.NewUserMessage(prompt)},
		GenerationConfig: model.GenerationConfig{Stream: false},
	}
	text, err := drain(ctx, llm, req)
	if err != nil {
		return fmt.Errorf("summary: model call: %w", err)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("summary: the model returned an empty summary")
	}

	// The CAS: version must still be what was read, and the covered sequence
	// may only move forward.
	res, err := scope.Exec(ctx, `
		UPDATE sessions SET summary = ?, summary_covered_seq = ?, summary_version = ?
		WHERE tenant_id = ? AND session_pk = ? AND summary_version = ? AND summary_covered_seq <= ?`,
		text, maxSeq, summaryVer+1, tenantID, sessionPK, summaryVer, maxSeq)
	if err != nil {
		return fmt.Errorf("summary: apply: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// A newer summary (or another writer) won; discarding this one is
		// the whole point of the guarded update.
		return nil
	}
	return nil
}

type eventLine struct {
	seq     uint32
	author  string
	payload string
}

func (s *Service) loadUncovered(ctx context.Context, scope controlplane.Scope, tenantID string, sessionPK int64, coveredSeq uint32) ([]eventLine, uint32, error) {
	rows, err := scope.Query(ctx, `
		SELECT seq, author, payload FROM session_events
		WHERE tenant_id = ? AND session_pk = ? AND seq > ?
		ORDER BY seq LIMIT ?`, tenantID, sessionPK, coveredSeq, maxEventsPerRun)
	if err != nil {
		return nil, 0, fmt.Errorf("summary: load events: %w", err)
	}
	defer rows.Close()
	var (
		out    []eventLine
		maxSeq uint32
	)
	for rows.Next() {
		var e eventLine
		if err := rows.Scan(&e.seq, &e.author, &e.payload); err != nil {
			return nil, 0, fmt.Errorf("summary: scan event: %w", err)
		}
		// The payload is the framework event JSON; extracting the text is
		// best-effort — an event with no readable text still advances the
		// covered sequence, because refusing to cover it would strand the
		// summary forever on one odd event.
		e.payload = extractText(e.payload)
		out = append(out, e)
		if e.seq > maxSeq {
			maxSeq = e.seq
		}
	}
	return out, maxSeq, rows.Err()
}

func (s *Service) modelFor(ctx context.Context, scope controlplane.Scope, tenantID string, revisionID int64) (modelName, baseURL, apiKey string, err error) {
	var apiKeyRef string
	row, err := scope.QueryRow(ctx, `
		SELECT mp.model_name, mp.base_url, mp.api_key_ref
		FROM agent_revisions ar
		JOIN model_profiles mp ON mp.profile_id = ar.model_profile_id AND mp.tenant_id = ar.tenant_id
		WHERE ar.tenant_id = ? AND ar.revision_id = ?`, tenantID, revisionID)
	if err != nil {
		return "", "", "", err
	}
	switch err := row.Scan(&modelName, &baseURL, &apiKeyRef); {
	case errors.Is(err, sql.ErrNoRows):
		return "", "", "", fmt.Errorf("summary: revision %d has no model profile", revisionID)
	case err != nil:
		return "", "", "", fmt.Errorf("summary: load model profile: %w", err)
	}
	apiKey, err = s.resolver.Resolve(apiKeyRef)
	if err != nil {
		return "", "", "", fmt.Errorf("summary: resolve api key ref: %w", err)
	}
	return modelName, baseURL, apiKey, nil
}

// HandleJob adapts Summarize to the jobs runner.
func (s *Service) HandleJob(ctx context.Context, tenantID, kind string, payload []byte) error {
	if kind != KindSessionSummary {
		return fmt.Errorf("summary: unknown job kind %q", kind)
	}
	var p struct {
		SessionPK int64 `json:"session_pk"`
	}
	if err := jsonUnmarshal(payload, &p); err != nil {
		return err
	}
	return s.Summarize(ctx, tenantID, p.SessionPK)
}
