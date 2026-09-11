// Package contentsafety implements the durable, fail-closed policy stage
// used before a model call and before an Outbox/canonical replay is created.
// Only hashes and policy metadata are persisted; request text exists solely
// for the duration of Check.
package contentsafety

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Phase string

const (
	PhaseInput  Phase = "input"
	PhaseOutput Phase = "output"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusAllowed Status = "allowed"
	StatusBlocked Status = "blocked"
	StatusUnknown Status = "unknown"
)

var (
	ErrUnavailable = errors.New("content safety unavailable")
	ErrBlocked     = errors.New("content safety blocked content")
	ErrInvalid     = errors.New("invalid content safety request")
)

type Request struct {
	TenantID       string
	AppName        string
	SessionID      string
	MessageKey     string
	ConfigRevision string
	Phase          Phase
	PolicyVersion  string
	ContentHash    string
	Text           string
}

type Decision struct {
	TenantID      string `json:"tenant_id"`
	MessageKey    string `json:"-"`
	Phase         Phase  `json:"phase"`
	Status        Status `json:"status"`
	PolicyVersion string `json:"policy_version"`
	ContentHash   string `json:"content_hash"`
	DecisionHash  string `json:"decision_hash"`
	ErrorCode     string `json:"error_code,omitempty"`
	Attempts      int    `json:"attempts"`
	EvaluatedAt   time.Time
}

type Checker interface {
	Check(context.Context, Request) (Decision, error)
}

type Evaluator func(Request) (Status, error)

// MemoryChecker is intentionally node-local and only for local/demo/tests.
// It still follows the same fail-closed state machine as the PostgreSQL
// checker so tests exercise production semantics instead of a boolean mock.
type MemoryChecker struct {
	mu        sync.Mutex
	decisions map[string]Decision
	evaluate  Evaluator
}

func NewMemory(evaluate Evaluator) *MemoryChecker {
	if evaluate == nil {
		evaluate = DefaultEvaluator
	}
	return &MemoryChecker{decisions: make(map[string]Decision), evaluate: evaluate}
}

func (m *MemoryChecker) Check(_ context.Context, req Request) (Decision, error) {
	if err := Validate(req); err != nil {
		return Decision{}, err
	}
	key := requestKey(req)
	m.mu.Lock()
	if existing, ok := m.decisions[key]; ok {
		if existing.ContentHash != req.ContentHash {
			m.mu.Unlock()
			return Decision{}, ErrInvalid
		}
		m.mu.Unlock()
		return existing, decisionError(existing)
	}
	status, err := m.evaluate(req)
	if err != nil {
		status = StatusUnknown
	}
	decision := newDecision(req, status, 1)
	if err != nil {
		decision.ErrorCode = observability.ErrorCategory(err)
	}
	m.decisions[key] = decision
	m.mu.Unlock()
	if err != nil {
		return decision, fmt.Errorf("%w: evaluator", ErrUnavailable)
	}
	return decision, decisionError(decision)
}

func (m *MemoryChecker) Decisions() []Decision {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Decision, 0, len(m.decisions))
	for _, decision := range m.decisions {
		out = append(out, decision)
	}
	return out
}

// Postgres persists exactly one decision for a tenant/message/phase/policy
// key. A pending or expired lease is claimable by another node; the row is
// never treated as allowed merely because a prior node disappeared.
type Postgres struct {
	pool     *pgxpool.Pool
	owner    string
	leaseTTL time.Duration
	evaluate Evaluator
}

func NewPostgres(pool *pgxpool.Pool, owner string, leaseTTL time.Duration, evaluate Evaluator) (*Postgres, error) {
	if pool == nil || owner == "" {
		return nil, errors.New("content safety postgres requires pool and owner")
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	if evaluate == nil {
		evaluate = DefaultEvaluator
	}
	return &Postgres{pool: pool, owner: owner, leaseTTL: leaseTTL, evaluate: evaluate}, nil
}

func (p *Postgres) Check(ctx context.Context, req Request) (decision Decision, err error) {
	if err := Validate(req); err != nil {
		return Decision{}, err
	}
	ctx, finish := observability.StartStorage(ctx, "content_safety.check", "postgres", req.TenantID, req.ConfigRevision)
	defer func() { finish(err) }()
	ctx = contextOrBackground(ctx)
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: begin", ErrUnavailable)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existing Decision
	var leaseOwner string
	var leaseExpires *time.Time
	var evaluatedAt *time.Time
	var lastErrorCode string
	var attempts int
	err = tx.QueryRow(ctx, `
		SELECT status, policy_version, content_hash, decision_hash, attempts,
		       evaluated_at, lease_owner, lease_expires_at, last_error_code
		FROM content_safety_decisions
		WHERE tenant_id=$1 AND message_key=$2 AND phase=$3 AND policy_version=$4
		FOR UPDATE SKIP LOCKED`, req.TenantID, req.MessageKey, string(req.Phase), req.PolicyVersion).
		Scan(&existing.Status, &existing.PolicyVersion, &existing.ContentHash, &existing.DecisionHash,
			&attempts, &evaluatedAt, &leaseOwner, &leaseExpires, &lastErrorCode)
	if err == nil {
		if evaluatedAt != nil {
			existing.EvaluatedAt = *evaluatedAt
		}
		existing.TenantID, existing.MessageKey, existing.Phase, existing.Attempts, existing.ErrorCode = req.TenantID, req.MessageKey, req.Phase, attempts, lastErrorCode
		if existing.ContentHash != req.ContentHash {
			return Decision{}, ErrInvalid
		}
		if existing.Status == StatusAllowed || existing.Status == StatusBlocked {
			if err := tx.Commit(ctx); err != nil {
				return Decision{}, fmt.Errorf("%w: commit", ErrUnavailable)
			}
			return existing, decisionError(existing)
		}
		if existing.Status == StatusPending {
			if leaseExpires != nil && leaseExpires.After(time.Now().UTC()) {
				return Decision{}, fmt.Errorf("%w: lease held", ErrUnavailable)
			}
		} else if existing.Status == StatusUnknown && leaseOwner != "" && leaseExpires != nil && leaseExpires.After(time.Now().UTC()) {
			return Decision{}, fmt.Errorf("%w: lease held", ErrUnavailable)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Decision{}, fmt.Errorf("%w: read decision", ErrUnavailable)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content_safety_decisions
		(tenant_id, app_name, session_id, message_key, config_revision, phase,
		 policy_version, content_hash, status, attempts, lease_owner, lease_expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',1,$9,clock_timestamp()+$10::interval)
		ON CONFLICT (tenant_id, message_key, phase, policy_version) DO UPDATE
		SET app_name=EXCLUDED.app_name, session_id=EXCLUDED.session_id,
		    config_revision=EXCLUDED.config_revision, content_hash=EXCLUDED.content_hash,
		    status='pending', attempts=content_safety_decisions.attempts+1,
		    lease_owner=EXCLUDED.lease_owner, lease_expires_at=EXCLUDED.lease_expires_at,
		    updated_at=clock_timestamp()`,
		req.TenantID, req.AppName, req.SessionID, req.MessageKey, req.ConfigRevision,
		string(req.Phase), req.PolicyVersion, req.ContentHash, p.owner, intervalLiteral(p.leaseTTL)); err != nil {
		return Decision{}, fmt.Errorf("%w: claim", ErrUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return Decision{}, fmt.Errorf("%w: claim commit", ErrUnavailable)
	}
	status, evalErr := p.evaluate(req)
	if evalErr != nil {
		status = StatusUnknown
	}
	decision = newDecision(req, status, attempts+1)
	if evalErr != nil {
		decision.ErrorCode = observability.ErrorCategory(evalErr)
	}
	if err := p.finish(ctx, decision, p.owner); err != nil {
		return Decision{}, err
	}
	if evalErr != nil {
		return decision, fmt.Errorf("%w: evaluator", ErrUnavailable)
	}
	return decision, decisionError(decision)
}

func (p *Postgres) finish(ctx context.Context, decision Decision, owner string) error {
	result, err := p.pool.Exec(contextOrBackground(ctx), `
		UPDATE content_safety_decisions
		SET status=$6, decision_hash=$7, evaluated_at=clock_timestamp(),
		    last_error_code=$8,
		    lease_owner='', lease_expires_at=NULL, updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND message_key=$2 AND phase=$3 AND policy_version=$4
		  AND lease_owner=$5`, decision.TenantID, decisionKey(decision), string(decision.Phase), decision.PolicyVersion,
		owner, string(decision.Status), decision.DecisionHash, decision.ErrorCode)
	if err != nil {
		return fmt.Errorf("%w: persist", ErrUnavailable)
	}
	if result.RowsAffected() != 1 {
		// The lease may have expired and another node may have taken over while
		// the evaluator was running. Do not let the stale evaluator authorize a
		// model call or create an Outbox result.
		return fmt.Errorf("%w: lease lost", ErrUnavailable)
	}
	return nil
}

func (p *Postgres) Reclaim(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	result, err := p.pool.Exec(contextOrBackground(ctx), `
		UPDATE content_safety_decisions
		SET status='pending', lease_owner='', lease_expires_at=NULL, updated_at=clock_timestamp()
		WHERE decision_id IN (
			SELECT decision_id FROM content_safety_decisions
			WHERE status='pending' AND lease_expires_at < clock_timestamp()
			ORDER BY updated_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)`, limit)
	if err != nil {
		return 0, fmt.Errorf("%w: reclaim", ErrUnavailable)
	}
	return int(result.RowsAffected()), nil
}

func Validate(req Request) error {
	if req.TenantID == "" || req.AppName == "" || req.SessionID == "" || req.MessageKey == "" ||
		req.PolicyVersion == "" || (req.Phase != PhaseInput && req.Phase != PhaseOutput) || req.ContentHash == "" {
		return ErrInvalid
	}
	if len(req.ContentHash) != 64 {
		return ErrInvalid
	}
	return nil
}

func DefaultEvaluator(req Request) (Status, error) {
	text := strings.ToLower(req.Text)
	for _, blocked := range []string{"content-safety-block", "malware", "credential-exfiltration"} {
		if strings.Contains(text, blocked) {
			return StatusBlocked, nil
		}
	}
	return StatusAllowed, nil
}

// OutputCandidateKey separates regenerated outputs after an uncommitted turn
// fails. An allowed decision for one candidate never authorizes another text
// or configuration revision. Existing message-keyed rows remain immutable.
func OutputCandidateKey(messageKey, revision, text string) string {
	encoded, _ := json.Marshal([]string{"output-candidate-v1", messageKey, revision, HashContent(text)})
	return "output-v1:" + HashContent(string(encoded))
}

func HashContent(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func decisionError(decision Decision) error {
	switch decision.Status {
	case StatusBlocked:
		return ErrBlocked
	case StatusUnknown, StatusPending:
		return ErrUnavailable
	default:
		return nil
	}
}

func newDecision(req Request, status Status, attempts int) Decision {
	payload, _ := json.Marshal([]string{req.TenantID, string(req.Phase), req.PolicyVersion, req.ContentHash, string(status)})
	return Decision{TenantID: req.TenantID, MessageKey: req.MessageKey, Phase: req.Phase, Status: status, PolicyVersion: req.PolicyVersion,
		ContentHash: req.ContentHash, DecisionHash: HashContent(string(payload)), Attempts: attempts, EvaluatedAt: time.Now().UTC()}
}

func requestKey(req Request) string {
	return req.TenantID + "\x1f" + req.MessageKey + "\x1f" + string(req.Phase) + "\x1f" + req.PolicyVersion
}

func decisionKey(decision Decision) string {
	return decision.MessageKey
}

func intervalLiteral(duration time.Duration) string {
	return fmt.Sprintf("%d milliseconds", duration.Milliseconds())
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
