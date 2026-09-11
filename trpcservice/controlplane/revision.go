package controlplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// App is one row of agent_apps: an identity plus the one mutable thing it
// owns, a pointer at its currently published revision.
type App struct {
	ID                int64
	TenantID          string
	PublicID          string
	Name              string
	CurrentRevisionID sql.NullInt64
}

// RevisionSpec is everything a publisher supplies for a new immutable
// revision. It intentionally has no id or timestamp: those belong to the row
// once it exists, and letting a caller set them would let one pretend to
// "re-publish" a revision it did not just define.
type RevisionSpec struct {
	Instruction      string
	ModelProfileID   int64
	BackendProfileID int64
	MaxLLMCalls      int
	MessageTimeoutMS int
	Guardrails       json.RawMessage
	Tools            json.RawMessage
	KnowledgeBases   json.RawMessage
}

// Revision is a published, immutable agent definition.
type Revision struct {
	ID           int64
	TenantID     string
	AppID        int64
	RevisionNo   uint32
	Spec         RevisionSpec
	ManifestHash string
}

// ErrConcurrentPublish means the app's published pointer moved between the
// caller reading it and trying to change it. The correct response is to
// re-read and decide again, never to force the write through.
var ErrConcurrentPublish = errors.New("controlplane: the published revision changed before this update could land")

// CreateApp registers an app with no published revision yet. A missing
// tenant row fails on the foreign key, which is the point: this cannot create
// an orphan.
func (s Scope) CreateApp(ctx context.Context, publicID, name string) (int64, error) {
	if publicID == "" {
		return 0, fmt.Errorf("controlplane: app public id is required")
	}
	res, err := s.Exec(ctx,
		"INSERT INTO agent_apps (tenant_id, public_id, name) VALUES (?, ?, ?)",
		s.tenantID, publicID, name)
	if err != nil {
		return 0, fmt.Errorf("controlplane: create app %q: %w", publicID, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("controlplane: create app id: %w", err)
	}
	return id, nil
}

// GetApp reads an app by its tenant-visible public id.
func (s Scope) GetApp(ctx context.Context, publicID string) (App, error) {
	row, err := s.QueryRow(ctx, `
		SELECT app_id, tenant_id, public_id, name, current_revision_id
		FROM agent_apps WHERE tenant_id = ? AND public_id = ?`, s.tenantID, publicID)
	if err != nil {
		return App{}, err
	}
	var a App
	switch err := row.Scan(&a.ID, &a.TenantID, &a.PublicID, &a.Name, &a.CurrentRevisionID); {
	case errors.Is(err, sql.ErrNoRows):
		return App{}, ErrNotFound
	case err != nil:
		return App{}, fmt.Errorf("controlplane: get app: %w", err)
	}
	return a, nil
}

// ListApps returns every app in the scope, published or not.
func (s Scope) ListApps(ctx context.Context) ([]App, error) {
	rows, err := s.Query(ctx, `
		SELECT app_id, tenant_id, public_id, name, current_revision_id
		FROM agent_apps WHERE tenant_id = ? ORDER BY public_id`, s.tenantID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list apps: %w", err)
	}
	defer rows.Close()
	var out []App
	for rows.Next() {
		var a App
		if err := rows.Scan(&a.ID, &a.TenantID, &a.PublicID, &a.Name, &a.CurrentRevisionID); err != nil {
			return nil, fmt.Errorf("controlplane: scan app: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// PublishRevision inserts a new immutable revision and moves the app's
// pointer to it, in one transaction.
//
// The transaction locks the app row (SELECT ... FOR UPDATE) before reading
// its current pointer, so two publishers of the same app serialise on that
// row instead of both computing "next revision = n+1" from the same snapshot
// and racing to write it. The revision number is only unique because of that
// lock; the unique index on (app_id, revision_no) is the backstop that makes
// a bug here loud instead of silent.
func (s Scope) PublishRevision(ctx context.Context, appPublicID string, spec RevisionSpec) (*Revision, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	var out *Revision
	err := s.WithTx(ctx, func(tx *TxScope) error {
		var appID int64
		err := tx.QueryRow(ctx,
			"SELECT app_id FROM agent_apps WHERE tenant_id = ? AND public_id = ? FOR UPDATE",
			s.tenantID, appPublicID).Scan(&appID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("controlplane: lock app: %w", err)
		}
		if err := tx.checkProfileIDs(ctx, s.tenantID, spec); err != nil {
			return err
		}
		// Publishing is the last moment a broken reference is cheap to
		// refuse: the pins are validated against this app's own active rows
		// here, and the knowledge bindings are recorded for retrieval — see
		// pinrefs.go for why this lives in the transaction that creates the
		// revision.
		if err := tx.checkToolPins(ctx, s.tenantID, appID, spec.Tools); err != nil {
			return err
		}

		var nextNo uint32
		if err := tx.QueryRow(ctx,
			"SELECT COALESCE(MAX(revision_no), 0) + 1 FROM agent_revisions WHERE tenant_id = ? AND app_id = ?",
			s.tenantID, appID).Scan(&nextNo); err != nil {
			return fmt.Errorf("controlplane: next revision number: %w", err)
		}

		manifest, err := json.Marshal(spec)
		if err != nil {
			return fmt.Errorf("controlplane: manifest: %w", err)
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(manifest))

		res, err := tx.Exec(ctx, `
			INSERT INTO agent_revisions
				(tenant_id, app_id, revision_no, instruction, model_profile_id, backend_profile_id,
				 max_llm_calls, message_timeout_ms, guardrails, tools, knowledge_bases, manifest_hash)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			s.tenantID, appID, nextNo, spec.Instruction, spec.ModelProfileID, spec.BackendProfileID,
			spec.MaxLLMCalls, spec.MessageTimeoutMS,
			nullableJSON(spec.Guardrails), nullableJSON(spec.Tools), nullableJSON(spec.KnowledgeBases),
			hash)
		if err != nil {
			return fmt.Errorf("controlplane: insert revision: %w", err)
		}
		revID, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("controlplane: revision id: %w", err)
		}

		if err := tx.movePointer(ctx, s.tenantID, appID, revID); err != nil {
			return err
		}
		if err := tx.resolveKnowledgeBindings(ctx, s.tenantID, appID, revID, spec.KnowledgeBases); err != nil {
			return err
		}
		out = &Revision{
			ID: revID, TenantID: s.tenantID, AppID: appID,
			RevisionNo: nextNo, Spec: spec, ManifestHash: hash,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RollbackToRevision moves an app's pointer to a revision that already
// exists, without touching its content. expectCurrent is the revision id the
// caller believes is live right now; if it is not, nothing changes and the
// call fails with ErrConcurrentPublish rather than silently overwriting a
// newer decision.
func (s Scope) RollbackToRevision(ctx context.Context, appPublicID string, targetRevisionID, expectCurrent int64) error {
	return s.WithTx(ctx, func(tx *TxScope) error {
		var appID int64
		var current sql.NullInt64
		err := tx.QueryRow(ctx,
			"SELECT app_id, current_revision_id FROM agent_apps WHERE tenant_id = ? AND public_id = ? FOR UPDATE",
			s.tenantID, appPublicID).Scan(&appID, &current)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("controlplane: lock app: %w", err)
		}
		if current.Int64 != expectCurrent {
			return ErrConcurrentPublish
		}

		var ownerApp int64
		err = tx.QueryRow(ctx,
			"SELECT app_id FROM agent_revisions WHERE tenant_id = ? AND revision_id = ?",
			s.tenantID, targetRevisionID).Scan(&ownerApp)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("controlplane: load target revision: %w", err)
		}
		if ownerApp != appID {
			// Same tenant, different app: the composite foreign key from
			// agent_apps cannot catch this because both sides are the
			// tenant's own rows, so the check belongs here.
			return fmt.Errorf("%w: revision %d belongs to app %d, not %d",
				ErrCrossTenantReference, targetRevisionID, ownerApp, appID)
		}
		return tx.movePointer(ctx, s.tenantID, appID, targetRevisionID)
	})
}

func (t *TxScope) movePointer(ctx context.Context, tenantID string, appID, revisionID int64) error {
	if _, err := t.Exec(ctx,
		"UPDATE agent_apps SET current_revision_id = ? WHERE tenant_id = ? AND app_id = ?",
		revisionID, tenantID, appID); err != nil {
		return fmt.Errorf("controlplane: move published pointer: %w", err)
	}
	return nil
}

// CurrentRevision loads the revision an app is publishing right now.
func (s Scope) CurrentRevision(ctx context.Context, appPublicID string) (*Revision, error) {
	var r Revision
	var spec RevisionSpec
	var guardrails, tools, knowledgeBases jsonCol
	row, err := s.QueryRow(ctx, `
		SELECT ar.revision_id, ar.tenant_id, ar.app_id, ar.revision_no, ar.manifest_hash,
		       ar.instruction, ar.model_profile_id, ar.backend_profile_id,
		       ar.max_llm_calls, ar.message_timeout_ms, ar.guardrails, ar.tools, ar.knowledge_bases
		FROM agent_apps aa
		JOIN agent_revisions ar ON ar.revision_id = aa.current_revision_id AND ar.app_id = aa.app_id
		WHERE aa.tenant_id = ? AND aa.public_id = ?`, s.tenantID, appPublicID)
	if err != nil {
		return nil, err
	}
	switch err := row.Scan(&r.ID, &r.TenantID, &r.AppID, &r.RevisionNo, &r.ManifestHash,
		&spec.Instruction, &spec.ModelProfileID, &spec.BackendProfileID,
		&spec.MaxLLMCalls, &spec.MessageTimeoutMS,
		&guardrails, &tools, &knowledgeBases); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("controlplane: current revision: %w", err)
	}
	spec.Guardrails, spec.Tools, spec.KnowledgeBases = guardrails.RawMessage, tools.RawMessage, knowledgeBases.RawMessage
	r.Spec = spec
	return &r, nil
}

// GetRevisionByID loads one exact revision, without going through an app's
// current pointer. A session fixed to a revision needs this: the app may have
// published or rolled back several times since, and "what does this session's
// fixed version actually say" cannot go through "whatever is current now".
func (s Scope) GetRevisionByID(ctx context.Context, revisionID int64) (*Revision, error) {
	var r Revision
	var spec RevisionSpec
	var guardrails, tools, knowledgeBases jsonCol
	row, err := s.QueryRow(ctx, `
		SELECT revision_id, tenant_id, app_id, revision_no, manifest_hash,
		       instruction, model_profile_id, backend_profile_id,
		       max_llm_calls, message_timeout_ms, guardrails, tools, knowledge_bases
		FROM agent_revisions WHERE tenant_id = ? AND revision_id = ?`, s.tenantID, revisionID)
	if err != nil {
		return nil, err
	}
	switch err := row.Scan(&r.ID, &r.TenantID, &r.AppID, &r.RevisionNo, &r.ManifestHash,
		&spec.Instruction, &spec.ModelProfileID, &spec.BackendProfileID,
		&spec.MaxLLMCalls, &spec.MessageTimeoutMS,
		&guardrails, &tools, &knowledgeBases); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("controlplane: get revision by id: %w", err)
	}
	spec.Guardrails, spec.Tools, spec.KnowledgeBases = guardrails.RawMessage, tools.RawMessage, knowledgeBases.RawMessage
	r.Spec = spec
	return &r, nil
}

func (spec RevisionSpec) validate() error {
	if spec.Instruction == "" {
		return errors.New("controlplane: a revision needs an instruction")
	}
	if spec.ModelProfileID == 0 || spec.BackendProfileID == 0 {
		return errors.New("controlplane: a revision needs a model profile and a backend profile")
	}
	return nil
}

// checkProfileIDs proves the referenced profiles are this tenant's own rows.
// The foreign keys in agent_revisions do not (yet) reach across to
// model_profiles/backend_profiles with a tenant composite, so this closes the
// gap at the point of use rather than trusting the publisher's input.
func (t *TxScope) checkProfileIDs(ctx context.Context, tenantID string, spec RevisionSpec) error {
	for _, p := range []struct {
		kind string
		id   int64
		tbl  string
	}{
		{"model", spec.ModelProfileID, "model_profiles"},
		{"backend", spec.BackendProfileID, "backend_profiles"},
	} {
		var id int64
		// The table name is a constant chosen above, never caller input; it
		// cannot be a placeholder because it is an identifier.
		q := "SELECT profile_id FROM " + p.tbl + " WHERE tenant_id = ? AND profile_id = ?"
		err := t.QueryRow(ctx, q, tenantID, p.id).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: no %s profile %d in this tenant", ErrCrossTenantReference, p.kind, p.id)
		}
		if err != nil {
			return fmt.Errorf("controlplane: check %s profile: %w", p.kind, err)
		}
	}
	return nil
}

// nullableJSON turns an absent JSON field into SQL NULL rather than an empty
// string, so a column keeps its "no policy here" meaning instead of
// pretending "{}" was what the publisher wrote.
func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

// jsonCol reads a JSON column that may be SQL NULL. database/sql will not
// scan NULL into a *json.RawMessage — a named slice type is not one of the
// pointer types it treats as nullable — and that failure was measured, not
// anticipated: it is what a publisher with no guardrails sets first.
type jsonCol struct {
	json.RawMessage
}

func (j *jsonCol) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		j.RawMessage = nil
		return nil
	case []byte:
		j.RawMessage = append(json.RawMessage(nil), v...)
		return nil
	case string:
		j.RawMessage = json.RawMessage(v)
		return nil
	}
	return fmt.Errorf("controlplane: cannot scan %T into a JSON column", src)
}
