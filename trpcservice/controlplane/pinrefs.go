package controlplane

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Publish-time validation of what a revision pins (approved plan: "发布前
// 验证归属、引用及可装配性").
//
// The shapes parsed here are the same JSON the execution side reads
// (trpcservice/tool.BuildPinned, knowledge bindings). They are redeclared
// rather than imported because the dependency direction is fixed:
// controlplane is the bottom of the stack, and a bottom layer that imports
// its callers is how a codebase acquires its first cycle.
type pinnedToolRef struct {
	Name    string `json:"name"`
	Version uint32 `json:"version"`
}

type pinnedKBRef struct {
	PublicID string `json:"public_id"`
}

// checkToolPins proves every pinned tool is an active binding of *this* app
// at exactly the pinned version. A revision that names a tool the app does
// not own would otherwise be discovered at execution time — after a user has
// already been told their message was accepted.
func (t *TxScope) checkToolPins(ctx context.Context, tenantID string, appID int64, raw json.RawMessage) error {
	pins, err := parsePins[pinnedToolRef](raw, "tools")
	if err != nil {
		return err
	}
	for _, p := range pins {
		if p.Name == "" || p.Version == 0 {
			return fmt.Errorf("controlplane: tool pin %+v needs a name and a version", p)
		}
		var toolID int64
		row := t.QueryRow(ctx, `
			SELECT tool_id FROM tool_bindings
			WHERE tenant_id = ? AND app_id = ? AND name = ? AND version = ? AND status = 'active'`,
			tenantID, appID, p.Name, p.Version)
		switch err := row.Scan(&toolID); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: tool %s@%d is not an active binding of this app",
				ErrCrossTenantReference, p.Name, p.Version)
		case err != nil:
			return fmt.Errorf("controlplane: check tool pin: %w", err)
		}
	}
	return nil
}

// resolveKnowledgeBindings validates the pinned knowledge bases and records
// them in knowledge_bindings — the table retrieval reads to know which KBs a
// session's fixed revision may search. Recording them at publish time is
// what makes a later kb disable a per-search SQL check instead of a
// re-publication.
func (t *TxScope) resolveKnowledgeBindings(ctx context.Context, tenantID string, appID, revisionID int64, raw json.RawMessage) error {
	pins, err := parsePins[pinnedKBRef](raw, "knowledge_bases")
	if err != nil {
		return err
	}
	for _, p := range pins {
		if p.PublicID == "" {
			return errors.New("controlplane: a knowledge pin has an empty public_id")
		}
		var kbID int64
		row := t.QueryRow(ctx, `
			SELECT kb_id FROM knowledge_bases
			WHERE tenant_id = ? AND app_id = ? AND public_id = ? AND status = 'active'`,
			tenantID, appID, p.PublicID)
		switch err := row.Scan(&kbID); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: knowledge base %q is not an active base of this app",
				ErrCrossTenantReference, p.PublicID)
		case err != nil:
			return fmt.Errorf("controlplane: check knowledge pin: %w", err)
		}
		if _, err := t.Exec(ctx, `
			INSERT INTO knowledge_bindings (tenant_id, revision_id, kb_id)
			VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE kb_id = kb_id`,
			tenantID, revisionID, kbID); err != nil {
			return fmt.Errorf("controlplane: bind knowledge base: %w", err)
		}
	}
	return nil
}

// KnowledgeBindingIDs lists the kb ids a revision is bound to, in stable
// order. The execution side calls it once per claim: the list is small, and
// re-reading it per tool call would not make the binding any truer.
func (s Scope) KnowledgeBindingIDs(ctx context.Context, revisionID int64) ([]int64, error) {
	rows, err := s.Query(ctx, `
		SELECT kb_id FROM knowledge_bindings
		WHERE tenant_id = ? AND revision_id = ?
		ORDER BY kb_id`, s.tenantID, revisionID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: list knowledge bindings: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("controlplane: scan knowledge binding: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// parsePins decodes the {"pinned":[...]} envelope shared by tools and
// knowledge bases. Unknown fields are refused: a revision is a manifest, and
// a field the platform silently ignores is a field its author believed was
// in effect.
func parsePins[T any](raw json.RawMessage, what string) ([]T, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var envelope struct {
		Pinned []T `json:"pinned"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("controlplane: %s pins: %w", what, err)
	}
	return envelope.Pinned, nil
}
