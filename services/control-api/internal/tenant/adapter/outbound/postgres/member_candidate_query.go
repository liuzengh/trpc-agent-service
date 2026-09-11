package postgresadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

// MemberCandidateReader implements the Tenant-owned candidate read port. It is
// deliberately read-only and projects only the public Identity fields needed
// to choose a new member.
type MemberCandidateReader struct {
	db DB
}

func NewMemberCandidateReader(db DB) *MemberCandidateReader {
	return &MemberCandidateReader{db: db}
}

type memberCandidateRecord struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
}

func (r *MemberCandidateReader) SearchMemberCandidates(
	ctx context.Context,
	tenantID, queryText string,
	page application.Page,
) (application.MemberCandidatePage, error) {
	const query = `
		WITH matching AS (
			SELECT
				a.id AS user_id,
				a.username,
				a.normalized_username,
				a.display_name,
				CASE
					WHEN a.normalized_username = $2 THEN 0
					WHEN strpos(a.normalized_username, $2) = 1 THEN 1
					WHEN strpos(a.normalized_username, $2) > 0 THEN 2
					WHEN strpos(lower(a.display_name), $2) = 1 THEN 3
					ELSE 4
				END AS match_rank
			FROM user_accounts AS a
			WHERE a.status = 'ACTIVE'
				AND (
					strpos(a.normalized_username, $2) > 0
					OR strpos(lower(a.display_name), $2) > 0
				)
				AND NOT EXISTS (
					SELECT 1
					FROM tenant_memberships AS m
					WHERE m.tenant_id = $1 AND m.user_id = a.id
				)
		), candidate_page AS (
			SELECT user_id, username, normalized_username, display_name, match_rank
			FROM matching
			ORDER BY match_rank, normalized_username, user_id
			LIMIT $3 OFFSET $4
		)
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'user_id', p.user_id,
						'username', p.username,
						'display_name', p.display_name
					) ORDER BY p.match_rank, p.normalized_username, p.user_id
				)
				FROM candidate_page AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM matching)
	`
	var encoded []byte
	var total int
	if err := r.db.QueryRow(
		ctx, query, tenantID, queryText, page.Limit, page.Offset,
	).Scan(&encoded, &total); err != nil {
		return application.MemberCandidatePage{}, fmt.Errorf("query member candidates: %w", err)
	}
	var records []memberCandidateRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.MemberCandidatePage{}, fmt.Errorf("decode member candidates: %w", err)
	}
	candidates := make([]application.MemberCandidate, 0, len(records))
	for _, record := range records {
		candidates = append(candidates, application.MemberCandidate{
			UserID: record.UserID, Username: record.Username, DisplayName: record.DisplayName,
		})
	}
	return application.MemberCandidatePage{
		Candidates: candidates, Offset: page.Offset, Limit: page.Limit, Total: total,
	}, nil
}

var _ application.MemberCandidateQuery = (*MemberCandidateReader)(nil)
