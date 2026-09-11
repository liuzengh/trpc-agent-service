package application

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

const maxMemberCandidateQueryLength = 256

func (s *Service) ListMyTenants(ctx context.Context, userID string) ([]domain.TenantMembership, error) {
	return s.deps.Store.ListMyTenants(ctx, userID)
}

func (s *Service) GetTenant(
	ctx context.Context,
	tenantID, userID string,
) (domain.TenantMembership, error) {
	return s.requireMembership(ctx, tenantID, userID)
}

func (s *Service) ListMembers(
	ctx context.Context,
	tenantID, userID string,
) ([]domain.Membership, error) {
	if _, err := s.requireOwner(ctx, tenantID, userID); err != nil {
		return nil, err
	}
	return s.deps.Store.ListMembers(ctx, tenantID)
}

type SearchMemberCandidatesQuery struct {
	TenantID    string
	ActorUserID string
	Query       string
	Page        Page
}

func (s *Service) SearchMemberCandidates(
	ctx context.Context,
	input SearchMemberCandidatesQuery,
) (MemberCandidatePage, error) {
	if _, err := s.requireOwner(ctx, input.TenantID, input.ActorUserID); err != nil {
		return MemberCandidatePage{}, err
	}
	query := strings.TrimSpace(input.Query)
	if query == "" || utf8.RuneCountInString(query) > maxMemberCandidateQueryLength {
		return MemberCandidatePage{}, ErrInvalidCandidateQuery
	}
	query = strings.ToLower(query)
	if s == nil || s.deps.Candidates == nil {
		return MemberCandidatePage{}, fmt.Errorf("search member candidates: incomplete dependencies")
	}
	page := normalizePage(input.Page)
	result, err := s.deps.Candidates.SearchMemberCandidates(ctx, input.TenantID, query, page)
	if err != nil {
		return MemberCandidatePage{}, fmt.Errorf("search member candidates: %w", err)
	}
	result.Offset = page.Offset
	result.Limit = page.Limit
	return result, nil
}

func (s *Service) ListTenants(ctx context.Context, page Page) (TenantPage, error) {
	page = normalizePage(page)
	return s.deps.Store.ListTenants(ctx, page)
}

func normalizePage(page Page) Page {
	if page.Offset < 0 {
		page.Offset = 0
	}
	if page.Limit <= 0 {
		page.Limit = 20
	}
	if page.Limit > 100 {
		page.Limit = 100
	}
	return page
}
