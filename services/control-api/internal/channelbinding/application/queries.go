package application

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type Page struct {
	After string
	Limit int
}

func NormalizePage(page Page) (Page, error) {
	if page.After != "" && !domain.ValidID(page.After) {
		return Page{}, invalid("/cursor")
	}
	if page.Limit == 0 {
		page.Limit = 50
	}
	if page.Limit < 1 || page.Limit > 100 {
		return Page{}, invalid("/page_size")
	}
	return page, nil
}

type AccountDetails struct {
	Account            AccountView       `json:"account"`
	Binding            *domain.Binding   `json:"binding,omitempty"`
	RouteGeneration    int64             `json:"route_generation"`
	EventID            string            `json:"route_event_id,omitempty"`
	Distribution       string            `json:"distribution"`
	GatewayApplication string            `json:"gateway_application"`
	Observations       []ObservationView `json:"observations"`
}
type BindingDetails struct {
	Binding            domain.Binding `json:"binding"`
	RouteGeneration    int64          `json:"route_generation"`
	EventID            string         `json:"route_event_id,omitempty"`
	Distribution       string         `json:"distribution"`
	GatewayApplication string         `json:"gateway_application"`
}
type AccountPage struct {
	Accounts   []AccountView `json:"accounts"`
	NextCursor string        `json:"next_cursor,omitempty"`
}
type BindingPage struct {
	Bindings   []domain.Binding `json:"bindings"`
	NextCursor string           `json:"next_cursor,omitempty"`
}
type ReadStore interface {
	ReadAccount(context.Context, string, string) (AccountDetails, error)
	ReadBinding(context.Context, string, string) (BindingDetails, error)
	ListAccounts(context.Context, string, Page) (AccountPage, error)
	ListBindings(context.Context, string, Page) (BindingPage, error)
}
type QueryService struct {
	store  ReadStore
	access TenantAccess
}

func NewQueryService(store ReadStore, access TenantAccess) (*QueryService, error) {
	if store == nil || access == nil {
		return nil, ErrDependencyUnavailable
	}
	return &QueryService{store, access}, nil
}
func (s *QueryService) authorize(ctx context.Context, actor Actor) error {
	if !domain.ValidID(actor.TenantID) || !domain.ValidID(actor.UserID) {
		return ErrPermissionDenied
	}
	ok, err := s.access.IsActiveMember(ctx, actor.TenantID, actor.UserID)
	if err != nil {
		return ErrDependencyUnavailable
	}
	if !ok {
		return ErrPermissionDenied
	}
	return nil
}
func (s *QueryService) GetAccount(ctx context.Context, actor Actor, id string) (AccountDetails, error) {
	if err := s.authorize(ctx, actor); err != nil {
		return AccountDetails{}, err
	}
	if !domain.ValidID(id) {
		return AccountDetails{}, invalid("/account_id")
	}
	return s.store.ReadAccount(ctx, actor.TenantID, id)
}
func (s *QueryService) GetBinding(ctx context.Context, actor Actor, id string) (BindingDetails, error) {
	if err := s.authorize(ctx, actor); err != nil {
		return BindingDetails{}, err
	}
	if !domain.ValidID(id) {
		return BindingDetails{}, invalid("/binding_id")
	}
	return s.store.ReadBinding(ctx, actor.TenantID, id)
}
func (s *QueryService) ListAccounts(ctx context.Context, actor Actor, page Page) (AccountPage, error) {
	if err := s.authorize(ctx, actor); err != nil {
		return AccountPage{}, err
	}
	page, err := NormalizePage(page)
	if err != nil {
		return AccountPage{}, err
	}
	return s.store.ListAccounts(ctx, actor.TenantID, page)
}
func (s *QueryService) ListBindings(ctx context.Context, actor Actor, page Page) (BindingPage, error) {
	if err := s.authorize(ctx, actor); err != nil {
		return BindingPage{}, err
	}
	page, err := NormalizePage(page)
	if err != nil {
		return BindingPage{}, err
	}
	return s.store.ListBindings(ctx, actor.TenantID, page)
}
