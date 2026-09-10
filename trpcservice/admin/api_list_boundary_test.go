package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

type adminTenantListStub struct {
	tenant.Repository
	err  error
	next string
}

func (s adminTenantListStub) List(context.Context, []string, string, string, string, int) ([]*tenant.Tenant, string, error) {
	return nil, s.next, s.err
}

type adminAppListStub struct {
	appmodel.Repository
	err  error
	next string
}

func (s adminAppListStub) List(context.Context, string, string, string, string, int) ([]*appmodel.App, string, error) {
	return nil, s.next, s.err
}

func (s adminAppListStub) ListRevisions(context.Context, string, string, string, string, string, int) ([]*appmodel.Revision, string, error) {
	return nil, s.next, s.err
}

type adminModelListStub struct {
	modelprofile.Repository
	err  error
	next string
}

func (s adminModelListStub) List(context.Context, string, string, string, string, int) ([]*modelprofile.Profile, string, error) {
	return nil, s.next, s.err
}

type adminBackendListStub struct {
	backend.Repository
	err  error
	next string
}

func (s adminBackendListStub) List(context.Context, string, string, string, string, int) ([]*backend.Profile, string, error) {
	return nil, s.next, s.err
}

type adminBindingListStub struct {
	channels.Repository
	err  error
	next string
}

func (s adminBindingListStub) List(context.Context, string, string, string, string, int) ([]*channels.Binding, string, error) {
	return nil, s.next, s.err
}

func TestAdminRouteFallbacksAndListDependencyErrors(t *testing.T) {
	handler, auth := testHandler(t)
	principal := Principal{SubjectID: "admin", Global: true}
	ctx := context.Background()

	requests := []struct {
		name   string
		path   string
		method string
	}{
		{name: "empty admin path", path: "/admin/v1", method: http.MethodGet},
		{name: "me wrong method", path: "/admin/v1/me", method: http.MethodPost},
		{name: "unknown resource", path: "/admin/v1/unknown", method: http.MethodGet},
	}
	for _, tc := range requests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, nil)
			request.Header.Set("Authorization", "Bearer admin-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}

	for _, tc := range []struct {
		name    string
		call    func(*Handler, *http.Request) error
		install func(*Handler, error, string)
	}{
		{name: "tenants unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.tenants(ctx, r, principal)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Tenants = noTenantListRepository{Repository: h.config.Tenants}
		}},
		{name: "apps unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.apps(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Apps = noAppListRepository{Repository: h.config.Apps}
		}},
		{name: "revisions unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.revisions(ctx, r, principal, "tenant", "app", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Apps = noRevisionListRepository{Repository: h.config.Apps}
		}},
		{name: "models unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.models(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Models = noModelListRepository{Repository: h.config.Models}
		}},
		{name: "backends unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.backends(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Backends = noBackendListRepository{Repository: h.config.Backends}
		}},
		{name: "bindings unsupported", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.bindings(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Bindings = noBindingListRepository{Repository: h.config.Bindings}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := testHandler(t)
			tc.install(h, errListUnsupported, "")
			err := tc.call(h, httptest.NewRequest(http.MethodGet, "/admin/v1?limit=1", nil))
			if !errors.Is(err, errListUnsupported) {
				t.Fatalf("error = %v, want %v", err, errListUnsupported)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		call    func(*Handler, *http.Request) error
		install func(*Handler, error, string)
	}{
		{name: "tenants list error", call: func(h *Handler, r *http.Request) error { _, _, err := h.tenants(ctx, r, principal); return err }, install: func(h *Handler, err error, next string) {
			h.config.Tenants = adminTenantListStub{Repository: h.config.Tenants, err: err}
		}},
		{name: "apps list error", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.apps(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Apps = adminAppListStub{Repository: h.config.Apps, err: err}
		}},
		{name: "revisions list error", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.revisions(ctx, r, principal, "tenant", "app", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Apps = adminAppListStub{Repository: h.config.Apps, err: err}
		}},
		{name: "models list error", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.models(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Models = adminModelListStub{Repository: h.config.Models, err: err}
		}},
		{name: "backends list error", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.backends(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Backends = adminBackendListStub{Repository: h.config.Backends, err: err}
		}},
		{name: "bindings list error", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.bindings(ctx, r, principal, "tenant", nil)
			return err
		}, install: func(h *Handler, err error, next string) {
			h.config.Bindings = adminBindingListStub{Repository: h.config.Bindings, err: err}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := testHandler(t)
			want := errors.New("list failed")
			tc.install(h, want, "")
			err := tc.call(h, httptest.NewRequest(http.MethodGet, "/admin/v1?limit=1", nil))
			if !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		call    func(*Handler, *http.Request) (int, any, error)
		install func(*Handler, string)
	}{
		{name: "tenants invalid next", call: func(h *Handler, r *http.Request) (int, any, error) { return h.tenants(ctx, r, principal) }, install: func(h *Handler, next string) {
			h.config.Tenants = adminTenantListStub{Repository: h.config.Tenants, next: next}
		}},
		{name: "apps invalid next", call: func(h *Handler, r *http.Request) (int, any, error) { return h.apps(ctx, r, principal, "tenant", nil) }, install: func(h *Handler, next string) { h.config.Apps = adminAppListStub{Repository: h.config.Apps, next: next} }},
		{name: "revisions invalid next", call: func(h *Handler, r *http.Request) (int, any, error) {
			return h.revisions(ctx, r, principal, "tenant", "app", nil)
		}, install: func(h *Handler, next string) { h.config.Apps = adminAppListStub{Repository: h.config.Apps, next: next} }},
		{name: "models invalid next", call: func(h *Handler, r *http.Request) (int, any, error) { return h.models(ctx, r, principal, "tenant", nil) }, install: func(h *Handler, next string) {
			h.config.Models = adminModelListStub{Repository: h.config.Models, next: next}
		}},
		{name: "backends invalid next", call: func(h *Handler, r *http.Request) (int, any, error) {
			return h.backends(ctx, r, principal, "tenant", nil)
		}, install: func(h *Handler, next string) {
			h.config.Backends = adminBackendListStub{Repository: h.config.Backends, next: next}
		}},
		{name: "bindings invalid next", call: func(h *Handler, r *http.Request) (int, any, error) {
			return h.bindings(ctx, r, principal, "tenant", nil)
		}, install: func(h *Handler, next string) {
			h.config.Bindings = adminBindingListStub{Repository: h.config.Bindings, next: next}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := testHandler(t)
			tc.install(h, "invalid")
			_, _, err := tc.call(h, httptest.NewRequest(http.MethodGet, "/admin/v1?limit=1", nil))
			if !errors.Is(err, errInvalidRequest) {
				t.Fatalf("error = %v, want %v", err, errInvalidRequest)
			}
		})
	}

	_ = auth
}

func TestAdminCollectionListsRejectInvalidRepositoryCursorParameter(t *testing.T) {
	ctx := context.Background()
	principal := Principal{SubjectID: "admin", Global: true}
	request := func() *http.Request {
		return httptest.NewRequest(http.MethodGet, "/admin/v1?cursor=not-base64", nil)
	}
	for _, tc := range []struct {
		name string
		call func(*Handler, *http.Request) error
	}{
		{name: "tenants", call: func(h *Handler, r *http.Request) error { _, _, err := h.tenants(ctx, r, principal); return err }},
		{name: "apps", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.apps(ctx, r, principal, "tenant", nil)
			return err
		}},
		{name: "revisions", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.revisions(ctx, r, principal, "tenant", "app", nil)
			return err
		}},
		{name: "models", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.models(ctx, r, principal, "tenant", nil)
			return err
		}},
		{name: "backends", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.backends(ctx, r, principal, "tenant", nil)
			return err
		}},
		{name: "bindings", call: func(h *Handler, r *http.Request) error {
			_, _, err := h.bindings(ctx, r, principal, "tenant", nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := testHandler(t)
			if err := tc.call(h, request()); !errors.Is(err, errInvalidRequest) {
				t.Fatalf("error = %v, want %v", err, errInvalidRequest)
			}
		})
	}
}

type noTenantListRepository struct{ tenant.Repository }
type noAppListRepository struct{ appmodel.Repository }
type noRevisionListRepository struct{ appmodel.Repository }
type noModelListRepository struct{ modelprofile.Repository }
type noBackendListRepository struct{ backend.Repository }
type noBindingListRepository struct{ channels.Repository }
