package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// ListOptions is the common, deliberately small query contract for all admin
// collection endpoints. Repositories own filtering and stable ordering.
type ListOptions struct {
	Query  string
	Status string
	Cursor string
	Limit  int
}

// TenantLister lists tenant roots that are visible through the Admin
// collection endpoint. Implementations must apply tenant scopes before
// pagination so an out-of-scope tenant can neither be returned nor inferred
// from a page boundary.
type TenantLister interface {
	List(context.Context, []string, string, string, string, int) ([]*tenant.Tenant, string, error)
}

// AppLister lists tenant-scoped Agent Apps.
type AppLister interface {
	List(context.Context, string, string, string, string, int) ([]*appmodel.App, string, error)
}

// RevisionLister lists revisions belonging to an Agent App.
type RevisionLister interface {
	ListRevisions(context.Context, string, string, string, string, string, int) ([]*appmodel.Revision, string, error)
}

// ModelLister lists tenant-scoped Model Profiles.
type ModelLister interface {
	List(context.Context, string, string, string, string, int) ([]*modelprofile.Profile, string, error)
}

// BackendLister lists tenant-scoped Backend Profiles.
type BackendLister interface {
	List(context.Context, string, string, string, string, int) ([]*backend.Profile, string, error)
}

// BindingLister lists tenant-scoped Channel Bindings.
type BindingLister interface {
	List(context.Context, string, string, string, string, int) ([]*channels.Binding, string, error)
}

var errListUnsupported = errors.New("admin list operation is not supported by repository")

type listEnvelope struct {
	Items      any    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	Total      *int   `json:"total,omitempty"`
}

func newListEnvelope(items any, nextCursor string) (listEnvelope, error) {
	envelope := listEnvelope{Items: items}
	if nextCursor == "" {
		return envelope, nil
	}
	offset, err := strconv.Atoi(nextCursor)
	if err != nil || offset < 0 {
		return listEnvelope{}, fmt.Errorf("%w: invalid repository cursor", errInvalidRequest)
	}
	envelope.NextCursor = encodeCursor(offset)
	return envelope, nil
}

func listOptions(r *http.Request) ListOptions {
	if r == nil {
		return ListOptions{Limit: 50}
	}
	values := r.URL.Query()
	limit, _ := strconv.Atoi(values.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	return ListOptions{
		Query: strings.TrimSpace(values.Get("q")), Status: strings.TrimSpace(values.Get("status")),
		Cursor: strings.TrimSpace(values.Get("cursor")), Limit: limit,
	}
}

// repositoryListOptions converts the externally opaque page cursor to the
// numeric offset used by the current repository implementations.
func repositoryListOptions(r *http.Request) (ListOptions, error) {
	options := listOptions(r)
	if options.Cursor == "" {
		return options, nil
	}
	offset, err := decodeCursor(options.Cursor)
	if err != nil {
		return ListOptions{}, err
	}
	options.Cursor = strconv.Itoa(offset)
	return options, nil
}

func listURLValues(options ListOptions) url.Values {
	values := url.Values{}
	if options.Query != "" {
		values.Set("q", options.Query)
	}
	if options.Status != "" {
		values.Set("status", options.Status)
	}
	if options.Cursor != "" {
		values.Set("cursor", options.Cursor)
	}
	values.Set("limit", strconv.Itoa(options.Limit))
	return values
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid cursor", errInvalidRequest)
	}
	offset, err := strconv.Atoi(string(raw))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: invalid cursor", errInvalidRequest)
	}
	return offset, nil
}
