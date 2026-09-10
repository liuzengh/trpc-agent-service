package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestListOptionsAndCursorContract(t *testing.T) {
	defaults := listOptions(nil)
	if defaults.Limit != 50 {
		t.Fatalf("default list options = %+v", defaults)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?q=%20hello%20&status=active&cursor=next&limit=999", nil)
	options := listOptions(request)
	if options.Query != "hello" || options.Status != "active" || options.Cursor != "next" || options.Limit != 200 {
		t.Fatalf("normalized list options = %+v", options)
	}
	values := listURLValues(options)
	if values.Get("q") != "hello" || values.Get("status") != "active" || values.Get("cursor") != "next" || values.Get("limit") != "200" {
		t.Fatalf("list URL values = %q", values.Encode())
	}
	encoded := encodeCursor(17)
	if decoded, err := decodeCursor(encoded); err != nil || decoded != 17 {
		t.Fatalf("cursor round trip = decoded=%d err=%v", decoded, err)
	}
	if _, err := decodeCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
	negative := base64.RawURLEncoding.EncodeToString([]byte("-1"))
	if _, err := decodeCursor(negative); err == nil {
		t.Fatal("negative cursor was accepted")
	}
}

func TestAdminCollectionCursorRoundTripIsOpaque(t *testing.T) {
	handler, _ := testHandler(t)
	first, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "first", DisplayName: "First"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.config.Tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "second", DisplayName: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := NewStaticAuthenticator("admin-token", []string{first.TenantID, second.TenantID})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = authenticator

	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?limit=1", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body=%s", response.Code, response.Body.String())
	}
	var firstPage struct {
		Data struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"next_cursor"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Data.Items) != 1 || firstPage.Data.NextCursor == "" {
		t.Fatalf("first page = %s", response.Body.String())
	}
	if offset, err := decodeCursor(firstPage.Data.NextCursor); err != nil || offset != 1 {
		t.Fatalf("opaque next cursor = %q, offset=%d, err=%v", firstPage.Data.NextCursor, offset, err)
	}

	request = httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?limit=1&cursor="+firstPage.Data.NextCursor, nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("second page status = %d, body=%s", response.Code, response.Body.String())
	}
	var secondPage struct {
		Data struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"next_cursor"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &secondPage); err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Data.Items) != 1 || secondPage.Data.NextCursor != "" {
		t.Fatalf("second page = %s", response.Body.String())
	}
}

func TestAdminCollectionRejectsMalformedCursor(t *testing.T) {
	handler, _ := testHandler(t)
	authenticator, err := NewStaticAuthenticator("admin-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	handler.config.Authenticator = authenticator
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?cursor=not-base64", nil)
	request.Header.Set("Authorization", "Bearer admin-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("malformed cursor status = %d, body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"error":"invalid_request"`) {
		t.Fatalf("malformed cursor body = %s", response.Body.String())
	}
}
