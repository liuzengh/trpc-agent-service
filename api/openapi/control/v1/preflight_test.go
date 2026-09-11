package controlv1_test

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestPreflightPublicRoutesAndPrivateIsolation(t *testing.T) {
	doc := loadControlOpenAPI(t)
	base := "/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights"
	p := doc.Paths.Value(base)
	if p == nil || p.Post == nil {
		t.Fatal("preflight creation absent")
	}
	if !hasRequiredHeader(p.Post, "Idempotency-Key") {
		t.Fatal("idempotency required")
	}
	assertResponseStatuses(t, p.Post, []string{"202", "400", "401", "403", "404", "409", "422", "429", "503"})
	q := doc.Paths.Value(base + "/{preflight_id}")
	if q == nil || q.Get == nil {
		t.Fatal("preflight read absent")
	}
	for path := range doc.Paths.Map() {
		if len(path) >= 9 && path[:9] == "/internal" {
			t.Fatal("private workload route in public API")
		}
	}
}

func TestPreflightPrivateOpenAPIIsValidAndClosed(t *testing.T) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	doc, err := loader.LoadFromFile("preflight-internal.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// The pinned kin-openapi supports 3.0 security kinds but not 3.1 mutualTLS.
	// Validate the exact 3.1 security shape explicitly, without misrepresenting
	// client certificates as browser/API-key authentication in the published spec.
	mtls := doc.Components.SecuritySchemes["workloadMTLS"]
	if mtls == nil || mtls.Value == nil || mtls.Value.Type != "mutualTLS" || mtls.Value.In != "" || mtls.Value.Name != "" || mtls.Value.Scheme != "" {
		t.Fatal("invalid mTLS security scheme")
	}
	options := openapi3.AllowExtraSiblingFields("const", "$schema", "$id", "$defs", "propertyNames", "if", "then", "else", "prefixItems")
	if err = doc.Info.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = doc.Paths.Validate(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	for _, schema := range doc.Components.Schemas {
		if err = schema.Validate(context.Background(), options); err != nil {
			t.Fatal(err)
		}
	}
	if doc.Paths.Len() != 3 {
		t.Fatal("private operation count")
	}
	for _, path := range []string{"/internal/v1/channel-preflights:claim", "/internal/v1/channel-preflights/{preflight_id}/credentials:resolve", "/internal/v1/channel-preflights/{preflight_id}:complete"} {
		p := doc.Paths.Value(path)
		if p == nil || p.Post == nil {
			t.Fatal("private operation absent", path)
		}
		if p.Post.Security == nil || len(*p.Post.Security) != 1 {
			t.Fatal("private mTLS security absent")
		}
		for _, status := range []string{"200", "204"} {
			if r := p.Post.Responses.Value(status); r != nil {
				if r.Value.Headers["Cache-Control"] == nil {
					t.Fatal("missing no-store", path, status)
				}
			}
		}
	}
}
