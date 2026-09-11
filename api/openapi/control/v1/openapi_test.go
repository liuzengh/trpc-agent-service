package controlv1_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestControlOpenAPIIsValid(t *testing.T) {
	document := loadControlOpenAPI(t)
	if err := document.Validate(context.Background(), openapi3.AllowExtraSiblingFields("const", "$schema", "$id", "$defs", "propertyNames", "if", "then", "else", "prefixItems")); err != nil {
		t.Fatalf("validate OpenAPI: %v", err)
	}
}

func TestControlOpenAPIContainsAgentV1Routes(t *testing.T) {
	document := loadControlOpenAPI(t)
	assertRoutes(t, document, "/v1/tenants/{tenant_id}/agents", []string{
		"GET /v1/tenants/{tenant_id}/agents",
		"GET /v1/tenants/{tenant_id}/agents/{agent_id}",
		"GET /v1/tenants/{tenant_id}/agents/{agent_id}/draft",
		"GET /v1/tenants/{tenant_id}/agents/{agent_id}/versions",
		"GET /v1/tenants/{tenant_id}/agents/{agent_id}/versions/{version_number}",
		"PATCH /v1/tenants/{tenant_id}/agents/{agent_id}",
		"POST /v1/tenants/{tenant_id}/agents",
		"POST /v1/tenants/{tenant_id}/agents/{agent_id}/draft/validate",
		"POST /v1/tenants/{tenant_id}/agents/{agent_id}/versions",
		"PUT /v1/tenants/{tenant_id}/agents/{agent_id}/draft",
	})
}

func TestControlOpenAPIContainsRuntimeProfileV1Routes(t *testing.T) {
	document := loadControlOpenAPI(t)
	assertRoutes(t, document, "/v1/tenants/{tenant_id}/runtime-profiles", []string{
		"GET /v1/tenants/{tenant_id}/runtime-profiles",
		"GET /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}",
		"GET /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/draft",
		"GET /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/revisions",
		"GET /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/revisions/{revision_number}",
		"PATCH /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}",
		"POST /v1/tenants/{tenant_id}/runtime-profiles",
		"POST /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/credentials/update",
		"POST /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/draft/validate",
		"POST /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/revisions",
		"PUT /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/draft",
	})
}

func TestControlOpenAPIContainsDeploymentV1Routes(t *testing.T) {
	document := loadControlOpenAPI(t)
	assertRoutes(t, document, "/v1/tenants/{tenant_id}/deployments", []string{
		"GET /v1/tenants/{tenant_id}/deployments",
		"GET /v1/tenants/{tenant_id}/deployments/{deployment_id}",
		"GET /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions",
		"GET /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions/{revision_number}",
		"GET /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions/{revision_number}/artifacts/{filename}",
		"PATCH /v1/tenants/{tenant_id}/deployments/{deployment_id}",
		"POST /v1/tenants/{tenant_id}/deployments",
		"POST /v1/tenants/{tenant_id}/deployments/{deployment_id}/backend-migrations",
		"POST /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions",
		"POST /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions/{revision_number}/knowledge/{resource}/import",
		"POST /v1/tenants/{tenant_id}/deployments/{deployment_id}/validate",
		"PUT /v1/tenants/{tenant_id}/deployments/{deployment_id}/revisions/{revision_number}/artifacts/{filename}",
	})
}

func TestControlOpenAPIDeploymentSchemasExposeFrozenContract(t *testing.T) {
	document := loadControlOpenAPI(t)
	for _, name := range []string{
		"CreateDeploymentRequest",
		"CreateDeploymentResponse",
		"UpdateDeploymentRequest",
		"Deployment",
		"DeploymentPage",
		"DeploymentInput",
		"DeploymentValidationDiagnostic",
		"DeploymentValidationReport",
		"DeploymentRevisionSummary",
		"DeploymentRevision",
		"DeploymentRevisionPage",
		"RuntimeManifestView",
		"PublishDeploymentRevisionRequest",
		"PublishDeploymentRevisionResponse",
		"DeploymentValidationErrorResponse",
	} {
		if document.Components.Schemas[name] == nil || document.Components.Schemas[name].Value == nil {
			t.Errorf("components.schemas.%s is missing or unresolved", name)
		}
	}

	input := document.Components.Schemas["DeploymentInput"].Value
	if input.AdditionalProperties.Has == nil || *input.AdditionalProperties.Has {
		t.Fatal("DeploymentInput must be closed")
	}
	if got := sortedPropertyNames(input); strings.Join(got, ",") != "agent,profile,schema_version" {
		t.Fatalf("DeploymentInput properties = %v", got)
	}
	for _, forbidden := range []string{
		"bindings", "environment", "environment_id", "latest", "model_bindings",
		"profile_latest", "tool_bindings", "worker_options",
	} {
		if input.Properties[forbidden] != nil {
			t.Errorf("DeploymentInput exposes forbidden field %q", forbidden)
		}
	}

	summary := document.Components.Schemas["DeploymentRevisionSummary"].Value
	for _, forbidden := range []string{"input", "manifest", "manifest_content", "manifest_view"} {
		if summary.Properties[forbidden] != nil || containsString(summary.Required, forbidden) {
			t.Errorf("DeploymentRevisionSummary exposes heavy field %q", forbidden)
		}
	}
	page := document.Components.Schemas["DeploymentRevisionPage"].Value
	items := page.Properties["revisions"]
	if items == nil || items.Value == nil || items.Value.Items == nil ||
		items.Value.Items.Ref != "#/components/schemas/DeploymentRevisionSummary" {
		t.Fatal("DeploymentRevisionPage must contain metadata summaries")
	}

	revision := document.Components.Schemas["DeploymentRevision"].Value
	for _, required := range []string{"input", "input_digest", "manifest_id", "manifest_digest", "manifest_view"} {
		if revision.Properties[required] == nil || !containsString(revision.Required, required) {
			t.Errorf("DeploymentRevision must require %q", required)
		}
	}
	for _, internal := range []string{
		"agent_version_id", "agent_spec_digest", "manifest", "manifest_content",
		"profile_revision_id", "profile_spec_digest",
	} {
		if revision.Properties[internal] != nil {
			t.Errorf("DeploymentRevision exposes internal field %q", internal)
		}
	}
}

func TestControlOpenAPIDeploymentCommandsUseExactCASAndIdempotency(t *testing.T) {
	document := loadControlOpenAPI(t)
	tenantParameter := document.Components.Parameters["TenantID"]
	if tenantParameter == nil || tenantParameter.Value == nil || tenantParameter.Value.Schema == nil ||
		tenantParameter.Value.Schema.Value == nil || tenantParameter.Value.Schema.Value.MaxLength == nil ||
		*tenantParameter.Value.Schema.Value.MaxLength != 128 {
		t.Fatal("TenantID must share the Deployment handler's 128-character opaque ID limit")
	}
	base := "/v1/tenants/{tenant_id}/deployments"
	create := document.Paths.Value(base).Post
	publish := document.Paths.Value(base + "/{deployment_id}/revisions").Post
	validate := document.Paths.Value(base + "/{deployment_id}/validate").Post
	patch := document.Paths.Value(base + "/{deployment_id}").Patch
	if create == nil || publish == nil || validate == nil || patch == nil {
		t.Fatal("Deployment command operations are unresolved")
	}
	if !hasRequiredHeader(create, "Idempotency-Key") || !hasRequiredHeader(publish, "Idempotency-Key") {
		t.Fatal("Deployment create and publish must require Idempotency-Key")
	}
	if hasRequiredHeader(validate, "Idempotency-Key") || hasRequiredHeader(patch, "Idempotency-Key") {
		t.Fatal("Deployment validate and metadata PATCH must not accept Idempotency-Key")
	}

	validateBody := validate.RequestBody.Value.Content["application/json"]
	if validateBody == nil || validateBody.Schema == nil ||
		validateBody.Schema.Ref != "#/components/schemas/DeploymentInput" {
		t.Fatal("Deployment validate body must be the closed DeploymentInput directly")
	}
	publishBody := publish.RequestBody.Value.Content["application/json"]
	if publishBody == nil || publishBody.Schema == nil ||
		publishBody.Schema.Ref != "#/components/schemas/PublishDeploymentRevisionRequest" {
		t.Fatal("Deployment publish body must use PublishDeploymentRevisionRequest")
	}
	publishRequest := document.Components.Schemas["PublishDeploymentRevisionRequest"].Value
	expectedLatest := publishRequest.Properties["expected_latest_revision_number"]
	if expectedLatest == nil || expectedLatest.Value == nil || !expectedLatest.Value.Nullable ||
		!containsString(publishRequest.Required, "expected_latest_revision_number") ||
		!containsString(publishRequest.Required, "input") {
		t.Fatal("publish must require nullable expected_latest_revision_number and exact input")
	}
	update := document.Components.Schemas["UpdateDeploymentRequest"].Value
	if !containsString(update.Required, "expected_metadata_revision") || update.MinProps != 2 {
		t.Fatal("metadata update must require CAS plus name and/or description")
	}
}

func TestControlOpenAPIDeploymentStatusMapping(t *testing.T) {
	document := loadControlOpenAPI(t)
	base := "/v1/tenants/{tenant_id}/deployments"
	create := document.Paths.Value(base).Post
	validate := document.Paths.Value(base + "/{deployment_id}/validate").Post
	publish := document.Paths.Value(base + "/{deployment_id}/revisions").Post
	assertResponseStatuses(t, create, []string{"200", "201", "400", "401", "403", "409", "413", "500"})
	assertResponseStatuses(t, document.Paths.Value(base).Get, []string{"200", "400", "401", "403", "500"})
	assertResponseStatuses(t, document.Paths.Value(base+"/{deployment_id}").Get, []string{"200", "400", "401", "403", "404", "500"})
	assertResponseStatuses(t, document.Paths.Value(base+"/{deployment_id}").Patch, []string{"200", "400", "401", "403", "404", "409", "413", "500"})
	assertResponseStatuses(t, validate, []string{"200", "400", "401", "403", "404", "413", "500", "503"})
	assertResponseStatuses(t, publish, []string{"200", "201", "400", "401", "403", "404", "409", "413", "422", "500", "503"})
	assertResponseStatuses(t, document.Paths.Value(base+"/{deployment_id}/revisions").Get, []string{"200", "400", "401", "403", "404", "500"})
	assertResponseStatuses(t, document.Paths.Value(base+"/{deployment_id}/revisions/{revision_number}").Get, []string{"200", "400", "401", "403", "404", "500"})
	if validate.Responses.Value("422") != nil {
		t.Fatal("validate compatibility failures belong in 200 valid=false, not 422")
	}
}

func TestControlOpenAPIRuntimeManifestViewIsExplicitlyRedacted(t *testing.T) {
	document := loadControlOpenAPI(t)
	view := document.Components.Schemas["RuntimeManifestView"].Value
	seen := map[*openapi3.Schema]bool{}
	credentialPresentCount := 0
	var visit func(*openapi3.Schema)
	visit = func(schema *openapi3.Schema) {
		if schema == nil || seen[schema] {
			return
		}
		seen[schema] = true
		for name, property := range schema.Properties {
			if name == "credential_present" {
				credentialPresentCount++
			}
			if name == "credential_id" || strings.HasSuffix(name, "_credential_id") ||
				containsString([]string{
					"association_token", "audience_digest", "ciphertext", "configured",
					"credential", "credential_revision", "nonce", "password", "purpose",
					"status", "value",
				}, name) {
				t.Errorf("RuntimeManifestView exposes forbidden field %q", name)
			}
			if property != nil {
				visit(property.Value)
			}
		}
		if schema.Items != nil {
			visit(schema.Items.Value)
		}
		if schema.AdditionalProperties.Schema != nil {
			visit(schema.AdditionalProperties.Schema.Value)
		}
		for _, branch := range append(append(append([]*openapi3.SchemaRef{}, schema.OneOf...), schema.AnyOf...), schema.AllOf...) {
			if branch != nil {
				visit(branch.Value)
			}
		}
	}
	visit(view)
	if credentialPresentCount < 5 {
		t.Fatalf("RuntimeManifestView has %d credential_present projections, want at least 5", credentialPresentCount)
	}
}

func hasRequiredHeader(operation *openapi3.Operation, name string) bool {
	for _, parameter := range operation.Parameters {
		if parameter.Value != nil && parameter.Value.In == "header" &&
			parameter.Value.Name == name && parameter.Value.Required {
			return true
		}
	}
	return false
}

func sortedPropertyNames(schema *openapi3.Schema) []string {
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func assertResponseStatuses(t *testing.T, operation *openapi3.Operation, want []string) {
	t.Helper()
	if operation == nil || operation.Responses == nil {
		t.Fatal("operation responses are unresolved")
	}
	got := make([]string, 0, operation.Responses.Len())
	for status := range operation.Responses.Map() {
		got = append(got, status)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("response statuses = %v, want %v", got, want)
	}
}

func TestControlOpenAPIRuntimeProfileSchemasExposeFrozenContract(t *testing.T) {
	document := loadControlOpenAPI(t)
	wantSchemas := []string{
		"RuntimeProfile",
		"RuntimeProfileDraft",
		"RuntimeProfileRevision",
		"RuntimeProfileRevisionSummary",
		"RuntimeProfileConfig",
		"RuntimeProfilePublishedRevision",
		"RuntimeProfileWrite",
		"RuntimeProfileCredentialUpdate",
		"RuntimeProfileValidationDiagnostic",
		"RuntimeProfileValidationReport",
		"RuntimeProfileValidationErrorResponse",
	}
	for _, name := range wantSchemas {
		if document.Components.Schemas[name] == nil {
			t.Errorf("components.schemas.%s is missing", name)
		}
	}

	if document.Components.Schemas["RuntimeProfileSpec"] != nil {
		t.Fatal("internal Canonical Spec must not be a public HTTP schema")
	}

	report := document.Components.Schemas["RuntimeProfileValidationReport"]
	if report == nil || report.Value == nil {
		t.Fatal("RuntimeProfileValidationReport is unresolved")
	}
	schemaVersion := report.Value.Properties["schema_version"]
	if schemaVersion == nil || schemaVersion.Value == nil {
		t.Fatal("RuntimeProfileValidationReport.schema_version is missing")
	}
	if len(schemaVersion.Value.Enum) != 0 {
		t.Fatalf("RuntimeProfileValidationReport.schema_version enum = %#v, want unrestricted string", schemaVersion.Value.Enum)
	}

	summary := document.Components.Schemas["RuntimeProfileRevisionSummary"]
	if summary == nil || summary.Value == nil {
		t.Fatal("RuntimeProfileRevisionSummary is unresolved")
	}
	if _, hasSpec := summary.Value.Properties["spec"]; hasSpec {
		t.Fatal("RuntimeProfileRevisionSummary must not expose spec")
	}
	if containsString(summary.Value.Required, "spec") {
		t.Fatal("RuntimeProfileRevisionSummary must not require spec")
	}

	revision := document.Components.Schemas["RuntimeProfileRevision"]
	if revision == nil || revision.Value == nil {
		t.Fatal("RuntimeProfileRevision is unresolved")
	}
	if revision.Value.Properties["spec"] != nil || revision.Value.Properties["config"] == nil {
		t.Fatal("RuntimeProfileRevision must expose only redacted config, not Canonical spec")
	}
	if !containsString(revision.Value.Required, "config") {
		t.Fatal("RuntimeProfileRevision must require config")
	}

	page := document.Components.Schemas["RuntimeProfileRevisionPage"]
	if page == nil || page.Value == nil || page.Value.Properties["revisions"] == nil ||
		page.Value.Properties["revisions"].Value == nil ||
		page.Value.Properties["revisions"].Value.Items == nil {
		t.Fatal("RuntimeProfileRevisionPage.revisions items are unresolved")
	}
	const wantSummaryRef = "#/components/schemas/RuntimeProfileRevisionSummary"
	if got := page.Value.Properties["revisions"].Value.Items.Ref; got != wantSummaryRef {
		t.Fatalf("RuntimeProfileRevisionPage.revisions items ref = %q, want %q", got, wantSummaryRef)
	}

	publish := document.Paths.Value("/v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/revisions")
	if publish == nil || publish.Post == nil {
		t.Fatal("publish Runtime Profile Revision operation is missing")
	}
	for _, status := range []string{"200", "201", "422"} {
		if publish.Post.Responses.Value(status) == nil {
			t.Errorf("publish Runtime Profile Revision response %s is missing", status)
		}
	}
}

func TestRuntimeProfilePublicationRequestStaysAgentAndEnvironmentIndependent(t *testing.T) {
	document := loadControlOpenAPI(t)
	request := document.Components.Schemas["RuntimeProfileDraftRevisionRequest"]
	if request == nil || request.Value == nil {
		t.Fatal("RuntimeProfileDraftRevisionRequest is unresolved")
	}
	schema := request.Value
	if len(schema.Properties) != 1 || schema.Properties["expected_revision"] == nil ||
		len(schema.Required) != 1 || schema.Required[0] != "expected_revision" ||
		schema.AdditionalProperties.Has == nil || *schema.AdditionalProperties.Has {
		t.Fatal("publication must accept only expected_revision, not Environment, AgentVersion, or resource mappings")
	}
	for _, suffix := range []string{"/draft/validate", "/revisions"} {
		path := document.Paths.Value("/v1/tenants/{tenant_id}/runtime-profiles/{profile_id}" + suffix)
		if path == nil || path.Post == nil || path.Post.RequestBody == nil || path.Post.RequestBody.Value == nil {
			t.Fatalf("publication operation %s is unresolved", suffix)
		}
		body := path.Post.RequestBody.Value.Content["application/json"]
		if body == nil || body.Schema == nil || body.Schema.Ref != "#/components/schemas/RuntimeProfileDraftRevisionRequest" {
			t.Fatalf("publication operation %s must use the static revision request", suffix)
		}
	}
}

func loadControlOpenAPI(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	document, err := loader.LoadFromFile("openapi.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI: %v", err)
	}
	return document
}

func assertRoutes(t *testing.T, document *openapi3.T, prefix string, want []string) {
	t.Helper()
	var got []string
	for path, item := range document.Paths.Map() {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		for method := range item.Operations() {
			got = append(got, method+" "+path)
		}
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("OpenAPI routes under %s = %#v, want %#v", prefix, got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("OpenAPI routes under %s = %#v, want %#v", prefix, got, want)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestRuntimeProfileCredentialOpenAPISeparatesWriteReadAndPublishedViews(t *testing.T) {
	document := loadControlOpenAPI(t)
	for _, route := range []struct{ path, method, schema string }{{"/draft", "PUT", "RuntimeProfileWrite"}, {"/credentials/update", "POST", "RuntimeProfileCredentialUpdate"}} {
		path := document.Paths.Value("/v1/tenants/{tenant_id}/runtime-profiles/{profile_id}" + route.path)
		if path == nil {
			t.Fatalf("missing credential route %s", route.path)
		}
		op := path.GetOperation(route.method)
		if op == nil || op.RequestBody == nil || op.RequestBody.Value == nil {
			t.Fatal("credential request is missing")
		}
		media := op.RequestBody.Value.Content["application/json"]
		if media == nil || media.Schema == nil || media.Schema.Ref != "#/components/schemas/"+route.schema {
			t.Fatal("credential route uses wrong DTO")
		}
		hasKey := false
		for _, p := range op.Parameters {
			if p.Value != nil && p.Value.In == "header" && p.Value.Name == "Idempotency-Key" && p.Value.Required {
				hasKey = true
			}
		}
		if !hasKey {
			t.Fatal("credential mutation requires Idempotency-Key")
		}
	}
	write := document.Components.Schemas["RuntimeProfileWrite"].Value
	for _, field := range []string{"expected_draft_revision", "credential_protocol_version", "config"} {
		if !containsString(write.Required, field) {
			t.Errorf("write must require %s", field)
		}
	}
	for _, field := range []string{"expected_revision", "spec", "api_key_ref", "credential_id"} {
		if write.Properties[field] != nil {
			t.Errorf("write exposed legacy/internal %s", field)
		}
	}
	published := document.Components.Schemas["RuntimeProfilePublishedRevision"].Value
	if published.Properties["credential_states"] != nil || published.Properties["spec"] != nil || published.Properties["config"] == nil {
		t.Fatal("published retry must be static redacted config")
	}
	state := document.Components.Schemas["RuntimeProfileCredentialState"].Value
	for _, field := range []string{"value", "credential_id", "ciphertext", "secret_ref"} {
		if state.Properties[field] != nil {
			t.Errorf("credential state exposed %s", field)
		}
	}
	seen := map[*openapi3.Schema]bool{}
	var visit func(*openapi3.Schema)
	visit = func(schema *openapi3.Schema) {
		if schema == nil || seen[schema] {
			return
		}
		seen[schema] = true
		for name, p := range schema.Properties {
			if strings.HasSuffix(name, "_ref") || strings.HasSuffix(name, "credential_id") || name == "password" || name == "value" {
				t.Errorf("public config exposes %s", name)
			}
			if p != nil {
				visit(p.Value)
			}
		}
		if schema.AdditionalProperties.Schema != nil {
			visit(schema.AdditionalProperties.Schema.Value)
		}
		for _, p := range schema.OneOf {
			visit(p.Value)
		}
	}
	visit(document.Components.Schemas["RuntimeProfileConfig"].Value)
}

func TestRuntimeProfileDraftCredentialActionsUseOnlyZeroCredentialRevision(t *testing.T) {
	schema := loadControlOpenAPI(t).Components.Schemas["RuntimeProfileCredentialAction"].Value
	for _, action := range []string{"keep", "clear", "replace"} {
		t.Run(action, func(t *testing.T) {
			input := map[string]any{"action": action, "expected_credential_revision": float64(0)}
			if action == "replace" {
				input["value"] = "test-input"
			}
			if err := schema.VisitJSON(input); err != nil {
				t.Fatalf("zero revision rejected: %v", err)
			}
			input["expected_credential_revision"] = float64(1)
			if err := schema.VisitJSON(input); err == nil {
				t.Fatal("Draft credential action accepted live Credential CAS")
			}
		})
	}
}

func TestRuntimeProfilePublishedCredentialTargetMatchesCategoryPurpose(t *testing.T) {
	schema := loadControlOpenAPI(t).Components.Schemas["RuntimeProfilePublishedCredentialTarget"].Value
	for _, test := range []struct {
		category, purpose string
		valid             bool
	}{
		{"models", "api_key", true}, {"tools", "bearer_token", true},
		{"knowledge", "qdrant_api_key", true}, {"knowledge", "embedding_api_key", true},
		{"storage", "dsn", true}, {"models", "dsn", false}, {"tools", "api_key", false},
	} {
		t.Run(test.category+"/"+test.purpose, func(t *testing.T) {
			input := map[string]any{"profile_revision_number": float64(1), "category": test.category, "resource_name": "primary", "purpose_field": test.purpose, "association_token": strings.Repeat("a", 64)}
			err := schema.VisitJSON(input)
			if (err == nil) != test.valid {
				t.Fatalf("category/purpose validity = %t, want %t", err == nil, test.valid)
			}
		})
	}
}

func TestDeploymentValidationReportFixtures(t *testing.T) {
	schema := loadControlOpenAPI(t).Components.Schemas["DeploymentValidationReport"].Value
	for _, name := range []string{"valid.json", "unused-warning.json", "missing-resource.json", "credential-unavailable.json"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("examples", "deployment-reports", name))
			if err != nil {
				t.Fatal(err)
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			if err := schema.VisitJSON(value); err != nil {
				t.Fatalf("report fixture violates HTTP contract: %v", err)
			}
			report := value.(map[string]any)
			valid := true
			for _, item := range report["diagnostics"].([]any) {
				if item.(map[string]any)["severity"] == "error" {
					valid = false
				}
			}
			if report["valid"] != valid {
				t.Fatal("report validity disagrees with diagnostics")
			}
		})
	}
}

func TestControlOpenAPIContainsTenantMemberCandidateRoute(t *testing.T) {
	document := loadControlOpenAPI(t)
	candidatePath := document.Paths.Find("/v1/tenants/{tenant_id}/member-candidates")
	if candidatePath == nil || candidatePath.Get == nil {
		t.Fatal("member candidate GET route is missing")
	}
	if candidatePath.Get.OperationID != "searchTenantMemberCandidates" {
		t.Fatalf("member candidate operationId = %q", candidatePath.Get.OperationID)
	}
	var queryRequired bool
	for _, parameter := range candidatePath.Get.Parameters {
		if parameter.Value != nil && parameter.Value.Name == "query" {
			queryRequired = parameter.Value.Required
		}
	}
	if !queryRequired {
		t.Fatal("member candidate query parameter must be required")
	}
	response := candidatePath.Get.Responses.Value("200")
	if response == nil || response.Value == nil ||
		response.Value.Content.Get("application/json") == nil ||
		response.Value.Content.Get("application/json").Schema.Ref != "#/components/schemas/MemberCandidatePage" {
		t.Fatal("member candidate 200 response must use MemberCandidatePage")
	}
}

func TestControlOpenAPIContainsImplementedChannelRoutes(t *testing.T) {
	document := loadControlOpenAPI(t)
	assertRoutes(t, document, "/v1/tenants/{tenant_id}/channel-", []string{
		"GET /v1/tenants/{tenant_id}/channel-accounts",
		"GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}",
		"GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}",
		"GET /v1/tenants/{tenant_id}/channel-bindings",
		"GET /v1/tenants/{tenant_id}/channel-bindings/{binding_id}",
		"PATCH /v1/tenants/{tenant_id}/channel-accounts/{account_id}",
		"POST /v1/tenants/{tenant_id}/channel-accounts",
		"POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials/{purpose}/update",
		"POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/enabled",
		"POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights",
		"POST /v1/tenants/{tenant_id}/channel-bindings",
		"POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/enabled",
		"POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/target",
		"POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/traffic",
	})
	for path := range document.Paths.Map() {
		if strings.HasPrefix(path, "/internal/") {
			t.Fatal("workload endpoint mixed into public Session API")
		}
	}
	for _, path := range []string{"/v1/tenants/{tenant_id}/channel-accounts", "/v1/tenants/{tenant_id}/channel-bindings"} {
		op := document.Paths.Value(path).Post
		if op.Responses.Status(201) == nil || op.RequestBody == nil {
			t.Fatal("create contract missing")
		}
	}
}
