package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

func TestListRevisionSummariesDoesNotReadPublicationJSONB(t *testing.T) {
	publishedAt := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal([]map[string]any{{
		"id": "dpr_1", "tenant_id": "tnt_1", "deployment_id": "dep_1",
		"revision_number": 3, "schema_version": "v1",
		"agent_id": "agt_1", "agent_version_number": 7,
		"profile_id": "rpf_1", "profile_revision_number": 4,
		"input_digest": "sha256:input", "manifest_id": "rmf_1",
		"manifest_digest": "sha256:manifest", "published_by": "usr_1",
		"published_at": publishedAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	db := &summaryDBStub{row: summaryRowStub{values: []any{true, encoded, 1}}}
	store := postgresadapter.NewStore(db)

	page, err := store.ListRevisionSummaries(
		context.Background(), "tnt_1", "dep_1", application.Page{Offset: 2, Limit: 10},
	)
	if err != nil {
		t.Fatalf("ListRevisionSummaries() error = %v", err)
	}
	if page.Total != 1 || len(page.Revisions) != 1 ||
		page.Revisions[0].ID != "dpr_1" ||
		page.Revisions[0].ManifestDigest != "sha256:manifest" {
		t.Fatalf("page = %#v", page)
	}
	query := strings.ToLower(db.query)
	for _, forbidden := range []string{
		"input_jsonb", "content_jsonb", "result_jsonb", "payload_jsonb", "select *",
	} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("summary list query reads %s:\n%s", forbidden, db.query)
		}
	}
	for _, column := range []string{
		"id", "tenant_id", "deployment_id", "revision_number", "schema_version",
		"agent_id", "agent_version_number", "profile_id", "profile_revision_number",
		"input_digest", "manifest_id", "manifest_digest", "published_by", "published_at",
	} {
		if !strings.Contains(query, column) {
			t.Fatalf("summary list query does not select %s:\n%s", column, db.query)
		}
	}
	if len(db.args) != 4 || db.args[0] != "tnt_1" || db.args[1] != "dep_1" ||
		db.args[2] != 10 || db.args[3] != 2 {
		t.Fatalf("query args = %#v", db.args)
	}
}

func TestGetPublishedRevisionReadsBothCanonicalJSONBColumns(t *testing.T) {
	db := &summaryDBStub{row: summaryRowStub{err: pgx.ErrNoRows}}
	store := postgresadapter.NewStore(db)

	_, err := store.GetPublishedRevision(context.Background(), "tnt_1", "dep_1", 3)
	if !errors.Is(err, application.ErrDeploymentRevisionNotFound) {
		t.Fatalf("GetPublishedRevision() error = %v", err)
	}
	query := strings.ToLower(db.query)
	for _, required := range []string{"input_jsonb", "content_jsonb"} {
		if !strings.Contains(query, required) {
			t.Fatalf("full revision query does not read %s:\n%s", required, db.query)
		}
	}
}

type summaryDBStub struct {
	query string
	args  []any
	row   pgx.Row
}

func (*summaryDBStub) Begin(context.Context) (pgx.Tx, error) { return nil, nil }

func (stub *summaryDBStub) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	stub.query = query
	stub.args = append([]any(nil), args...)
	return stub.row
}

func (*summaryDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

type summaryRowStub struct {
	values []any
	err    error
}

func (stub summaryRowStub) Scan(dest ...any) error {
	if stub.err != nil {
		return stub.err
	}
	for index, value := range stub.values {
		switch target := dest[index].(type) {
		case *bool:
			*target = value.(bool)
		case *[]byte:
			*target = append((*target)[:0], value.([]byte)...)
		case *int:
			*target = value.(int)
		default:
			panic("unsupported scan destination")
		}
	}
	return nil
}

func TestGetPublishedRevisionPreservesMissingManifestAsIntegrityFailure(t *testing.T) {
	db := &summaryDBStub{row: missingManifestRow{}}
	_, err := postgresadapter.NewStore(db).GetPublishedRevision(context.Background(), "tnt_1", "dep_1", 1)
	if !errors.Is(err, application.ErrPublicationIntegrity) {
		t.Fatalf("GetPublishedRevision() = %v, want publication integrity failure", err)
	}
	if !strings.Contains(strings.ToLower(db.query), "left join runtime_manifests") {
		t.Fatalf("missing manifest would disappear from the full query: %s", db.query)
	}
}

func TestListRevisionSummariesRejectsMissingManifestInsteadOfHidingTheRevision(t *testing.T) {
	encoded := []byte(`[{"id":"dpr_1","tenant_id":"tnt_1","deployment_id":"dep_1","revision_number":1,"manifest_id":null,"manifest_digest":null}]`)
	db := &summaryDBStub{row: summaryRowStub{values: []any{true, encoded, 1}}}
	_, err := postgresadapter.NewStore(db).ListRevisionSummaries(context.Background(), "tnt_1", "dep_1", application.Page{Limit: 10})
	if !errors.Is(err, application.ErrPublicationIntegrity) {
		t.Fatalf("ListRevisionSummaries() = %v, want publication integrity failure", err)
	}
	if !strings.Contains(strings.ToLower(db.query), "left join runtime_manifests") {
		t.Fatalf("missing manifest would disappear from the summary query: %s", db.query)
	}
}

type missingManifestRow struct{}

func (missingManifestRow) Scan(dest ...any) error {
	// These are the COALESCE values from a real revision with no joined
	// Manifest. The mapper must reject it before treating it as a publication.
	for _, target := range dest {
		switch typed := target.(type) {
		case *string:
			*typed = ""
		case *int64:
			*typed = 1
		case *json.RawMessage:
			*typed = json.RawMessage(`{}`)
		case *time.Time:
			*typed = time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
		default:
			panic("unexpected scan target")
		}
	}
	*dest[0].(*string) = "dpr_1"
	return nil
}
