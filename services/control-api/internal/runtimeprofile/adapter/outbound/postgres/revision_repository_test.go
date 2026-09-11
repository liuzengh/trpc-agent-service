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

	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

func TestListProfileRevisionsSelectsOnlySummaryColumns(t *testing.T) {
	publishedAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	encoded, err := json.Marshal([]map[string]any{{
		"id": "rpr_1", "tenant_id": "tnt_1", "profile_id": "rpf_1",
		"revision_number": 3, "source_draft_revision": 8,
		"schema_version": "v1", "spec_digest": "sha256:digest",
		"published_by": "usr_1", "published_at": publishedAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	db := &summaryDBStub{row: summaryRowStub{values: []any{true, encoded, 1}}}
	store := postgresadapter.NewStore(db)

	page, err := store.ListProfileRevisionSummaries(
		context.Background(), "tnt_1", "rpf_1", application.Page{Offset: 2, Limit: 10},
	)
	if err != nil {
		t.Fatalf("ListProfileRevisionSummaries() error = %v", err)
	}
	if page.Total != 1 || len(page.Revisions) != 1 ||
		page.Revisions[0].ID != "rpr_1" || page.Revisions[0].SpecDigest != "sha256:digest" {
		t.Fatalf("page = %#v", page)
	}
	query := strings.ToLower(db.query)
	if strings.Contains(query, "spec_jsonb") || strings.Contains(query, "select *") {
		t.Fatalf("summary list query reads full revision data:\n%s", db.query)
	}
	for _, column := range []string{
		"id", "tenant_id", "profile_id", "revision_number", "source_draft_revision",
		"schema_version", "spec_digest", "published_by", "published_at",
	} {
		if !strings.Contains(query, column) {
			t.Fatalf("summary list query does not select %s:\n%s", column, db.query)
		}
	}
	if len(db.args) != 4 || db.args[0] != "tnt_1" || db.args[1] != "rpf_1" ||
		db.args[2] != 10 || db.args[3] != 2 {
		t.Fatalf("query args = %#v", db.args)
	}
}

func TestGetProfileRevisionKeepsFullSpecColumn(t *testing.T) {
	db := &summaryDBStub{row: summaryRowStub{err: pgx.ErrNoRows}}
	store := postgresadapter.NewStore(db)

	_, err := store.GetProfileRevision(context.Background(), "tnt_1", "rpf_1", 3)
	if !errors.Is(err, application.ErrProfileRevisionNotFound) {
		t.Fatalf("GetProfileRevision() error = %v", err)
	}
	query := strings.ToLower(db.query)
	if !strings.Contains(query, "spec_jsonb") {
		t.Fatalf("full revision query no longer reads canonical Spec:\n%s", db.query)
	}
}

type summaryDBStub struct {
	query string
	args  []any
	row   pgx.Row
}

func (stub *summaryDBStub) Begin(context.Context) (pgx.Tx, error) { return nil, nil }

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
