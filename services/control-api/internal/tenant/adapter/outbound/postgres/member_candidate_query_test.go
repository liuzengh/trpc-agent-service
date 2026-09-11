package postgresadapter_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

func TestMemberCandidateReaderReturnsActiveNonMemberPage(t *testing.T) {
	encoded, err := json.Marshal([]map[string]string{{
		"user_id": "user-2", "username": "alice", "display_name": "Alice",
	}})
	if err != nil {
		t.Fatal(err)
	}
	db := &dbStub{row: rowStub{values: []any{encoded, 1}}}
	reader := postgresadapter.NewMemberCandidateReader(db)

	page, err := reader.SearchMemberCandidates(
		context.Background(), "tenant-1", "ali", application.Page{Offset: 5, Limit: 10},
	)
	if err != nil {
		t.Fatalf("SearchMemberCandidates() error = %v", err)
	}
	if page.Offset != 5 || page.Limit != 10 || page.Total != 1 ||
		len(page.Candidates) != 1 || page.Candidates[0].UserID != "user-2" {
		t.Fatalf("page = %#v", page)
	}
	if len(db.queryArgs) != 4 || db.queryArgs[0] != "tenant-1" ||
		db.queryArgs[1] != "ali" || db.queryArgs[2] != 10 || db.queryArgs[3] != 5 {
		t.Fatalf("query args = %#v", db.queryArgs)
	}
	for _, clause := range []string{
		"a.status = 'ACTIVE'",
		"strpos(a.normalized_username, $2)",
		"strpos(lower(a.display_name), $2)",
		"NOT EXISTS",
		"m.tenant_id = $1 AND m.user_id = a.id",
		"ORDER BY match_rank, normalized_username, user_id",
	} {
		if !strings.Contains(db.query, clause) {
			t.Fatalf("query does not contain %q: %s", clause, db.query)
		}
	}
}
