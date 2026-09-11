package postgresadapter

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

func TestInsertDeploymentRevisionUsesOwnerVerifiedValuesWithoutReadingSourceTables(t *testing.T) {
	revision := publicationCommitFixture().Revision
	q := &revisionWriteStub{row: rowFunc(func(dest ...any) error {
		*dest[0].(*string) = revision.ID
		return nil
	})}
	if err := insertDeploymentRevision(context.Background(), q, revision); err != nil {
		t.Fatal(err)
	}
	statement := strings.ToLower(q.statement)
	for _, forbidden := range []string{"select", "join", "agent_versions", "runtime_profile_revisions"} {
		if strings.Contains(statement, forbidden) {
			t.Fatalf("Deployment persistence reads another module using %q: %s", forbidden, q.statement)
		}
	}
	if !strings.Contains(statement, "insert into deployment_revisions") ||
		!strings.Contains(statement, "values") || !strings.Contains(statement, "returning id") {
		t.Fatalf("unexpected revision write: %s", q.statement)
	}
	wantArgs := []any{
		revision.TenantID, revision.ID, revision.DeploymentID, revision.RevisionNumber,
		revision.SchemaVersion, []byte(revision.CanonicalInput), revision.InputDigest,
		revision.Input.Agent.AgentID, revision.AgentVersionID, revision.Input.Agent.VersionNumber,
		revision.AgentSchemaVersion, revision.AgentSpecDigest, revision.Input.Profile.ProfileID,
		revision.ProfileRevisionID, revision.Input.Profile.RevisionNumber,
		revision.ProfileSchemaVersion, revision.ProfileSpecDigest, revision.PublishedBy, revision.PublishedAt,
	}
	if !reflect.DeepEqual(q.args, wantArgs) {
		t.Fatalf("persisted arguments = %#v, want %#v", q.args, wantArgs)
	}
}

func TestInsertDeploymentRevisionPreservesTenantScopedForeignKeyFailures(t *testing.T) {
	for _, constraint := range []string{
		"deployment_revisions_agent_version_fk",
		"deployment_revisions_profile_revision_fk",
	} {
		t.Run(constraint, func(t *testing.T) {
			failure := &pgconn.PgError{Code: "23503", ConstraintName: constraint}
			q := &revisionWriteStub{row: rowFunc(func(...any) error { return failure })}
			err := insertDeploymentRevision(context.Background(), q, publicationCommitFixture().Revision)
			if !errors.Is(err, failure) {
				t.Fatalf("insertDeploymentRevision() = %v, want preserved foreign key failure", err)
			}
		})
	}
}

func TestPublicationStoreStillRejectsSourceMetadataDisagreement(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.DeploymentRevision)
	}{
		{"agent version identity", func(r *domain.DeploymentRevision) { r.AgentVersionID = "agv_other" }},
		{"agent schema", func(r *domain.DeploymentRevision) { r.AgentSchemaVersion = "other" }},
		{"agent digest", func(r *domain.DeploymentRevision) { r.AgentSpecDigest = "sha256:" + strings.Repeat("f", 64) }},
		{"profile revision identity", func(r *domain.DeploymentRevision) { r.ProfileRevisionID = "rpr_other" }},
		{"profile schema", func(r *domain.DeploymentRevision) { r.ProfileSchemaVersion = "other" }},
		{"profile digest", func(r *domain.DeploymentRevision) { r.ProfileSpecDigest = "sha256:" + strings.Repeat("f", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commit := publicationCommitFixture()
			test.mutate(&commit.Revision)
			if err := validatePublicationCommit(commit); !errors.Is(err, application.ErrPublicationIntegrity) {
				t.Fatalf("validatePublicationCommit() = %v, want source/manifest integrity error", err)
			}
		})
	}
}

type revisionWriteStub struct {
	statement string
	args      []any
	row       pgx.Row
}

func (q *revisionWriteStub) QueryRow(_ context.Context, statement string, args ...any) pgx.Row {
	q.statement = statement
	q.args = append([]any(nil), args...)
	return q.row
}

func (*revisionWriteStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unexpected Exec")
}
