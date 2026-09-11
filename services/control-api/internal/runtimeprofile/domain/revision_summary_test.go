package domain_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestProfileRevisionSummaryContainsMetadataButNeverSpec(t *testing.T) {
	publishedAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	revision := domain.ProfileRevision{
		ID: "rpr_1", TenantID: "tnt_1", ProfileID: "rpf_1",
		RevisionNumber: 3, SourceDraftRevision: 8, SchemaVersion: "v1",
		Spec: json.RawMessage(`{"schema_version":"v1"}`), SpecDigest: "sha256:digest",
		PublishedBy: "usr_1", PublishedAt: publishedAt,
	}
	summary := revision.Summary()
	if summary.ID != revision.ID || summary.TenantID != revision.TenantID ||
		summary.ProfileID != revision.ProfileID || summary.RevisionNumber != revision.RevisionNumber ||
		summary.SourceDraftRevision != revision.SourceDraftRevision ||
		summary.SchemaVersion != revision.SchemaVersion || summary.SpecDigest != revision.SpecDigest ||
		summary.PublishedBy != revision.PublishedBy || !summary.PublishedAt.Equal(publishedAt) {
		t.Fatalf("summary = %#v", summary)
	}
	if _, exists := reflect.TypeOf(summary).FieldByName("Spec"); exists {
		t.Fatal("ProfileRevisionSummary must not contain Spec")
	}
}
