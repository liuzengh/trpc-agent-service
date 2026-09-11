package domain_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestNewRuntimeProfileAndMetadataUpdate(t *testing.T) {
	now := time.Date(2026, time.September, 3, 8, 30, 0, 0, time.FixedZone("CST", 8*60*60))
	profile, err := domain.NewRuntimeProfile(
		"profile-1", "tenant-1", "  Production  ", "  Primary runtime  ", "user-1", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "Production" || profile.Description != "Primary runtime" ||
		profile.CreatedAt.Location() != time.UTC || profile.UpdatedAt.Location() != time.UTC ||
		!profile.CreatedAt.Equal(now) || !profile.UpdatedAt.Equal(now) {
		t.Fatalf("profile = %#v", profile)
	}

	later := now.Add(time.Hour)
	if err := profile.UpdateMetadata(" Runtime V2 ", " Updated ", later); err != nil {
		t.Fatal(err)
	}
	if profile.Name != "Runtime V2" || profile.Description != "Updated" ||
		!profile.UpdatedAt.Equal(later) || profile.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated profile = %#v", profile)
	}
}

func TestRuntimeProfileRejectsInvalidInvariants(t *testing.T) {
	now := time.Now()
	for name, values := range map[string][5]string{
		"id":          {"", "tenant", "name", "", "user"},
		"tenant":      {"id", "", "name", "", "user"},
		"name":        {"id", "tenant", " ", "", "user"},
		"creator":     {"id", "tenant", "name", "", ""},
		"long name":   {"id", "tenant", strings.Repeat("界", 129), "", "user"},
		"description": {"id", "tenant", "name", strings.Repeat("界", 4097), "user"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := domain.NewRuntimeProfile(values[0], values[1], values[2], values[3], values[4], now)
			if !errors.Is(err, domain.ErrInvalidRuntimeProfile) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	var nilProfile *domain.RuntimeProfile
	if err := nilProfile.UpdateMetadata("name", "", now); !errors.Is(err, domain.ErrInvalidRuntimeProfile) {
		t.Fatalf("nil update error = %v", err)
	}
	invalidRevision := int64(0)
	profile := domain.RuntimeProfile{
		ID: "id", TenantID: "tenant", Name: "name", CreatedBy: "user",
		LatestRevisionNumber: &invalidRevision,
	}
	if err := profile.Validate(); !errors.Is(err, domain.ErrInvalidRuntimeProfile) {
		t.Fatalf("invalid revision error = %v", err)
	}
}

func TestDraftAndRevisionCloneOwnTheirDocuments(t *testing.T) {
	draft := domain.ProfileDraft{Spec: []byte(`{"schema_version":"v1"}`)}
	draftClone := draft.Clone()
	draftClone.Spec[0] = '['
	if bytes.Equal(draft.Spec, draftClone.Spec) || draft.Spec[0] != '{' {
		t.Fatalf("draft clone aliases source: %q / %q", draft.Spec, draftClone.Spec)
	}

	revision := domain.ProfileRevision{Spec: []byte(`{"schema_version":"v1"}`)}
	revisionClone := revision.Clone()
	revisionClone.Spec[0] = '['
	if bytes.Equal(revision.Spec, revisionClone.Spec) || revision.Spec[0] != '{' {
		t.Fatalf("revision clone aliases source: %q / %q", revision.Spec, revisionClone.Spec)
	}
}
