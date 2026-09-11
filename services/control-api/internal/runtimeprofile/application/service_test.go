package application_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const validSpec = `{
  "schema_version":"v1",
  "credential_protocol_version":"v1",
  "models":{},
  "tools":{},
  "knowledge":{},
  "storage":{}
}`

var testClock = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

func TestRuntimeProfileV1FullLifecycle(t *testing.T) {
	store := newMemoryStore()
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	ctx := context.Background()

	created, err := service.CreateRuntimeProfile(ctx, application.CreateRuntimeProfileCommand{
		TenantID: "tnt_a", ActorUserID: "usr_author", Name: "  Primary  ",
		Description: " Shared resources ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Profile.Name != "Primary" || created.Profile.Description != "Shared resources" ||
		created.Draft.Revision != 1 || string(created.Draft.Spec) != `{}` {
		t.Fatalf("created = %#v", created)
	}

	name := "Production"
	updated, err := service.UpdateRuntimeProfile(ctx, application.UpdateRuntimeProfileCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID, ActorUserID: "usr_author",
		Name: &name,
	})
	if err != nil || updated.Name != name || updated.Description != "Shared resources" {
		t.Fatalf("updated = %#v, err = %v", updated, err)
	}
	got, err := service.GetRuntimeProfile(ctx, "tnt_a", created.Profile.ID, "usr_author")
	if err != nil || got.Name != name {
		t.Fatalf("get profile = %#v, err = %v", got, err)
	}
	profiles, err := service.ListRuntimeProfiles(
		ctx, "tnt_a", "usr_author", application.Page{},
	)
	if err != nil || profiles.Total != 1 || len(profiles.Profiles) != 1 {
		t.Fatalf("profile page = %#v, err = %v", profiles, err)
	}

	input := emptyProfileWrite(1)
	input.Config.Models["primary"] = application.ModelConfig{}
	incomplete, err := service.SaveCredentialDraft(ctx, application.SaveCredentialDraftCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID, ActorUserID: "usr_author", IdempotencyKey: "incomplete", Write: input,
	})
	if err != nil || incomplete.DraftRevision != 2 {
		t.Fatalf("save incomplete = %#v, err=%v", incomplete, err)
	}
	report, err := service.ValidateProfileDraft(ctx, application.ValidateProfileDraftCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID,
		ActorUserID: "usr_author", ExpectedRevision: 2,
	})
	if err != nil || report.Valid || len(report.Diagnostics) == 0 {
		t.Fatalf("validate incomplete = %#v, err = %v", report, err)
	}

	draft, err := service.SaveCredentialDraft(ctx, application.SaveCredentialDraftCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID, ActorUserID: "usr_author", IdempotencyKey: "complete", Write: emptyProfileWrite(2),
	})
	if err != nil || draft.DraftRevision != 3 {
		t.Fatalf("save valid = %#v, err=%v", draft, err)
	}
	report, err = service.ValidateProfileDraft(ctx, application.ValidateProfileDraftCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID,
		ActorUserID: "usr_author", ExpectedRevision: 3,
	})
	if err != nil || !report.Valid || report.DraftRevision != 3 {
		t.Fatalf("validate valid = %#v, err = %v", report, err)
	}

	published, err := service.PublishProfileRevision(ctx, application.PublishProfileRevisionCommand{
		TenantID: "tnt_a", ProfileID: created.Profile.ID,
		ActorUserID: "usr_author", ExpectedRevision: 3,
	})
	if err != nil || !published.Created || published.Revision.RevisionNumber != 1 ||
		published.Revision.SourceDraftRevision != 3 || !published.Report.Valid {
		t.Fatalf("publication = %#v, err = %v", published, err)
	}
	revision, err := service.GetProfileRevision(
		ctx, "tnt_a", created.Profile.ID, "usr_author", 1,
	)
	if err != nil || revision.ID != published.Revision.ID ||
		string(revision.Spec) != string(published.Revision.Spec) {
		t.Fatalf("get revision = %#v, err = %v", revision, err)
	}
	revisions, err := service.ListProfileRevisions(
		ctx, "tnt_a", created.Profile.ID, "usr_author", application.Page{},
	)
	if err != nil || revisions.Total != 1 || len(revisions.Revisions) != 1 ||
		revisions.Revisions[0].ID != published.Revision.ID {
		t.Fatalf("revision page = %#v, err = %v", revisions, err)
	}

	storedDraft, err := service.GetProfileDraft(
		ctx, "tnt_a", created.Profile.ID, "usr_author",
	)
	if err != nil || storedDraft.Revision != 3 {
		t.Fatalf("get draft = %#v, err = %v", storedDraft, err)
	}
}

func TestSaveCredentialDraftRejectsInvalidInputWithoutMutation(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	input := emptyProfileWrite(1)
	input.CredentialProtocolVersion = "unknown"
	_, err := service.SaveCredentialDraft(context.Background(), application.SaveCredentialDraftCommand{
		TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author", IdempotencyKey: "invalid", Write: input,
	})
	if !errors.Is(err, domain.ErrCredentialInput) {
		t.Fatalf("invalid input error = %v", err)
	}
	draft, getErr := store.GetProfileDraft(context.Background(), "tnt_a", "rpf_1")
	if getErr != nil || draft.Revision != 1 || string(draft.Spec) != "{}" {
		t.Fatal("invalid write mutated draft")
	}
}

func TestPublishImmediateRetryDoesNotReadDraftOrGenerateID(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, sequences := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	command := application.PublishProfileRevisionCommand{
		TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author",
		ExpectedRevision: 2,
	}
	first, err := service.PublishProfileRevision(context.Background(), command)
	if err != nil || !first.Created {
		t.Fatalf("first = %#v, err = %v", first, err)
	}
	draftReads := store.draftReadCount()
	generatedIDs := sequences.revision.Load()
	retry, err := service.PublishProfileRevision(context.Background(), command)
	if err != nil || retry.Created || retry.Revision.ID != first.Revision.ID {
		t.Fatalf("retry = %#v, err = %v", retry, err)
	}
	if retry.Report.Diagnostics != nil || retry.Report.Valid {
		t.Fatalf("retry unexpectedly recomputed report = %#v", retry.Report)
	}
	if store.draftReadCount() != draftReads || sequences.revision.Load() != generatedIDs {
		t.Fatalf("retry read mutable draft or generated ID: reads %d -> %d, ids %d -> %d",
			draftReads, store.draftReadCount(), generatedIDs, sequences.revision.Load())
	}
}

func TestPublishDelayedRetrySurvivesDraftAdvancement(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, sequences := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	command := application.PublishProfileRevisionCommand{
		TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author",
		ExpectedRevision: 2,
	}
	first, err := service.PublishProfileRevision(context.Background(), command)
	if err != nil || !first.Created {
		t.Fatalf("first = %#v, err = %v", first, err)
	}
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 2)
	draftReads := store.draftReadCount()
	generatedIDs := sequences.revision.Load()
	retry, err := service.PublishProfileRevision(context.Background(), command)
	if err != nil || retry.Created || retry.Revision.ID != first.Revision.ID {
		t.Fatalf("delayed retry = %#v, err = %v", retry, err)
	}
	if store.draftReadCount() != draftReads || sequences.revision.Load() != generatedIDs {
		t.Fatal("delayed retry touched the mutable draft path")
	}
}

func TestPublishUnpublishedStaleSourceConflicts(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 2)

	_, err := service.PublishProfileRevision(
		context.Background(), application.PublishProfileRevisionCommand{
			TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author",
			ExpectedRevision: 2,
		},
	)
	if !errors.Is(err, application.ErrDraftRevisionConflict) {
		t.Fatalf("stale unpublished error = %v", err)
	}
}

func TestConcurrentSameSourceCreatesOneRevision(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	command := application.PublishProfileRevisionCommand{
		TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author",
		ExpectedRevision: 2,
	}

	const callers = 24
	results := make(chan application.PublishProfileRevisionResult, callers)
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := service.PublishProfileRevision(context.Background(), command)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent publish: %v", err)
	}
	created := 0
	ids := map[string]struct{}{}
	for result := range results {
		if result.Created {
			created++
		}
		ids[result.Revision.ID] = struct{}{}
	}
	if created != 1 || len(ids) != 1 || store.revisionCount("tnt_a", "rpf_1") != 1 {
		t.Fatalf("created = %d, ids = %d, stored = %d", created, len(ids), store.revisionCount("tnt_a", "rpf_1"))
	}
}

func TestSameContentFromDifferentSourcesCreatesNewRevision(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	first := publish(t, service, "tnt_a", "rpf_1", "usr_author", 2)
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 2)
	second := publish(t, service, "tnt_a", "rpf_1", "usr_author", 3)
	if !first.Created || !second.Created || second.Revision.RevisionNumber != 2 ||
		first.Revision.ID == second.Revision.ID || first.Revision.SpecDigest != second.Revision.SpecDigest {
		t.Fatalf("first = %#v, second = %#v", first, second)
	}
}

func TestTenantIsolationAndNonNilEmptyPages(t *testing.T) {
	store := newMemoryStore()
	service, _ := newTestService(t, store, map[string]bool{
		"tnt_a/usr_author": true, "tnt_b/usr_author": true, "tnt_empty/usr_author": true,
	})
	ctx := context.Background()
	created, err := service.CreateRuntimeProfile(ctx, application.CreateRuntimeProfileCommand{
		TenantID: "tnt_a", ActorUserID: "usr_author", Name: "Primary",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetRuntimeProfile(
		ctx, "tnt_b", created.Profile.ID, "usr_author",
	); !errors.Is(err, application.ErrRuntimeProfileNotFound) {
		t.Fatalf("cross-tenant object error = %v", err)
	}
	if _, err := service.GetRuntimeProfile(
		ctx, "tnt_a", created.Profile.ID, "usr_outsider",
	); !errors.Is(err, application.ErrTenantForbidden) {
		t.Fatalf("outsider error = %v", err)
	}
	emptyProfiles, err := service.ListRuntimeProfiles(
		ctx, "tnt_empty", "usr_author", application.Page{},
	)
	if err != nil || emptyProfiles.Profiles == nil || len(emptyProfiles.Profiles) != 0 {
		t.Fatalf("empty profile page = %#v, err = %v", emptyProfiles, err)
	}
	emptyRevisions, err := service.ListProfileRevisions(
		ctx, "tnt_a", created.Profile.ID, "usr_author", application.Page{},
	)
	if err != nil || emptyRevisions.Revisions == nil || len(emptyRevisions.Revisions) != 0 {
		t.Fatalf("empty revision page = %#v, err = %v", emptyRevisions, err)
	}
}

func TestFullRevisionReadEnforcesIntegrityWhileSummaryListDoesNotLoadSpec(t *testing.T) {
	store := seededStore("tnt_a", "rpf_1")
	service, _ := newTestService(t, store, map[string]bool{"tnt_a/usr_author": true})
	saveValidDraft(t, service, "tnt_a", "rpf_1", "usr_author", 1)
	result := publish(t, service, "tnt_a", "rpf_1", "usr_author", 2)
	store.corruptRevision("tnt_a", "rpf_1", result.Revision.ID)

	if _, err := service.GetProfileRevision(
		context.Background(), "tnt_a", "rpf_1", "usr_author", 1,
	); err == nil {
		t.Fatal("GetProfileRevision accepted a corrupted immutable revision")
	}
	page, err := service.ListProfileRevisions(
		context.Background(), "tnt_a", "rpf_1", "usr_author", application.Page{},
	)
	if err != nil || len(page.Revisions) != 1 || page.Revisions[0].ID != result.Revision.ID {
		t.Fatalf("summary list = %#v, err = %v", page, err)
	}
	if _, err := service.PublishProfileRevision(
		context.Background(), application.PublishProfileRevisionCommand{
			TenantID: "tnt_a", ProfileID: "rpf_1", ActorUserID: "usr_author",
			ExpectedRevision: 2,
		},
	); err == nil {
		t.Fatal("idempotent publication accepted a corrupted immutable revision")
	}
}

func saveValidDraft(
	t *testing.T,
	service *application.Service,
	tenantID, profileID, userID string,
	expected int64,
) domain.ProfileDraft {
	t.Helper()
	_, err := service.SaveCredentialDraft(context.Background(), application.SaveCredentialDraftCommand{
		TenantID: tenantID, ProfileID: profileID, ActorUserID: userID, IdempotencyKey: fmt.Sprintf("save-%d", expected), Write: emptyProfileWrite(expected),
	})
	if err != nil {
		t.Fatalf("save valid draft at %d: %v", expected, err)
	}
	draft, err := service.GetProfileDraft(context.Background(), tenantID, profileID, userID)
	if err != nil {
		t.Fatal(err)
	}

	return draft
}

func publish(
	t *testing.T,
	service *application.Service,
	tenantID, profileID, userID string,
	expected int64,
) application.PublishProfileRevisionResult {
	t.Helper()
	result, err := service.PublishProfileRevision(
		context.Background(), application.PublishProfileRevisionCommand{
			TenantID: tenantID, ProfileID: profileID, ActorUserID: userID,
			ExpectedRevision: expected,
		},
	)
	if err != nil {
		t.Fatalf("publish source %d: %v", expected, err)
	}
	return result
}

type idSequences struct {
	profile  atomic.Int64
	revision atomic.Int64
}

func newTestService(
	t *testing.T,
	store *memoryStore,
	members map[string]bool,
) (*application.Service, *idSequences) {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	credentials := &credentialMemoryStore{base: store, records: map[string]domain.ProfileCredential{}, receipts: map[string]application.CredentialReceipt{}}
	sequences := &idSequences{}
	return application.NewService(application.Dependencies{
		Store: store, TenantAccess: accessStub{members: members},
		Credentials: credentials, Cipher: cipher, OwnerAccess: credentialOwnerStub(members),
		NewCredentialID: func() (string, error) { return "crd_00000000000000000000000000000001", nil },
		NewProfileID: func() (string, error) {
			return fmt.Sprintf("rpf_%d", sequences.profile.Add(1)), nil
		},
		NewRevisionID: func() (string, error) {
			return fmt.Sprintf("rpr_%d", sequences.revision.Add(1)), nil
		},
		Now: func() time.Time { return testClock },
	}), sequences
}

type accessStub struct {
	members map[string]bool
}

func (stub accessStub) IsActiveMember(
	_ context.Context, tenantID, userID string,
) (bool, error) {
	return stub.members[tenantID+"/"+userID], nil
}

type memoryStore struct {
	mu         sync.Mutex
	profiles   map[string]domain.RuntimeProfile
	drafts     map[string]domain.ProfileDraft
	revisions  map[string][]domain.ProfileRevision
	draftReads int64
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		profiles:  make(map[string]domain.RuntimeProfile),
		drafts:    make(map[string]domain.ProfileDraft),
		revisions: make(map[string][]domain.ProfileRevision),
	}
}

func seededStore(tenantID, profileID string) *memoryStore {
	store := newMemoryStore()
	key := resourceKey(tenantID, profileID)
	store.profiles[key] = domain.RuntimeProfile{
		ID: profileID, TenantID: tenantID, Name: "Primary", CreatedBy: "usr_author",
		CreatedAt: testClock, UpdatedAt: testClock,
	}
	store.drafts[key] = domain.ProfileDraft{
		TenantID: tenantID, ProfileID: profileID, Revision: 1,
		Spec: json.RawMessage(`{}`), UpdatedBy: "usr_author", UpdatedAt: testClock,
	}
	return store
}

func resourceKey(tenantID, profileID string) string { return tenantID + "/" + profileID }

func (store *memoryStore) CreateRuntimeProfile(
	_ context.Context, profile domain.RuntimeProfile, draft domain.ProfileDraft,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(profile.TenantID, profile.ID)
	store.profiles[key] = cloneProfile(profile)
	store.drafts[key] = draft.Clone()
	return nil
}

func (store *memoryStore) GetRuntimeProfile(
	_ context.Context, tenantID, profileID string,
) (domain.RuntimeProfile, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	profile, ok := store.profiles[resourceKey(tenantID, profileID)]
	if !ok {
		return domain.RuntimeProfile{}, application.ErrRuntimeProfileNotFound
	}
	return cloneProfile(profile), nil
}

func (store *memoryStore) ListRuntimeProfiles(
	_ context.Context, tenantID string, page application.Page,
) (application.RuntimeProfilePage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	profiles := make([]domain.RuntimeProfile, 0)
	for _, profile := range store.profiles {
		if profile.TenantID == tenantID {
			profiles = append(profiles, cloneProfile(profile))
		}
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID < profiles[j].ID })
	total := len(profiles)
	profiles = pageSlice(profiles, page.Offset, page.Limit)
	return application.RuntimeProfilePage{Profiles: profiles, Total: total}, nil
}

func (store *memoryStore) UpdateRuntimeProfile(
	_ context.Context, profile domain.RuntimeProfile,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(profile.TenantID, profile.ID)
	if _, ok := store.profiles[key]; !ok {
		return application.ErrRuntimeProfileNotFound
	}
	store.profiles[key] = cloneProfile(profile)
	return nil
}

func (store *memoryStore) GetProfileDraft(
	_ context.Context, tenantID, profileID string,
) (domain.ProfileDraft, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.draftReads++
	draft, ok := store.drafts[resourceKey(tenantID, profileID)]
	if !ok {
		return domain.ProfileDraft{}, application.ErrRuntimeProfileNotFound
	}
	return draft.Clone(), nil
}

func (store *memoryStore) SaveProfileDraft(
	_ context.Context, draft domain.ProfileDraft, expectedRevision int64,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(draft.TenantID, draft.ProfileID)
	current, ok := store.drafts[key]
	if !ok {
		return application.ErrRuntimeProfileNotFound
	}
	if current.Revision != expectedRevision {
		return application.ErrDraftRevisionConflict
	}
	store.drafts[key] = draft.Clone()
	return nil
}

func (store *memoryStore) FindRevisionBySourceDraft(
	_ context.Context, tenantID, profileID string, sourceDraftRevision int64,
) (domain.ProfileRevision, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, revision := range store.revisions[resourceKey(tenantID, profileID)] {
		if revision.SourceDraftRevision == sourceDraftRevision {
			return revision.Clone(), true, nil
		}
	}
	return domain.ProfileRevision{}, false, nil
}

func (store *memoryStore) PublishProfileRevision(
	_ context.Context, candidate domain.ProfileRevision, expectedRevision int64,
) (domain.ProfileRevision, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(candidate.TenantID, candidate.ProfileID)

	// Match the PostgreSQL transaction: idempotency wins over mutable state.
	for _, revision := range store.revisions[key] {
		if revision.SourceDraftRevision == candidate.SourceDraftRevision {
			return revision.Clone(), false, nil
		}
	}
	draft, ok := store.drafts[key]
	if !ok {
		return domain.ProfileRevision{}, false, application.ErrRuntimeProfileNotFound
	}
	if draft.Revision != expectedRevision || candidate.SourceDraftRevision != expectedRevision {
		return domain.ProfileRevision{}, false, application.ErrDraftRevisionConflict
	}
	profile, ok := store.profiles[key]
	if !ok {
		return domain.ProfileRevision{}, false, application.ErrRuntimeProfileNotFound
	}
	candidate.RevisionNumber = 1
	if profile.LatestRevisionNumber != nil {
		candidate.RevisionNumber = *profile.LatestRevisionNumber + 1
	}
	store.revisions[key] = append(store.revisions[key], candidate.Clone())
	latest := candidate.RevisionNumber
	profile.LatestRevisionNumber = &latest
	profile.UpdatedAt = candidate.PublishedAt
	store.profiles[key] = profile
	return candidate.Clone(), true, nil
}

func (store *memoryStore) GetProfileRevision(
	_ context.Context, tenantID, profileID string, revisionNumber int64,
) (domain.ProfileRevision, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, revision := range store.revisions[resourceKey(tenantID, profileID)] {
		if revision.RevisionNumber == revisionNumber {
			return revision.Clone(), nil
		}
	}
	return domain.ProfileRevision{}, application.ErrProfileRevisionNotFound
}

func (store *memoryStore) ListProfileRevisionSummaries(
	_ context.Context, tenantID, profileID string, page application.Page,
) (application.ProfileRevisionSummaryPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(tenantID, profileID)
	if _, ok := store.profiles[key]; !ok {
		return application.ProfileRevisionSummaryPage{}, application.ErrRuntimeProfileNotFound
	}
	stored := store.revisions[key]
	revisions := make([]domain.ProfileRevisionSummary, 0, len(stored))
	for index := len(stored) - 1; index >= 0; index-- {
		revisions = append(revisions, stored[index].Summary())
	}
	total := len(revisions)
	revisions = pageSlice(revisions, page.Offset, page.Limit)
	return application.ProfileRevisionSummaryPage{Revisions: revisions, Total: total}, nil
}

func (store *memoryStore) draftReadCount() int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.draftReads
}

func (store *memoryStore) revisionCount(tenantID, profileID string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.revisions[resourceKey(tenantID, profileID)])
}

func (store *memoryStore) corruptRevision(tenantID, profileID, revisionID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := resourceKey(tenantID, profileID)
	for index := range store.revisions[key] {
		if store.revisions[key][index].ID == revisionID {
			store.revisions[key][index].SpecDigest = "sha256:" + fmt.Sprintf("%064d", 0)
			return
		}
	}
}

func cloneProfile(profile domain.RuntimeProfile) domain.RuntimeProfile {
	if profile.LatestRevisionNumber != nil {
		latest := *profile.LatestRevisionNumber
		profile.LatestRevisionNumber = &latest
	}
	return profile
}

func pageSlice[T any](values []T, offset, limit int) []T {
	if offset >= len(values) {
		return values[:0]
	}
	end := offset + limit
	if end > len(values) {
		end = len(values)
	}
	return values[offset:end]
}

var _ application.Store = (*memoryStore)(nil)

func emptyProfileWrite(expected int64) application.ProfileWrite {
	return application.ProfileWrite{ExpectedDraftRevision: expected, CredentialProtocolVersion: "v1", Config: application.ProfileConfig{
		Models: map[string]application.ModelConfig{}, Tools: map[string]application.ToolConfig{},
		Knowledge: map[string]application.KnowledgeConfig{}, Storage: map[string]application.StorageConfig{},
	}}
}
