package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestCredentialPublicReadsNeverExposePrivateMaterial(t *testing.T) {
	h := newCredentialHarness(t)
	secret := "private-model-value-for-redaction"
	h.save(t, modelCredentialCommand(1, "seed", secret))
	revision := publishCredentialHarness(t, h)
	id := h.spec(t).Models["primary"].APIKeyCredentialID
	draft, err := h.service.GetCredentialDraft(context.Background(), "tnt_a", "rpf_a", "usr_member")
	if err != nil {
		t.Fatal(err)
	}
	published, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_member", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []application.ProfileRead{draft, published} {
		assertRedactedProfile(t, read, id, secret)
		state := read.CredentialStates["models"]["primary"]["api_key"]
		if !state.Configured || state.Status != domain.CredentialActive || state.CredentialRevision != 1 || len(state.AssociationToken) != 64 {
			t.Fatal("public view omitted non-secret credential state")
		}
	}
	if published.SpecDigest != revision.SpecDigest || published.RevisionNumber != 1 {
		t.Fatal("published view lost immutable identity")
	}
	if draft.CredentialStates["models"]["primary"]["api_key"].AssociationToken == published.CredentialStates["models"]["primary"]["api_key"].AssociationToken {
		t.Fatal("draft and published association tokens have the same target")
	}
}

func TestPublishedProfileReadIsStaticAndChecksDigest(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "private-value"))
	revision := publishCredentialHarness(t, h)
	view, err := application.PublishedProfileRead(revision)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(view)
	if len(view.CredentialStates) != 0 || bytes.Contains(encoded, []byte("credential_states")) || view.SpecDigest != revision.SpecDigest {
		t.Fatal("publication receipt contains dynamic state")
	}
	id := h.spec(t).Models["primary"].APIKeyCredentialID
	assertRedactedProfile(t, view, id, "private-value")
	for _, mode := range []string{"digest", "spec", "schema"} {
		t.Run(mode, func(t *testing.T) {
			bad := revision.Clone()
			switch mode {
			case "digest":
				bad.SpecDigest = "sha256:" + strings.Repeat("0", 64)
			case "spec":
				bad.Spec = bytes.ReplaceAll(bad.Spec, []byte("example-model"), []byte("different-model"))
			case "schema":
				bad.SchemaVersion = "unsupported"
			}
			if result, err := application.PublishedProfileRead(bad); err == nil || result.ProfileID != "" {
				t.Fatal("corrupt published material was returned")
			}
		})
	}
}

func TestLiveCredentialRotationChangesOnlyActiveValue(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "old-private-value"))
	revision := publishCredentialHarness(t, h)
	draftBefore := h.draft()
	oldRecord := h.store.records[h.spec(t).Models["primary"].APIKeyCredentialID].Clone()
	before, err := application.PublishedProfileRead(revision)
	if err != nil {
		t.Fatal(err)
	}
	command := liveModelCommand(t, h, "rotate", 1, "new-private-value")
	result, err := h.service.UpdateUsedProfileCredential(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.CredentialRevision != 2 || result.Status != domain.CredentialActive {
		t.Fatal("rotation result did not advance live CAS")
	}
	current := h.store.records[oldRecord.ID]
	if current.ID != oldRecord.ID || current.AudienceDigest != oldRecord.AudienceDigest || h.decrypt(t, current) != "new-private-value" {
		t.Fatal("live rotation changed association or failed to update value")
	}
	if !reflect.DeepEqual(draftBefore, h.draft()) || !reflect.DeepEqual(revision, h.store.base.revisions[resourceKey("tnt_a", "rpf_a")][0]) {
		t.Fatal("live rotation rewrote an immutable snapshot or draft")
	}
	after, err := application.PublishedProfileRead(revision)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("publication replay changed after live rotation")
	}
	view, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
	if err != nil {
		t.Fatal(err)
	}
	if view.SpecDigest != revision.SpecDigest || view.CredentialStates["models"]["primary"]["api_key"].CredentialRevision != 2 {
		t.Fatal("public read failed to separate static digest from current credential metadata")
	}
	beforeState := h.snapshot(t)
	stale := liveModelCommand(t, h, "stale-cas", 1, "third-private-value")
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), stale); !errors.Is(err, domain.ErrCredentialConflict) {
		t.Fatalf("stale live CAS accepted: %v", err)
	}
	if beforeState != h.snapshot(t) {
		t.Fatal("stale live CAS changed state")
	}
}

func TestLiveCredentialClearIsTerminalAndIdempotencyIsStable(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed", "first-private-value"))
	publishCredentialHarness(t, h)
	rotate := liveModelCommand(t, h, "rotate-stable", 1, "second-private-value")
	original, err := h.service.UpdateUsedProfileCredential(context.Background(), rotate)
	if err != nil {
		t.Fatal(err)
	}
	clearCommand := liveModelCommand(t, h, "clear", 2, "")
	clearCommand.Update.Action = "clear"
	clearCommand.Update.Value = nil
	cleared, err := h.service.UpdateUsedProfileCredential(context.Background(), clearCommand)
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Status != domain.CredentialCleared || cleared.CredentialRevision != 3 {
		t.Fatal("live clear did not advance to terminal state")
	}
	id := h.spec(t).Models["primary"].APIKeyCredentialID
	if len(h.store.records[id].Ciphertext) != 0 {
		t.Fatal("clear retained a decryptable current value")
	}
	replay, err := h.service.UpdateUsedProfileCredential(context.Background(), rotate)
	if err != nil || !reflect.DeepEqual(original, replay) {
		t.Fatalf("delayed replay changed original operation result: %v", err)
	}
	if h.store.records[id].Status != domain.CredentialCleared || h.store.records[id].Revision != 3 {
		t.Fatal("replay resurrected cleared credential")
	}
	reactivate := liveModelCommand(t, h, "reactivate", 3, "attempted-private-value")
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), reactivate); !errors.Is(err, domain.ErrCredentialUnavailable) {
		t.Fatalf("cleared ID accepted new live value: %v", err)
	}
	changed := rotate
	different := "different-request-value"
	changed.Update.Value = &different
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), changed); !errors.Is(err, application.ErrCredentialIdempotencyConflict) {
		t.Fatalf("live idempotency key accepted different payload: %v", err)
	}
}

func TestLiveCredentialRequiresOwnerAndPublishedAssociation(t *testing.T) {
	for _, mode := range []string{"member", "draft-token", "wrong-purpose", "wrong-resource", "wrong-revision", "tampered-token"} {
		t.Run(mode, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "old-private-value"))
			publishCredentialHarness(t, h)
			command := liveModelCommand(t, h, "attempt", 1, "new-private-value")
			want := domain.ErrCredentialAssociation
			switch mode {
			case "member":
				command.ActorUserID = "usr_member"
				want = application.ErrTenantForbidden
			case "draft-token":
				draft, err := h.service.GetCredentialDraft(context.Background(), "tnt_a", "rpf_a", "usr_owner")
				if err != nil {
					t.Fatal(err)
				}
				command.Update.Target.AssociationToken = draft.CredentialStates["models"]["primary"]["api_key"].AssociationToken
			case "wrong-purpose":
				command.Update.Target.PurposeField = "bearer_token"
				want = domain.ErrCredentialInput
			case "wrong-resource":
				command.Update.Target.ResourceName = "secondary"
			case "wrong-revision":
				command.Update.Target.ProfileRevisionNumber = 2
				want = application.ErrProfileRevisionNotFound
			case "tampered-token":
				command.Update.Target.AssociationToken = strings.Repeat("0", 64)
			}
			before := h.snapshot(t)
			if _, err := h.service.UpdateUsedProfileCredential(context.Background(), command); !errors.Is(err, want) {
				t.Fatalf("%s: got %v want %v", mode, err, want)
			}
			if h.snapshot(t) != before {
				t.Fatal("unauthorized live update modified state")
			}
		})
	}
}

func TestLiveCredentialCrossProfileTokenIsRejected(t *testing.T) {
	h := newCredentialHarness(t)
	h.save(t, modelCredentialCommand(1, "seed-a", "private-a-value"))
	publishCredentialHarness(t, h)
	command := liveModelCommand(t, h, "cross-profile", 1, "changed-private-value")
	created, err := h.service.CreateRuntimeProfile(context.Background(), application.CreateRuntimeProfileCommand{TenantID: "tnt_a", ActorUserID: "usr_owner", Name: "Other profile"})
	if err != nil {
		t.Fatal(err)
	}
	other := modelCredentialCommand(1, "seed-b", "private-b-value")
	other.ProfileID = created.Profile.ID
	if _, err := h.service.SaveCredentialDraft(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	publish(t, h.service, "tnt_a", created.Profile.ID, "usr_owner", 2)
	command.ProfileID = created.Profile.ID
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), command); !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatalf("other profile accepted source association token: %v", err)
	}
}

func TestLiveStorageRotationAllowsPasswordOnly(t *testing.T) {
	h := newCredentialHarness(t)
	value := "postgresql://app:old-private-password@db.example.test/state?sslmode=require"
	command := modelCredentialCommand(1, "dsn-seed", "unused")
	command.Write.Config.Models = map[string]application.ModelConfig{}
	command.Write.Config.Storage["session"] = application.StorageConfig{Kind: domain.StorageKindPostgresState}
	command.Write.Credentials = credentialAction("storage", "session", "dsn", application.CredentialAction{Action: "replace", Value: &value})
	h.save(t, command)
	revision := publishCredentialHarness(t, h)
	view, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
	if err != nil {
		t.Fatal(err)
	}
	target := application.PublishedCredentialTarget{ProfileRevisionNumber: 1, Category: "storage", ResourceName: "session", PurposeField: "dsn", AssociationToken: view.CredentialStates["storage"]["session"]["dsn"].AssociationToken}
	newValue := "postgresql://app:new-private-password@db.example.test/state?sslmode=require"
	live := application.UpdateUsedCredentialCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: "dsn-rotate", Update: application.CredentialUpdate{Target: target, Action: "replace", ExpectedCredentialRevision: 1, Value: &newValue}}
	if _, err := h.service.UpdateUsedProfileCredential(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	id := h.spec(t).Storage["session"].DSNCredentialID
	if h.decrypt(t, h.store.records[id]) != "new-private-password" || !reflect.DeepEqual(revision, h.store.base.revisions[resourceKey("tnt_a", "rpf_a")][0]) {
		t.Fatal("password-only rotation altered fixed storage target")
	}
	for _, changed := range []string{
		"postgresql://app:p@other.example.test/state?sslmode=require",
		"postgresql://other:p@db.example.test/state?sslmode=require",
		"postgresql://app:p@db.example.test/other?sslmode=require",
		"postgresql://app:p@db.example.test/state?sslmode=disable",
	} {
		before := h.snapshot(t)
		live.IdempotencyKey = changed
		live.Update.ExpectedCredentialRevision = 2
		live.Update.Value = &changed
		if _, err := h.service.UpdateUsedProfileCredential(context.Background(), live); !errors.Is(err, domain.ErrCredentialAssociation) {
			t.Fatalf("live DSN target change accepted: %v", err)
		}
		if before != h.snapshot(t) {
			t.Fatal("rejected storage retarget changed state")
		}
	}
}

func TestLiveCredentialTransactionFailureRollsBackValueAndReceipt(t *testing.T) {
	for _, stage := range []string{"receipt", "commit"} {
		t.Run(stage, func(t *testing.T) {
			h := newCredentialHarness(t)
			h.save(t, modelCredentialCommand(1, "seed", "old-private-value"))
			publishCredentialHarness(t, h)
			command := liveModelCommand(t, h, "fail-live", 1, "new-private-value")
			before := h.snapshot(t)
			h.store.failAt = stage
			if result, err := h.service.UpdateUsedProfileCredential(context.Background(), command); !errors.Is(err, errCredentialTestTransaction) || result != (application.CredentialUpdateResult{}) {
				t.Fatalf("injected live failure returned partial result: %v", err)
			}
			if h.snapshot(t) != before {
				t.Fatal("live transaction failed without reverting value/CAS/receipt")
			}
		})
	}
}

func publishCredentialHarness(t *testing.T, h *credentialHarness) domain.ProfileRevision {
	t.Helper()
	return publish(t, h.service, "tnt_a", "rpf_a", "usr_owner", h.draft().Revision).Revision
}
func liveModelCommand(t *testing.T, h *credentialHarness, key string, expected int64, value string) application.UpdateUsedCredentialCommand {
	t.Helper()
	view, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
	if err != nil {
		t.Fatal(err)
	}
	return application.UpdateUsedCredentialCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: key, Update: application.CredentialUpdate{
		Target: application.PublishedCredentialTarget{ProfileRevisionNumber: 1, Category: "models", ResourceName: "primary", PurposeField: "api_key", AssociationToken: view.CredentialStates["models"]["primary"]["api_key"].AssociationToken}, Action: "replace", ExpectedCredentialRevision: expected, Value: &value,
	}}
}
func assertRedactedProfile(t *testing.T, view application.ProfileRead, id, secret string) {
	t.Helper()
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{id, secret, "credential_id", "ciphertext", "api_key_ref", "secret_ref"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("public profile view contains forbidden field/material %q", forbidden)
		}
	}
}
