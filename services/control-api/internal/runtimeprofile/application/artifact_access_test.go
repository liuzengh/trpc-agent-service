package application_test

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"testing"
)

func TestArtifactOwnerResolveFixedRevision(t *testing.T) {
	targets := &artifactTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	h.save(t, artifactCommand())
	publishCredentialHarness(t, h)
	r := h.spec(t).Storage["artifact"]
	cmd := application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ProfileRevisionNumber: 1, Uses: []application.CredentialUse{{CredentialID: r.AccessKeyIDCredentialID, Purpose: "access_key_id", AudienceDigest: r.CredentialAudienceDigest}, {CredentialID: r.SecretAccessKeyCredentialID, Purpose: "secret_access_key", AudienceDigest: r.CredentialAudienceDigest}}}
	targets.offline = true
	batch, err := h.service.ResolveArtifactForOwner(context.Background(), cmd)
	if err != nil || len(batch.Credentials) != 2 {
		t.Fatal(err)
	}
	batch.Clear()
	for _, mode := range []string{"member", "tenant", "purpose", "audience", "revision"} {
		bad := cmd
		bad.Uses = append([]application.CredentialUse(nil), cmd.Uses...)
		switch mode {
		case "member":
			bad.ActorUserID = "usr_member"
		case "tenant":
			bad.TenantID = "tnt_b"
		case "purpose":
			bad.Uses[0].Purpose = "api_key"
		case "audience":
			bad.Uses[0].AudienceDigest = "wrong"
		case "revision":
			bad.ProfileRevisionNumber = 2
		}
		if b, e := h.service.ResolveArtifactForOwner(context.Background(), bad); e == nil || len(b.Credentials) > 0 {
			t.Fatal("unauthorized", mode)
		}
	}
}
