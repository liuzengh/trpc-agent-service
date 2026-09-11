package executionv1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAttemptProofClosedCodec(t *testing.T) {
	r := AttemptRequest{WorkloadIdentity: "worker", ExecutionToken: "opaque", ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
	raw, err := EncodeAttemptRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeAttemptRequest(raw); err != nil || got != r {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"manifest_id"`, `"MANIFEST_ID"`, 1), strings.Replace(string(raw), `"execution_token":"opaque"`, `"execution_token":null`, 1), strings.Replace(string(raw), `"execution_token":"opaque"`, `"execution_token":"opaque","execution_token":"other"`, 1), string(raw) + `{}`} {
		if _, err := DecodeAttemptRequest([]byte(bad)); err == nil {
			t.Fatal("ambiguous request accepted")
		}
	}
	response := AttemptResponse{TenantID: "tenant", ProfileID: "profile", ProfileRevisionNumber: 1, RunID: "run", AttemptID: "attempt", WorkerID: "worker", LeaseEpoch: 1, ExpiresAt: time.Now().UTC().Truncate(time.Second), ManifestID: r.ManifestID, ManifestDigest: r.ManifestDigest, AllowedUses: []CredentialUse{{CredentialID: "crd_test", Purpose: "dsn", AudienceDigest: r.ManifestDigest}}}
	raw, err = EncodeAttemptResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeAttemptResponse(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"purpose"`, `"Purpose"`, 1), strings.Replace(string(raw), `"lease_epoch":1`, `"lease_epoch":0`, 1), strings.Replace(string(raw), `"allowed_uses":[`, `"allowed_uses":null,"other":[`, 1)} {
		if _, err = DecodeAttemptResponse([]byte(bad)); err == nil {
			t.Fatal("ambiguous response accepted")
		}
	}
	response.AllowedUses = append(response.AllowedUses, response.AllowedUses[0])
	raw, _ = json.Marshal(response)
	if _, err = DecodeAttemptResponse(raw); err == nil {
		t.Fatal("duplicate use accepted")
	}
}
