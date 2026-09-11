package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

const managedFixture = `{"schema_version":"v1","credential_protocol_version":"v1","models":{},"tools":{},"knowledge":{"docs":{"kind":"managed_knowledge","backend_id":"vectors","backend_revision":1,"embedding":{"model":"embed","base_url":"https://embed.example","api_key_credential_id":"crd_00000000000000000000000000000001","dimensions":3}}},"storage":{"session":{"kind":"managed_session","backend_id":"redis","backend_revision":2},"memory":{"kind":"managed_memory","backend_id":"pg","backend_revision":1},"artifact":{"kind":"managed_artifact","backend_id":"s3","backend_revision":1}}}`

func TestManagedPublicationRoundTrip(t *testing.T) {
	c, r := ValidateForPublication([]byte(managedFixture), 1)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	var spec Spec
	if err := json.Unmarshal(c.Document, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Storage["session"].BackendID != "redis" || spec.Storage["session"].BackendRevision != 2 {
		t.Fatal(spec.Storage)
	}
	for _, field := range []string{"dsn_credential_id", "destination", "qdrant_api_key_credential_id", "collection", "host"} {
		if strings.Contains(string(c.Document), `"`+field+`"`) {
			t.Fatal("inactive field", field)
		}
	}
	c2, r := ValidateForPublication(c.Document, 1)
	if !r.Valid || c.Digest != c2.Digest {
		t.Fatal("unstable canonical")
	}
	if got := spec.Storage["artifact"].ProvidedCapabilities(); len(got) != 1 || got[0] != "storage.artifact" {
		t.Fatal(got)
	}
}
func TestManagedPublicationRejectsInvalidBranch(t *testing.T) {
	for _, input := range []string{
		strings.Replace(managedFixture, `"backend_id":"redis"`, `"backend_id":"../redis"`, 1),
		strings.Replace(managedFixture, `"backend_revision":2`, `"backend_revision":0`, 1),
		strings.Replace(managedFixture, `"backend_revision":2`, `"backend_revision":2,"destination":{}`, 1),
		strings.Replace(managedFixture, `"session":{"kind"`, `"other":{"kind"`, 1),
		strings.Replace(managedFixture, `"backend_id":"vectors"`, `"backend_id":"vectors","host":""`, 1),
	} {
		_, r := ValidateForPublication([]byte(input), 1)
		if r.Valid {
			t.Fatal("accepted invalid managed resource")
		}
	}
	r := StorageResource{Kind: StorageKindManagedSession, DSNCredentialID: "private"}
	if _, err := json.Marshal(r); err == nil {
		t.Fatal("discarded credential")
	}
}
