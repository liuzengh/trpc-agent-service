package artifact

import (
	"strings"
	"testing"
)

func validArtifact() Artifact {
	return Artifact{TenantID: "tenant-a", ID: "artifact-a", SessionID: "session-a", MessageID: "message-a", ObjectKey: "tenants/tenant-a/files/a.txt", MIMEType: "text/plain", SizeBytes: 1, SHA256: strings.Repeat("a", 64), Status: StatusReady}
}

func TestArtifactValidation(t *testing.T) {
	if err := validArtifact().Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := validArtifact()
	invalid.ObjectKey = "../tenant-a/file"
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected traversal rejection")
	}
	invalid = validArtifact()
	invalid.ObjectKey = "tenants/tenant-b/file"
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected cross-tenant key rejection")
	}
	invalid = validArtifact()
	invalid.SHA256 = "bad"
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected hash rejection")
	}
}

func TestArtifactStatuses(t *testing.T) {
	value := validArtifact()
	value.Status = StatusPending
	value.SHA256 = ""
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	ready, err := value.Transition(StatusReady)
	if err == nil {
		t.Fatal("expected ready transition to require hash")
	}
	value.SHA256 = strings.Repeat("a", 64)
	ready, err = value.Transition(StatusReady)
	if err != nil || ready.Status != StatusReady {
		t.Fatalf("expected pending -> ready: %v %+v", err, ready)
	}
	if _, err := ready.Transition(StatusPending); err == nil {
		t.Fatal("expected ready -> pending rejection")
	}
}
