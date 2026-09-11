package sessionstore

import (
	"encoding/json"
	"errors"
	"testing"
)

func fixture() Candidate {
	return Candidate{Identity: Identity{TenantID: "tenant-a", SessionID: "session-a", RunID: "run-a", AttemptID: "attempt-a"}, ContentVersion: ContentVersion, Snapshot: json.RawMessage(`{"version":"snapshot-fixture","events":["user","assistant"]}`)}
}
func TestCandidateIdentityDigestAndCapacity(t *testing.T) {
	c := fixture()
	body, head, err := c.Encode(1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Decode(body, c.Identity.TenantID, c.Identity.SessionID, head, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err = Decode(body, "tenant-b", c.Identity.SessionID, head, 1024); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	c.Snapshot = json.RawMessage(`{"events":["different"]}`)
	_, changed, err := c.Encode(1024)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Ref != head.Ref || changed.Digest == head.Digest {
		t.Fatal("fixed key/content digest contract lost")
	}
	if _, err = Decode(body, "tenant-a", "session-a", changed, 1024); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, _, err = c.Encode(2); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	c = fixture()
	c.Parent = head
	_, parented, err := c.Encode(1024)
	if err != nil {
		t.Fatal(err)
	}
	if parented.Ref != head.Ref || parented.Digest == head.Digest {
		t.Fatal("parent not digest-bound")
	}
}
func TestCandidateDistinctTupleEncoding(t *testing.T) {
	a := fixture()
	a.Identity.TenantID = "a:b"
	a.Identity.SessionID = "c"
	b := fixture()
	b.Identity.TenantID = "a"
	b.Identity.SessionID = "b:c"
	_, ah, _ := a.Encode(1024)
	_, bh, _ := b.Encode(1024)
	if ah.Ref == bh.Ref {
		t.Fatal("ambiguous tuple collision")
	}
	for _, h := range []Head{{Ref: "path/to/file", Digest: "x"}, {Ref: "", Digest: ah.Digest}, {Ref: ah.Ref, Digest: ""}} {
		if h.Validate() == nil {
			t.Fatalf("invalid ref passed: %+v", h)
		}
	}
}
