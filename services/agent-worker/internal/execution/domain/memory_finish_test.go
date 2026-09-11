package domain

import (
	"encoding/json"
	"testing"
)

func TestFinishDigestMemoryBindingAndLegacyCompatibility(t *testing.T) {
	f := Finish{Grant: Grant{AttemptID: "attempt", Generation: 2, LeaseEpoch: 3}, Status: Succeeded, Candidate: Candidate{Ref: "candidate", Digest: Digest([]byte("snapshot"))}, FinalText: "answer"}
	legacy := struct {
		Attempt           string
		Generation, Epoch int64
		Status            Status
		Candidate         Candidate
		Text, Reason      string
	}{f.Grant.AttemptID, f.Grant.Generation, f.Grant.LeaseEpoch, f.Status, f.Candidate, f.FinalText, f.Reason}
	body, _ := json.Marshal(legacy)
	if FinishDigest(f) != Digest(body) {
		t.Fatal("no-Memory historical FinishDigest changed")
	}
	original := FinishDigest(f)
	f.MemoryDigest = Digest([]byte("memory-one"))
	if FinishDigest(f) == original {
		t.Fatal("accepted result does not bind Memory")
	}
	one := FinishDigest(f)
	f.MemoryDigest = Digest([]byte("memory-two"))
	if FinishDigest(f) == one {
		t.Fatal("different Memory candidate shares accepted identity")
	}
}
