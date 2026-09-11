package executionv1

import (
	"strings"
	"testing"
)

func TestFinalProofCodec(t *testing.T) {
	r := FinalRequest{IntentID: "i", Digest: "sha256:" + strings.Repeat("a", 64), AdmissionID: "a", RunID: "r", AttemptID: "at", CompletionID: "c", ExecutionGeneration: 1, Sequence: 1}
	raw, e := EncodeFinalRequest(r)
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeFinalRequest(raw)
	if e != nil || got != r {
		t.Fatal(got, e)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"intent_id":"i"`, `"intent_id":"i","intent_id":"x"`, 1), strings.Replace(string(raw), `"intent_id"`, `"Intent_ID"`, 1), strings.Replace(string(raw), `"sequence":1`, `"sequence":1.0`, 1), string(raw) + `{}`, strings.Replace(string(raw), `"sequence":1`, `"sequence":2`, 1), strings.Replace(string(raw), `"intent_id":"i"`, `"intent_id":null`, 1)} {
		if _, e := DecodeFinalRequest([]byte(bad)); e == nil {
			t.Fatal("accepted", bad)
		}
	}
	p := FinalResponse{FinalRequest: r, TenantID: "t", ManifestDigest: r.Digest}
	raw, e = EncodeFinalResponse(p)
	if e != nil {
		t.Fatal(e)
	}
	q, e := DecodeFinalResponse(raw)
	if e != nil || q != p {
		t.Fatal(q, e)
	}
}
