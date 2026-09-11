package executionv1

import (
	"strings"
	"testing"
)

func TestReplyArtifactRequestRequiredClosedVersionZero(t *testing.T) {
	r := ReplyArtifactRequest{IntentID: "intent", RunID: "run", CompletionID: "completion", Name: "报告.txt", Version: 0}
	raw, err := EncodeReplyArtifactRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReplyArtifactRequest(raw)
	if err != nil || decoded != r {
		t.Fatal(decoded, err)
	}
	text := string(raw)
	invalid := []string{"null", text + "{}", strings.Replace(text, `,"version":0`, "", 1), strings.Replace(text, `"version":0`, `"version":null`, 1), strings.Replace(text, `"version":0`, `"version":-1`, 1), strings.Replace(text, `"version":0`, `"version":0.0`, 1), strings.Replace(text, `"version":0`, `"version":"0"`, 1), strings.Replace(text, `"version":0`, `"version":0,"version":1`, 1), strings.Replace(text, `"name"`, `"Name"`, 1), strings.Repeat(" ", MaxReplyArtifactRequestBytes) + text}
	for _, key := range []string{"tenant_id", "scope", "url", "path", "secret", "manifest_ref"} {
		invalid = append(invalid, strings.Replace(text, "{", `{"`+key+`":"injected",`, 1))
	}
	for _, name := range []string{"", ".", "..", "../x", "a/b", "a\\b", "x\n", "a\tb", "a\u0085b", strings.Repeat("x", 256)} {
		copy := r
		copy.Name = name
		if _, e := EncodeReplyArtifactRequest(copy); e == nil {
			t.Fatalf("invalid name %q", name)
		}
	}
	for i, body := range invalid {
		if _, e := DecodeReplyArtifactRequest([]byte(body)); e == nil {
			t.Fatalf("accepted case %d", i)
		}
	}
}
