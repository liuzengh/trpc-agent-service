package wecom

import (
	"strings"
	"testing"
)

func TestSafeMessageShapeOmitsProviderValues(t *testing.T) {
	payload := []byte(`{"msgid":"message-secret","from":{"userid":"user-secret"},"file":{"url":"https://provider.example/file-secret","aeskey":"aes-secret"},"msgtype":"file"}`)

	shape := safeMessageShape("aibot_msg_callback", payload)
	for _, secret := range []string{"message-secret", "user-secret", "provider.example", "file-secret", "aes-secret"} {
		if strings.Contains(shape, secret) {
			t.Fatalf("shape leaked provider value %q: %s", secret, shape)
		}
	}
	for _, expected := range []string{
		"cmd=aibot_msg_callback",
		"top_keys=file,from,msgid,msgtype",
		"from=object_keys=userid",
		"file=object_keys=aeskey,url",
	} {
		if !strings.Contains(shape, expected) {
			t.Fatalf("shape %q missing %q", shape, expected)
		}
	}
}

func TestSafeMessageShapeReportsMalformedBodyWithoutBody(t *testing.T) {
	shape := safeMessageShape("aibot_msg_callback", []byte(`{"secret":"do-not-log"`))
	if shape != "cmd=aibot_msg_callback body=invalid-json" {
		t.Fatalf("shape = %q", shape)
	}
}
