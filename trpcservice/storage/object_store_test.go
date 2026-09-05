package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalObjectKey(t *testing.T) {
	key, err := CanonicalObjectKey("tenant-a", "artifact-a")
	if err != nil {
		t.Fatalf("canonical key: %v", err)
	}
	if key != "tenants/tenant-a/artifacts/artifact-a" {
		t.Fatalf("unexpected key %q", key)
	}
	for _, value := range []string{"", "../escape", "/absolute", "a/b", `a\\b`, "a b", "..", "a..b", strings.Repeat("a", 129)} {
		if _, err := CanonicalObjectKey("tenant-a", value); !errors.Is(err, ErrObjectInvalid) {
			t.Errorf("artifact ID %q: got %v, want ErrObjectInvalid", value, err)
		}
		if _, err := CanonicalObjectKey(value, "artifact-a"); !errors.Is(err, ErrObjectInvalid) {
			t.Errorf("tenant ID %q: got %v, want ErrObjectInvalid", value, err)
		}
	}
}

func TestValidateObjectUpload(t *testing.T) {
	body := []byte("bounded object")
	digest := sha256.Sum256(body)
	valid := ObjectUpload{ArtifactID: "artifact-a", MIMEType: "text/plain", ExpectedSize: int64(len(body)), ExpectedSHA256: hex.EncodeToString(digest[:]), Body: bytes.NewReader(body)}
	if err := ValidateObjectUpload(valid, int64(len(body))); err != nil {
		t.Fatalf("valid upload: %v", err)
	}

	tooLarge := valid
	tooLarge.ExpectedSize++
	if err := ValidateObjectUpload(tooLarge, int64(len(body))); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("too large: got %v", err)
	}

	for name, mutate := range map[string]func(*ObjectUpload){
		"missing body": func(value *ObjectUpload) { value.Body = nil },
		"invalid ID":   func(value *ObjectUpload) { value.ArtifactID = "../escape" },
		"invalid MIME": func(value *ObjectUpload) { value.MIMEType = "text/plain\r\nX-Evil: yes" },
		"control MIME": func(value *ObjectUpload) { value.MIMEType = "text/plain\tbad" },
		"invalid hash": func(value *ObjectUpload) { value.ExpectedSHA256 = strings.Repeat("z", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if err := ValidateObjectUpload(value, int64(len(body))); !errors.Is(err, ErrObjectInvalid) {
				t.Fatalf("got %v, want ErrObjectInvalid", err)
			}
		})
	}
}
