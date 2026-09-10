package cosclient

import (
	"testing"
)

func TestValidateEndpoint(t *testing.T) {
	t.Parallel()
	if err := ValidateEndpoint("https://bucket.cos.ap-guangzhou.myqcloud.com"); err != nil {
		t.Fatalf("validate endpoint: %v", err)
	}
	if err := ValidateEndpoint("http://bucket.example.com"); err == nil {
		t.Fatal("validate endpoint accepted insecure url")
	}
}

func TestParseCredentials(t *testing.T) {
	t.Parallel()
	credential, err := parseCredentials(`{"secret_id":"id","secret_key":"key"}`)
	if err != nil {
		t.Fatalf("parse credentials: %v", err)
	}
	if credential.SecretID != "id" || credential.SecretKey != "key" {
		t.Fatalf("credentials = %#v", credential)
	}
	if _, err := parseCredentials(`{"secret_id":"id","secret_key":"key","extra":"value"}`); err == nil {
		t.Fatal("parse credentials accepted an unknown field")
	}
	if _, err := parseCredentials(`{"secret_id":"id","secret_key":"key"}{}`); err == nil {
		t.Fatal("parse credentials accepted multiple values")
	}
}
