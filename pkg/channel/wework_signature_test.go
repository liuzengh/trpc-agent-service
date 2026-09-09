//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const testToken = "test-webhook-token-value"

// readCloser wraps a string as a request body.
func readCloser(s string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(s))
}

// signWeWork reproduces the WeCom scheme: sha1 over the lexicographically
// sorted concatenation of token, timestamp, nonce and the encrypted payload.
func signWeWork(token, timestamp, nonce, encrypt string) string {
	parts := []string{token, timestamp, nonce, encrypt}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}

func weWorkBody(encrypt string) string {
	return fmt.Sprintf("<xml><Encrypt><![CDATA[%s]]></Encrypt></xml>", encrypt)
}

// weWorkRequest builds a signed request. Callers tamper with the result to
// express a specific attack.
func weWorkRequest(t *testing.T, encrypt string) (*http.Request, *tenant.ChannelBinding) {
	t.Helper()

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	nonce := "test-nonce-1234"
	sig := signWeWork(testToken, timestamp, nonce, encrypt)

	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, timestamp, nonce),
		strings.NewReader(weWorkBody(encrypt)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return req, &tenant.ChannelBinding{Token: testToken}
}

// setQuery rewrites one query parameter on an already-signed request.
func setQuery(req *http.Request, key, value string) {
	q := req.URL.Query()
	q.Set(key, value)
	req.URL.RawQuery = q.Encode()
}

// TestWeWork_VerifySignature_AcceptsValid is the control: without it, tests
// asserting rejection could pass simply because verification rejects everything.
func TestWeWork_VerifySignature_AcceptsValid(t *testing.T) {
	a := NewWeWorkAdapter()
	req, binding := weWorkRequest(t, "encrypted-payload-abc")

	if err := a.VerifySignature(req, binding); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestWeWork_VerifySignature_RejectsDuplicateQueryParameters(t *testing.T) {
	a := NewWeWorkAdapter()
	req, binding := weWorkRequest(t, "encrypted-payload-abc")
	query := req.URL.Query()
	query.Add("nonce", "second")
	req.URL.RawQuery = query.Encode()
	if err := a.VerifySignature(req, binding); err == nil {
		t.Fatal("duplicate nonce query parameter was accepted")
	}
}

func TestWeWorkVerifySignatureRejectsDeepXMLBeforeUnmarshal(t *testing.T) {
	const encrypt = "encrypted-payload-abc"
	req, binding := weWorkRequest(t, encrypt)
	opening := strings.Repeat("<nested>", maxWeWorkXMLDepth)
	closing := strings.Repeat("</nested>", maxWeWorkXMLDepth)
	req.Body = readCloser("<xml>" + opening + "<Encrypt>" + encrypt + "</Encrypt>" + closing + "</xml>")
	if err := NewWeWorkAdapter().VerifySignature(req, binding); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("deep XML error=%v, want nesting rejection", err)
	}
}

func TestDecodeWeWorkXMLRejectsMultipleDocumentsAndWrongRoot(t *testing.T) {
	for _, body := range []string{
		"<xml><Encrypt>x</Encrypt></xml><xml><Encrypt>x</Encrypt></xml>",
		"<callback><Encrypt>x</Encrypt></callback>",
	} {
		var destination weWorkMessageXML
		if err := decodeWeWorkXML([]byte(body), &destination); err == nil {
			t.Fatalf("decodeWeWorkXML accepted %q", body)
		}
	}
}

// TestWeWork_VerifySignature_BodyStillReadable guards a subtle break: the
// verifier consumes req.Body, so it must restore it or every downstream parse
// sees an empty body.
func TestWeWork_VerifySignature_BodyStillReadable(t *testing.T) {
	a := NewWeWorkAdapter()
	encrypt := "encrypted-payload-abc"
	req, binding := weWorkRequest(t, encrypt)

	if err := a.VerifySignature(req, binding); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	rest, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("re-read body: %v", err)
	}
	if !strings.Contains(string(rest), encrypt) {
		t.Errorf("body not restored after verification, got %q", string(rest))
	}
}

func TestWeWork_VerifySignature_RejectsAttacks(t *testing.T) {
	validEncrypt := "encrypted-payload-abc"

	tests := []struct {
		name   string
		attack func(t *testing.T) (*http.Request, *tenant.ChannelBinding)
	}{
		{
			// Forged signature with no knowledge of the shared token.
			name: "forged signature",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				setQuery(req, "msg_signature", strings.Repeat("a", 40))
				return req, b
			},
		},
		{
			// Payload swapped after signing: signature covers the old body.
			name: "payload tampered after signing",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				req.Body = readCloser(weWorkBody("malicious-payload-xyz"))
				return req, b
			},
		},
		{
			// Attacker signs with a token they control; server must use its own.
			name: "signature computed with wrong token",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, _ := weWorkRequest(t, validEncrypt)
				return req, &tenant.ChannelBinding{Token: "attacker-chosen-token"}
			},
		},
		{
			// Replay of a capture from outside the 300s window.
			name: "replay with stale timestamp",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				old := fmt.Sprintf("%d", time.Now().Add(-10*time.Minute).Unix())
				nonce := "test-nonce-1234"
				sig := signWeWork(testToken, old, nonce, validEncrypt)
				req, err := http.NewRequest(http.MethodPost,
					fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, old, nonce),
					strings.NewReader(weWorkBody(validEncrypt)))
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				return req, &tenant.ChannelBinding{Token: testToken}
			},
		},
		{
			// A future timestamp must not buy an unbounded replay window.
			name: "future timestamp beyond window",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				future := fmt.Sprintf("%d", time.Now().Add(10*time.Minute).Unix())
				nonce := "test-nonce-1234"
				sig := signWeWork(testToken, future, nonce, validEncrypt)
				req, err := http.NewRequest(http.MethodPost,
					fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, future, nonce),
					strings.NewReader(weWorkBody(validEncrypt)))
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				return req, &tenant.ChannelBinding{Token: testToken}
			},
		},
		{
			name: "non-numeric timestamp",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				setQuery(req, "timestamp", "not-a-number")
				return req, b
			},
		},
		{
			name: "missing signature parameter",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				setQuery(req, "msg_signature", "")
				return req, b
			},
		},
		{
			name: "missing nonce parameter",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				setQuery(req, "nonce", "")
				return req, b
			},
		},
		{
			name: "empty signature and empty token",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, _ := weWorkRequest(t, validEncrypt)
				setQuery(req, "msg_signature", "")
				return req, &tenant.ChannelBinding{Token: ""}
			},
		},
		{
			name: "malformed xml body",
			attack: func(t *testing.T) (*http.Request, *tenant.ChannelBinding) {
				req, b := weWorkRequest(t, validEncrypt)
				req.Body = readCloser("<xml><Encrypt>unclosed")
				return req, b
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewWeWorkAdapter()
			req, binding := tc.attack(t)

			if err := a.VerifySignature(req, binding); err == nil {
				t.Error("attack accepted: VerifySignature returned nil")
			}
		})
	}
}

// TestWeWork_VerifySignature_TimestampWindowBoundary pins the accepted window so
// it cannot be widened unnoticed.
func TestWeWork_VerifySignature_TimestampWindowBoundary(t *testing.T) {
	validEncrypt := "encrypted-payload-abc"

	tests := []struct {
		name       string
		offset     time.Duration
		wantAccept bool
	}{
		{"just inside past window", -299 * time.Second, true},
		{"just outside past window", -301 * time.Second, false},
		{"just inside future window", 299 * time.Second, true},
		{"just outside future window", 301 * time.Second, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := fmt.Sprintf("%d", time.Now().Add(tc.offset).Unix())
			nonce := "boundary-nonce"
			sig := signWeWork(testToken, ts, nonce, validEncrypt)

			req, err := http.NewRequest(http.MethodPost,
				fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, ts, nonce),
				strings.NewReader(weWorkBody(validEncrypt)))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			err = NewWeWorkAdapter().VerifySignature(req, &tenant.ChannelBinding{Token: testToken})
			if tc.wantAccept && err != nil {
				t.Errorf("offset %v rejected: %v", tc.offset, err)
			}
			if !tc.wantAccept && err == nil {
				t.Errorf("offset %v accepted, want rejection", tc.offset)
			}
		})
	}
}

func TestWeWork_VerifySignature_RejectsInt64TimestampExtremes(t *testing.T) {
	const encrypt = "encrypted-payload-abc"
	for _, timestamp := range []string{"-9223372036854775808", "9223372036854775807"} {
		t.Run(timestamp, func(t *testing.T) {
			nonce := "extreme-timestamp"
			sig := signWeWork(testToken, timestamp, nonce, encrypt)
			req, err := http.NewRequest(http.MethodPost,
				fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, timestamp, nonce),
				strings.NewReader(weWorkBody(encrypt)))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			if err := NewWeWorkAdapter().VerifySignature(req, &tenant.ChannelBinding{Token: testToken}); err == nil {
				t.Fatal("extreme timestamp was accepted")
			}
		})
	}
}

// TestWeWork_VerifySignature_UnconfiguredTokenFailsClosed covers the
// misconfiguration path. The signature only proves knowledge of the shared
// token, so an empty token lets any caller compute a signature the server
// accepts. Verification must reject the binding before comparing.
func TestWeWork_VerifySignature_UnconfiguredTokenFailsClosed(t *testing.T) {
	encrypt := "encrypted-payload-abc"
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	nonce := "unconfigured-nonce"

	// The attacker signs with the empty token they can guess is in use.
	sig := signWeWork("", timestamp, nonce, encrypt)

	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("/webhook?msg_signature=%s&timestamp=%s&nonce=%s", sig, timestamp, nonce),
		strings.NewReader(weWorkBody(encrypt)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	err = NewWeWorkAdapter().VerifySignature(req, &tenant.ChannelBinding{Token: ""})
	if err == nil {
		t.Fatal("unconfigured token accepted a self-signed webhook: fail-open")
	}
	if !strings.Contains(err.Error(), "no webhook token configured") {
		t.Errorf("rejected with %v, want an unconfigured-token error", err)
	}
}
