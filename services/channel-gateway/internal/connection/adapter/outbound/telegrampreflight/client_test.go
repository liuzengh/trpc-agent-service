package telegrampreflight

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

const fixtureToken = "123:synthetic_preflight_fixture"
const identityJSON = `{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"fixture"}}`

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func webhookJSON(target string, pending int64, at int64, message string) string {
	b, _ := json.Marshal(map[string]any{"ok": true, "result": map[string]any{
		"url": target, "has_custom_certificate": false, "pending_update_count": pending,
		"last_error_date": at, "last_error_message": message,
	}})
	return string(b)
}

func request(expected *string) app.ProbeRequest {
	return app.ProbeRequest{Token: app.NewSecret(fixtureToken), ExpectedIdentity: "123", ExpectedWebhook: expected}
}

func testClient(t *testing.T, identity, webhook string) (*Client, *[]string) {
	t.Helper()
	methods := []string{}
	c := &Client{transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "api.telegram.org" || r.Method != http.MethodPost || r.Header.Get("Accept-Encoding") != "identity" {
			t.Fatal("unexpected network request")
		}
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > callTimeout {
			t.Fatal("missing bounded request deadline")
		}
		method := strings.TrimPrefix(r.URL.Path, "/bot"+fixtureToken+"/")
		methods = append(methods, method)
		if method == "getMe" {
			return response(identity), nil
		}
		if method != "getWebhookInfo" {
			t.Fatal("write method reached network")
		}
		return response(webhook), nil
	})}
	return c, &methods
}

func TestConstructionAndCloseAreLocal(t *testing.T) {
	var requests, closes int
	c := &Client{transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("unexpected call")
	}), closeIdle: func() { closes++ }}
	if _, err := c.newBot(fixtureToken); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if requests != 0 || closes != 1 {
		t.Fatalf("requests=%d closes=%d", requests, closes)
	}
}

func TestIdentityAndWebhookBranches(t *testing.T) {
	expected := "https://gateway.example/v1/telegram/cha_test"
	for _, tt := range []struct {
		name, target, code, relation string
		expected                     *string
		pending                      int64
		errorAt                      int64
		message                      string
	}{
		{"match", expected, "WEBHOOK_MATCH", "MATCH", &expected, 0, 0, ""},
		{"different", "http://169.254.169.254/private-secret", "WEBHOOK_DIFFERENT", "DIFFERENT", &expected, 2, 1720000000, "https://user:secret@private/secret-path"},
		{"none", "", "WEBHOOK_NONE", "NONE", &expected, 0, 0, ""},
		{"invalid-origin-existing", "http://localhost/private-secret", "WEBHOOK_COMPARISON_UNAVAILABLE", "UNKNOWN", nil, 3, 0, "past-error-secret"},
		{"invalid-origin-none", "", "WEBHOOK_NONE", "NONE", nil, 0, 0, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, methods := testClient(t, identityJSON, webhookJSON(tt.target, tt.pending, tt.errorAt, tt.message))
			out, err := c.Inspect(context.Background(), request(tt.expected))
			if err != nil || out.IdentityCode != "BOT_IDENTITY_MATCH" || out.IdentityMatch == nil || !*out.IdentityMatch || out.WebhookCode != tt.code || out.Relation != tt.relation || out.Presence == nil || *out.Presence != (tt.target != "") || out.PendingUpdates == nil || *out.PendingUpdates != tt.pending || out.HasLastError == nil || *out.HasLastError != (tt.message != "" || tt.errorAt > 0) {
				t.Fatalf("unexpected declassified result: %+v err=%v", out, err)
			}
			if tt.errorAt > 0 && (out.LastErrorAt == nil || out.LastErrorAt.Unix() != tt.errorAt || out.LastErrorAt.Location() != time.UTC) {
				t.Fatal("error timestamp missing or not UTC")
			}
			if tt.errorAt == 0 && out.LastErrorAt != nil {
				t.Fatal("invented timestamp")
			}
			if strings.Join(*methods, ",") != "getMe,getWebhookInfo" {
				t.Fatal(*methods)
			}
			raw, _ := json.Marshal(out)
			for _, forbidden := range []string{fixtureToken, "private-secret", "secret-path", "past-error-secret"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatal("sensitive observation escaped adapter")
				}
			}
		})
	}
}

func TestIdentityFailureNeverReadsWebhook(t *testing.T) {
	for _, tt := range []struct{ name, body, code string }{
		{"wrong-id", `{"ok":true,"result":{"id":999,"is_bot":true,"first_name":"other-secret-name"}}`, "BOT_IDENTITY_MISMATCH"},
		{"non-bot", `{"ok":true,"result":{"id":123,"is_bot":false,"first_name":"person"}}`, "BOT_IDENTITY_MISMATCH"},
		{"unauthorized", `{"ok":false,"error_code":401,"description":"secret"}`, "TOKEN_REJECTED"},
		{"missing-id", `{"ok":true,"result":{"is_bot":true,"first_name":"fixture"}}`, "PROVIDER_RESPONSE_INVALID"},
		{"missing-is-bot", `{"ok":true,"result":{"id":123,"first_name":"fixture"}}`, "PROVIDER_RESPONSE_INVALID"},
		{"null-id", `{"ok":true,"result":{"id":null,"is_bot":true,"first_name":"fixture"}}`, "PROVIDER_RESPONSE_INVALID"},
		{"missing-first-name", `{"ok":true,"result":{"id":123,"is_bot":true}}`, "PROVIDER_RESPONSE_INVALID"},
		{"invalid-sdk-field", `{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"fixture","username":false}}`, "PROVIDER_RESPONSE_INVALID"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, methods := testClient(t, tt.body, "")
			out, err := c.Inspect(context.Background(), request(nil))
			if err != nil || out.IdentityCode != tt.code || out.WebhookCode != "NOT_EXECUTED" || out.Presence != nil || out.PendingUpdates != nil || out.HasLastError != nil || len(*methods) != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", out, err, len(*methods))
			}
		})
	}
}

func TestMalformedStoredTokenIsLocalFailureNotClaimError(t *testing.T) {
	for _, token := range []string{"", "opaque-not-a-token", "123:secret/path", "123:secret?query", "123:secret#fragment", "123:secret@host", "123:secret\n", "0:secret", "123:", strings.Repeat("x", (16<<10)+1)} {
		c, methods := testClient(t, identityJSON, "")
		req := request(nil)
		req.Token = app.NewSecret(token)
		out, err := c.Inspect(context.Background(), req)
		if err != nil || out.IdentityCode != "TOKEN_REJECTED" || out.IdentityMatch != nil || out.WebhookCode != "NOT_EXECUTED" || out.Presence != nil || out.PendingUpdates != nil || out.HasLastError != nil || len(*methods) != 0 {
			t.Fatalf("out=%+v err=%v calls=%d", out, err, len(*methods))
		}
	}
}

func TestLargeSavedIdentityIsComparedWithoutNumericCoercion(t *testing.T) {
	c, methods := testClient(t, identityJSON, "")
	req := request(nil)
	req.ExpectedIdentity = strings.Repeat("9", 1024)
	out, err := c.Inspect(context.Background(), req)
	if err != nil || out.IdentityCode != "BOT_IDENTITY_MISMATCH" || out.IdentityMatch == nil || *out.IdentityMatch || len(*methods) != 1 {
		t.Fatalf("out=%+v err=%v calls=%d", out, err, len(*methods))
	}
}

func TestMalformedWebhookCannotFabricateZeroBacklog(t *testing.T) {
	for _, body := range []string{
		`{"ok":true,"result":{}}`,
		`{"ok":true,"result":null}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":null}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":-1}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":9007199254740992}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0.5}}`,
		`{"ok":true,"result":{"url":null,"has_custom_certificate":false,"pending_update_count":0}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":null,"pending_update_count":0}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"last_error_date":-1}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"last_error_date":253402300800}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"last_error_message":{}}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"pending_update_count":1}}`,
		`{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0}} {}`,
		`{"ok":true,"result":`,
		string([]byte{0xff}),
	} {
		c, _ := testClient(t, identityJSON, body)
		out, err := c.Inspect(context.Background(), request(nil))
		if err != nil || out.IdentityCode != "BOT_IDENTITY_MATCH" || out.WebhookCode != "PROVIDER_RESPONSE_INVALID" || out.Presence != nil || out.PendingUpdates != nil || out.HasLastError != nil || out.LastErrorAt != nil {
			t.Fatalf("invalid response accepted: %+v err=%v", out, err)
		}
	}
}

func TestSDKCaseAliasesCannotOverrideValidatedFields(t *testing.T) {
	expected := "https://gateway.example/v1/telegram/cha_test"
	for _, tt := range []struct {
		name, identity, webhook string
		identityFailure         bool
	}{
		{"url-conflict", identityJSON, `{"ok":true,"result":{"url":"https://old.example/private","URL":"` + expected + `","has_custom_certificate":false,"pending_update_count":0}}`, false},
		{"url-alias-only", identityJSON, `{"ok":true,"result":{"URL":"` + expected + `","has_custom_certificate":false,"pending_update_count":0}}`, false},
		{"result-conflict", `{"ok":true,"result":{"id":999,"is_bot":true,"first_name":"other"},"RESULT":{"id":123,"is_bot":true,"first_name":"fixture"}}`, "", true},
		{"result-alias-only", `{"ok":true,"RESULT":{"id":123,"is_bot":true,"first_name":"fixture"}}`, "", true},
		{"id-conflict", `{"ok":true,"result":{"id":999,"ID":123,"is_bot":true,"first_name":"fixture"}}`, "", true},
		{"id-alias-only", `{"ok":true,"result":{"ID":123,"is_bot":true,"first_name":"fixture"}}`, "", true},
		{"last-error-conflict", identityJSON, `{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"last_error_date":1720000000,"LAST_ERROR_DATE":0}}`, false},
		{"last-error-alias-only", identityJSON, `{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"LAST_ERROR_DATE":1720000000}}`, false},
		{"pending-conflict", identityJSON, `{"ok":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":7,"PENDING_UPDATE_COUNT":0}}`, false},
		{"pending-alias-only", identityJSON, `{"ok":true,"result":{"url":"","has_custom_certificate":false,"PENDING_UPDATE_COUNT":0}}`, false},
		{"ok-conflict", `{"ok":true,"OK":true,"result":{"id":123,"is_bot":true,"first_name":"fixture"}}`, "", true},
		{"optional-sdk-field-alias", `{"ok":true,"result":{"id":123,"is_bot":true,"first_name":"fixture","USERNAME":"alias"}}`, "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, calls := testClient(t, tt.identity, tt.webhook)
			out, err := c.Inspect(context.Background(), request(&expected))
			if err != nil {
				t.Fatal(err)
			}
			if tt.identityFailure {
				if out.IdentityCode != "PROVIDER_RESPONSE_INVALID" || out.IdentityMatch != nil || len(*calls) != 1 {
					t.Fatalf("case alias overrode identity validation: %+v", out)
				}
			} else if out.IdentityCode != "BOT_IDENTITY_MATCH" || out.WebhookCode != "PROVIDER_RESPONSE_INVALID" || len(*calls) != 2 {
				t.Fatalf("case alias overrode webhook validation: %+v", out)
			}
			if out.Presence != nil || out.PendingUpdates != nil || out.HasLastError != nil || out.LastErrorAt != nil {
				t.Fatal("ambiguous provider fields produced observations")
			}
		})
	}
}

func TestUnknownProviderFieldsRemainForwardCompatible(t *testing.T) {
	identity := `{"ok":true,"future_envelope":{"v":1},"result":{"id":123,"is_bot":true,"first_name":"fixture","future_user":{"ID":"not-an-identity"}}}`
	webhook := `{"ok":true,"future_envelope_flag":true,"result":{"url":"","has_custom_certificate":false,"pending_update_count":0,"future_webhook":{"URL":"not-a-network-target"}}}`
	c, calls := testClient(t, identity, webhook)
	out, err := c.Inspect(context.Background(), request(nil))
	if err != nil || out.IdentityCode != "BOT_IDENTITY_MATCH" || out.WebhookCode != "WEBHOOK_NONE" || len(*calls) != 2 {
		t.Fatalf("unknown provider extension rejected: %+v err=%v", out, err)
	}
}

func TestSecondRequestFailureAndNoRetry(t *testing.T) {
	for _, tt := range []struct {
		status int
		code   string
	}{
		{401, "TOKEN_REJECTED"}, {403, "PROVIDER_UNAVAILABLE"},
		{429, "PROVIDER_RATE_LIMITED"}, {503, "PROVIDER_UNAVAILABLE"}, {302, "PROVIDER_UNAVAILABLE"},
	} {
		t.Run(fmt.Sprint(tt.status), func(t *testing.T) {
			calls := 0
			c := &Client{transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return response(identityJSON), nil
				}
				r := response("secret-body")
				r.StatusCode = tt.status
				r.Header.Set("Location", "https://private.example/secret")
				r.Header.Set("Retry-After", "3600")
				return r, nil
			})}
			out, err := c.Inspect(context.Background(), request(nil))
			if err != nil || calls != 2 || out.IdentityCode != "BOT_IDENTITY_MATCH" || out.WebhookCode != tt.code || out.Presence != nil || out.Relation != "UNKNOWN" || out.PendingUpdates != nil || out.HasLastError != nil {
				t.Fatalf("out=%+v calls=%d err=%v", out, calls, err)
			}
		})
	}
}

func TestRequestAllowlist(t *testing.T) {
	calls := 0
	tr := &readOnlyTransport{token: fixtureToken, next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return response(identityJSON), nil
	})}
	for _, method := range []string{"setWebhook", "deleteWebhook", "getUpdates", "sendMessage", "close", "logOut", "getMe/other"} {
		r, _ := http.NewRequest(http.MethodPost, "https://api.telegram.org/bot"+fixtureToken+"/"+method, nil)
		if _, err := tr.RoundTrip(r); err == nil {
			t.Fatal("forbidden method accepted")
		}
	}
	for _, target := range []string{
		"http://api.telegram.org/bot" + fixtureToken + "/getMe",
		"https://127.0.0.1/bot" + fixtureToken + "/getMe",
		"https://api.telegram.org:444/bot" + fixtureToken + "/getMe",
		"https://api.telegram.org/bot" + fixtureToken + "/getMe?redirect=secret",
		"https://user:secret@api.telegram.org/bot" + fixtureToken + "/getMe",
		"https://api.telegram.org/bot" + fixtureToken + "/getMe#secret",
	} {
		r, _ := http.NewRequest(http.MethodPost, target, nil)
		if _, err := tr.RoundTrip(r); err == nil {
			t.Fatal("forbidden target accepted")
		}
	}
	r, _ := http.NewRequest(http.MethodGet, "https://api.telegram.org/bot"+fixtureToken+"/getMe", nil)
	if _, err := tr.RoundTrip(r); err == nil {
		t.Fatal("GET accepted")
	}
	if calls != 0 {
		t.Fatal("forbidden request reached transport")
	}
}

func TestResponseBounds(t *testing.T) {
	for _, mutate := range []func(*http.Response){
		func(r *http.Response) { r.Header.Set("Content-Encoding", "gzip") },
		func(r *http.Response) { r.Uncompressed = true },
		func(r *http.Response) { r.Header.Set("X-Huge", strings.Repeat("x", maxHeaderBytes)) },
		func(r *http.Response) { r.ContentLength = maxBodyBytes + 1 },
		func(r *http.Response) {
			r.ContentLength = -1
			r.Body = io.NopCloser(strings.NewReader(identityJSON + strings.Repeat(" ", maxBodyBytes)))
		},
		func(r *http.Response) { r.Body = failingBody{} },
	} {
		c := &Client{transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			r := response(identityJSON)
			mutate(r)
			return r, nil
		})}
		out, err := c.Inspect(context.Background(), request(nil))
		if err != nil || out.IdentityCode != "PROVIDER_RESPONSE_INVALID" {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	}
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failingBody) Close() error             { return errors.New("close-secret") }

func TestErrorsAndLogsAreDeclassified(t *testing.T) {
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	for _, rt := range []roundTripFunc{
		func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("proxy-password secret-path %s", fixtureToken)
		},
		func(*http.Request) (*http.Response, error) {
			return response(`{"ok":false,"error_code":401,"description":"` + fixtureToken + `"}`), nil
		},
		func(*http.Request) (*http.Response, error) {
			r := response(identityJSON)
			r.Body = failingBody{}
			return r, nil
		},
	} {
		c := &Client{transport: rt}
		out, err := c.Inspect(context.Background(), request(nil))
		raw, _ := json.Marshal(out)
		all := string(raw) + fmt.Sprint(err) + logs.String()
		for _, secret := range []string{fixtureToken, "proxy-password", "secret-path", "close-secret"} {
			if strings.Contains(all, secret) {
				t.Fatal("external text escaped")
			}
		}
	}
	if logs.Len() != 0 {
		t.Fatal("SDK logged error text")
	}
}

func TestCancellationAndConcurrentInspect(t *testing.T) {
	var calls atomic.Int64
	c := &Client{transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Inspect(ctx, request(nil)); !errors.Is(err, app.ErrExpired) || calls.Load() != 0 {
		t.Fatal("canceled task started I/O")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			out, err := c.Inspect(ctx, request(nil))
			if err != nil || out.IdentityCode != "PROVIDER_TIMEOUT" {
				t.Errorf("out=%+v err=%v", out, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 4 {
		t.Fatal("unexpected retry count")
	}
}

func TestRealTLSViaPrivateTransportSeam(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("unexpected request")
		}
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			_, _ = io.WriteString(w, identityJSON)
			return
		}
		_, _ = io.WriteString(w, webhookJSON("http://169.254.169.254/secret", 1, 0, ""))
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	base := server.Client().Transport
	c := &Client{transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "api.telegram.org" {
			t.Fatal("production destination was altered before allowlist")
		}
		copy := r.Clone(r.Context())
		u := *r.URL
		u.Host = endpoint.Host
		copy.URL = &u
		copy.Host = endpoint.Host
		return base.RoundTrip(copy)
	})}
	out, err := c.Inspect(context.Background(), request(nil))
	if err != nil || out.IdentityCode != "BOT_IDENTITY_MATCH" || out.WebhookCode != "WEBHOOK_COMPARISON_UNAVAILABLE" || calls.Load() != 2 {
		t.Fatalf("out=%+v err=%v calls=%d", out, err, calls.Load())
	}
}
