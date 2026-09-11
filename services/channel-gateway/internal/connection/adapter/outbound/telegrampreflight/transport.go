package telegrampreflight

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/go-telegram/bot/models"
	"github.com/gowebpki/jcs"
)

const (
	maxBodyBytes         = 64 << 10
	maxHeaderBytes       = 16 << 10
	maxSafeInteger int64 = 9007199254740991
)

var (
	envelopeFields = []string{"ok", "result", "description", "error_code", "parameters"}
	userFields     = sdkFieldNames(reflect.TypeFor[models.User]())
	webhookFields  = sdkFieldNames(reflect.TypeFor[models.WebhookInfo]())
)

type readOnlyTransport struct {
	next     http.RoundTripper
	token    string
	endpoint string
}

func invalidResponse() error { return &providerError{code: "PROVIDER_RESPONSE_INVALID"} }

func (t *readOnlyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	validOrigin := r != nil && r.URL != nil && ((t.endpoint == "" || t.endpoint == "https://api.telegram.org") && r.URL.Scheme == "https" && r.URL.Hostname() == "api.telegram.org" && (r.URL.Port() == "" || r.URL.Port() == "443") || t.endpoint == "http://channel-lab:8080" && r.URL.Scheme == "http" && r.URL.Host == "channel-lab:8080")
	if r == nil || r.URL == nil || r.Method != http.MethodPost || !validOrigin || (r.Host != "" && r.Host != r.URL.Host) || r.URL.User != nil || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.RawPath != "" || r.URL.Opaque != "" || !validToken(t.token) {
		return nil, invalidResponse()
	}
	method := strings.TrimPrefix(r.URL.Path, "/bot"+t.token+"/")
	if method != "getMe" && method != "getWebhookInfo" || r.URL.Path != "/bot"+t.token+"/"+method {
		return nil, invalidResponse()
	}
	if t.next == nil {
		return nil, invalidResponse()
	}
	r.Header.Set("Accept-Encoding", "identity")
	res, err := t.next.RoundTrip(r)
	if err != nil {
		if res != nil && res.Body != nil {
			_ = res.Body.Close()
		}
		code := "PROVIDER_NETWORK"
		var timeout net.Error
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
			code = "PROVIDER_TIMEOUT"
		}
		return nil, &providerError{code: code}
	}
	if res == nil || res.Body == nil {
		return nil, invalidResponse()
	}
	defer res.Body.Close()
	headerSize := len(res.Status) + 16
	for key, values := range res.Header {
		for _, value := range values {
			headerSize += len(key) + len(value) + 4
		}
	}
	if headerSize > maxHeaderBytes || res.Uncompressed || (res.Header.Get("Content-Encoding") != "" && res.Header.Get("Content-Encoding") != "identity") || res.ContentLength > maxBodyBytes {
		return nil, invalidResponse()
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBodyBytes+1))
	if err != nil {
		if r.Context().Err() != nil {
			return nil, &providerError{code: "PROVIDER_TIMEOUT"}
		}
		return nil, invalidResponse()
	}
	if len(body) > maxBodyBytes {
		return nil, invalidResponse()
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, &providerError{code: statusCode(res.StatusCode)}
	}
	if err = validateResponse(method, body); err != nil {
		return nil, err
	}
	copy := *res
	copy.Body = io.NopCloser(bytes.NewReader(body))
	copy.ContentLength = int64(len(body))
	return &copy, nil
}

func statusCode(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "TOKEN_REJECTED"
	case http.StatusTooManyRequests:
		return "PROVIDER_RATE_LIMITED"
	default:
		return "PROVIDER_UNAVAILABLE"
	}
}

// Require the fields whose zero-values could otherwise fabricate a successful
// diagnostic. Unknown provider fields remain forward-compatible, but duplicate
// keys, malformed/trailing JSON and non-finite numbers are rejected by JCS.
func validateResponse(method string, body []byte) error {
	if !utf8.Valid(body) {
		return invalidResponse()
	}
	if _, err := jcs.Transform(body); err != nil {
		return invalidResponse()
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil || envelope == nil {
		return invalidResponse()
	}
	if !exactKnownNames(envelope, envelopeFields) {
		return invalidResponse()
	}
	ok, valid := boolField(envelope, "ok")
	if !valid {
		return invalidResponse()
	}
	if !ok {
		code, valid := intField(envelope, "error_code", 100, 599)
		if !valid {
			return invalidResponse()
		}
		return &providerError{code: statusCode(int(code))}
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(envelope["result"], &result) != nil || result == nil {
		return invalidResponse()
	}
	if method == "getMe" {
		if !exactKnownNames(result, userFields) {
			return invalidResponse()
		}
		if _, valid = intField(result, "id", 1, maxSafeInteger); !valid {
			return invalidResponse()
		}
		if _, valid = boolField(result, "is_bot"); !valid {
			return invalidResponse()
		}
		if _, valid = stringField(result, "first_name"); !valid {
			return invalidResponse()
		}
		return nil
	}
	if !exactKnownNames(result, webhookFields) {
		return invalidResponse()
	}
	if _, valid = stringField(result, "url"); !valid {
		return invalidResponse()
	}
	if _, valid = boolField(result, "has_custom_certificate"); !valid {
		return invalidResponse()
	}
	if _, valid = intField(result, "pending_update_count", 0, maxSafeInteger); !valid {
		return invalidResponse()
	}
	if _, exists := result["last_error_date"]; exists {
		if _, valid = intField(result, "last_error_date", 0, 253402300799); !valid {
			return invalidResponse()
		}
	}
	if _, exists := result["last_error_message"]; exists {
		if _, valid = stringField(result, "last_error_message"); !valid {
			return invalidResponse()
		}
	}
	return nil
}

// encoding/json's struct decoder accepts case-insensitive aliases, unlike the
// exact map validation above. Reject aliases for every SDK-consumed field before
// handing it the original JSON; otherwise URL or ID can override a checked value.
// Unrelated provider extensions remain allowed, including their nested fields.
func exactKnownNames(fields map[string]json.RawMessage, known []string) bool {
	for key := range fields {
		for _, canonical := range known {
			if key != canonical && strings.EqualFold(key, canonical) {
				return false
			}
		}
	}
	return true
}

// Derive names from the pinned SDK rather than maintaining an incomplete copy
// of its optional fields. These two SDK models contain direct JSON-tagged fields.
func sdkFieldNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			names = append(names, name)
		}
	}
	return names
}

func boolField(fields map[string]json.RawMessage, key string) (bool, bool) {
	raw, exists := fields[key]
	return bytes.Equal(raw, []byte("true")), exists && (bytes.Equal(raw, []byte("true")) || bytes.Equal(raw, []byte("false")))
}

func stringField(fields map[string]json.RawMessage, key string) (string, bool) {
	raw, exists := fields[key]
	var value string
	if !exists || len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func intField(fields map[string]json.RawMessage, key string, min, max int64) (int64, bool) {
	raw, exists := fields[key]
	var value int64
	if !exists || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &value) != nil || value < min || value > max {
		return 0, false
	}
	return value, true
}
