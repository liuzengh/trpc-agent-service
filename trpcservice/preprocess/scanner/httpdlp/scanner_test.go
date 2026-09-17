package httpdlp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestScannerContract(t *testing.T) {
	authorized := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/healthz":
			return response(http.StatusNoContent, ""), nil
		case "/v1/media:scan":
			if request.Header.Get("Authorization") != "Bearer scoped" {
				return response(http.StatusUnauthorized, ""), nil
			}
			return response(http.StatusOK, "{\"verdict\":\"clean\",\"policy_version\":\"policy-7\"}"), nil
		default:
			return response(http.StatusNotFound, ""), nil
		}
	})}
	scanner := Scanner{Endpoint: "https://dlp.invalid", Client: client, ProbeTenantID: "tenant-a",
		Authorize: func(_ context.Context, tenantID string, request *http.Request) error {
			if tenantID != "tenant-a" {
				return runtime.ErrTenantScope
			}
			authorized++
			request.Header.Set("Authorization", "Bearer scoped")
			return nil
		}}
	if err := scanner.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if authorized != 1 {
		t.Fatalf("probe authorizer calls=%d", authorized)
	}
	result, err := scanner.ScanMediaInput(context.Background(), "tenant-a", []byte("safe"), "text/plain")
	if err != nil || result.Verdict != preprocess.ScanClean || result.Version != "dlp:policy-7" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := scanner.ScanMediaInput(context.Background(), "tenant-b", []byte("safe"), "text/plain"); err != runtime.ErrTenantScope {
		t.Fatalf("tenant scope err=%v", err)
	}
}

func TestScannerFailsClosed(t *testing.T) {
	responses := []string{
		"{\"verdict\":\"unknown\",\"policy_version\":\"policy-7\"}",
		"{\"verdict\":\"clean\",\"policy_version\":\"\"}",
		"{\"verdict\":\"clean\",\"policy_version\":\"policy-7\",\"extra\":true}",
	}
	for _, body := range responses {
		t.Run(body, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(http.StatusOK, body), nil
			})}
			scanner := Scanner{Endpoint: "https://dlp.invalid", Client: client,
				Authorize: func(context.Context, string, *http.Request) error { return nil }}
			if _, err := scanner.ScanMediaInput(context.Background(), "tenant-a", []byte("safe"), "text/plain"); err != runtime.ErrBackendUnavailable {
				t.Fatalf("body=%s err=%v", body, err)
			}
		})
	}
	scanner := Scanner{Endpoint: "http://scanner.invalid",
		Authorize: func(context.Context, string, *http.Request) error { return nil }}
	if _, err := scanner.ScanMediaInput(context.Background(), "tenant-a", []byte("safe"), "text/plain"); err != runtime.ErrInvalidEnvelope {
		t.Fatalf("insecure endpoint err=%v", err)
	}
}

func TestScannerRequiresAuthorizer(t *testing.T) {
	scanner := Scanner{Endpoint: "https://dlp.invalid", Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, ""), nil
	})}}
	if _, err := scanner.ScanMediaInput(context.Background(), "tenant-a", []byte("safe"), "text/plain"); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("authorizer err=%v", err)
	}
	scanner.ProbeTenantID = "tenant-a"
	if err := scanner.Probe(context.Background()); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("probe authorizer err=%v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
