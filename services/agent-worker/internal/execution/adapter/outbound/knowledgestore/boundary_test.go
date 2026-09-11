package knowledgestore

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddingResponseRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte("fixture-private-key"))
	}))
	defer server.Close()
	req, _ := http.NewRequestWithContext(context.Background(), "POST", server.URL, nil)
	res, err := (redactedTransport{http.DefaultTransport}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 401 || strings.Contains(string(body), "fixture-private-key") || !strings.Contains(string(body), "provider_error") {
		t.Fatal("unredacted provider diagnostic")
	}
}
func TestVectorValidation(t *testing.T) {
	for _, v := range [][]float64{nil, {1, 2}, {math.NaN(), 1, 2}, {math.Inf(1), 1, 2}, {math.MaxFloat64, 1, 2}} {
		if validVector(v, 3) {
			t.Fatal("invalid vector accepted")
		}
	}
	if !validVector([]float64{1, 0, 0}, 3) {
		t.Fatal("valid vector rejected")
	}
}
