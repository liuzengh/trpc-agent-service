// This is a disposable Compose acceptance issuer. It generates an ephemeral
// RSA key on every container start and is never a production identity source.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"
)

var signingKey *rsa.PrivateKey

const issuer = "http://oidc-fixture:8081"

func main() {
	var err error
	signingKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/jwks", jwks)
	http.HandleFunc("/token", token)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := http.ListenAndServe(":8081", nil); err != nil {
		panic(err)
	}
}

func jwks(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": "acceptance-1", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(signingKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
	}}})
}

func token(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	role := query.Get("role")
	if role == "" {
		role = "viewer"
	}
	tenant := query.Get("tenant")
	if tenant == "" {
		tenant = "acme"
	}
	claims := map[string]any{"iss": issuer, "aud": "trpc-admin", "sub": "acceptance-" + role,
		"roles": []string{role}, "tenants": []string{tenant}, "exp": time.Now().Add(10 * time.Minute).Unix()}
	value, err := sign(claims)
	if err != nil {
		http.Error(w, "token unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(value))
}

func sign(claims map[string]any) (string, error) {
	encode := func(value any) (string, error) {
		data, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return base64.RawURLEncoding.EncodeToString(data), nil
	}
	header, err := encode(map[string]string{"alg": "RS256", "kid": "acceptance-1", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	body, err := encode(claims)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(header + "." + body))
	signature, err := rsa.SignPKCS1v15(rand.Reader, signingKey, 5, digest[:])
	if err != nil {
		return "", err
	}
	return header + "." + body + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
