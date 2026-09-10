// Package cosclient constructs authenticated COS clients for service backends.
package cosclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type credentials struct {
	SecretID  string `json:"secret_id"`
	SecretKey string `json:"secret_key"`
}

// New creates an authenticated COS client for a validated bucket endpoint.
func New(endpoint, credential string) (*cos.Client, error) {
	endpointURL, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	values, err := parseCredentials(credential)
	if err != nil {
		return nil, err
	}
	return cos.NewClient(&cos.BaseURL{BucketURL: endpointURL}, newHTTPClient(values)), nil
}

func newHTTPClient(values credentials) *http.Client {
	return &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  values.SecretID,
			SecretKey: values.SecretKey,
		},
	}
}

// ValidateEndpoint checks an operator-controlled COS bucket endpoint.
func ValidateEndpoint(value string) error {
	_, err := parseEndpoint(value)
	return err
}

func parseEndpoint(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("operator cos endpoint must be an absolute https url")
	}
	return parsed, nil
}

func parseCredentials(value string) (credentials, error) {
	var result credentials
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return credentials{}, fmt.Errorf("decode cos credentials: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return credentials{}, errors.New("decode cos credentials: multiple json values")
		}
		return credentials{}, fmt.Errorf("decode cos credentials: %w", err)
	}
	if result.SecretID == "" || result.SecretKey == "" {
		return credentials{}, errors.New("cos credentials require secret_id and secret_key")
	}
	return result, nil
}
