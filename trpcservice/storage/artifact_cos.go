package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// COSConfig is the connection_ref JSON for the official framework COS artifact backend.
type COSConfig struct {
	BucketURL string `json:"bucket_url"`
	SecretID  string `json:"secret_id"`
	SecretKey string `json:"secret_key"`
}

func ParseCOSConfig(value string) (COSConfig, error) {
	var configuration COSConfig
	if err := json.Unmarshal([]byte(value), &configuration); err != nil {
		return COSConfig{}, fmt.Errorf("decode COS configuration: %w", err)
	}
	if strings.TrimSpace(configuration.BucketURL) == "" || strings.TrimSpace(configuration.SecretID) == "" ||
		strings.TrimSpace(configuration.SecretKey) == "" {
		return COSConfig{}, errors.New("COS bucket_url, secret_id, and secret_key are required")
	}
	parsed, err := url.Parse(strings.TrimSpace(configuration.BucketURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.RawQuery != "" {
		return COSConfig{}, errors.New("COS bucket_url must be an absolute URL without a query")
	}
	configuration.BucketURL = strings.TrimRight(strings.TrimSpace(configuration.BucketURL), "/")
	return configuration, nil
}
