package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// S3Config is the connection_ref JSON for the official framework S3 artifact backend.
type S3Config struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token,omitempty"`
	PathStyle    *bool  `json:"path_style,omitempty"`
}

func ParseS3Config(value string) (S3Config, error) {
	var configuration S3Config
	if err := json.Unmarshal([]byte(value), &configuration); err != nil {
		return S3Config{}, fmt.Errorf("decode S3 configuration: %w", err)
	}
	if strings.TrimSpace(configuration.Bucket) == "" || strings.TrimSpace(configuration.AccessKey) == "" ||
		strings.TrimSpace(configuration.SecretKey) == "" {
		return S3Config{}, errors.New("S3 bucket, access_key, and secret_key are required")
	}
	if strings.TrimSpace(configuration.Region) == "" {
		configuration.Region = "us-east-1"
	}
	if endpoint := strings.TrimSpace(configuration.Endpoint); endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.RawQuery != "" {
			return S3Config{}, errors.New("S3 endpoint must be an absolute URL without a query")
		}
		configuration.Endpoint = strings.TrimRight(endpoint, "/")
	}
	return configuration, nil
}

func (c S3Config) usePathStyle() bool {
	if c.PathStyle != nil {
		return *c.PathStyle
	}
	return strings.TrimSpace(c.Endpoint) != ""
}
