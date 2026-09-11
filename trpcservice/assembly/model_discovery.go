package assembly

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const defaultDiscoveryTimeout = 8 * time.Second

// openAIModelsResponse matches the standard OpenAI GET /models response.
type openAIModelsResponse struct {
	Object string `json:"object"`
	Data   []struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	} `json:"data"`
}

// DiscoverOpenAIModels queries the provider's /models endpoint to dynamically
// list available model IDs.
func DiscoverOpenAIModels(ctx context.Context, httpClient *http.Client, baseURL, apiKey string) ([]string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, fmt.Errorf("base_url is required for model discovery")
	}

	endpoint := strings.TrimRight(baseURL, "/") + "/models"
	ctx, cancel := context.WithTimeout(ctx, defaultDiscoveryTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create discovery request for %q: %w", endpoint, err)
	}

	if strings.TrimSpace(apiKey) != "" {
		request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "trpc-agent-service-discovery/1.0")

	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("discover models from %q: %w", endpoint, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		bodySnippet, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return nil, fmt.Errorf("discover models from %q returned status %d: %s", endpoint, response.StatusCode, strings.TrimSpace(string(bodySnippet)))
	}

	var payload openAIModelsResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode discovery response from %q: %w", endpoint, err)
	}

	unique := make(map[string]struct{}, len(payload.Data))
	results := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		name := strings.TrimSpace(item.ID)
		if name == "" {
			continue
		}
		if _, exists := unique[name]; exists {
			continue
		}
		unique[name] = struct{}{}
		results = append(results, name)
	}

	sort.Strings(results)
	return results, nil
}
