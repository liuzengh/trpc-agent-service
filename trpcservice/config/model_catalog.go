package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type ModelInputKind string

const (
	ModelInputImage ModelInputKind = "image"
	ModelInputAudio ModelInputKind = "audio"
	ModelInputFile  ModelInputKind = "file"
)

// ModelCatalog is the single mutable source of truth for platform-managed
// model providers and dynamically discovered model IDs. Static provider
// configuration is immutable after construction; only discovered IDs change.
type ModelCatalog struct {
	mu         sync.RWMutex
	providers  map[string]ModelProviderConfig
	order      []string
	discovered map[string]map[string]struct{}
}

func NewModelCatalog(providers []ModelProviderConfig) (*ModelCatalog, error) {
	catalog := &ModelCatalog{
		providers:  make(map[string]ModelProviderConfig, len(providers)),
		order:      make([]string, 0, len(providers)),
		discovered: make(map[string]map[string]struct{}),
	}
	for _, provider := range providers {
		id := strings.TrimSpace(provider.ID)
		if id == "" {
			return nil, fmt.Errorf("model provider id is required")
		}
		if _, exists := catalog.providers[id]; exists {
			return nil, fmt.Errorf("duplicate model provider %q", id)
		}
		provider = cloneModelProviderConfig(provider)
		provider.ID = id
		for index := range provider.Models {
			provider.Models[index].Name = strings.TrimSpace(provider.Models[index].Name)
		}
		catalog.providers[id] = provider
		catalog.order = append(catalog.order, id)
	}
	return catalog, nil
}

func (c *ModelCatalog) Providers() []ModelProviderConfig {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	providers := make([]ModelProviderConfig, 0, len(c.order))
	for _, id := range c.order {
		providers = append(providers, cloneModelProviderConfig(c.providers[id]))
	}
	return providers
}

func (c *ModelCatalog) Provider(providerID string) (ModelProviderConfig, bool) {
	if c == nil {
		return ModelProviderConfig{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	provider, ok := c.providers[strings.TrimSpace(providerID)]
	if !ok {
		return ModelProviderConfig{}, false
	}
	return cloneModelProviderConfig(provider), true
}

func (c *ModelCatalog) AllowsModel(providerID, modelName string) bool {
	providerID = strings.TrimSpace(providerID)
	modelName = strings.TrimSpace(modelName)
	if c == nil || providerID == "" || modelName == "" {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	provider, ok := c.providers[providerID]
	if !ok {
		return false
	}
	for _, configured := range provider.Models {
		if strings.TrimSpace(configured.Name) == modelName {
			return true
		}
	}
	_, ok = c.discovered[providerID][modelName]
	return ok
}

// ValidateInputs fails before a provider call when any model that may be used
// by the configured failover chain has not explicitly declared support for a
// required non-text input.
func (c *ModelCatalog) ValidateInputs(model ModelConfig, required []ModelInputKind) error {
	if len(required) == 0 {
		return nil
	}
	if c == nil {
		return errors.New("model catalog is required for non-text input validation")
	}
	models := []ModelCandidate{{ProviderID: model.ProviderID, Name: model.Name}}
	models = append(models, model.FailoverCandidates...)
	seenKinds := make(map[ModelInputKind]struct{}, len(required))
	for _, kind := range required {
		seenKinds[kind] = struct{}{}
	}
	for _, candidate := range models {
		capabilities, ok := c.capabilities(candidate.ProviderID, candidate.Name)
		if !ok {
			return fmt.Errorf("model %q/%q has no declared input capabilities", candidate.ProviderID, candidate.Name)
		}
		for kind := range seenKinds {
			if supportsInput(capabilities, kind) {
				continue
			}
			return fmt.Errorf("model %q/%q does not support %s input", candidate.ProviderID, candidate.Name, kind)
		}
	}
	return nil
}

// GuaranteedInputCapabilities returns the non-text inputs supported by every
// model in the configured primary/failover chain. Missing declarations remain
// unknown rather than being inferred from provider protocol compatibility.
func (c *ModelCatalog) GuaranteedInputCapabilities(model ModelConfig) (ModelInputCapabilities, bool) {
	candidates := []ModelCandidate{{ProviderID: model.ProviderID, Name: model.Name}}
	candidates = append(candidates, model.FailoverCandidates...)
	if c == nil || len(candidates) == 0 {
		return ModelInputCapabilities{}, false
	}
	var guaranteed ModelInputCapabilities
	for index, candidate := range candidates {
		capabilities, ok := c.capabilities(candidate.ProviderID, candidate.Name)
		if !ok || capabilities == nil || capabilities.Input == nil {
			return ModelInputCapabilities{}, false
		}
		if index == 0 {
			guaranteed = *capabilities.Input
			continue
		}
		guaranteed.Image = guaranteed.Image && capabilities.Input.Image
		guaranteed.Audio = guaranteed.Audio && capabilities.Input.Audio
		guaranteed.File = guaranteed.File && capabilities.Input.File
	}
	return guaranteed, true
}

func (c *ModelCatalog) capabilities(providerID, modelName string) (*ModelCapabilities, bool) {
	providerID = strings.TrimSpace(providerID)
	modelName = strings.TrimSpace(modelName)
	if providerID == "" || modelName == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	provider, ok := c.providers[providerID]
	if !ok {
		return nil, false
	}
	for _, configured := range provider.Models {
		if strings.TrimSpace(configured.Name) != modelName {
			continue
		}
		if configured.Capabilities == nil || configured.Capabilities.Input == nil {
			return configured.Capabilities, true
		}
		copyCapabilities := *configured.Capabilities
		copyInput := *configured.Capabilities.Input
		copyCapabilities.Input = &copyInput
		return &copyCapabilities, true
	}
	// Dynamically discovered models have no trustworthy capability metadata.
	if _, discovered := c.discovered[providerID][modelName]; discovered {
		return nil, true
	}
	return nil, false
}

func supportsInput(capabilities *ModelCapabilities, kind ModelInputKind) bool {
	if capabilities == nil || capabilities.Input == nil {
		return false
	}
	switch kind {
	case ModelInputImage:
		return capabilities.Input.Image
	case ModelInputAudio:
		return capabilities.Input.Audio
	case ModelInputFile:
		return capabilities.Input.File
	default:
		return false
	}
}

func (c *ModelCatalog) ReplaceDiscoveredModels(providerID string, models []string) {
	if c == nil {
		return
	}
	providerID = strings.TrimSpace(providerID)
	set := make(map[string]struct{}, len(models))
	for _, modelName := range models {
		if name := strings.TrimSpace(modelName); name != "" {
			set[name] = struct{}{}
		}
	}
	c.mu.Lock()
	if _, exists := c.providers[providerID]; !exists {
		c.mu.Unlock()
		return
	}
	c.discovered[providerID] = set
	c.mu.Unlock()
}

func (c *ModelCatalog) RemoveDiscoveredModel(providerID, modelName string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if set := c.discovered[strings.TrimSpace(providerID)]; set != nil {
		delete(set, strings.TrimSpace(modelName))
	}
	c.mu.Unlock()
}

func (c *ModelCatalog) ListDiscoveredModels(providerID string) []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	set := c.discovered[strings.TrimSpace(providerID)]
	models := make([]string, 0, len(set))
	for modelName := range set {
		models = append(models, modelName)
	}
	c.mu.RUnlock()
	sort.Strings(models)
	return models
}

func (c *ModelCatalog) snapshot() (map[string]ModelProviderConfig, map[string]map[string]struct{}) {
	if c == nil {
		return nil, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	providers := make(map[string]ModelProviderConfig, len(c.providers))
	for id, provider := range c.providers {
		providers[id] = cloneModelProviderConfig(provider)
	}
	discovered := make(map[string]map[string]struct{}, len(c.discovered))
	for providerID, models := range c.discovered {
		copySet := make(map[string]struct{}, len(models))
		for modelName := range models {
			copySet[modelName] = struct{}{}
		}
		discovered[providerID] = copySet
	}
	return providers, discovered
}

func cloneModelProviderConfig(provider ModelProviderConfig) ModelProviderConfig {
	cloned := provider
	cloned.Models = make([]ModelPricingConfig, len(provider.Models))
	copy(cloned.Models, provider.Models)
	for index := range cloned.Models {
		if provider.Models[index].CachedPromptCostMicrosPerMillionTokens != nil {
			cachedRate := *provider.Models[index].CachedPromptCostMicrosPerMillionTokens
			cloned.Models[index].CachedPromptCostMicrosPerMillionTokens = &cachedRate
		}
		if provider.Models[index].Capabilities == nil {
			continue
		}
		capabilities := *provider.Models[index].Capabilities
		capabilities.ReasoningEfforts = append([]string(nil), provider.Models[index].Capabilities.ReasoningEfforts...)
		if provider.Models[index].Capabilities.Input != nil {
			input := *provider.Models[index].Capabilities.Input
			capabilities.Input = &input
		}
		cloned.Models[index].Capabilities = &capabilities
	}
	return cloned
}
