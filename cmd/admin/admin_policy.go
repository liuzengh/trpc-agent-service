//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const redactedValue = "***REDACTED***"

func preserveMaskedSecrets(current, update *tenant.Tenant) {
	models := make(map[string]tenant.ModelConfig, len(current.Models))
	for _, model := range current.Models {
		models[model.Provider+"\x00"+model.ModelName] = model
	}
	for i := range update.Models {
		key := update.Models[i].Provider + "\x00" + update.Models[i].ModelName
		if previous, ok := models[key]; ok {
			preserveCredential(previous.APIKey, previous.APIKeyRef, &update.Models[i].APIKey, &update.Models[i].APIKeyRef)
		}
	}

	channels := make(map[string]tenant.ChannelBinding, len(current.Channels))
	for _, binding := range current.Channels {
		channels[channelIdentity(binding)] = binding
	}
	for i := range update.Channels {
		if previous, ok := channels[channelIdentity(update.Channels[i])]; ok {
			binding := &update.Channels[i]
			preserveCredential(previous.Token, previous.TokenRef, &binding.Token, &binding.TokenRef)
			preserveCredential(previous.Secret, previous.SecretRef, &binding.Secret, &binding.SecretRef)
			legacyAESSelected := !isMaskedOrOmitted(binding.Config["encoding_aes_key"])
			aesSelected := legacyAESSelected || binding.EncodingAESKeyRef != "" || !isMaskedOrOmitted(binding.EncodingAESKey)
			if legacyAESSelected && binding.EncodingAESKeyRef == "" && isMaskedOrOmitted(binding.EncodingAESKey) {
				// The supported config alias is also an explicit inline source.
				// Do not resurrect the old canonical value or SecretRef over it.
				binding.EncodingAESKey = ""
			} else {
				preserveCredential(previous.EncodingAESKey, previous.EncodingAESKeyRef, &binding.EncodingAESKey, &binding.EncodingAESKeyRef)
			}
			if update.Channels[i].WebhookKey == "" {
				update.Channels[i].WebhookKey = previous.WebhookKey
			}
			if update.Channels[i].AccountID == "" {
				update.Channels[i].AccountID = previous.AccountID
			}
			previousConfig := previous.Config
			if aesSelected {
				previousConfig = cloneStringMap(previous.Config)
				delete(previousConfig, "encoding_aes_key")
			}
			if binding.Config == nil {
				binding.Config = cloneStringMap(previousConfig)
			} else {
				preserveMapSecrets(previousConfig, binding.Config, []string{"encoding_aes_key", "corp_secret", "token", "secret"})
			}
			if aesSelected && isMaskedOrOmitted(binding.Config["encoding_aes_key"]) {
				delete(binding.Config, "encoding_aes_key")
			}
		}
	}
	if update.Storage.SessionBackend == "" {
		update.Storage.SessionBackend = current.Storage.SessionBackend
	}
	if update.Storage.MemoryBackend == "" {
		update.Storage.MemoryBackend = current.Storage.MemoryBackend
	}
	if update.Storage.SessionProfile == "" {
		update.Storage.SessionProfile = current.Storage.SessionProfile
	}
	if update.Storage.MemoryProfile == "" {
		update.Storage.MemoryProfile = current.Storage.MemoryProfile
	}
	if update.Storage.SessionConfig == nil {
		update.Storage.SessionConfig = cloneStringMap(current.Storage.SessionConfig)
	} else {
		preserveMapSecrets(current.Storage.SessionConfig, update.Storage.SessionConfig, storageSecretKeys())
	}
	if update.Storage.MemoryConfig == nil {
		update.Storage.MemoryConfig = cloneStringMap(current.Storage.MemoryConfig)
	} else {
		preserveMapSecrets(current.Storage.MemoryConfig, update.Storage.MemoryConfig, storageSecretKeys())
	}
}

// An explicit source wins over preservation; conflicting explicit sources are
// left intact for validation. A mask is never stored as a credential value.
func preserveCredential(previousValue, previousRef string, value, ref *string) {
	if !isMaskedOrOmitted(*value) {
		return
	}
	if *ref != "" {
		*value = ""
		return
	}
	if previousValue != "" {
		*value = previousValue
	} else if previousRef != "" {
		*value = ""
		*ref = previousRef
	}
}

func preserveMapSecrets(current, update map[string]string, keys []string) {
	seen := make(map[string]struct{}, len(keys)+len(current)+len(update))
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	for key := range current {
		seen[key] = struct{}{}
	}
	for key := range update {
		seen[key] = struct{}{}
	}
	for key := range seen {
		if !isSensitiveConfigKey(key) {
			continue
		}
		if isMaskedOrOmitted(update[key]) && current[key] != "" {
			update[key] = current[key]
		}
	}
}

func isSensitiveConfigKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(key), "-", "_"), ".", "_"))
	switch normalized {
	case "token", "secret", "password", "passwd", "dsn", "url", "api_key", "apikey",
		"access_key", "access_token", "secret_key", "credential", "credentials", "authorization",
		"auth", "corp_secret", "encoding_aes_key":
		return true
	}
	for _, part := range strings.Split(normalized, "_") {
		switch part {
		case "token", "secret", "password", "passwd", "apikey", "credential", "credentials", "authorization":
			return true
		}
	}
	return false
}

// redactMask returns a fixed placeholder for any non-empty secret so callers
// can tell "set" from "unset" without exposing the value.
func redactMask(s string) string {
	if s == "" {
		return ""
	}
	return redactedValue
}

// redactTenant returns a copy of t with all secret-bearing fields masked, so
// the Admin API never emits decrypted model API keys or channel credentials in
// its JSON responses. Slices are copied so the caller's tenant is untouched.
func redactTenant(t *tenant.Tenant) *tenant.Tenant {
	if t == nil {
		return nil
	}
	c := *t
	c.Agents = make([]tenant.AgentConfig, len(t.Agents))
	copy(c.Agents, t.Agents)
	for i := range c.Agents {
		c.Agents[i].Tools = append([]string(nil), t.Agents[i].Tools...)
		c.Agents[i].Metadata = cloneStringMap(t.Agents[i].Metadata)
		if t.Agents[i].Runtime != nil {
			runtimeCopy := *t.Agents[i].Runtime
			runtimeCopy.Nodes = append([]tenant.AgentRuntimeNode(nil), t.Agents[i].Runtime.Nodes...)
			for nodeIndex := range runtimeCopy.Nodes {
				runtimeCopy.Nodes[nodeIndex].Tools = append(
					[]string(nil), t.Agents[i].Runtime.Nodes[nodeIndex].Tools...,
				)
			}
			runtimeCopy.Edges = append([]tenant.AgentRuntimeEdge(nil), t.Agents[i].Runtime.Edges...)
			c.Agents[i].Runtime = &runtimeCopy
		}
	}
	c.Models = make([]tenant.ModelConfig, len(t.Models))
	copy(c.Models, t.Models)
	for i := range c.Models {
		c.Models[i].APIKey = redactMask(c.Models[i].APIKey)
	}
	c.Channels = make([]tenant.ChannelBinding, len(t.Channels))
	copy(c.Channels, t.Channels)
	for i := range c.Channels {
		c.Channels[i].Config = cloneStringMap(t.Channels[i].Config)
		c.Channels[i].WebhookURL = redactWebhookURL(c.Channels[i].WebhookURL)
		c.Channels[i].Token = redactMask(c.Channels[i].Token)
		c.Channels[i].Secret = redactMask(c.Channels[i].Secret)
		c.Channels[i].EncodingAESKey = redactMask(c.Channels[i].EncodingAESKey)
		redactMapSecrets(c.Channels[i].Config, []string{"encoding_aes_key", "corp_secret", "token", "secret"})
	}
	c.Storage.SessionConfig = cloneStringMap(t.Storage.SessionConfig)
	c.Storage.MemoryConfig = cloneStringMap(t.Storage.MemoryConfig)
	redactMapSecrets(c.Storage.SessionConfig, storageSecretKeys())
	redactMapSecrets(c.Storage.MemoryConfig, storageSecretKeys())
	return &c
}

// noStoreAdminResponses prevents browsers, proxies, and shared caches from
// retaining tenant configuration or control-plane responses. It is applied at
// the protected route boundary so errors are covered as well as successful JSON.
func noStoreAdminResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// redactWebhookURL preserves a usable public route while removing URL
// components that commonly carry credentials (userinfo, query, and fragment).
// An invalid configured value is replaced wholesale rather than echoed into an
// Admin response.
func redactWebhookURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return redactedValue
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func redactMapSecrets(config map[string]string, keys []string) {
	known := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		known[key] = struct{}{}
	}
	for key, value := range config {
		if _, explicitlyKnown := known[key]; explicitlyKnown || isSensitiveConfigKey(key) {
			if value != "" {
				config[key] = redactedValue
			}
		}
	}
}

func tenantIDFromPath(path string) (string, bool) {
	const prefix = "/api/v1/tenants/"
	if len(path) <= len(prefix) || path[:len(prefix)] != prefix {
		return "", false
	}
	tenantID := path[len(prefix):]
	if tenantID == "" || len(tenantID) > 64 {
		return "", false
	}
	for _, character := range tenantID {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') && character != '-' && character != '_' {
			return "", false
		}
	}
	return tenantID, true
}

func writeAdminAuthorizationError(w http.ResponseWriter, err error) {
	if errors.Is(err, adminauth.ErrUnauthenticated) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	http.Error(w, "Forbidden", http.StatusForbidden)
}

func storageSecretKeys() []string {
	return []string{"dsn", "url", "password", "access_key", "secret_key", "token"}
}

func channelIdentity(binding tenant.ChannelBinding) string {
	if binding.AccountID != "" {
		return binding.Type + "\x00" + binding.AccountID
	}
	if binding.WebhookKey != "" {
		return binding.Type + "\x00" + binding.WebhookKey
	}
	return binding.Type + "\x00" + binding.AppID
}

func isMaskedOrOmitted(value string) bool {
	return value == "" || value == redactedValue
}

func tenantConfigContainsRedacted(config tenant.TenantConfig) bool {
	return tenantContainsRedacted(&tenant.Tenant{
		Models: config.Models, Channels: config.Channels, Storage: config.Storage,
	})
}

func tenantContainsRedacted(value *tenant.Tenant) bool {
	for _, model := range value.Models {
		if model.APIKey == redactedValue {
			return true
		}
	}
	for _, binding := range value.Channels {
		if binding.Token == redactedValue || binding.Secret == redactedValue || binding.EncodingAESKey == redactedValue {
			return true
		}
		for _, secret := range binding.Config {
			if secret == redactedValue {
				return true
			}
		}
	}
	for _, config := range []map[string]string{value.Storage.SessionConfig, value.Storage.MemoryConfig} {
		for _, secret := range config {
			if secret == redactedValue {
				return true
			}
		}
	}
	return false
}
