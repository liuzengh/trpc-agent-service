// Package modelclient builds production model clients from immutable provider
// profiles and tenant-scoped credentials.
package modelclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/generation"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

const (
	defaultTimeout = 60 * time.Second
	maximumTimeout = 10 * time.Minute

	// The upstream OpenAI compatibility adapter treats DeepSeek as text-only
	// by default, which is correct for the regular v4 models but would silently
	// replace a vision image with an attachment-unavailable hint. Keep this
	// narrow until another DeepSeek vision model is explicitly catalogued.
	deepSeekVisionModel    = "deepseek-v4-flash-vision-exp"
	fakeDeterministicModel = "fake-deterministic-v1"
)

type ProfileReader interface {
	GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error)
}

// Resolver implements agent.ModelResolver. It intentionally has no
// environment-variable fallback and does not resolve current/latest profiles.
type Resolver struct {
	Profiles    ProfileReader
	Secrets     secrets.Provider
	Credentials *generation.Pool
	Subject     string
}

func (r Resolver) ResolveModel(ctx context.Context, tenantID string, ref profile.VersionedRef) (model.Model, error) {
	if r.Profiles == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if tenantID == "" || ref.ID == "" || ref.Version < 1 {
		return nil, runtime.ErrInvalidEnvelope
	}
	value, err := r.Profiles.GetModel(ctx, tenantID, ref.ID, ref.Version)
	if err != nil {
		return nil, err
	}
	if value.TenantID != tenantID || value.ProfileID != ref.ID {
		return nil, runtime.ErrTenantScope
	}
	if value.Version != ref.Version || value.ContentDigest == "" {
		return nil, runtime.ErrVersionMismatch
	}
	if value.Status != "active" && value.Status != "suspended" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if value.Provider == "fake" {
		return resolveFakeModel(value)
	}
	if value.Provider != "deepseek" || value.SchemaVersion != 1 || value.Model == "" || value.SecretRef.Ref == "" || value.SecretRef.Version < 1 ||
		(r.Secrets == nil && r.Credentials == nil) || strings.TrimSpace(r.Subject) != r.Subject || r.Subject == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := validateEndpoint(value.Endpoint); err != nil {
		return nil, err
	}
	timeout, retryAttempts, bufferSize, err := options(value.Options)
	if err != nil {
		return nil, err
	}
	scope := secrets.Scope{TenantID: tenantID, Subject: r.Subject, Purpose: secrets.PurposeModelCall,
		ResourceID: value.ProfileID, ResourceVersion: value.Version}
	credential, release, err := r.resolveCredential(ctx, scope, value.SecretRef)
	if err != nil {
		clear(credential.Bytes)
		return nil, err
	}
	defer release()
	defer clear(credential.Bytes)
	apiKey := strings.TrimSpace(string(credential.Bytes))
	if credential.Version != value.SecretRef.Version || apiKey == "" || strings.ContainsAny(apiKey, "\r\n\x00") {
		return nil, runtime.ErrVersionMismatch
	}
	opts := []openai.Option{
		openai.WithAPIKey(apiKey),
		openai.WithBaseURL(value.Endpoint),
		openai.WithHTTPClientOptions(model.WithHTTPClientTimeout(timeout)),
	}
	if bufferSize > 0 {
		opts = append(opts, openai.WithChannelBufferSize(bufferSize))
	}
	if isDeepSeekVisionModel(value.Model) {
		opts = append(opts, openai.WithTextOnlyMessageContent(false))
	}
	resolved := model.Model(openai.New(value.Model, opts...))
	if retryAttempts > 1 {
		resolved = timeoutRetryModel{Model: resolved, attempts: retryAttempts}
	}
	return resolved, nil
}

type fakeModel struct {
	modelName string
	response  string
	deltas    []string
}

func resolveFakeModel(value provider.ModelProfileSnapshot) (model.Model, error) {
	if value.SchemaVersion != 1 || value.Model != fakeDeterministicModel || value.Endpoint != "" || value.SecretRef.Ref != "" || value.SecretRef.Version != 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	response, exists := value.Options["response"]
	if !exists || response == "" || len(response) > 65536 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	var deltas []string
	if script, exists := value.Options["stream_deltas"]; exists {
		if err := json.Unmarshal([]byte(script), &deltas); err != nil || len(deltas) == 0 || len(deltas) > 128 {
			return nil, runtime.ErrCapabilityUnsupported
		}
		var total int
		for _, delta := range deltas {
			if delta == "" {
				return nil, runtime.ErrCapabilityUnsupported
			}
			total += len(delta)
			if total > 65536 {
				return nil, runtime.ErrCapabilityUnsupported
			}
		}
	}
	return fakeModel{modelName: value.Model, response: response, deltas: deltas}, nil
}

func (m fakeModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stream := request != nil && request.GenerationConfig.Stream
	deltas := m.deltas
	if len(deltas) == 0 {
		deltas = []string{m.response}
	}
	content := m.response
	if len(m.deltas) != 0 {
		content = strings.Join(m.deltas, "")
	}
	if !stream {
		responses := make(chan *model.Response, 1)
		responses <- fakeResponse(m.modelName, content, "", true)
		close(responses)
		return responses, nil
	}
	responses := make(chan *model.Response, len(deltas))
	for index, delta := range deltas {
		responses <- fakeResponse(m.modelName, content, delta, index == len(deltas)-1)
	}
	close(responses)
	return responses, nil
}

func (m fakeModel) Info() model.Info { return model.Info{Name: m.modelName} }

func fakeResponse(modelName, content, delta string, done bool) *model.Response {
	response := &model.Response{ID: "fake-deterministic-response", Object: model.ObjectTypeChatCompletion,
		Model: modelName, Done: done, Choices: []model.Choice{{Index: 0}}}
	if delta == "" {
		response.Choices[0].Message = model.NewAssistantMessage(content)
	} else {
		response.Choices[0].Delta = model.Message{Role: model.RoleAssistant, Content: delta}
	}
	if done {
		response.Usage = &model.Usage{PromptTokens: 1, CompletionTokens: int(len([]rune(content))), TotalTokens: 1 + int(len([]rune(content)))}
	}
	return response
}

type timeoutRetryModel struct {
	model.Model
	attempts int
}

func (m timeoutRetryModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	for attempt := 1; ; attempt++ {
		result, err := m.Model.GenerateContent(ctx, request)
		if err == nil || attempt >= m.attempts || ctx.Err() != nil || !isTimeout(err) {
			return result, err
		}
	}
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

func isDeepSeekVisionModel(name string) bool { return name == deepSeekVisionModel }

func (r Resolver) resolveCredential(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, func(), error) {
	if r.Credentials == nil {
		value, err := r.Secrets.Resolve(ctx, scope, ref)
		return value, func() {}, err
	}
	lease, err := r.Credentials.Acquire(ctx, scope, ref)
	if err != nil {
		return secrets.SecretValue{}, func() {}, err
	}
	value, err := lease.Secret()
	if err != nil {
		lease.Release()
		return secrets.SecretValue{}, func() {}, err
	}
	return value, lease.Release, nil
}

func validateEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return runtime.ErrCapabilityUnsupported
	}
	if !strings.EqualFold(parsed.Hostname(), "api.deepseek.com") || parsed.Port() != "" {
		return runtime.ErrCapabilityUnsupported
	}
	path := strings.TrimSuffix(parsed.EscapedPath(), "/")
	if path != "" && path != "/v1" {
		return runtime.ErrCapabilityUnsupported
	}
	return nil
}

func options(input map[string]string) (time.Duration, int, int, error) {
	timeout := defaultTimeout
	retryAttempts := 1
	bufferSize := 0
	for name, value := range input {
		switch name {
		case "timeout_ms":
			milliseconds, err := strconv.ParseInt(value, 10, 64)
			if err != nil || milliseconds < 100 || milliseconds > maximumTimeout.Milliseconds() {
				return 0, 0, 0, runtime.ErrCapabilityUnsupported
			}
			timeout = time.Duration(milliseconds) * time.Millisecond
		case "timeout_retry_attempts":
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 || parsed > 3 {
				return 0, 0, 0, runtime.ErrCapabilityUnsupported
			}
			retryAttempts = parsed
		case "channel_buffer_size":
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 1 || parsed > 4096 {
				return 0, 0, 0, runtime.ErrCapabilityUnsupported
			}
			bufferSize = parsed
		default:
			return 0, 0, 0, runtime.ErrCapabilityUnsupported
		}
	}
	return timeout, retryAttempts, bufferSize, nil
}
