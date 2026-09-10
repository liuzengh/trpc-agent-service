package modelclient

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/generation"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type timeoutModelStub struct{ calls atomic.Int64 }

func (s *timeoutModelStub) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	s.calls.Add(1)
	return nil, context.DeadlineExceeded
}

func (*timeoutModelStub) Info() model.Info { return model.Info{Name: "timeout-stub"} }

func TestTimeoutRetryModelRetriesOnlyTimeoutsBeforeAStream(t *testing.T) {
	stub := &timeoutModelStub{}
	_, err := (timeoutRetryModel{Model: stub, attempts: 3}).GenerateContent(context.Background(), &model.Request{})
	if !errors.Is(err, context.DeadlineExceeded) || stub.calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, stub.calls.Load())
	}
}

type profileReaderStub struct{ value provider.ModelProfileSnapshot }

func (s profileReaderStub) GetModel(context.Context, string, string, int64) (provider.ModelProfileSnapshot, error) {
	return s.value, nil
}

type secretProviderStub struct {
	scope secrets.Scope
	ref   secrets.SecretRef
	value secrets.SecretValue
	err   error
	calls int
}

func (s *secretProviderStub) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	s.calls++
	s.scope, s.ref = scope, ref
	return s.value, s.err
}

func TestResolverBuildsExactScopedOpenAIClient(t *testing.T) {
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("test-key"), Version: 9}}
	resolver := Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}
	resolved, err := resolver.ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	if resolved == nil || secret.scope != (secrets.Scope{TenantID: "tenant-a", Subject: "worker-model", Purpose: secrets.PurposeModelCall,
		ResourceID: "model", ResourceVersion: 3}) || secret.ref != (secrets.SecretRef{Ref: "secret/model", Version: 9}) {
		t.Fatalf("scope=%#v ref=%#v model=%T", secret.scope, secret.ref, resolved)
	}
}

func TestResolverUsesCredentialGenerationWhenConfigured(t *testing.T) {
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("test-key"), Version: 9}}
	resolver := Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Credentials: generation.New(secret), Subject: "worker-model"}
	if _, err := resolver.ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3}); err != nil {
		t.Fatal(err)
	}
	if secret.calls != 1 {
		t.Fatalf("secret resolutions=%d", secret.calls)
	}
}

func TestResolverBuildsDeterministicFakeModelWithoutSecretsOrNetwork(t *testing.T) {
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("must-not-be-read"), Version: 1}}
	resolver := Resolver{Profiles: profileReaderStub{fakeProfile(map[string]string{"response": "fixed reply"})}, Secrets: secret}
	resolved, err := resolver.ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "fake", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := &model.Request{Messages: []model.Message{model.NewUserMessage("same input")}}
	responses, err := resolved.GenerateContent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	response := <-responses
	if response == nil || !response.Done || response.Choices[0].Message.Content != "fixed reply" || secret.calls != 0 {
		t.Fatalf("response=%#v secret calls=%d", response, secret.calls)
	}
	if extra := <-responses; extra != nil {
		t.Fatalf("unexpected extra response=%#v", extra)
	}
	again, err := resolved.GenerateContent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second := <-again
	if second == nil || second.Choices[0].Message.Content != response.Choices[0].Message.Content || second.ID != response.ID || second.Created != response.Created {
		t.Fatalf("first=%#v second=%#v", response, second)
	}
}

func TestResolverFakeModelStreamsConfiguredDeltasDeterministically(t *testing.T) {
	resolver := Resolver{Profiles: profileReaderStub{fakeProfile(map[string]string{"response": "ignored", "stream_deltas": `["hel","lo","!"]`})}}
	resolved, err := resolver.ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "fake", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	responses, err := resolved.GenerateContent(context.Background(), &model.Request{GenerationConfig: model.GenerationConfig{Stream: true}})
	if err != nil {
		t.Fatal(err)
	}
	var deltas []string
	var terminal *model.Response
	for response := range responses {
		deltas = append(deltas, response.Choices[0].Delta.Content)
		if response.Done {
			terminal = response
		}
	}
	if got := strings.Join(deltas, ""); got != "hello!" || terminal == nil || terminal.Usage == nil || terminal.Usage.CompletionTokens != len([]rune("hello!")) {
		t.Fatalf("deltas=%q terminal=%#v", got, terminal)
	}
}

func TestResolverFailsClosedWithoutLeakingIntoProviderFallbacks(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*provider.ModelProfileSnapshot, *secretProviderStub)
		error  error
	}{
		{name: "provider", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Provider = "openai" }, error: runtime.ErrCapabilityUnsupported},
		{name: "endpoint", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) {
			p.Endpoint = "http://api.deepseek.com"
		}, error: runtime.ErrCapabilityUnsupported},
		{name: "private endpoint", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Endpoint = "https://127.0.0.1/v1" }, error: runtime.ErrCapabilityUnsupported},
		{name: "endpoint path", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) {
			p.Endpoint = "https://api.deepseek.com/beta"
		}, error: runtime.ErrCapabilityUnsupported},
		{name: "unknown option", mutate: func(p *provider.ModelProfileSnapshot, _ *secretProviderStub) { p.Options["retry"] = "3" }, error: runtime.ErrCapabilityUnsupported},
		{name: "empty credential", mutate: func(_ *provider.ModelProfileSnapshot, s *secretProviderStub) { s.value.Bytes = []byte(" \n") }, error: runtime.ErrVersionMismatch},
		{name: "credential version", mutate: func(_ *provider.ModelProfileSnapshot, s *secretProviderStub) { s.value.Version = 8 }, error: runtime.ErrVersionMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validProfile()
			secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte("key"), Version: 9}}
			test.mutate(&value, secret)
			_, err := (Resolver{Profiles: profileReaderStub{value}, Secrets: secret, Subject: "worker-model"}).
				ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
			if !errors.Is(err, test.error) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestResolverNeverFallsBackToDeepSeekEnvironmentCredential(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "must-not-be-used")
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: []byte(" \n"), Version: 9}}
	_, err := (Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}).
		ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("got %v", err)
	}
}

func TestResolverClearsCredentialBytesOnProviderError(t *testing.T) {
	bytes := []byte("secret-bytes")
	secret := &secretProviderStub{value: secrets.SecretValue{Bytes: bytes, Version: 9}, err: runtime.ErrBackendUnavailable}
	_, err := (Resolver{Profiles: profileReaderStub{validProfile()}, Secrets: secret, Subject: "worker-model"}).
		ResolveModel(context.Background(), "tenant-a", profile.VersionedRef{ID: "model", Version: 3})
	if !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("got %v", err)
	}
	for index, value := range bytes {
		if value != 0 {
			t.Fatalf("credential byte %d was not cleared", index)
		}
	}
}

func TestDeepSeekVisionModelIsExplicitlyOptedOutOfTextOnlyCompatibilityMode(t *testing.T) {
	if !isDeepSeekVisionModel("deepseek-v4-flash-vision-exp") {
		t.Fatal("vision model must preserve image content parts")
	}
	if isDeepSeekVisionModel("deepseek-v4-flash-vision") {
		t.Fatal("only the catalogued vision model may bypass text-only compatibility")
	}
}

func validProfile() provider.ModelProfileSnapshot {
	return provider.ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "model", Status: "active", SchemaVersion: 1,
		Provider: "deepseek", Model: "deepseek-v4-flash-vision-exp", Endpoint: "https://api.deepseek.com",
		Options:   map[string]string{"timeout_ms": "1000", "channel_buffer_size": "32"},
		SecretRef: secrets.SecretRef{Ref: "secret/model", Version: 9}, ContentDigest: "digest", Version: 3}
}

func fakeProfile(options map[string]string) provider.ModelProfileSnapshot {
	return provider.ModelProfileSnapshot{TenantID: "tenant-a", ProfileID: "fake", Status: "active", SchemaVersion: 1,
		Provider: "fake", Model: "fake-deterministic-v1", Options: options, ContentDigest: "digest", Version: 1}
}
