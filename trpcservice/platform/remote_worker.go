package platform

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type RemoteRunnerAdapter struct {
	endpoint string
	token    string
	client   *http.Client
	keyID    string
	secret   []byte
}

func NewRemoteRunnerAdapter(endpoint, token string) *RemoteRunnerAdapter {
	return NewSignedRemoteRunnerAdapter(endpoint, token, "development", []byte(token))
}

func NewSignedRemoteRunnerAdapter(endpoint, token, keyID string, secret []byte) *RemoteRunnerAdapter {
	return &RemoteRunnerAdapter{endpoint: strings.TrimRight(endpoint, "/"), token: token, client: &http.Client{}, keyID: keyID, secret: append([]byte(nil), secret...)}
}

func (a *RemoteRunnerAdapter) Run(ctx context.Context, request RunnerRequest) (RunnerResponse, error) {
	events, err := a.RunEvents(ctx, request)
	if err != nil {
		return RunnerResponse{}, err
	}
	response := RunnerResponse{}
	for event := range events {
		if event.Type == "message.completed" || event.Type == "run.completed" {
			if output := event.Data["output"]; output != "" {
				response.Output = output
			}
			if event.Data["usage_known"] == "true" {
				response.UsageKnown = true
				fmt.Sscanf(event.Data["usage_tokens"], "%d", &response.UsageTokens)
			}
		}
	}
	if response.Output == "" {
		return RunnerResponse{}, &runtimeError{code: "worker_unavailable"}
	}
	return response, nil
}

func (a *RemoteRunnerAdapter) RunEvents(ctx context.Context, request RunnerRequest) (<-chan RuntimeEvent, error) {
	if request.TraceParent == "" {
		request.TraceParent = newTraceParent(request.TraceID)
	}
	manifest, err := signExecutionManifest(request, a.keyID, a.secret, time.Now().UTC())
	if err != nil {
		return nil, &runtimeError{code: "worker_unavailable", err: err}
	}
	payload, err := json.Marshal(map[string]string{"manifest": manifest})
	if err != nil {
		return nil, &runtimeError{code: "worker_unavailable", err: err}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint+"/internal/worker/run", bytes.NewReader(payload))
	if err != nil {
		return nil, &runtimeError{code: "worker_unavailable", err: err}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+a.token)
	httpRequest.Header.Set("traceparent", request.TraceParent)
	response, err := a.client.Do(httpRequest)
	if err != nil {
		return nil, &runtimeError{code: "worker_unavailable", err: err}
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		var apiErr errorResponse
		_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&apiErr)
		code := "worker_unavailable"
		if apiErr.Error.Code != "" {
			code = apiErr.Error.Code
		}
		return nil, &runtimeError{code: code}
	}
	if response.Header.Get("Content-Type") != "text/event-stream" {
		response.Body.Close()
		return nil, &runtimeError{code: "worker_unavailable"}
	}
	events := make(chan RuntimeEvent, 4)
	go func() {
		defer close(events)
		defer response.Body.Close()
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		sawTerminal := false
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event RuntimeEvent
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) != nil {
				continue
			}
			if event.Type == "run.completed" || event.Type == "run.failed" || event.Type == "run.cancelled" {
				sawTerminal = true
			}
			select {
			case <-ctx.Done():
				return
			case events <- event:
			}
		}
		if !sawTerminal && ctx.Err() == nil {
			events <- RuntimeEvent{Type: "run.failed", Data: map[string]string{"error": "worker_unavailable"}}
		}
	}()
	return events, nil
}

func (a *RemoteRunnerAdapter) Close() error { return nil }

type WorkerServerConfig struct {
	Token          string
	Factory        AgentFactory
	BeforeRun      func(RunnerRequest, DeploymentVersion)
	ManifestKeys   map[string][]byte
	Now            func() time.Time
	ToolGovernance ToolGovernance
}

type WorkerServer struct {
	config    WorkerServerConfig
	versions  *workerVersionStore
	runner    *FrameworkRunnerAdapter
	closeOnce sync.Once
	closeErr  error
}

func NewWorkerServer(config WorkerServerConfig) *WorkerServer {
	if config.Token == "" {
		config.Token = "development-worker"
	}
	if len(config.ManifestKeys) == 0 {
		config.ManifestKeys = map[string][]byte{"development": []byte(config.Token)}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	store := newWorkerVersionStore()
	runner := NewFrameworkRunnerAdapter(store.Resolve, config.Factory)
	runner.SetToolGovernance(config.ToolGovernance)
	return &WorkerServer{config: config, versions: store, runner: runner}
}

// Close cancels and drains active executions and closes cached framework
// Runners. It is safe to call more than once during shutdown.
func (s *WorkerServer) Close() error {
	s.closeOnce.Do(func() {
		if s.runner != nil {
			s.closeErr = s.runner.Close()
		}
	})
	return s.closeErr
}

func (s *WorkerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/internal/worker/run" {
		writeError(w, http.StatusNotFound, "not_found", "worker endpoint was not found")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	expected := []byte(s.config.Token)
	provided := []byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if len(expected) == 0 || subtle.ConstantTimeCompare(provided, expected) != 1 {
		writeError(w, http.StatusUnauthorized, "worker_unauthorized", "worker authorization failed")
		return
	}
	var envelope struct {
		Manifest string `json:"manifest"`
	}
	if err := decodeStrict(r, &envelope); err != nil || envelope.Manifest == "" {
		writeError(w, http.StatusBadRequest, "invalid_execution_manifest", "Execution Manifest is invalid")
		return
	}
	request, err := verifyExecutionManifest(envelope.Manifest, s.config.ManifestKeys, s.config.Now().UTC())
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_execution_manifest", "Execution Manifest verification failed")
		return
	}
	if !validTraceParent(request.TraceParent) || r.Header.Get("traceparent") != request.TraceParent {
		writeError(w, http.StatusUnauthorized, "invalid_execution_manifest", "Execution Manifest trace context is invalid")
		return
	}
	if request.TenantID == "" || request.AppID == "" ||
		request.SessionID == "" || request.RequestID == "" || request.DeploymentID == "" || request.VersionID == "" ||
		request.Input == "" || request.Version == nil {
		writeError(w, http.StatusBadRequest, "invalid_worker_request", "resolved worker request is invalid")
		return
	}
	version := *request.Version
	if version.ID != request.VersionID || version.TenantID != request.TenantID || version.AgentAppID != request.AppID ||
		version.DeploymentID != request.DeploymentID || len(version.Config) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_worker_version", "resolved Deployment Version is invalid")
		return
	}
	version.Active = true
	if err := s.versions.Put(version); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_worker_version", "resolved Deployment Version is invalid")
		return
	}
	request.Version = nil
	if s.config.BeforeRun != nil {
		s.config.BeforeRun(request, version)
	}
	events, err := s.runner.RunEvents(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "worker_unavailable", "worker execution failed")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	for event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
	}
}

var traceParentPattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-(?:0[1-9a-f]|[1-9a-f][0-9a-f])$`)

func validTraceParent(value string) bool { return traceParentPattern.MatchString(value) }

type executionManifestClaims struct {
	Request   RunnerRequest `json:"request"`
	IssuedAt  int64         `json:"iat"`
	ExpiresAt int64         `json:"exp"`
}

func signExecutionManifest(request RunnerRequest, keyID string, secret []byte, now time.Time) (string, error) {
	if keyID == "" || len(secret) < 8 {
		return "", errors.New("execution manifest signing key is invalid")
	}
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT", "kid": keyID})
	claims, err := json.Marshal(executionManifestClaims{Request: request, IssuedAt: now.Unix(), ExpiresAt: now.Add(30 * time.Second).Unix()})
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func verifyExecutionManifest(compact string, keys map[string][]byte, now time.Time) (RunnerRequest, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return RunnerRequest{}, errors.New("invalid execution manifest")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return RunnerRequest{}, err
	}
	var header map[string]string
	if json.Unmarshal(headerBytes, &header) != nil || header["alg"] != "HS256" || header["typ"] != "JWT" {
		return RunnerRequest{}, errors.New("invalid execution manifest header")
	}
	secret := keys[header["kid"]]
	if len(secret) < 8 {
		return RunnerRequest{}, errors.New("unknown execution manifest key")
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return RunnerRequest{}, errors.New("invalid execution manifest signature")
	}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return RunnerRequest{}, err
	}
	var claims executionManifestClaims
	if json.Unmarshal(claimsBytes, &claims) != nil || claims.IssuedAt > now.Add(5*time.Second).Unix() || claims.ExpiresAt < now.Unix() {
		return RunnerRequest{}, errors.New("expired execution manifest")
	}
	return claims.Request, nil
}

type workerVersionStore struct {
	mu       sync.RWMutex
	versions map[string]DeploymentVersion
}

func newWorkerVersionStore() *workerVersionStore {
	return &workerVersionStore{versions: make(map[string]DeploymentVersion)}
}

func (s *workerVersionStore) Put(version DeploymentVersion) error {
	if version.ID == "" || version.TenantID == "" || version.AgentAppID == "" || version.DeploymentID == "" || len(version.Config) == 0 {
		return errors.New("invalid worker version")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := versionRefKey(DeploymentVersionRef{TenantID: version.TenantID, VersionID: version.ID})
	if existing, exists := s.versions[key]; exists {
		existing.Config = cloneConfig(existing.Config)
		version.Config = cloneConfig(version.Config)
		if fmt.Sprintf("%v", existing.Config) != fmt.Sprintf("%v", version.Config) {
			return errors.New("worker version conflict")
		}
		return nil
	}
	version.Config = cloneConfig(version.Config)
	s.versions[key] = version
	return nil
}

func (s *workerVersionStore) Resolve(_ context.Context, ref DeploymentVersionRef) (DeploymentVersion, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	version, exists := s.versions[versionRefKey(ref)]
	if exists {
		version.Config = cloneConfig(version.Config)
	}
	return version, exists, nil
}
