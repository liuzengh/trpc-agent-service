package wecom

import (
	"context"
	"errors"
	"time"
)

// AuthenticationError distinguishes a received rejection from an unknown ACK.
// It preserves no remote message or credentials and still matches ErrAuth.
type AuthenticationError struct {
	Certainty    Certainty
	Code         ErrorCode
	ProviderCode int64
}

func (e *AuthenticationError) Error() string { return "wecom: authentication failed" }
func (e *AuthenticationError) Unwrap() error { return ErrAuth }

// ProbeResult reports only the bounded subscription observation. Authenticated
// means the provider accepted this BotID/Secret pair, not message delivery.
type ProbeResult struct {
	Code          string `json:"code"`
	Authenticated *bool  `json:"authenticated"`
}

// ProbeAuthentication performs an explicit, bounded real subscription then
// closes it. It may replace another connection for the same bot. The caller
// must obtain opt-in; it is NOT a read-only credential check. No business reply,
// automatic reconnect, persistent callback or runtime admission is performed.
func ProbeAuthentication(ctx context.Context, cfg Config, opts ...Option) (ProbeResult, error) {
	if ctx == nil {
		return ProbeResult{}, ErrInvalidConfig
	}
	if ctx.Err() != nil {
		return ProbeResult{}, ctx.Err()
	}
	cfg.MaxReconnects = 0
	cfg.CloseTimeout = time.Second
	client, err := NewClient(cfg, opts...)
	if err != nil {
		return ProbeResult{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- client.Run(runCtx, func(context.Context, Event) error { return nil }) }()
	defer func() {
		cancel()
		closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = client.Close(closeCtx)
	}()
	states := client.States()
	for {
		select {
		case _, ok := <-states:
			if !ok {
				states = nil
				continue
			}
			if result, observed := client.authenticationObservation(); observed {
				return result, nil
			}
		case err := <-done:
			if result, observed := client.authenticationObservation(); observed {
				return result, nil
			}
			return probeFailure(err), nil
		case <-runCtx.Done():
			if ctx.Err() != nil {
				return ProbeResult{}, ctx.Err()
			}
			if result, observed := client.authenticationObservation(); observed {
				return result, nil
			}
			return ProbeResult{Code: "PROVIDER_TIMEOUT"}, nil
		}
	}
}
func probeFailure(err error) ProbeResult {
	code := "PROVIDER_UNAVAILABLE"
	var auth *AuthenticationError
	switch {
	case errors.As(err, &auth):
		if auth.Code == CodeAckTimeout || auth.Code == CodeCanceled {
			code = "PROVIDER_TIMEOUT"
		}
		if auth.Code == CodeWrite {
			code = "PROVIDER_NETWORK"
		}
		if auth.Certainty == Rejected {
			no := false
			return ProbeResult{Code: "WECOM_AUTH_REJECTED", Authenticated: &no}
		}
	case errors.Is(err, ErrProtocol):
		code = "PROVIDER_RESPONSE_INVALID"
	case errors.Is(err, ErrDial):
		code = "PROVIDER_NETWORK"
	case errors.Is(err, ErrReplaced):
		code = "WECOM_CONNECTION_REPLACED"
	case errors.Is(err, ErrCanceled), errors.Is(err, context.DeadlineExceeded):
		code = "PROVIDER_TIMEOUT"
	}
	return ProbeResult{Code: code}
}

// Lifecycle notifications are lossy; a matched ACK is not. Probe never reconnects,
// so this generation's observation remains valid even after Run has terminated.
func (c *Client) authenticationObservation() (ProbeResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.authAck == nil {
		return ProbeResult{}, false
	}
	authenticated := c.authAck.ErrCode == 0
	code := "WECOM_AUTH_REJECTED"
	if authenticated {
		code = "WECOM_AUTHENTICATED"
	}
	return ProbeResult{Code: code, Authenticated: &authenticated}, true
}
