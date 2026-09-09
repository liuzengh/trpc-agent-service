package channels

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const RealtimeMessages = "realtime"
const ReliableMessages = "reliable"

// MessagePolicy applies to unstarted chat requests, never execution recovery.
// A reliable subscription deliberately opts into processing older messages.
type MessagePolicy struct {
	Mode          string `json:"mode,omitempty"`
	MaxAgeSeconds int    `json:"max_age_seconds,omitempty"`
}

func (p MessagePolicy) Effective() (MessagePolicy, error) {
	if p.Mode == "" {
		p.Mode = RealtimeMessages
	}
	if p.Mode != RealtimeMessages && p.Mode != ReliableMessages {
		return p, errors.New("message policy mode must be realtime or reliable")
	}
	if p.MaxAgeSeconds == 0 {
		p.MaxAgeSeconds = 120
	}
	if p.MaxAgeSeconds < 30 || p.MaxAgeSeconds > 120 {
		return p, errors.New("message max_age_seconds must be between 30 and 120")
	}
	return p, nil
}
func ParseMessagePolicy(raw json.RawMessage) (MessagePolicy, error) {
	var config struct {
		Policy json.RawMessage `json:"message_policy"`
	}
	if json.Unmarshal(raw, &config) != nil {
		return MessagePolicy{}, errors.New("invalid message policy config")
	}
	var p MessagePolicy
	if len(config.Policy) > 0 {
		if bytes.Equal(bytes.TrimSpace(config.Policy), []byte("null")) {
			return p, errors.New("message policy must be an object")
		}
		d := json.NewDecoder(bytes.NewReader(config.Policy))
		d.DisallowUnknownFields()
		if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF {
			return p, errors.New("invalid message policy")
		}
	}
	return p.Effective()
}
func (p MessagePolicy) MaxAge() time.Duration { return time.Duration(p.MaxAgeSeconds) * time.Second }
func RealtimeChannel(kind string) bool {
	return kind == "telegram" || kind == "wecom_mcp" || kind == "wecom"
}
