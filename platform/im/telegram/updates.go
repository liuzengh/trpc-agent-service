package telegram

import (
	"context"
	"encoding/json"
)

// PollRequest is explicit on every request: omitting allowed_updates would
// inherit server state from another receiver. Negative offsets are forbidden.
type PollRequest struct {
	Offset         int64    `json:"offset"`
	Limit          int      `json:"limit"`
	TimeoutSeconds int      `json:"timeout"`
	AllowedUpdates []string `json:"allowed_updates"`
}

// PollOnce never advances an offset. The raw Update objects retain unknown
// fields for transport-independent canonicalization by the application.
func (c *Client) PollOnce(ctx context.Context, r PollRequest) ([]json.RawMessage, error) {
	if r.Offset < 0 || r.Offset > 9007199254740991 || r.Limit < 1 || r.Limit > 100 || r.TimeoutSeconds < 0 || r.TimeoutSeconds > 20 || r.AllowedUpdates == nil || len(r.AllowedUpdates) > 64 {
		return nil, ErrRequest
	}
	for _, v := range r.AllowedUpdates {
		if v == "" || len(v) > 128 {
			return nil, ErrRequest
		}
	}
	var updates []json.RawMessage
	if err := c.call(ctx, "getUpdates", r, &updates); err != nil {
		return nil, err
	}
	if updates == nil || len(updates) > r.Limit {
		return nil, ErrProtocol
	}
	var previous int64 = -1
	for _, raw := range updates {
		var u struct {
			ID *int64 `json:"update_id"`
		}
		if json.Unmarshal(raw, &u) != nil || u.ID == nil || *u.ID < 0 || *u.ID >= 9007199254740991 || *u.ID < r.Offset || *u.ID <= previous {
			return nil, ErrProtocol
		}
		previous = *u.ID
	}
	return updates, nil
}
