package telegram

import (
	"context"
	"net/url"
)

type WebhookInfo struct {
	URL                string   `json:"url"`
	PendingUpdateCount int64    `json:"pending_update_count"`
	AllowedUpdates     []string `json:"allowed_updates,omitempty"`
}

func (c *Client) WebhookInfo(ctx context.Context) (WebhookInfo, error) {
	var response struct {
		URL            *string  `json:"url"`
		Pending        *int64   `json:"pending_update_count"`
		AllowedUpdates []string `json:"allowed_updates"`
	}
	if err := c.call(ctx, "getWebhookInfo", struct{}{}, &response); err != nil {
		return WebhookInfo{}, err
	}
	if response.URL == nil || response.Pending == nil || len(*response.URL) > 8192 || *response.Pending < 0 || *response.Pending > 9007199254740991 {
		return WebhookInfo{}, ErrProtocol
	}
	return WebhookInfo{URL: *response.URL, PendingUpdateCount: *response.Pending, AllowedUpdates: response.AllowedUpdates}, nil
}

// DeleteWebhook explicitly preserves pending updates. There is deliberately no
// drop option on the ordinary application client.
func (c *Client) DeleteWebhook(ctx context.Context) error {
	var ok bool
	if err := c.call(ctx, "deleteWebhook", struct {
		Drop bool `json:"drop_pending_updates"`
	}{false}, &ok); err != nil {
		return err
	}
	if !ok {
		return ErrProtocol
	}
	return nil
}

func (c *Client) SetWebhook(ctx context.Context, address, secret string, allowed []string) error {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || !secretPattern.MatchString(secret) || allowed == nil {
		return ErrRequest
	}
	var ok bool
	p := struct {
		URL     string   `json:"url"`
		Secret  string   `json:"secret_token"`
		Allowed []string `json:"allowed_updates"`
		Drop    bool     `json:"drop_pending_updates"`
	}{address, secret, allowed, false}
	if err := c.call(ctx, "setWebhook", p, &ok); err != nil {
		return err
	}
	if !ok {
		return ErrProtocol
	}
	return nil
}
