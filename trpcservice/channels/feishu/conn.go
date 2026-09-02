// Feishu (Lark) long-connection implementation of channels.Conn.
//
// The Lark SDK's ws.Client establishes the WSS channel and the event
// dispatcher routes im.message.receive_v1 events here; the raw JSON is
// forwarded verbatim so the adapter's existing Event normalization is reused.
// Replies go out over the Lark OpenAPI (im/message/create) — the long
// connection only receives events.
//
// NOTE: requires live app credentials to actually connect; the code is
// unit-tested for the message-building side only.
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// Conn implements channels.Conn over the Lark long connection.
type Conn struct {
	appID     string
	appSecret string

	events chan []byte
	api    *lark.Client
	client *larkws.Client

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewConn builds a Lark long-connection client and starts it in the
// background. Recv surfaces incoming events; Send calls the OpenAPI.
func NewConn(appID, appSecret string) *Conn {
	c := &Conn{
		appID:     appID,
		appSecret: appSecret,
		events:    make(chan []byte, 64),
		api:       lark.NewClient(appID, appSecret),
		done:      make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	handler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, ev *larkim.P2MessageReceiveV1) error {
			raw, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			select {
			case c.events <- raw:
			default: // drop when the adapter is not draining
			}
			return nil
		})
	c.client = larkws.NewClient(appID, appSecret, larkws.WithEventHandler(handler))
	go func() {
		_ = c.client.Start(ctx)
		close(c.done)
	}()
	return c
}

// Recv returns the next event's JSON, or the ctx/connection shutdown error.
func (c *Conn) Recv(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.events:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errors.New("feishu: long connection closed")
	}
}

// Send delivers a text message to a chat over the Lark OpenAPI.
func (c *Conn) Send(ctx context.Context, target, text string) error {
	content, _ := json.Marshal(map[string]string{"text": text})
	body := larkim.NewCreateMessageReqBodyBuilder().
		ReceiveId(target).
		MsgType(larkim.MsgTypeText).
		Content(string(content)).
		Build()
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType("chat_id").
		Body(body).
		Build()
	resp, err := c.api.Im.Message.Create(ctx, req)
	if err != nil {
		return fmt.Errorf("feishu: create message: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("feishu: create message code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

// Close stops the long connection.
func (c *Conn) Close() error {
	c.once.Do(func() { c.cancel() })
	return nil
}
