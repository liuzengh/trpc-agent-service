// base_adapter.go provides the Adapter boilerplate shared by every IM
// platform adapter: the inbound channel, in-memory dedup by platform message
// id, and the Conn plumbing. Platform adapters embed BaseAdapter and supply
// only their protocol-specific decode, so adding a channel no longer means
// re-copying the dedup / pump / send skeleton.
package channels

import (
	"context"
	"fmt"
	"sync"
)

// DecodeFn maps one raw platform event to a normalized InboundMessage. ok
// reports whether the event should be emitted (false skips malformed or
// filtered events, e.g. a group message without an @mention).
type DecodeFn func(raw []byte) (in *InboundMessage, ok bool)

// BaseAdapter implements the channels.Adapter boilerplate. It owns the
// inbound channel, the in-memory seen-set, and the Conn; embedding adapters
// implement Start by calling Run with their platform-specific DecodeFn.
type BaseAdapter struct {
	name    string
	conn    Conn
	inbound chan *InboundMessage

	mu   sync.Mutex
	seen map[string]struct{}
}

// NewBaseAdapter returns a base adapter bound to the given connection.
func NewBaseAdapter(name string, conn Conn) *BaseAdapter {
	return &BaseAdapter{
		name:    name,
		conn:    conn,
		inbound: make(chan *InboundMessage, 64),
		seen:    make(map[string]struct{}),
	}
}

// Name returns the stable channel identifier.
func (a *BaseAdapter) Name() string { return a.name }

// Inbound returns the channel of normalized inbound messages.
func (a *BaseAdapter) Inbound() <-chan *InboundMessage { return a.inbound }

// Send delivers a normalized outbound message back to the platform.
func (a *BaseAdapter) Send(ctx context.Context, msg *OutboundMessage) error {
	if msg == nil || msg.Inbound == nil {
		return fmt.Errorf("channels: outbound requires inbound context")
	}
	chatID := msg.Inbound.ChatID
	chatType := msg.Inbound.ChatType
	switch msg.Kind {
	case KindCard:
		card := Card{}
		for _, seg := range msg.Segments {
			switch seg.Type {
			case "markdown", "text":
				card.Content += seg.Text
			case "title":
				card.Title = seg.Text
			}
			// Buttons travel with their callback payload; a channel that cannot
			// render them drops them, which is why the card body also carries
			// the "reply 批准/拒绝" instruction.
			card.Actions = append(card.Actions, seg.Actions...)
		}
		if card.Content == "" {
			card.Content = msg.Text()
		}
		return a.conn.SendCard(ctx, chatID, chatType, card)
	case KindStream:
		return a.conn.SendStream(ctx, chatID, chatType, msg.Stream)
	default:
		return a.conn.Send(ctx, chatID, chatType, msg.Text())
	}
}

// Stop closes the underlying connection.
func (a *BaseAdapter) Stop(_ context.Context) error {
	return a.conn.Close()
}

// Seen reports whether a platform message id was already observed, recording
// it on first sight. This in-memory set is a first line of defense; the
// worker's Redis idempotency key is the cross-node dedup.
func (a *BaseAdapter) Seen(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[id]; ok {
		return true
	}
	a.seen[id] = struct{}{}
	return false
}

// Run consumes raw events from the Conn until ctx is done, decoding each with
// decode and emitting the result. It returns nil on ctx cancellation and the
// Conn's error otherwise (e.g. io.EOF when a mock/connection closes).
func (a *BaseAdapter) Run(ctx context.Context, decode DecodeFn) error {
	for {
		raw, err := a.conn.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return err
		}
		in, ok := decode(raw)
		if !ok || in == nil || in.PlatformMsgID == "" {
			continue // skip malformed or undeduplicatable events
		}
		if a.Seen(in.PlatformMsgID) {
			continue
		}
		select {
		case a.inbound <- in:
		case <-ctx.Done():
			return nil
		}
	}
}
