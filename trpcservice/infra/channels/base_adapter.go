// base_adapter.go provides the Adapter boilerplate shared by every IM
// platform adapter: the inbound channel, in-memory dedup by platform message
// id, and the Conn plumbing. Platform adapters embed BaseAdapter and supply
// only their protocol-specific decode, so adding a channel no longer means
// re-copying the dedup / pump / send skeleton.
package channels

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
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

// Media fetch bounds. A platform attachment is fetched inline (before the turn
// reaches the model) and inlined as a content part, so both the time and the
// size must be bounded: an unbounded fetch would stall the gateway or blow up
// the model prompt.
const (
	mediaFetchTimeout = 15 * time.Second
	maxMediaBytes     = 8 << 20 // 8 MiB per attachment
)

// fetchMedia fills Data for every attachment that carries a fetched payload,
// using the Conn's downloader. Failures are recorded on the attachment (never
// fatal): the message still reaches the worker, which then tells the user the
// attachment could not be read and audits it.
func (a *BaseAdapter) fetchMedia(ctx context.Context, msg *InboundMessage) {
	if msg == nil || len(msg.Media) == 0 {
		return
	}
	dl, ok := a.conn.(MediaDownloader)
	for i := range msg.Media {
		att := &msg.Media[i]
		if len(att.Data) > 0 {
			continue
		}
		if !ok {
			att.FetchError = "channel does not support attachment download"
			continue
		}
		fetchCtx, cancel := context.WithTimeout(ctx, mediaFetchTimeout)
		data, mime, err := dl.DownloadMedia(fetchCtx, *att)
		cancel()
		switch {
		case err != nil:
			att.FetchError = err.Error()
			slog.Warn("channels: attachment fetch failed",
				"channel", a.name, "kind", att.Kind, "err", err)
		case len(data) == 0:
			att.FetchError = "attachment is empty"
		case len(data) > maxMediaBytes:
			att.FetchError = fmt.Sprintf("attachment exceeds the %d MiB limit", maxMediaBytes>>20)
			slog.Warn("channels: attachment too large, dropped",
				"channel", a.name, "kind", att.Kind, "bytes", len(data))
		default:
			att.Data = data
			if mime != "" {
				att.MimeType = mime
			}
		}
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
		// Attachments are fetched before the message enters the bus: the worker
		// must be able to hand the model real bytes (or an explicit "could not
		// read it" note), not a dangling platform reference.
		a.fetchMedia(ctx, in)
		select {
		case a.inbound <- in:
		case <-ctx.Done():
			return nil
		}
	}
}
