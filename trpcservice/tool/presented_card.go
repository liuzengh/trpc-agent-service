package tool

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type PresentedCardRecorder struct {
	mu   sync.Mutex
	card *channels.InteractiveCard
}

func NewPresentedCardRecorder() *PresentedCardRecorder { return &PresentedCardRecorder{} }

func (r *PresentedCardRecorder) Record(card channels.InteractiveCard) {
	if r == nil {
		return
	}
	copy := clonePresentedCard(card)
	r.mu.Lock()
	r.card = &copy
	r.mu.Unlock()
}

func (r *PresentedCardRecorder) Snapshot() *channels.InteractiveCard {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.card == nil {
		return nil
	}
	copy := clonePresentedCard(*r.card)
	return &copy
}

type presentedCardRecorderKey struct{}

func WithPresentedCardRecorder(ctx context.Context, recorder *PresentedCardRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, presentedCardRecorderKey{}, recorder)
}

func recordPresentedCard(ctx context.Context, card channels.InteractiveCard) error {
	if ctx == nil {
		return nil
	}
	recorder, ok := ctx.Value(presentedCardRecorderKey{}).(*PresentedCardRecorder)
	if !ok || recorder == nil {
		return nil
	}
	recorder.Record(card)
	return nil
}

func clonePresentedCard(card channels.InteractiveCard) channels.InteractiveCard {
	card.Actions = append([]channels.CardAction(nil), card.Actions...)
	return card
}
