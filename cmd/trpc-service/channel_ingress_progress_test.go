package main

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type progressCleanupSender struct {
	deleteCalls int
	updateCalls int
	deleteErr   error
	updateErr   error
	updatedText string
}

func (*progressCleanupSender) Send(context.Context, channels.ReplyTarget, channels.OutboundMessage) (channels.SendReceipt, error) {
	return channels.SendReceipt{}, nil
}

func (*progressCleanupSender) StartProgress(context.Context, channels.ReplyTarget) (channels.SendReceipt, error) {
	return channels.SendReceipt{}, nil
}

func (s *progressCleanupSender) UpdateProgress(_ context.Context, _ channels.ReplyTarget, _ string, text string) error {
	s.updateCalls++
	s.updatedText = text
	return s.updateErr
}

func (s *progressCleanupSender) DeleteMessage(context.Context, channels.ReplyTarget, string) error {
	s.deleteCalls++
	return s.deleteErr
}

type progressOnlySender struct{ progressCleanupSender }

func TestCancelProgressPrefersDeleteAndFallsBackToStatusUpdate(t *testing.T) {
	t.Parallel()
	ingress := &channelIngress{}
	target := channels.ReplyTarget{Channel: channels.Feishu, ConversationID: "chat-1"}

	t.Run("empty handles are no-op", func(t *testing.T) {
		ingress.cancelProgress(nil)
		ingress.cancelProgress(&inboundProgressHandle{})
	})

	t.Run("delete succeeds", func(t *testing.T) {
		sender := &progressCleanupSender{}
		ingress.cancelProgress(&inboundProgressHandle{sender: sender, target: target, id: "progress-1"})
		if sender.deleteCalls != 1 || sender.updateCalls != 0 {
			t.Fatalf("delete/update calls = %d/%d", sender.deleteCalls, sender.updateCalls)
		}
	})

	t.Run("delete failure falls back to update", func(t *testing.T) {
		sender := &progressCleanupSender{deleteErr: errors.New("cannot delete")}
		ingress.cancelProgress(&inboundProgressHandle{sender: sender, target: target, id: "progress-2"})
		if sender.deleteCalls != 1 || sender.updateCalls != 1 || sender.updatedText != "请求未提交，请重试。" {
			t.Fatalf("delete/update/text = %d/%d/%q", sender.deleteCalls, sender.updateCalls, sender.updatedText)
		}
	})

	t.Run("update failure is best effort", func(t *testing.T) {
		sender := &progressCleanupSender{deleteErr: errors.New("cannot delete"), updateErr: errors.New("cannot update")}
		ingress.cancelProgress(&inboundProgressHandle{sender: sender, target: target, id: "progress-3"})
		if sender.deleteCalls != 1 || sender.updateCalls != 1 {
			t.Fatalf("delete/update calls = %d/%d", sender.deleteCalls, sender.updateCalls)
		}
	})
}
